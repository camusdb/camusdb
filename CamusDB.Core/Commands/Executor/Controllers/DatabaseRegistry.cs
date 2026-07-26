
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Util;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Sequences;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Persistent registry that maps database names to stable opaque ids.
///
/// Backed by a reserved <c>_system/</c> key prefix in the single process-level shared
/// Kahuna node. Both standalone and cluster modes use the same shared node.
///
/// <para>Every database gets one <see cref="DatabaseRegistryEntry"/> persisted under a
/// name key (<c>dbregistry/db:{name}</c>) holding the full entry. The id→name direction
/// is served from the in-memory <c>byId</c> cache, rebuilt from the name entries on load,
/// so no separate persisted reverse key is needed.</para>
///
/// <para>Thread safety: a single <see cref="SemaphoreSlim"/> serialises all mutating
/// operations.  Read-only queries (<see cref="TryResolveId"/>, <see cref="Get"/>,
/// <see cref="GetById"/>, <see cref="List"/>) read the in-memory cache lock-free.</para>
/// </summary>
public sealed class DatabaseRegistry : IAsyncDisposable
{
    private readonly IKahuna kahuna;
    private readonly KvTransactionsManager transactions;
    private readonly string keyPrefix;

    // Cross-node cache coherence only matters when more than one node shares the persistent store. In
    // standalone mode this process owns the single registry instance, so its in-memory cache is always
    // authoritative — every mutation updates it in place under `writeSem`, and no other node can change
    // KV underneath it. The generation stamp (below) exists solely to invalidate a stale cache hit after
    // ANOTHER node mutates; with no other node it is pure overhead, so a cache hit skips the per-resolve
    // Kahuna generation read entirely. Only set true for genuine Raft cluster nodes.
    private readonly bool isClusterMode;

    // Stable local Raft node id (from configured NodeId or a hash of the node name; survives restart).
    // Stamped into every drop-intent marker this node writes so startup recovery can reclaim only its
    // own crash remnants and never delete a live drop-intent owned by another cluster node.
    private readonly int localNodeId;

    private readonly SemaphoreSlim writeSem = new(1, 1);
    private readonly ConcurrentDictionary<string, DatabaseRegistryEntry> byName = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DatabaseRegistryEntry> byId = new(StringComparer.Ordinal);

    // Cross-node cache-coherence stamp. The in-memory caches above are loaded once at OpenAsync and
    // lazily backfilled, so without this a name dropped/renamed on ANOTHER node stays resolvable here from
    // a stale cache hit — a namespace split-brain that, with deferred drop, lets this node read/mutate a
    // detached-but-retained keyspace. Every mutation (Register/Unregister/Rename) advances a shared,
    // Raft-replicated monotonic generation sequence; a cache HIT is trusted only while this node's
    // last-loaded generation still matches the authoritative one, otherwise the cache is revalidated
    // against KV before resolving. `loadedGeneration` is the generation this node's cache reflects; it is
    // read lock-free on the hit path (a stale read only forces a redundant, harmless revalidation).
    private long loadedGeneration;
    private string GenerationSequenceKey => $"{keyPrefix}dbregistry/generation";

    private static readonly HashSet<string> ReservedNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "_system",
        "information_schema",
    };

    private const int MaxRetries = 10;

    // A drop-intent fence carries a bounded lease (its KV key's native expiry, CamusDBConfig.FenceLeaseMs).
    // If the owner crashes without releasing it, the lease lapses and any node can then re-acquire — a
    // dead owner can no longer block relink/GC of an id forever. A live owner keeps the fence by renewing
    // the lease in the background (below) for as long as it holds it, so an operation that legitimately
    // outlives one lease period (e.g. a large keyspace purge) never has the fence stolen mid-flight.

    // Background lease renewers for fences this node currently holds, keyed by fence id (a database id or
    // a composite table-fence id). A renewer re-stamps the lease with a compare-and-set on this node's
    // owner value, so it renews only while this node still owns the fence and stops the moment the fence
    // is lost. Populated by AcquireDropIntentAsync, torn down by ReleaseDropIntentAsync / DisposeAsync.
    private readonly ConcurrentDictionary<string, CancellationTokenSource> fenceRenewers = new(StringComparer.Ordinal);

    // Database names are case-insensitive but case-preserving. The name is stored and displayed
    // in the exact case the user created it with (DatabaseRegistryEntry.Name), while lookups and
    // uniqueness are case-insensitive: the persistent KV key and the in-memory cache are both keyed
    // by this normalized (lower-cased) form. Normalizing the KV key is what makes "MyDb" and "mydb"
    // resolve to the same database and prevents a cross-node split-brain where two nodes each create
    // a differently-cased key for what should be one database. No physical path is named after the
    // database (keyspaces use the immutable id), so normalization only governs the registry keyspace.
    private static string Normalize(string name) => name.ToLowerInvariant();

    /// <summary>
    /// KV bucket that holds every registry key. Exposed so the snapshot-hold renewer can elect a
    /// single sweeping node via leadership of this bucket's Raft partition.
    /// </summary>
    public string RegistryBucket => $"{keyPrefix}dbregistry";
    private string NameKeyPrefix => $"{keyPrefix}dbregistry/db:";
    private string NameKey(string name) => $"{keyPrefix}dbregistry/db:{name}";
    private string SequenceKey => $"{keyPrefix}dbregistry/seq";

    private DatabaseRegistry(
        IKahuna kahuna,
        KvTransactionsManager transactions,
        string keyPrefix,
        int localNodeId,
        bool isClusterMode)
    {
        this.kahuna = kahuna;
        this.transactions = transactions;
        this.keyPrefix = keyPrefix;
        this.localNodeId = localNodeId;
        this.isClusterMode = isClusterMode;
    }

    // -----------------------------------------------------------------------
    // Id allocation — compact base62 from a persistent monotonic sequence
    // -----------------------------------------------------------------------

    private string TableSequenceKey => $"{keyPrefix}tableseq";

    /// <summary>
    /// Allocates the next database id from the persistent monotonic counter stored in the
    /// shared node's sequence (<c>dbregistry/seq</c> or <c>_system/dbregistry/seq</c> in
    /// cluster mode). The counter only ever moves forward — ids are never reused even after
    /// a DROP, so a recycled name gets a strictly higher id than the dropped database.
    /// The id is returned as a short base-62 string.
    /// </summary>
    public Task<string> AllocateIdAsync() => AllocateFromSequenceAsync(SequenceKey, "database");

    /// <summary>
    /// Allocates the next table id from the persistent per-store monotonic sequence
    /// (<c>_system/tableseq</c>). The counter is global to the store (not per-database) so
    /// table ids are globally unique: a table created in a branch cannot collide with any
    /// inherited ancestor table id even after DROP + recreate. The id is returned as a short
    /// base-62 string, which is never reused and contains none of the KV key separators
    /// (<c>/</c>, <c>:</c>, <c>~</c>).
    /// </summary>
    public Task<string> AllocateTableIdAsync() => AllocateFromSequenceAsync(TableSequenceKey, "table");

    /// <summary>
    /// Core sequence-allocation helper. Creates the sequence on first use (idempotent) and
    /// advances it once, returning the allocated counter value encoded as a base-62 string.
    /// Only the proposer/leader calls this; followers apply the pre-allocated id from the
    /// replicated payload and never invoke the allocator.
    /// </summary>
    private async Task<string> AllocateFromSequenceAsync(string seqName, string label)
    {
        // Ensure the sequence exists (idempotent — AlreadyExists is fine)
        (SequenceResponseType createType, _) = await kahuna.LocateAndCreateSequence(
            seqName, initialValue: 0, increment: 1, maxValue: null,
            SequenceDurability.Persistent, CancellationToken.None
        ).ConfigureAwait(false);

        if (createType is not (SequenceResponseType.Success or SequenceResponseType.AlreadyExists))
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to ensure {label} id sequence: {createType}");

        // Advance the counter atomically — cluster-safe across all nodes
        SequenceResponseType nextType;
        SequenceAllocation allocation;
        int retries = 0;
        do
        {
            if (retries > 0)
                await Task.Delay(retries * 10).ConfigureAwait(false);

            (nextType, allocation) = await kahuna.LocateAndNextSequenceValue(
                seqName, null, SequenceDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        while (nextType == SequenceResponseType.MustRetry && ++retries < MaxRetries);

        if (nextType != SequenceResponseType.Success)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to allocate {label} id: {nextType}");

        return Base62.Encode(allocation.Start);
    }

    // -----------------------------------------------------------------------
    // Cross-node cache-coherence generation stamp
    // -----------------------------------------------------------------------

    /// <summary>
    /// Reads the authoritative registry generation (the current value of the shared generation sequence)
    /// without advancing it. Returns 0 when the sequence does not exist yet (no mutation has occurred).
    /// A cache hit compares this against <c>loadedGeneration</c> to decide whether the local cache is
    /// still current.
    /// </summary>
    private async Task<long> ReadGenerationAsync()
    {
        (SequenceResponseType type, ReadOnlySequenceEntry? entry) = await kahuna.LocateAndGetSequence(
            GenerationSequenceKey, SequenceDurability.Persistent, CancellationToken.None
        ).ConfigureAwait(false);

        return type == SequenceResponseType.Success && entry is not null ? entry.CurrentValue : 0;
    }

    /// <summary>
    /// Advances the shared registry generation so every other node's next cache hit revalidates against
    /// KV. Called after a mutation (Register/Unregister/Rename) has durably committed, so the generation
    /// only moves once the change is visible. Best-effort: if the bump fails the mutation is already
    /// committed and must not be undone — coherence for that one change degrades to "observed on the next
    /// mutation or restart" rather than immediately, which is logged. Returns the new generation (or the
    /// current local value on failure) so the mutating node can adopt it and avoid revalidating itself.
    /// </summary>
    private async Task<long> BumpGenerationAsync()
    {
        (SequenceResponseType createType, _) = await kahuna.LocateAndCreateSequence(
            GenerationSequenceKey, initialValue: 0, increment: 1, maxValue: null,
            SequenceDurability.Persistent, CancellationToken.None
        ).ConfigureAwait(false);

        if (createType is SequenceResponseType.Success or SequenceResponseType.AlreadyExists)
        {
            SequenceResponseType nextType;
            SequenceAllocation allocation = default;
            int retries = 0;
            do
            {
                if (retries > 0)
                    await Task.Delay(retries * 10).ConfigureAwait(false);

                (nextType, allocation) = await kahuna.LocateAndNextSequenceValue(
                    GenerationSequenceKey, null, SequenceDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);
            }
            while (nextType == SequenceResponseType.MustRetry && ++retries < MaxRetries);

            if (nextType == SequenceResponseType.Success)
                return allocation.Start;
        }

        return Volatile.Read(ref loadedGeneration);
    }

    /// <summary>
    /// Marks the local cache as reflecting generation <paramref name="generation"/>. Called by a mutating
    /// path after it has both updated the in-memory cache in place and bumped the generation, so this node
    /// does not needlessly revalidate against its own just-applied change.
    /// </summary>
    private void AdoptGeneration(long generation)
    {
        // Only move forward — a concurrent revalidation may already have adopted a higher generation.
        long current = Volatile.Read(ref loadedGeneration);
        if (generation > current)
            Volatile.Write(ref loadedGeneration, generation);
    }

    /// <summary>
    /// Revalidates the in-memory caches against KV when a cache hit is found to be stale (the authoritative
    /// generation has moved past <c>loadedGeneration</c>). Reconciles in place — upserting present entries
    /// and removing names that have vanished from KV (dropped/renamed away on another node) — rather than
    /// clearing first, so a concurrent lock-free reader never observes a transiently empty cache. Serialized
    /// under <see cref="writeSem"/> so it cannot interleave with a mutation's cache update, with a
    /// double-check so concurrent hits collapse to a single reload.
    /// </summary>
    private async Task RevalidateFromKvAsync(long authoritativeGeneration)
    {
        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref loadedGeneration) >= authoritativeGeneration)
                return; // another hit already reloaded to at least this generation

            HashSet<string> present = new(StringComparer.Ordinal);

            KvTransaction tx = KvTransaction.CreateReadOnly();
            string namePrefix = NameKeyPrefix;

            await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
                tx.TransactionId, RegistryBucket, null, true, null, true, 1000,
                HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
            {
                if (!key.StartsWith(namePrefix, StringComparison.Ordinal) || entry.Value is null)
                    continue;

                DatabaseRegistryEntry loaded = MetaJsonSerializer.Deserialize(
                    entry.Value, MetaJsonContext.Default.DatabaseRegistryEntry);

                present.Add(Normalize(loaded.Name));
                byName[Normalize(loaded.Name)] = loaded;
                byId[loaded.Id] = loaded;
            }

            // Evict names that no longer exist in KV. Remove the id mapping only if it still points at the
            // evicted entry — a rename re-points byId[id] at the NEW name, which the upsert above already
            // wrote, so the id must not be dropped along with the old name. `name` here is the normalized
            // cache key, so compare it against the entry's normalized name.
            foreach (string name in byName.Keys.ToList())
            {
                if (present.Contains(name))
                    continue;

                if (byName.TryRemove(name, out DatabaseRegistryEntry? removed)
                    && byId.TryGetValue(removed.Id, out DatabaseRegistryEntry? currentById)
                    && Normalize(currentById.Name) == name)
                {
                    byId.TryRemove(removed.Id, out _);
                }
            }

            Volatile.Write(ref loadedGeneration, authoritativeGeneration);
        }
        finally
        {
            writeSem.Release();
        }
    }

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    /// <summary>
    /// Opens (or creates) the database registry against the process-level shared Kahuna node.
    /// Registry keys are namespaced under <c>_system/</c> in the shared keyspace.
    ///
    /// <para><paramref name="isClusterMode"/> must be <c>true</c> only for a genuine Raft cluster node
    /// where other nodes can mutate the shared registry concurrently. In standalone mode (the default)
    /// this process owns the single registry, so a cache hit is trusted without the per-resolve
    /// generation round-trip — see <see cref="isClusterMode"/>.</para>
    /// </summary>
    public static async Task<DatabaseRegistry> OpenAsync(EmbeddedKahuna sharedNode, bool isClusterMode = false)
    {
        ArgumentNullException.ThrowIfNull(sharedNode);

        Func<HLCTimestamp?, HLCTimestamp> mintLocalT = (floor) =>
        {
            if (floor.HasValue && !floor.Value.IsNull())
                return sharedNode.Raft.HybridLogicalClock.ReceiveEvent(sharedNode.Raft.GetLocalNodeId(), floor.Value);
            return sharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());
        };

        KvTransactionsManager txManager = new(sharedNode.Kahuna, mintLocalT);
        DatabaseRegistry registry = new(sharedNode.Kahuna, txManager, "_system/", sharedNode.Raft.GetLocalNodeId(), isClusterMode);

        // OpenAsync is kicked off eagerly during CommandExecutor construction, which a hosted service
        // can trigger before Program.cs calls StartAsync. Wait until the shared node has elected
        // leaders for every partition before scanning; otherwise the scan routes to a not-yet-created
        // partition and throws "Invalid partition".
        await sharedNode.WaitUntilStartedAsync().ConfigureAwait(false);

        await registry.LoadAsync().ConfigureAwait(false);
        return registry;
    }

    /// <summary>
    /// Test-only factory that routes the registry's own KV operations through <paramref name="kvOverride"/>
    /// (typically a fault-injecting fake) while still minting timestamps and the local node id from the
    /// real <paramref name="node"/>. Lets a test fault a specific registry operation (e.g. make
    /// <see cref="UnregisterAsync"/> throw, or force <see cref="HasDropIntentAsync"/> to see a present
    /// marker) without perturbing the descriptor/hold/metadata paths, which resolve their own node. Loads
    /// the in-memory cache like <see cref="OpenAsync"/> so name lookups behave normally.
    /// </summary>
    internal static async Task<DatabaseRegistry> OpenForTestingAsync(EmbeddedKahuna node, IKahuna kvOverride, bool isClusterMode = false)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(kvOverride);

        Func<HLCTimestamp?, HLCTimestamp> mintLocalT = (floor) =>
        {
            if (floor.HasValue && !floor.Value.IsNull())
                return node.Raft.HybridLogicalClock.ReceiveEvent(node.Raft.GetLocalNodeId(), floor.Value);
            return node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId());
        };

        KvTransactionsManager txManager = new(kvOverride, mintLocalT);
        DatabaseRegistry registry = new(kvOverride, txManager, "_system/", node.Raft.GetLocalNodeId(), isClusterMode);

        await node.WaitUntilStartedAsync().ConfigureAwait(false);
        await registry.LoadAsync().ConfigureAwait(false);
        return registry;
    }

    /// <summary>
    /// Test-only: reads whether the pending-create marker for <paramref name="branchId"/> is still
    /// present in the persistent registry. Used to assert that an indeterminate branch-create abort
    /// retained its recovery handle rather than clearing it.
    /// </summary>
    internal async Task<bool> PendingMarkerExistsForTestingAsync(string branchId)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? _) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, PendingKey(branchId), -1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None
        ).ConfigureAwait(false);
        return type == KeyValueResponseType.Get;
    }

    // -----------------------------------------------------------------------
    // Startup load
    // -----------------------------------------------------------------------

    // byId is rebuilt entirely from the db:{name} entries — each entry carries its Id.
    // There is no separate persisted id→name key; the in-memory byId is authoritative.
    /// <summary>
    /// Loads all registry entries into the in-memory caches at open time.
    ///
    /// <para>The registry is opened exactly once per process into a cached task, so a single failure
    /// here would stick for the node's lifetime and fail every later <c>SHOW DATABASES</c> /
    /// <c>OpenDatabase</c>. <see cref="OpenAsync"/> now waits for the shared node to elect leaders for
    /// every partition (<see cref="EmbeddedKahuna.WaitUntilStartedAsync"/>) before calling this, which
    /// closes the main boot race where the eagerly-started scan reached the node before the registry
    /// bucket's partition existed and failed with <see cref="Kommander.RaftException"/> ("Invalid
    /// partition"). The bounded retry below remains as a secondary guard against a momentary
    /// leadership blip during the scan (e.g. a re-election); it surfaces the error only if it persists
    /// past the window, since a persistent failure is a real one.</para>
    /// </summary>
    private async Task LoadAsync()
    {
        Stopwatch sw = Stopwatch.StartNew();
        const int retryDelayMs = 200;
        const int maxWaitMs = 30_000;

        while (true)
        {
            try
            {
                await LoadOnceAsync().ConfigureAwait(false);
                // Start coherent with the current generation so the first cache hit does not needlessly
                // revalidate. Best-effort: on failure loadedGeneration stays 0 and the first hit revalidates.
                Volatile.Write(ref loadedGeneration, await ReadGenerationAsync().ConfigureAwait(false));
                return;
            }
            catch (RaftException) when (sw.ElapsedMilliseconds < maxWaitMs)
            {
                // Partition still coming online during boot; wait and re-scan from a clean slate.
                await Task.Delay(retryDelayMs).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Performs a single registry scan into the in-memory caches. Runs as a zero-identity read-only
    /// snapshot (<see cref="HLCTimestamp.Zero"/>): the scan reads each key's latest committed value
    /// with no Kahuna transaction to start, commit, or roll back — a read-write transaction would
    /// hash-route its rollback to a user partition and could throw during the same startup race.
    /// Clears the caches first so a retried attempt after a partial scan starts clean.
    /// </summary>
    private async Task LoadOnceAsync()
    {
        byName.Clear();
        byId.Clear();

        KvTransaction tx = KvTransaction.CreateReadOnly();
        string namePrefix = NameKeyPrefix;

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            tx.TransactionId,
            RegistryBucket,
            null, true,
            null, true,
            1000,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            CancellationToken.None).ConfigureAwait(false))
        {
            if (!key.StartsWith(namePrefix, StringComparison.Ordinal) || entry.Value is null)
                continue;

            DatabaseRegistryEntry loaded = MetaJsonSerializer.Deserialize(
                entry.Value, MetaJsonContext.Default.DatabaseRegistryEntry);

            byName[Normalize(loaded.Name)] = loaded;
            byId[loaded.Id] = loaded;
        }
    }

    // -----------------------------------------------------------------------
    // Read-only queries (lock-free, in-memory cache + live-KV fallback)
    // -----------------------------------------------------------------------

    public bool TryResolveId(string name, out string id)
    {
        if (byName.TryGetValue(Normalize(name), out DatabaseRegistryEntry? entry))
        {
            id = entry.Id;
            return true;
        }

        id = "";
        return false;
    }

    /// <summary>
    /// Async variant: checks the in-memory cache first, then falls back to a live Kahuna
    /// read when the name is absent.  Returns the full <see cref="DatabaseRegistryEntry"/>
    /// (including ancestry) rather than only the id.  Required for multi-node clusters
    /// where a database created on another node has been written to the shared
    /// Raft-replicated store but has not yet been seen by this node's in-memory cache.
    /// </summary>
    public async Task<DatabaseRegistryEntry?> TryResolveEntryAsync(string name)
    {
        name = Normalize(name);

        if (byName.TryGetValue(name, out DatabaseRegistryEntry? cached))
        {
            // Standalone: this process owns the only registry, so its cache is always authoritative and a
            // hit needs no revalidation. Skipping the generation read here removes a Kahuna route (and its
            // string-building) from every database open — the dominant per-operation cost in single-node mode.
            if (!isClusterMode)
                return cached;

            // A cache hit is authoritative only while this node's cache is at the current generation.
            // If a mutation (possibly on another node) has advanced the generation since we last loaded,
            // the hit may be stale — revalidate against KV before trusting it, then re-resolve from the
            // reconciled cache (the name may now be gone, or repointed to a new id).
            long authGen = await ReadGenerationAsync().ConfigureAwait(false);
            if (authGen == Volatile.Read(ref loadedGeneration))
                return cached;

            await RevalidateFromKvAsync(authGen).ConfigureAwait(false);

            if (byName.TryGetValue(name, out DatabaseRegistryEntry? revalidated))
                return revalidated;
            return null; // the name was dropped/renamed away on another node
        }

        // Cache miss — try a live point-read from the persistent KV store.
        KvTransaction tx = await transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? kvEntry) =
                await kahuna.LocateAndTryGetValue(
                    tx.TransactionId, NameKey(name), -1,
                    HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);

            if (getType != KeyValueResponseType.Get || kvEntry?.Value is null)
                return null;

            DatabaseRegistryEntry entry = MetaJsonSerializer.Deserialize(
                kvEntry.Value, MetaJsonContext.Default.DatabaseRegistryEntry);

            // Backfill the local cache so subsequent reads are fast.
            byName[Normalize(entry.Name)] = entry;
            byId[entry.Id] = entry;
            return entry;
        }
        finally
        {
            await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Async variant: checks the in-memory cache first, then falls back to a live Kahuna
    /// read when the name is absent.  Required for multi-node clusters where a database
    /// created on another node has been written to the shared Raft-replicated store but
    /// has not yet been seen by this node's in-memory cache (which is only populated at
    /// <see cref="OpenAsync"/> time).
    /// </summary>
    public async Task<string?> TryResolveIdAsync(string name)
    {
        DatabaseRegistryEntry? entry = await TryResolveEntryAsync(name).ConfigureAwait(false);
        return entry?.Id;
    }

    public DatabaseRegistryEntry? Get(string name) =>
        byName.TryGetValue(Normalize(name), out DatabaseRegistryEntry? e) ? e : null;

    public DatabaseRegistryEntry? GetById(string id) =>
        byId.TryGetValue(id, out DatabaseRegistryEntry? e) ? e : null;

    public IReadOnlyList<DatabaseRegistryEntry> List() => [.. byName.Values];

    /// <summary>
    /// Returns <c>true</c> if any registered database has <paramref name="targetId"/> in
    /// its ancestry chain — i.e. the target is not a leaf and cannot be safely dropped.
    ///
    /// The check performs a persistent KV scan of the full registry so it reflects databases
    /// created on other nodes in a cluster. The in-memory <c>byId</c> cache is checked first
    /// as a fast path; the persistent scan runs only if the cache shows no descendants,
    /// catching the window where a concurrent sibling node registered a new branch after this
    /// node loaded its registry.
    ///
    /// <para>Ancestry is immutable (rename never touches it), so reading from the in-memory
    /// cache is safe for entries already there; only newly-registered entries on remote nodes
    /// can be missed by the cache alone.</para>
    /// </summary>
    public async Task<bool> HasLiveDescendantsAsync(string targetId)
    {
        // Fast path: check the in-memory cache.
        foreach (DatabaseRegistryEntry entry in byId.Values)
        {
            foreach (DatabaseBranchAncestor ancestor in entry.Ancestors)
            {
                if (ancestor.DatabaseId == targetId)
                    return true;
            }
        }

        // Persistent scan: catch branches registered on other nodes that this node's cache missed.
        string namePrefix = NameKeyPrefix;
        KvTransaction tx = await transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await foreach ((string key, ReadOnlyKeyValueEntry kve) in kahuna.LocateAndScanRange(
                tx.TransactionId,
                RegistryBucket,
                null, true,
                null, true,
                1000,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                CancellationToken.None).ConfigureAwait(false))
            {
                if (!key.StartsWith(namePrefix, StringComparison.Ordinal) || kve.Value is null)
                    continue;

                DatabaseRegistryEntry loaded = MetaJsonSerializer.Deserialize(
                    kve.Value, MetaJsonContext.Default.DatabaseRegistryEntry);

                foreach (DatabaseBranchAncestor ancestor in loaded.Ancestors)
                {
                    if (ancestor.DatabaseId == targetId)
                        return true;
                }
            }
        }
        finally
        {
            await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }

        return false;
    }

    // -----------------------------------------------------------------------
    // Mutations (serialised by writeSem)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Atomically registers <paramref name="name"/> → <paramref name="id"/> in the
    /// persistent store and the in-memory cache.
    /// </summary>
    /// <param name="ancestors">
    /// Branch ancestry chain, nearest parent first.  Pass <c>null</c> or an empty list
    /// for root databases.  The list is stored verbatim and is immutable after registration.
    /// </param>
    /// <exception cref="CamusDBException">
    ///   <c>DatabaseAlreadyExists</c> if the name is already registered or reserved.
    /// </exception>
    public async Task<DatabaseRegistryEntry> RegisterAsync(
        string name, string id,
        IReadOnlyList<DatabaseBranchAncestor>? ancestors = null,
        string? immediateParentHoldId = null)
    {
        // Preserve the original case for storage/display; key the KV write and cache by the normalized form.
        string normalized = Normalize(name);

        if (ReservedNames.Contains(normalized))
            throw new CamusDBException(
                CamusDBErrorCodes.DatabaseNameReserved,
                $"'{name}' is a reserved database name");

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            if (byName.ContainsKey(normalized))
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseAlreadyExists,
                    $"Database '{name}' is already registered");

            // One id maps to at most one live name. Reject registering an id that is already live under
            // a different name — the guard that stops a stale-orphan relink from minting a second alias
            // for one physical keyspace. (Fresh CREATE always allocates an unregistered id; only relink
            // passes a reused id, and it checks authoritative state under the fence before calling here.)
            if (byId.TryGetValue(id, out DatabaseRegistryEntry? existingById)
                && !string.Equals(existingById.Name, name, StringComparison.OrdinalIgnoreCase))
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseAlreadyExists,
                    $"Database id '{id}' is already registered under name '{existingById.Name}'");

            DatabaseRegistryEntry entry = new()
            {
                Id = id,
                Name = name,
                CreatedAt = DateTime.UtcNow,
                Ancestors = ancestors is { Count: > 0 } ? [.. ancestors] : [],
                ImmediateParentHoldId = immediateParentHoldId ?? "",
            };

            byte[] entryBytes = MetaJsonSerializer.Serialize(entry, MetaJsonContext.Default.DatabaseRegistryEntry);

            KvTransaction tx = await transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);
            try
            {
                // ifAbsent=true: write only when key is currently absent (SetIfNotExists).
                // If another node races to register the same name and commits first, this
                // returns false — throw DatabaseAlreadyExists rather than silently overwriting
                // the winning node's entry and splitting the namespace into two id-based spaces.
                bool written = await WriteRegistryKey(tx, NameKey(normalized), entryBytes, ifAbsent: true).ConfigureAwait(false);
                if (!written)
                {
                    await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                    throw new CamusDBException(
                        CamusDBErrorCodes.DatabaseAlreadyExists,
                        $"Database '{name}' is already registered");
                }
                await transactions.CommitAsync(tx).ConfigureAwait(false);
            }
            catch
            {
                await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                throw;
            }

            byName[normalized] = entry;
            byId[id] = entry;

            // Advance the shared generation so other nodes revalidate their caches and observe this new
            // name; adopt it locally so this node does not revalidate against its own just-applied change.
            AdoptGeneration(await BumpGenerationAsync().ConfigureAwait(false));
            return entry;
        }
        finally
        {
            writeSem.Release();
        }
    }

    /// <summary>
    /// Removes the registry entry for <paramref name="name"/> from both the persistent
    /// store and the in-memory cache. No-op if the name is not registered.
    /// </summary>
    public async Task UnregisterAsync(string name)
    {
        name = Normalize(name);

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            if (!byName.TryGetValue(name, out DatabaseRegistryEntry? entry))
                return;

            KvTransaction tx = await transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);
            try
            {
                await DeleteRegistryKey(tx, NameKey(name)).ConfigureAwait(false);
                await transactions.CommitAsync(tx).ConfigureAwait(false);
            }
            catch
            {
                await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                throw;
            }

            byName.TryRemove(name, out _);
            byId.TryRemove(entry.Id, out _);

            // Advance the shared generation so other nodes drop their now-stale cache hit for this name.
            AdoptGeneration(await BumpGenerationAsync().ConfigureAwait(false));
        }
        finally
        {
            writeSem.Release();
        }
    }

    /// <summary>
    /// Renames <paramref name="oldName"/> to <paramref name="newName"/> atomically.
    /// The id is preserved; only the name entry changes.
    /// </summary>
    /// <exception cref="CamusDBException">
    ///   <c>DatabaseDoesntExist</c> if <paramref name="oldName"/> is not registered;
    ///   <c>DatabaseAlreadyExists</c> if <paramref name="newName"/> is already taken or reserved.
    /// </exception>
    public async Task RenameAsync(string oldName, string newName)
    {
        // Preserve the original case of the new name for storage/display; key by the normalized form.
        string normalizedOld = Normalize(oldName);
        string normalizedNew = Normalize(newName);

        if (ReservedNames.Contains(normalizedNew))
            throw new CamusDBException(
                CamusDBErrorCodes.DatabaseNameReserved,
                $"'{newName}' is a reserved database name");

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            if (!byName.TryGetValue(normalizedOld, out DatabaseRegistryEntry? existing))
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseDoesntExist,
                    $"Database '{oldName}' is not registered");

            // A pure case-change rename (mydb -> MyDb) keeps the same normalized key, so the "already
            // exists" guard must skip it — the new name occupies the same cache slot as the old one.
            if (normalizedOld != normalizedNew && byName.ContainsKey(normalizedNew))
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseAlreadyExists,
                    $"Database '{newName}' is already registered");

            // Ancestry is immutable: copy the list so the updated entry owns its own instance.
            // ImmediateParentHoldId must be carried over — a branch keeps the same snapshot-floor
            // hold on its parent across a rename; dropping it here would make the renewer skip the
            // branch and the drop path unable to release the hold, losing the frozen view on expiry.
            DatabaseRegistryEntry updated = new()
            {
                Id = existing.Id,
                Name = newName,
                CreatedAt = existing.CreatedAt,
                Ancestors = existing.Ancestors.Count > 0 ? [.. existing.Ancestors] : [],
                ImmediateParentHoldId = existing.ImmediateParentHoldId,
            };

            byte[] updatedBytes = MetaJsonSerializer.Serialize(updated, MetaJsonContext.Default.DatabaseRegistryEntry);

            // A case-only rename (mydb -> MyDb) targets the SAME normalized KV key, so it must overwrite
            // that key in place rather than SetIfNotExists (which would see the existing key and fail) and
            // must not delete it afterward. A true rename to a different normalized name still uses
            // ifAbsent to guard against a concurrent node registering the new name.
            bool caseOnlyRename = normalizedOld == normalizedNew;

            KvTransaction tx = await transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);
            try
            {
                bool written = await WriteRegistryKey(tx, NameKey(normalizedNew), updatedBytes, ifAbsent: !caseOnlyRename).ConfigureAwait(false);
                if (!written)
                {
                    await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                    throw new CamusDBException(
                        CamusDBErrorCodes.DatabaseAlreadyExists,
                        $"Database '{newName}' is already registered");
                }
                if (!caseOnlyRename)
                    await DeleteRegistryKey(tx, NameKey(normalizedOld)).ConfigureAwait(false);
                await transactions.CommitAsync(tx).ConfigureAwait(false);
            }
            catch
            {
                await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                throw;
            }

            if (!caseOnlyRename)
                byName.TryRemove(normalizedOld, out _);
            byName[normalizedNew] = updated;
            byId[existing.Id] = updated;

            // Advance the shared generation so other nodes stop resolving the old name and pick up the new.
            AdoptGeneration(await BumpGenerationAsync().ConfigureAwait(false));
        }
        finally
        {
            writeSem.Release();
        }
    }

    // -----------------------------------------------------------------------
    // Drop-intent fence for cross-node drop-vs-branch-create atomicity
    // -----------------------------------------------------------------------

    // A DROP DATABASE on node A and a CREATE ... BRANCH FROM ... on node B can race: if A's
    // descendant scan completes before B registers the new child, A sees no descendants and
    // proceeds to purge the parent's keyspace, orphaning the child.
    //
    // The fence works via a persistent KV key per database id:
    //   A sets the key (SetIfNotExists) before its descendant scan and holds it through purge.
    //   B checks the key after RegisterAsync (not before — the Raft-linearized ordering means
    //   either A's set happened before B's register and B will observe it here, or B's register
    //   happened before A's set in which case A's subsequent descendant scan sees B's child and
    //   A aborts instead).
    // Together these two checks guarantee exactly one wins with no orphaned child.
    //
    // Keys: _system/dbregistry/drop-intent:{dbId}  (value is a single 0x01 sentinel byte)

    private string DropIntentKey(string dbId) => $"{keyPrefix}dbregistry/drop-intent:{dbId}";

    // A token minted once per DatabaseRegistry instance (i.e. once per process start). It is stamped
    // into every lifecycle marker this run writes so startup recovery can tell a marker THIS run created
    // (current epoch — a live, in-flight operation) from one left by a PRIOR run that crashed (a
    // different epoch — a genuine remnant). Without it, a startup scrub that clears "own" markers could
    // delete a marker the concurrently-started reclaimer or an incoming relink just acquired.
    private readonly string startupEpoch = Guid.NewGuid().ToString("N");

    // Value stamped into a node's own lifecycle markers (drop-intent, dropping): "{nodeId}:{epoch}".
    // The node-id distinguishes this node's markers from a *different* live node's; the epoch
    // distinguishes this run's live markers from a prior run's crash remnants.
    private byte[] LocalOwnerValue => System.Text.Encoding.UTF8.GetBytes($"{localNodeId}:{startupEpoch}");

    /// <summary>
    /// True if <paramref name="value"/> is a marker owned by <em>this</em> node but written by a
    /// <em>prior</em> run (a crash remnant): the node-id matches but the epoch differs (or is absent, as
    /// in a pre-epoch marker — always treated as prior-run). A marker from the current run (matching
    /// epoch) returns false, so startup recovery never touches a live, in-flight operation's marker.
    /// </summary>
    private bool IsOwnStaleMarker(byte[]? value)
    {
        if (value is null)
            return false;

        string s = System.Text.Encoding.UTF8.GetString(value);
        int colon = s.LastIndexOf(':');
        string nodePart = colon >= 0 ? s[..colon] : s;
        string epochPart = colon >= 0 ? s[(colon + 1)..] : "";
        return nodePart == localNodeId.ToString() && epochPart != startupEpoch;
    }

    /// <summary>
    /// Atomically acquires the drop-intent fence for <paramref name="dbId"/> with a bounded lease
    /// (<see cref="CamusDBConfig.FenceLeaseMs"/>) via <c>SetIfNotExists</c>. Returns <c>true</c> if this node now owns
    /// the fence; <c>false</c> if another node's <em>live</em> (non-expired) lease already holds it.
    ///
    /// <para><b>Lease.</b> The marker's KV key carries a native expiry, so a holder that crashes without
    /// releasing frees the fence automatically once the lease lapses — a dead owner can no longer block
    /// relink/GC of the id forever (the failure this fixes). On success a background renewer keeps the
    /// lease alive for as long as this node holds the fence, so a long operation (a large keyspace purge)
    /// is never interrupted by a competing acquire. The marker value is <c>{nodeId}:{epoch}</c> so
    /// startup recovery reclaims only this node's own prior-run remnants.</para>
    ///
    /// <para><b>Transient vs. genuine contention.</b> Only a real <c>SetIfNotExists</c> conflict
    /// (<c>NotSet</c> — a live lease is present) reports the fence as held; transient replication/retry
    /// statuses (<c>MustRetry</c>/<c>WaitingForReplication</c>) are retried with bounded backoff rather
    /// than mistaken for contention.</para>
    ///
    /// <para>The caller must call <see cref="ReleaseDropIntentAsync"/> on every exit path so the renewer
    /// stops and the fence frees immediately rather than only when its lease lapses.</para>
    /// </summary>
    public async Task<bool> AcquireDropIntentAsync(string dbId)
    {
        int retries = 0;
        while (true)
        {
            (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, DropIntentKey(dbId), LocalOwnerValue, null, -1,
                KeyValueFlags.SetIfNotExists, CamusDBConfig.FenceLeaseMs, KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);

            if (type == KeyValueResponseType.Set)
            {
                StartFenceRenewer(dbId);
                return true;
            }

            // NotSet = a live, non-expired lease genuinely holds the fence. A crashed owner's lease would
            // already have lapsed and this Set would have succeeded, so NotSet is real contention.
            if (type == KeyValueResponseType.NotSet)
                return false;

            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
                && ++retries < MaxRetries)
            {
                await Task.Delay(retries * 10).ConfigureAwait(false);
                continue;
            }

            // Any other status (or exhausted retries) is treated as "not acquired" — the caller leaves the
            // work for a later pass rather than proceeding without the fence.
            return false;
        }
    }

    /// <summary>
    /// Starts (or replaces) the background lease renewer for a fence this node just acquired. The renewer
    /// re-stamps the lease every <see cref="CamusDBConfig.FenceLeaseRenewIntervalMs"/> with a compare-and-set on this node's
    /// owner value, so it renews only while this node still owns the fence and self-terminates the moment
    /// the fence is lost (e.g. the lease lapsed during a stall and another node took it) or released.
    /// </summary>
    private void StartFenceRenewer(string dbId)
    {
        CancellationTokenSource cts = new();
        // Replace any prior renewer for this id (should not exist while we hold the fence, but be safe).
        if (fenceRenewers.TryRemove(dbId, out CancellationTokenSource? old))
        {
            try { old.Cancel(); } catch { }
            old.Dispose();
        }
        fenceRenewers[dbId] = cts;
        _ = RenewFenceLoopAsync(dbId, cts.Token);
    }

    private async Task RenewFenceLoopAsync(string dbId, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(CamusDBConfig.FenceLeaseRenewIntervalMs, ct).ConfigureAwait(false);

                // SetIfEqualToValue: renew (refresh the expiry) only if the stored value is still THIS
                // node's owner value. If the lease lapsed and another node re-acquired, the compare fails
                // (NotSet) and we stop renewing — we no longer own the fence and must not overwrite it.
                (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, DropIntentKey(dbId), LocalOwnerValue, LocalOwnerValue, -1,
                    KeyValueFlags.SetIfEqualToValue, CamusDBConfig.FenceLeaseMs, KeyValueDurability.Persistent, ct
                ).ConfigureAwait(false);

                if (type is KeyValueResponseType.Set
                    or KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                    continue; // renewed, or a transient status — try again next tick

                // NotSet (lost the fence) or any hard error: stop renewing.
                return;
            }
        }
        catch (OperationCanceledException) { /* released — normal */ }
        catch { /* best-effort renewal; a missed renewal only shortens the lease */ }
    }

    /// <summary>
    /// Returns <c>true</c> if a drop-intent marker is set for <paramref name="sourceId"/>, meaning a
    /// concurrent <see cref="DropDatabase"/> is actively processing the source and its keyspace may be
    /// purged at any moment. A branch-create that detects this after registering must unregister the
    /// newly-created branch and abort.
    ///
    /// <para>This read is the second half of the cross-node drop/create fence, so an <em>indeterminate</em>
    /// result must never be reported as "no drop". Only an authoritative key-absent response
    /// (<see cref="KeyValueResponseType.DoesNotExist"/>) returns <c>false</c>; a present marker
    /// (<see cref="KeyValueResponseType.Get"/>) returns <c>true</c>; transient statuses
    /// (<c>MustRetry</c>/<c>WaitingForReplication</c>) are retried with bounded backoff; and any other
    /// status, an exhausted retry, or an exception <b>throws</b> a retryable
    /// <see cref="CamusDBErrorCodes.TransactionMustRetry"/> so the create path keeps its published-child
    /// recovery state rather than treating the fence as clear. Mapping an unconfirmed read to "absent"
    /// would let a branch publish while its parent is being purged.</para>
    /// </summary>
    public async Task<bool> HasDropIntentAsync(string sourceId)
    {
        int retries = 0;
        while (true)
        {
            KeyValueResponseType type;
            try
            {
                (type, _) = await kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, DropIntentKey(sourceId), -1,
                    HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMustRetry,
                    $"Could not confirm drop-intent state for '{sourceId}' ({ex.Message}); retry the operation");
            }

            if (type == KeyValueResponseType.Get)
                return true;

            // Authoritative absence is the ONLY result that clears the fence.
            if (type == KeyValueResponseType.DoesNotExist)
                return false;

            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
                && ++retries < MaxRetries)
            {
                await Task.Delay(retries * 10).ConfigureAwait(false);
                continue;
            }

            // Any other status, or exhausted transient retries: the read is indeterminate. Do NOT report
            // "no drop" — surface a retryable error so the fence is re-evaluated rather than bypassed.
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionMustRetry,
                $"Drop-intent read for '{sourceId}' was indeterminate (status {type}); retry the operation");
        }
    }

    /// <summary>
    /// Releases the drop-intent fence for <paramref name="dbId"/>: stops its background lease renewer and
    /// removes the marker. Called after the fenced operation completes on every exit path.
    /// Best-effort on the delete: if the delete fails the marker is left, but its bounded lease means it
    /// frees automatically once the lease lapses rather than stranding forever (the pre-lease behavior).
    /// </summary>
    public async Task ReleaseDropIntentAsync(string dbId)
    {
        // Stop renewing first so the renewer cannot refresh the lease after we delete the marker.
        if (fenceRenewers.TryRemove(dbId, out CancellationTokenSource? cts))
        {
            try { await cts.CancelAsync().ConfigureAwait(false); } catch { }
            cts.Dispose();
        }

        try
        {
            await kahuna.LocateAndTryDeleteKeyValue(
                HLCTimestamp.Zero, DropIntentKey(dbId),
                KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        catch { }
    }

    // ── Drop-in-progress markers (crash-resumable keyspace purge) ──────────────────────────────
    //
    // DROP DATABASE unregisters the entry and then purges its keyspace with per-key autocommit
    // deletes — not one transaction. A crash mid-purge would orphan row/index/stats/meta data with
    // no reclaim. A "dropping" marker written before the unregister and cleared only after the purge
    // completes lets startup resume any interrupted purge. Owner-scoped (value = this node's id) so a
    // restarting node never resumes a drop another live node is actively running.
    //
    // Keys: _system/dbregistry/dropping:{dbId}  (value is the owning node id)

    private string DroppingKey(string dbId) => $"{keyPrefix}dbregistry/dropping:{dbId}";

    /// <summary>
    /// Marks database <paramref name="dbId"/> as drop-in-progress before its keyspace purge begins.
    /// Stamped with this node's id so startup recovery resumes only its own interrupted drops. Cleared
    /// via <see cref="ClearDroppingAsync"/> only after the purge fully completes.
    /// </summary>
    public async Task MarkDroppingAsync(string dbId)
    {
        string key = DroppingKey(dbId);
        KeyValueResponseType type;
        int retries = 0;

        do
        {
            if (retries > 0)
                await Task.Delay(retries * 10).ConfigureAwait(false);

            (type, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, LocalOwnerValue, null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && ++retries < MaxRetries);

        if (type != KeyValueResponseType.Set)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to write drop-in-progress marker for database id '{dbId}': {type}");
    }

    /// <summary>Removes the drop-in-progress marker for <paramref name="dbId"/> after a completed purge. Best-effort.</summary>
    public async Task ClearDroppingAsync(string dbId)
    {
        try
        {
            await kahuna.LocateAndTryDeleteKeyValue(
                HLCTimestamp.Zero, DroppingKey(dbId),
                KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        catch { }
    }

    /// <summary>
    /// Scans for drop-in-progress markers owned by <em>this</em> node and returns their database ids.
    /// Each is an interrupted drop this node started before a crash: the caller resumes the keyspace
    /// purge for any id no longer registered, then clears the marker via <see cref="ClearDroppingAsync"/>.
    /// A marker whose id is still registered means the crash preceded <see cref="UnregisterAsync"/> (no
    /// data was purged); the caller clears it without resuming. Owner-scoped so another live node's
    /// in-flight drop is never disturbed.
    /// </summary>
    public async Task<List<string>> LoadOwnDroppingIdsAsync()
    {
        string droppingPrefix = $"{keyPrefix}dbregistry/dropping:";
        List<string> ids = [];

        KvTransaction tx = await transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await foreach ((string key, ReadOnlyKeyValueEntry kve) in kahuna.LocateAndScanRange(
                tx.TransactionId,
                RegistryBucket,
                null, true,
                null, true,
                1000,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                CancellationToken.None).ConfigureAwait(false))
            {
                if (!key.StartsWith(droppingPrefix, StringComparison.Ordinal))
                    continue;

                if (IsOwnStaleMarker(kve.Value))
                    ids.Add(key[droppingPrefix.Length..]);
            }
        }
        finally
        {
            await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }

        return ids;
    }

    /// <summary>
    /// Scans the registry bucket for drop-intent keys owned by <em>this</em> node and deletes them.
    /// Called once at startup: a drop never spans a process restart, so any drop-intent stamped with
    /// this node's id that survived a restart is a crash remnant — left when a crash hit between
    /// <see cref="AcquireDropIntentAsync"/> and the release, permanently blocking future drops of that
    /// database id until cleared.
    ///
    /// <para><b>Owner-scoped on purpose.</b> In a cluster the drop-intent key is Raft-replicated and
    /// visible on every node. Deleting <em>all</em> drop-intents at startup would let a restarting
    /// node wipe a drop-intent that a different node currently holds for an in-flight drop, reopening
    /// the cross-node drop/create race this fence exists to close. A node only ever writes markers
    /// under its own id, and its own in-flight drops die with its crash, so clearing only own-owned
    /// markers is always safe and never touches another live node's fence.</para>
    ///
    /// Returns the number of own stale markers deleted. Best-effort: individual delete failures are
    /// swallowed.
    /// </summary>
    public async Task<int> ClearOwnStaleDropIntentsAsync()
    {
        string intentPrefix = $"{keyPrefix}dbregistry/drop-intent:";
        List<string> keys = [];

        KvTransaction tx = await transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await foreach ((string key, ReadOnlyKeyValueEntry kve) in kahuna.LocateAndScanRange(
                tx.TransactionId,
                RegistryBucket,
                null, true,
                null, true,
                1000,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                CancellationToken.None).ConfigureAwait(false))
            {
                if (!key.StartsWith(intentPrefix, StringComparison.Ordinal))
                    continue;

                // Only reclaim markers this node owns; leave another live node's fence untouched.
                if (IsOwnStaleMarker(kve.Value))
                    keys.Add(key);
            }
        }
        finally
        {
            await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }

        foreach (string key in keys)
        {
            try
            {
                await kahuna.LocateAndTryDeleteKeyValue(
                    HLCTimestamp.Zero, key,
                    KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);
            }
            catch { }
        }

        return keys.Count;
    }

    // -----------------------------------------------------------------------
    // Pending-create tracking for orphan-namespace recovery
    // -----------------------------------------------------------------------

    // Branch creation writes metadata before publishing the registry entry; a crash between the two
    // leaves an orphaned namespace. Tracking the allocated id in a persistent pending-set lets a
    // startup scrubber find and purge such orphans. The pending key is deleted on success and on the
    // abort path; only a crash between TrackPendingBranchAsync and the finally cleanup leaves it.
    //
    // Keys: _system/dbregistry/pending:{branchId}  (value is a single 0x01 sentinel byte)

    private string PendingKey(string branchId) => $"{keyPrefix}dbregistry/pending:{branchId}";

    /// <summary>
    /// Writes a persistent pending-create marker for <paramref name="branchId"/> so a startup
    /// scrubber can find orphaned branch metadata if the process crashes mid-creation.
    ///
    /// <para><b>This write is mandatory, not best-effort.</b> Callers must invoke this method
    /// inside a try block whose catch releases any resources allocated so far (snapshot hold,
    /// etc.) and must invoke <see cref="CopyMetaForBranchAsync"/> only if this method returns
    /// without throwing. The invariant this preserves: every meta namespace written by
    /// <see cref="CopyMetaForBranchAsync"/> is either registered in the persistent registry, or
    /// it has a pending-create marker that the startup scrubber can use to find and purge it.
    /// Without this guarantee a failed marker write followed by a crash after metadata copy would
    /// leave an unreachable orphan namespace with no recovery path.</para>
    ///
    /// <para>Kahuna errors are propagated to the caller; <see cref="ClearPendingBranchAsync"/>
    /// is best-effort and may be called even when no marker was written (idempotent delete).</para>
    /// </summary>
    public async Task TrackPendingBranchAsync(string branchId)
    {
        string key = PendingKey(branchId);
        KeyValueResponseType type;
        int retries = 0;

        do
        {
            if (retries > 0)
                await Task.Delay(retries * 10).ConfigureAwait(false);

            (type, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, [0x01], null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && ++retries < MaxRetries);

        if (type != KeyValueResponseType.Set)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to write pending-create marker for branch id '{branchId}': {type}");
    }

    /// <summary>
    /// Removes the pending-create marker for <paramref name="branchId"/>. Called on both the
    /// success path (after <see cref="RegisterAsync"/>) and the abort path so a successful or
    /// cleanly-aborted creation does not leave a spurious pending entry that the next startup
    /// would try to scrub.
    /// Best-effort: a failure is silently ignored.
    /// </summary>
    public async Task ClearPendingBranchAsync(string branchId)
    {
        try
        {
            await kahuna.LocateAndTryDeleteKeyValue(
                HLCTimestamp.Zero, PendingKey(branchId),
                KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        catch
        {
            // best-effort
        }
    }

    /// <summary>
    /// Scans the registry bucket for pending-create markers and returns the ids of any that are
    /// not currently registered. These represent branch namespaces written before the registry
    /// entry was committed — typically from a process crash during
    /// <see cref="CreateBranchDatabaseAsync"/>. The caller is responsible for purging the
    /// corresponding <c>{branchId}/meta/…</c> namespace and then calling
    /// <see cref="ClearPendingBranchAsync"/> for each returned id.
    /// </summary>
    public async Task<List<string>> LoadOrphanBranchIdsAsync()
    {
        string pendingPrefix = $"{keyPrefix}dbregistry/pending:";
        List<string> orphans = [];

        KvTransaction tx = await transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
                tx.TransactionId,
                RegistryBucket,
                null, true,
                null, true,
                1000,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                CancellationToken.None).ConfigureAwait(false))
            {
                if (!key.StartsWith(pendingPrefix, StringComparison.Ordinal))
                    continue;

                string branchId = key[pendingPrefix.Length..];
                if (!byId.ContainsKey(branchId))
                    orphans.Add(branchId);
            }
        }
        finally
        {
            await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }

        return orphans;
    }

    // -----------------------------------------------------------------------
    // Orphan database records (deferred drop / relink / GC reclamation)
    // -----------------------------------------------------------------------

    // A non-FORCE DROP DATABASE of a root database does not purge its keyspace; it unregisters the
    // name and writes an orphan record so the id + data remain recoverable via
    // CREATE DATABASE ... RELINK TO {id} until the garbage collector reclaims them after the
    // retention window. Records are KV-only (no in-memory cache): SHOW ORPHAN DATABASES and the GC
    // scan them on demand, which keeps orphan state out of the schema-replication path.
    //
    // Keys: _system/dbregistry/orphan:{dbId}  (value is a serialized OrphanDatabaseRecord)

    private string OrphanKeyPrefix => $"{keyPrefix}dbregistry/orphan:";
    private string OrphanKey(string dbId) => $"{OrphanKeyPrefix}{dbId}";

    /// <summary>
    /// Composite drop-intent fence id for a table orphan (<c>{dbId}:{tableId}</c>), used with
    /// <see cref="AcquireDropIntentAsync"/>. Both <c>CREATE TABLE ... RELINK</c> and the orphan
    /// reclaimer take this key so a relink and a GC purge of the same table id never interleave. It
    /// cannot collide with a database fence id: a bare database id contains no colon.
    /// </summary>
    public static string TableFenceId(string dbId, string tableId) => $"{dbId}:{tableId}";

    /// <summary>
    /// Persists an orphan record for a deferred-dropped database. Written <em>before</em>
    /// <see cref="UnregisterAsync"/> so a crash between the two leaves the database still live (stale
    /// record, harmless) rather than data stranded with no recovery path. Idempotent: a repeated drop
    /// simply refreshes the record (and its <see cref="OrphanDatabaseRecord.DroppedAt"/>).
    /// </summary>
    public async Task WriteDatabaseOrphanAsync(OrphanDatabaseRecord record)
    {
        byte[] value = MetaJsonSerializer.Serialize(record, MetaJsonContext.Default.OrphanDatabaseRecord);
        string key = OrphanKey(record.Id);
        KeyValueResponseType type;
        int retries = 0;

        do
        {
            if (retries > 0)
                await Task.Delay(retries * 10).ConfigureAwait(false);

            (type, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, value, null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        while (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && ++retries < MaxRetries);

        if (type != KeyValueResponseType.Set)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to write orphan record for database id '{record.Id}': {type}");
    }

    /// <summary>
    /// Reads the orphan record for <paramref name="dbId"/>, or <c>null</c> if none exists (never
    /// dropped as an orphan, or already reclaimed). Used by relink and by the GC to re-check the
    /// record still exists before acting.
    /// </summary>
    public async Task<OrphanDatabaseRecord?> TryGetDatabaseOrphanAsync(string dbId)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, OrphanKey(dbId), -1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None
        ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Get || entry?.Value is null)
            return null;

        return MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.OrphanDatabaseRecord);
    }

    /// <summary>
    /// Removes the orphan record for <paramref name="dbId"/>. Called on relink (recovery) and after a
    /// GC purge completes. Best-effort: a failure is swallowed; a stranded record is re-evaluated on the
    /// next GC pass and blocks nothing (the id is already unregistered).
    /// </summary>
    public async Task DeleteDatabaseOrphanAsync(string dbId)
    {
        try
        {
            await kahuna.LocateAndTryDeleteKeyValue(
                HLCTimestamp.Zero, OrphanKey(dbId),
                KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        catch { }
    }

    /// <summary>
    /// Scans the registry bucket and returns every orphaned-database record. Backing store for
    /// <c>SHOW ORPHAN DATABASES</c> and the GC reclamation sweep.
    /// </summary>
    public async Task<List<OrphanDatabaseRecord>> LoadDatabaseOrphansAsync()
    {
        string orphanPrefix = OrphanKeyPrefix;
        List<OrphanDatabaseRecord> orphans = [];

        KvTransaction tx = await transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await foreach ((string key, ReadOnlyKeyValueEntry kve) in kahuna.LocateAndScanRange(
                tx.TransactionId,
                RegistryBucket,
                null, true,
                null, true,
                1000,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                CancellationToken.None).ConfigureAwait(false))
            {
                if (!key.StartsWith(orphanPrefix, StringComparison.Ordinal) || kve.Value is null)
                    continue;

                orphans.Add(MetaJsonSerializer.Deserialize(kve.Value, MetaJsonContext.Default.OrphanDatabaseRecord));
            }
        }
        finally
        {
            await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }

        return orphans;
    }

    /// <summary>
    /// Authoritatively resolves the live registered name for a database <paramref name="id"/> by
    /// scanning persistent KV (not the local cache), or <c>null</c> if the id is not currently
    /// registered. Used by relink to decide, under the fence, whether an id is already live (and thus
    /// whether this is an idempotent retry, a conflicting second alias, or a fresh recovery) — a
    /// decision that must reflect registrations made on other cluster nodes.
    /// </summary>
    public async Task<string?> TryResolveNameByIdAsync(string id)
    {
        foreach (DatabaseRegistryEntry entry in await ScanAllEntriesAsync().ConfigureAwait(false))
        {
            if (string.Equals(entry.Id, id, StringComparison.Ordinal))
                return entry.Name;
        }
        return null;
    }

    /// <summary>
    /// Returns all registered database entries by scanning the persistent KV store directly,
    /// rather than reading only the in-memory cache. Hold filtering (non-empty
    /// <c>ImmediateParentHoldId</c>) is the caller's responsibility.
    ///
    /// <para>This is the authoritative source for the snapshot-hold renewer sweep: in a cluster,
    /// a branch created on another node after this node's startup load will not be in the local
    /// <see cref="byName"/> cache, so iterating only the cache would silently skip its hold renewal
    /// until the node restarted. Scanning persistent storage catches every registered database
    /// regardless of which node wrote it.</para>
    ///
    /// <para>As a side effect, any entry loaded from the scan that is absent from the local caches
    /// is backfilled into <c>byName</c> and <c>byId</c>, consistent with the lazy-load pattern
    /// used by <see cref="TryResolveEntryAsync"/>.</para>
    /// </summary>
    public async Task<IReadOnlyList<DatabaseRegistryEntry>> ScanAllEntriesAsync()
    {
        string namePrefix = NameKeyPrefix;
        List<DatabaseRegistryEntry> entries = [];

        KvTransaction tx = await transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await foreach ((string key, ReadOnlyKeyValueEntry kve) in kahuna.LocateAndScanRange(
                tx.TransactionId,
                RegistryBucket,
                null, true,
                null, true,
                1000,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                CancellationToken.None).ConfigureAwait(false))
            {
                if (!key.StartsWith(namePrefix, StringComparison.Ordinal) || kve.Value is null)
                    continue;

                DatabaseRegistryEntry loaded = MetaJsonSerializer.Deserialize(
                    kve.Value, MetaJsonContext.Default.DatabaseRegistryEntry);

                // Backfill cache for entries registered on other nodes.
                byName.TryAdd(loaded.Name, loaded);
                byId.TryAdd(loaded.Id, loaded);

                entries.Add(loaded);
            }
        }
        finally
        {
            await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }

        return entries;
    }

    // -----------------------------------------------------------------------
    // KV helpers — mirror CatalogsManager.WriteMetaKey / DeleteMetaKey
    // -----------------------------------------------------------------------

    /// <summary>
    /// Writes <paramref name="value"/> to <paramref name="key"/> within the supplied transaction.
    /// When <paramref name="ifAbsent"/> is <c>true</c>, uses <see cref="KeyValueFlags.SetIfNotExists"/>:
    /// the write succeeds only when the key is currently absent.  Returns <c>true</c> if the key
    /// was written, <c>false</c> if the key already existed (only possible when <paramref name="ifAbsent"/>
    /// is <c>true</c>; always returns <c>true</c> for plain writes).
    /// </summary>
    private async Task<bool> WriteRegistryKey(KvTransaction tx, string key, byte[] value, bool ifAbsent = false)
    {
        KeyValueResponseType lockType;
        int lockRetries = 0;

        // Stable per-operation ids reused across the retry loop so a replayed call folds once into the
        // coordinator working set; the write and its lock must fold, or the commit-from-working-set would
        // not persist this registry key.
        TransactionOperationId lockOperationId = TransactionOperationId.NewRandom();
        TransactionOperationId setOperationId = TransactionOperationId.NewRandom();

        do
        {
            if (lockRetries > 0)
                await Task.Delay(lockRetries * 10).ConfigureAwait(false);

            (lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                tx.TransactionId, key, 0,
                KeyValueDurability.Persistent, CancellationToken.None,
                coordinatorKey: tx.CoordinatorKey, operationId: lockOperationId
            ).ConfigureAwait(false);
        }
        while (lockType is KeyValueResponseType.AlreadyLocked or KeyValueResponseType.MustRetry
               && ++lockRetries < MaxRetries);

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to lock registry key '{key}': {lockType}");

        KeyValueFlags flags = ifAbsent ? KeyValueFlags.SetIfNotExists : KeyValueFlags.Set;
        KeyValueResponseType setType;
        int setRetries = 0;

        do
        {
            if (setRetries > 0)
                await Task.Delay(setRetries * 10).ConfigureAwait(false);

            (setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
                tx.TransactionId, key, value, null, -1,
                flags, 0,
                KeyValueDurability.Persistent, CancellationToken.None,
                coordinatorKey: tx.CoordinatorKey, operationId: setOperationId
            ).ConfigureAwait(false);
        }
        while (setType is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && ++setRetries < MaxRetries);

        // NotSet is only returned when ifAbsent=true and the key already exists — not an error.
        if (setType == KeyValueResponseType.NotSet)
            return false;

        if (setType != KeyValueResponseType.Set)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to write registry key '{key}': {setType}");

        tx.TrackModified(key, KeyValueDurability.Persistent);
        return true;
    }

    private async Task DeleteRegistryKey(KvTransaction tx, string key)
    {
        KeyValueResponseType lockType;
        int lockRetries = 0;

        // Stable per-operation ids reused across the retry loop (see WriteRegistryKey) so the delete and its
        // lock fold once into the coordinator working set and the commit persists the removal.
        TransactionOperationId lockOperationId = TransactionOperationId.NewRandom();
        TransactionOperationId deleteOperationId = TransactionOperationId.NewRandom();

        do
        {
            if (lockRetries > 0)
                await Task.Delay(lockRetries * 10).ConfigureAwait(false);

            (lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                tx.TransactionId, key, 0,
                KeyValueDurability.Persistent, CancellationToken.None,
                coordinatorKey: tx.CoordinatorKey, operationId: lockOperationId
            ).ConfigureAwait(false);
        }
        while (lockType is KeyValueResponseType.AlreadyLocked or KeyValueResponseType.MustRetry
               && ++lockRetries < MaxRetries);

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to lock registry key '{key}': {lockType}");

        KeyValueResponseType deleteType;
        int deleteRetries = 0;

        do
        {
            if (deleteRetries > 0)
                await Task.Delay(deleteRetries * 10).ConfigureAwait(false);

            (deleteType, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
                tx.TransactionId, key,
                KeyValueDurability.Persistent, CancellationToken.None,
                coordinatorKey: tx.CoordinatorKey, operationId: deleteOperationId
            ).ConfigureAwait(false);
        }
        while (deleteType is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && ++deleteRetries < MaxRetries);

        if (deleteType is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to delete registry key '{key}': {deleteType}");

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    // -----------------------------------------------------------------------

    public async ValueTask DisposeAsync()
    {
        // Stop every background fence-lease renewer this node still holds. Their leases then lapse on
        // their own, freeing the fences for another node without leaving a live renewer rooted here.
        foreach (KeyValuePair<string, CancellationTokenSource> kv in fenceRenewers)
        {
            try { await kv.Value.CancelAsync().ConfigureAwait(false); } catch { }
            kv.Value.Dispose();
        }
        fenceRenewers.Clear();

        // Roll back any transaction still active on the system store while the node is alive so the
        // coordinator releases their working set, then dispose the transactions manager to release the
        // system Kahuna node it references — an undisposed manager roots that node, leaking a whole node
        // per registry instance.
        try
        {
            await transactions.RollbackAllActiveAsync().ConfigureAwait(false);
        }
        catch
        {
            // best-effort: abandoned sessions are reclaimed by the coordinator reaper on timeout
        }

        transactions.Dispose();
        writeSem.Dispose();
    }
}
