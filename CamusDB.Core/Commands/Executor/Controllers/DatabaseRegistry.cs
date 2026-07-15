
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

    // Stable local Raft node id (from configured NodeId or a hash of the node name; survives restart).
    // Stamped into every drop-intent marker this node writes so startup recovery can reclaim only its
    // own crash remnants and never delete a live drop-intent owned by another cluster node.
    private readonly int localNodeId;

    private readonly SemaphoreSlim writeSem = new(1, 1);
    private readonly ConcurrentDictionary<string, DatabaseRegistryEntry> byName = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DatabaseRegistryEntry> byId = new(StringComparer.Ordinal);

    private static readonly HashSet<string> ReservedNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "_system",
        "information_schema",
    };

    private const int MaxRetries = 10;

    // Database names are normalised to lowercase at the registry boundary so that the
    // storage key, the in-memory cache, and the filesystem path are all consistent.
    // This mirrors the SQL parser's treatment of unquoted identifiers (table/column names
    // are lower-cased by the grammar's ToLowerInvariant productions).  HTTP callers that
    // send mixed-case names are silently folded — no separate "Foo" / "foo" databases.
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
        int localNodeId)
    {
        this.kahuna = kahuna;
        this.transactions = transactions;
        this.keyPrefix = keyPrefix;
        this.localNodeId = localNodeId;
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
    // Factory
    // -----------------------------------------------------------------------

    /// <summary>
    /// Opens (or creates) the database registry against the process-level shared Kahuna node.
    /// Registry keys are namespaced under <c>_system/</c> in the shared keyspace.
    /// </summary>
    public static async Task<DatabaseRegistry> OpenAsync(EmbeddedKahuna sharedNode)
    {
        ArgumentNullException.ThrowIfNull(sharedNode);

        Func<HLCTimestamp?, HLCTimestamp> mintLocalT = (floor) =>
        {
            if (floor.HasValue && !floor.Value.IsNull())
                return sharedNode.Raft.HybridLogicalClock.ReceiveEvent(sharedNode.Raft.GetLocalNodeId(), floor.Value);
            return sharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());
        };

        KvTransactionsManager txManager = new(sharedNode.Kahuna, mintLocalT);
        DatabaseRegistry registry = new(sharedNode.Kahuna, txManager, "_system/", sharedNode.Raft.GetLocalNodeId());

        // OpenAsync is kicked off eagerly during CommandExecutor construction, which a hosted service
        // can trigger before Program.cs calls StartAsync. Wait until the shared node has elected
        // leaders for every partition before scanning; otherwise the scan routes to a not-yet-created
        // partition and throws "Invalid partition".
        await sharedNode.WaitUntilStartedAsync().ConfigureAwait(false);

        await registry.LoadAsync().ConfigureAwait(false);
        return registry;
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

            byName[loaded.Name] = loaded;
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
            return cached;

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
            byName[entry.Name] = entry;
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
        name = Normalize(name);

        if (ReservedNames.Contains(name))
            throw new CamusDBException(
                CamusDBErrorCodes.DatabaseNameReserved,
                $"'{name}' is a reserved database name");

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            if (byName.ContainsKey(name))
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseAlreadyExists,
                    $"Database '{name}' is already registered");

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
                bool written = await WriteRegistryKey(tx, NameKey(name), entryBytes, ifAbsent: true).ConfigureAwait(false);
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

            byName[name] = entry;
            byId[id] = entry;
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
        oldName = Normalize(oldName);
        newName = Normalize(newName);

        if (ReservedNames.Contains(newName))
            throw new CamusDBException(
                CamusDBErrorCodes.DatabaseNameReserved,
                $"'{newName}' is a reserved database name");

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            if (!byName.TryGetValue(oldName, out DatabaseRegistryEntry? existing))
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseDoesntExist,
                    $"Database '{oldName}' is not registered");

            if (byName.ContainsKey(newName))
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

            KvTransaction tx = await transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);
            try
            {
                // ifAbsent=true: protect against a concurrent node registering newName between
                // our local byName check above and the KV commit.
                bool written = await WriteRegistryKey(tx, NameKey(newName), updatedBytes, ifAbsent: true).ConfigureAwait(false);
                if (!written)
                {
                    await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                    throw new CamusDBException(
                        CamusDBErrorCodes.DatabaseAlreadyExists,
                        $"Database '{newName}' is already registered");
                }
                await DeleteRegistryKey(tx, NameKey(oldName)).ConfigureAwait(false);
                await transactions.CommitAsync(tx).ConfigureAwait(false);
            }
            catch
            {
                await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                throw;
            }

            byName.TryRemove(oldName, out _);
            byName[newName] = updated;
            byId[existing.Id] = updated;
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

    // Value stamped into a node's own lifecycle markers (drop-intent, dropping) so startup recovery
    // can distinguish its own crash remnants from a marker a *different* live node currently holds.
    private byte[] LocalOwnerValue => System.Text.Encoding.UTF8.GetBytes(localNodeId.ToString());

    /// <summary>
    /// Atomically sets a persistent drop-in-progress marker for <paramref name="dbId"/> using
    /// <c>SetIfNotExists</c>. Returns <c>true</c> if the marker was written (the caller now owns
    /// the drop fence); returns <c>false</c> if another concurrent drop already holds the fence.
    /// The marker value is the owning node's id so startup recovery can safely reclaim only its own
    /// stale markers. The caller must call <see cref="ReleaseDropIntentAsync"/> on every exit path so
    /// the marker does not strand and block future drops.
    /// </summary>
    public async Task<bool> AcquireDropIntentAsync(string dbId)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, DropIntentKey(dbId), LocalOwnerValue, null, -1,
            KeyValueFlags.SetIfNotExists, 0, KeyValueDurability.Persistent, CancellationToken.None
        ).ConfigureAwait(false);
        return type == KeyValueResponseType.Set;
    }

    /// <summary>
    /// Returns <c>true</c> if a drop-in-progress marker is set for <paramref name="sourceId"/>,
    /// meaning a concurrent <see cref="DropDatabase"/> is actively processing the source and its
    /// keyspace may be purged at any moment. A branch-create that detects this after registering
    /// must unregister the newly-created branch and abort.
    /// </summary>
    public async Task<bool> HasDropIntentAsync(string sourceId)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? _) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, DropIntentKey(sourceId), -1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None
        ).ConfigureAwait(false);
        return type == KeyValueResponseType.Get;
    }

    /// <summary>
    /// Removes the drop-intent marker for <paramref name="dbId"/>. Called after
    /// <see cref="DropDatabase"/> completes (whether it succeeded or failed the descendant check).
    /// Best-effort: a failure is logged and swallowed; a stranded marker blocks future drops of
    /// the same id until the marker is manually cleared or the process restarts.
    /// </summary>
    public async Task ReleaseDropIntentAsync(string dbId)
    {
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
        byte[] ownerValue = LocalOwnerValue;
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

                if (kve.Value is not null && kve.Value.AsSpan().SequenceEqual(ownerValue))
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
        byte[] ownerValue = LocalOwnerValue;
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
                if (kve.Value is not null && kve.Value.AsSpan().SequenceEqual(ownerValue))
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
        KeyValueDurability lockDurability;
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

            (lockType, _, lockDurability, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
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

        tx.TrackLock(key, lockDurability);

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
        KeyValueDurability lockDurability;
        int lockRetries = 0;

        // Stable per-operation ids reused across the retry loop (see WriteRegistryKey) so the delete and its
        // lock fold once into the coordinator working set and the commit persists the removal.
        TransactionOperationId lockOperationId = TransactionOperationId.NewRandom();
        TransactionOperationId deleteOperationId = TransactionOperationId.NewRandom();

        do
        {
            if (lockRetries > 0)
                await Task.Delay(lockRetries * 10).ConfigureAwait(false);

            (lockType, _, lockDurability, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
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

        tx.TrackLock(key, lockDurability);

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
        // Roll back any transaction still active on the system store while the node is alive,
        // then dispose the transactions manager so its range-lock heartbeat loops are cancelled.
        // An undisposed manager keeps a heartbeat loop awaiting forever, which roots the manager
        // and the system Kahuna node it references — leaking a whole node per registry instance.
        try
        {
            await transactions.RollbackAllActiveAsync().ConfigureAwait(false);
        }
        catch
        {
            // best-effort: transactions.Dispose() below still cancels any remaining loops
        }

        transactions.Dispose();
        writeSem.Dispose();
    }
}
