
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
using CamusDB.Core.Util.Diagnostics;
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

    /// <summary>
    /// The KV client this registry's entries and recovery markers live on. A recovery step that
    /// destroys a namespace guarded by one of those markers runs through this same client, so the
    /// marker and the namespace are always observed through one node and a fault seen by the marker
    /// write is the fault the purge sees. It also lets the test-only client override of
    /// <see cref="OpenForTestingAsync"/> reach the purge, so a fault test covers the whole recovery
    /// unit and not only the marker.
    /// </summary>
    internal IKahuna Kahuna => kahuna;

    /// <summary>Configuration for this engine; injected, never ambient.</summary>
    private readonly CamusDBOptions options;
    private readonly KvTransactionsManager transactions;
    private readonly string keyPrefix;

    // Cross-node cache coherence only matters when more than one node shares the persistent store. In
    // standalone mode this process owns the single registry instance, so its in-memory cache is always
    // authoritative — every mutation updates it in place under `writeSem`, and no other node can change
    // KV underneath it. The generation stamp (below) exists solely to invalidate a stale cache hit after
    // ANOTHER node mutates; with no other node it is pure overhead, so a cache hit skips the per-resolve
    // Kahuna generation read entirely. Only set true for genuine Raft cluster nodes.
    private readonly bool isClusterMode;

    // Owner stamp (node id + per-run epoch) for the lifecycle markers this process writes.
    private readonly RegistryMarkerOwner markerOwner;

    /// <summary>Drop-intent fence, drop-in-progress and lost-snapshot-protection markers.</summary>
    public RegistryDropMarkers DropMarkers { get; }

    /// <summary>Pending-create markers for orphan branch-namespace recovery.</summary>
    public RegistryPendingBranches PendingBranches { get; }

    /// <summary>Orphan records of deferred-dropped databases (relink / GC reclamation).</summary>
    public RegistryOrphanRecords Orphans { get; }

    private readonly SemaphoreSlim writeSem = new(1, 1);
    private readonly ConcurrentDictionary<string, DatabaseRegistryEntry> byName = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DatabaseRegistryEntry> byId = new(StringComparer.Ordinal);

    // Cross-node cache-coherence stamp. The in-memory caches above are loaded once at OpenAsync and
    // lazily backfilled, so without this a name dropped/renamed on ANOTHER node stays resolvable here from
    // a stale cache hit — a namespace split-brain that, with deferred drop, lets this node read/mutate a
    // detached-but-retained keyspace. Every mutation (Register/Unregister/Rename) rewrites a single shared,
    // Raft-replicated key; a cache HIT is trusted only while this node's last-loaded generation still
    // matches the authoritative one, otherwise the cache is revalidated against KV before resolving.
    // `loadedGeneration` is the generation this node's cache reflects; it is read lock-free on the hit path
    // (a stale read only forces a redundant, harmless revalidation).
    private long loadedGeneration;

    // How long a generation read may be trusted, in milliseconds; 0 reads the stamp on every resolve (the
    // behaviour before the lease). See ReadLeasedGenerationAsync for the argument, and
    // WaitOutGenerationLeasesAsync for the half of it the mutating side carries.
    private readonly int generationLeaseMs;

    // The last generation read on the resolve path and the Stopwatch timestamp until which it may be
    // trusted. Replaced whole (never mutated) under `generationLeaseSync`; read lock-free.
    private sealed record GenerationLease(long Generation, long ExpiresAtTimestamp);

    private GenerationLease? generationLease;
    private readonly object generationLeaseSync = new();

    // Single-flight for a lease refresh: callers that find the lease expired queue here and take the one
    // read's result, so a lapsed lease costs one Kahuna read per node, not one per statement in flight.
    private readonly SemaphoreSlim generationLeaseSem = new(1, 1);

    // Names the foreground resolve path has already re-read at a generation the cache as a whole has not
    // reached, with the exact entry object the re-read confirmed. While the authoritative generation stays
    // at or below the recorded one and the cache still holds that same object, the name is known current
    // and a hit needs no second read. Keyed by normalized name; cleared by a full reconcile.
    private readonly ConcurrentDictionary<string, (long Generation, DatabaseRegistryEntry Entry)> nameValidatedAt = new(StringComparer.Ordinal);

    // Local cache-mutation epoch, paired with `cacheSync`. Besides the mutation paths (all under
    // `writeSem`), the caches are written by two lock-free backfills — the bucket scan in
    // ScanAllEntriesAsync and the point read on the miss path of TryResolveEntryAsync — which copy into
    // the cache what KV held when they READ it. A local mutation can land between that read and the cache
    // write: UnregisterAsync or RetractRegistrationAsync deletes the name and evicts it, then the backfill
    // adds the entry it read a moment earlier straight back. That phantom resolves a dead id, and in
    // standalone mode a cache hit is never revalidated, so it lives until restart — `CREATE DATABASE` of
    // the name fails with DatabaseAlreadyExists and an open reaches a purged keyspace. A background sweep
    // (snapshot-hold renewer, orphan reclaimer, TTL discovery) scanning while a DROP or an aborted
    // branch-create retracts is exactly that interleaving. Every mutation path therefore advances this
    // epoch together with its cache write, under `cacheSync`, and a backfill writes only while the epoch
    // still equals the value it captured before its read (see TryBackfill). The lock is held for
    // synchronous dictionary work only, never across an await; reads of the caches stay lock-free.
    private readonly object cacheSync = new();
    private long cacheMutationEpoch;

    /// <summary>
    /// The stamp's key. Its <em>revision</em> — not its value — is the generation: the store assigns each
    /// write of a key a strictly increasing revision, ordered by that key's partition leader, which is
    /// exactly the "has anything changed since I last looked" signal a cache needs.
    ///
    /// <para>Deliberately a plain key rather than the sequence this once was. A sequence's durable value is
    /// the high-water mark of a <em>reserved block</em>, so it does not move at all for the hundreds of
    /// allocations served from inside that block — a reader would see one unchanged number across hundreds
    /// of mutations and keep trusting a cache that had long gone stale. Nor are the values a sequence hands
    /// out ordered across nodes: a node draining an older block can issue a lower value later than another
    /// node issued a higher one, so they cannot be compared as generations at all. Sequences promise that
    /// no value is handed out twice, which is not what a change-detector needs.</para>
    /// </summary>
    private string GenerationKey => $"{keyPrefix}dbregistry/generation";

    private static readonly HashSet<string> ReservedNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "_system",
        "information_schema",
    };

    private const int MaxRetries = 10;

    // Payload for the generation stamp's key. The revision the store assigns each write is the generation,
    // so the bytes are never read — they exist only because a write needs a value. Carries the writer's
    // node id so a dump of the key says which node last moved the stamp.
    private byte[] GenerationMarker => System.Text.Encoding.UTF8.GetBytes(markerOwner.LocalNodeId.ToString());

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

    /// <summary>
    /// Test-only seam: the id counter's key, so a test can delete it and reproduce the counter reset
    /// that <see cref="AllocateIdAsync"/> recovers from. Production code never needs this.
    /// </summary>
    internal string IdSequenceKeyForTests => SequenceKey;

    private DatabaseRegistry(
        IKahuna kahuna,
        CamusDBOptions options,
        KvTransactionsManager transactions,
        string keyPrefix,
        int localNodeId,
        bool isClusterMode)
    {
        this.kahuna = kahuna;
        this.options = options;
        this.transactions = transactions;
        this.keyPrefix = keyPrefix;
        this.isClusterMode = isClusterMode;
        this.generationLeaseMs = isClusterMode ? Math.Max(0, options.RegistryGenerationLeaseMs) : 0;

        RegistryKeyspace keyspace = new(kahuna, transactions, keyPrefix);
        markerOwner = new RegistryMarkerOwner(localNodeId);
        DropMarkers = new RegistryDropMarkers(keyspace, markerOwner, options);
        PendingBranches = new RegistryPendingBranches(keyspace, markerOwner, ScanAllEntriesAsync);
        Orphans = new RegistryOrphanRecords(keyspace);
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
    public async Task<string> AllocateIdAsync()
    {
        // Normally one allocation suffices: the counter is persistent and monotonic, so it never
        // returns an id that is already live. It can, though, be re-created rather than resumed —
        // LocateAndCreateSequence reports Success (it created it) instead of AlreadyExists — and a
        // counter that restarts at 0 re-issues ids that registered databases still hold. Skipping ids
        // already present makes allocation self-healing in that case instead of failing the CREATE with
        // "id 'X' is already registered under name '…'".
        for (int attempt = 0; attempt < MaxRetries; attempt++)
        {
            string candidate = await AllocateFromSequenceAsync(SequenceKey, "database").ConfigureAwait(false);

            if (!byId.ContainsKey(candidate))
                return candidate;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.SystemSpaceCorrupt,
            "Database id allocation kept returning ids that are already registered; the id counter " +
            "appears to have been reset behind the registry.");
    }

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
    /// Core sequence-allocation helper. Advances the counter once — creating it first if this is its
    /// very first use — and returns the allocated value encoded as a base-62 string. Only the
    /// proposer/leader calls this; followers apply the pre-allocated id from the replicated payload
    /// and never invoke the allocator.
    /// </summary>
    private async Task<string> AllocateFromSequenceAsync(string seqName, string label)
    {
        (SequenceResponseType nextType, SequenceAllocation allocation) =
            await AdvanceSequenceAsync(seqName).ConfigureAwait(false);

        if (nextType != SequenceResponseType.Success)
            throw SequenceFailure(nextType, $"Failed to allocate {label} id");

        return Base62.Encode(allocation.Start);
    }

    /// <summary>
    /// Advances <paramref name="seqName"/> by one, creating the counter on the way if it does not exist
    /// yet, and riding out <see cref="SequenceResponseType.MustRetry"/> on both calls.
    ///
    /// <para>The counter is advanced first and created only on <see cref="SequenceResponseType.NotFound"/>:
    /// it exists on every allocation after the very first, so leading with an unconditional create would
    /// put an extra round trip — and an extra chance to fail — in front of every table id, for a call
    /// whose answer is almost always <c>AlreadyExists</c>.</para>
    ///
    /// <para>One budget covers the whole allocation rather than each call within it. The caller is a user's
    /// DDL statement waiting on a single answer, and it should wait out one election — not one per round
    /// trip, which is what a per-call budget would silently multiply it into.</para>
    /// </summary>
    private async Task<(SequenceResponseType, SequenceAllocation)> AdvanceSequenceAsync(string seqName)
    {
        ValueStopwatch elapsed = ValueStopwatch.StartNew();

        (SequenceResponseType nextType, SequenceAllocation allocation) = await RetryWhileMustRetryAsync(
            () => kahuna.LocateAndNextSequenceValue(
                seqName, null, SequenceDurability.Persistent, CancellationToken.None),
            elapsed
        ).ConfigureAwait(false);

        if (nextType != SequenceResponseType.NotFound)
            return (nextType, allocation);

        (SequenceResponseType createType, _) = await RetryWhileMustRetryAsync(
            () => kahuna.LocateAndCreateSequence(
                seqName, initialValue: 0, increment: 1, maxValue: null, blockSize: null,
                SequenceDurability.Persistent, CancellationToken.None),
            elapsed
        ).ConfigureAwait(false);

        // AlreadyExists is not a failure: another node created the counter between our advance and our
        // create, and its incarnation is the one to advance.
        if (createType is not (SequenceResponseType.Success or SequenceResponseType.AlreadyExists))
            return (createType, default);

        return await RetryWhileMustRetryAsync(
            () => kahuna.LocateAndNextSequenceValue(
                seqName, null, SequenceDurability.Persistent, CancellationToken.None),
            elapsed
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs a sequence call until it stops answering <see cref="SequenceResponseType.MustRetry"/>,
    /// bounded by the wall-clock <see cref="CamusDBOptions.SequenceRetryBudgetMs"/>.
    ///
    /// <para>The loop itself lives in <see cref="SequenceRetryPolicy"/>, shared with the user-sequence
    /// allocator. Both wait on the same thing — a partition election — so they must wait the same
    /// way; two copies is how the two budgets drift apart unnoticed until a failover.</para>
    ///
    /// <para><paramref name="elapsed"/> is started by the caller and shared across the calls making up one
    /// allocation, so the budget bounds the operation the user is waiting on rather than each round trip.</para>
    /// </summary>
    private Task<(SequenceResponseType, T)> RetryWhileMustRetryAsync<T>(
        Func<Task<(SequenceResponseType, T)>> sequenceCall,
        ValueStopwatch elapsed)
        => SequenceRetryPolicy.RetryWhileMustRetryAsync(sequenceCall, elapsed, options.SequenceRetryBudgetMs);

    /// <summary>
    /// The retry loop over any call whose result can say "not right now". Used by the generation
    /// stamp's write, which fails the same way the sequence calls do.
    /// </summary>
    private Task<T> RetryWhileAsync<T>(
        Func<Task<T>> call,
        Func<T, bool> shouldRetry,
        ValueStopwatch elapsed)
        => SequenceRetryPolicy.RetryWhileAsync(call, shouldRetry, elapsed, options.SequenceRetryBudgetMs);

    /// <summary>
    /// Builds the failure for a sequence call that never reached <see cref="SequenceResponseType.Success"/>.
    /// An exhausted <see cref="SequenceResponseType.MustRetry"/> means the partition owning the counter
    /// stayed leaderless for the whole retry window: nothing was allocated and nothing is damaged, so it is
    /// reported as transient unavailability the caller can simply re-issue. Any other response means the
    /// system keyspace did not answer as it must, which is what
    /// <see cref="CamusDBErrorCodes.SystemSpaceCorrupt"/> is for.
    /// </summary>
    private static CamusDBException SequenceFailure(SequenceResponseType type, string message) =>
        new(
            type == SequenceResponseType.MustRetry
                ? CamusDBErrorCodes.SequenceUnavailable
                : CamusDBErrorCodes.SystemSpaceCorrupt,
            $"{message}: {type}");

    // -----------------------------------------------------------------------
    // Cross-node cache-coherence generation stamp
    // -----------------------------------------------------------------------

    /// <summary>
    /// Reads the authoritative registry generation — the revision of <see cref="GenerationKey"/> — without
    /// moving it. Returns 0 when no mutation has ever been recorded. A cache hit compares this against
    /// <c>loadedGeneration</c> to decide whether the local cache is still current.
    ///
    /// <para>Reported as revision + 1 so that "never written" stays distinguishable from the first write,
    /// whose revision is 0. Without the offset, a node holding entries loaded before the very first
    /// mutation would compare 0 against 0 and trust a cache that had already been invalidated.</para>
    ///
    /// <para>A read that does not answer reports 0, which is the conservative direction: 0 matches no cache
    /// that has adopted a real generation, so the resolve revalidates against KV rather than trusting a
    /// possibly stale hit. The read runs on the ordinary <see cref="KahunaRetryPolicy"/> budget first: this
    /// runs under every database open, and while the stamp's partition is between leaders 
    /// an unretried read either threw the raw transport failure out of
    /// every statement on the node, or answered 0 and sent the resolve into a revalidation that could not
    /// answer either. Past the budget it still reports 0.</para>
    /// </summary>
    private async Task<long> ReadGenerationAsync()
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await KahunaRetryPolicy.RetryOnMustRetry(
            () => kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, GenerationKey, -1,
                HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None),
            CancellationToken.None
        ).ConfigureAwait(false);

        return type == KeyValueResponseType.Get && entry is not null ? entry.Revision + 1 : 0;
    }

    /// <summary>
    /// The generation as the foreground resolve path sees it: a read of the stamp that is reused for
    /// <see cref="CamusDBOptions.RegistryGenerationLeaseMs"/>, measured from the moment the read was
    /// <em>issued</em>, instead of a linearizable Kahuna read under every statement. Before the lease that
    /// read was the largest single cost of a point query in cluster mode (about 0.46 ms of a 1.1 ms query
    /// mean under load: one read per statement, two per autocommit read).
    ///
    /// <para><b>Why the lease keeps the guarantee.</b> A cache hit may be served from a generation read
    /// that is up to one lease old, so it may miss a mutation made in that window. Every mutation that can
    /// make a cached name <em>wrong</em> — a drop, a rename, a retracted registration — waits out one lease
    /// after moving the stamp before it returns (<see cref="WaitOutGenerationLeasesAsync"/>). A read issued
    /// before that stamp moved expires before the mutation completes, so a statement that trusts it started
    /// while the mutation was still in progress. Such a statement was always concurrent with the mutation,
    /// and the engine already handles one that opened its database before a drop finished. A statement that
    /// starts after the mutation returned can only hold a read issued after the stamp moved, and that read
    /// sees the move. A registration needs no wait: a name another node has never cached misses the cache
    /// and is read from KV. It is the same bounded-staleness argument as a schema lease, and it relies only
    /// on the nodes' monotonic clocks agreeing on the length of an interval, not on when it started.</para>
    ///
    /// <para>The lease length must be identical on every node (a node trusting a longer lease than the
    /// mutating node waits out would break the argument), so the setting is cluster-scoped and restart-only.
    /// A read that does not answer (0) is never leased, and a lease of 0 reads the stamp every time.</para>
    /// </summary>
    private async ValueTask<long> ReadLeasedGenerationAsync()
    {
        if (generationLeaseMs <= 0)
            return await ReadGenerationAsync().ConfigureAwait(false);

        if (TryReadLease(out long leased))
            return leased;

        await generationLeaseSem.WaitAsync().ConfigureAwait(false);
        try
        {
            // Another caller may have refreshed while this one queued; its read was issued no earlier than
            // this caller's own lease lapsed, and it is trusted only within its own lease.
            if (TryReadLease(out leased))
                return leased;

            long issuedAt = Stopwatch.GetTimestamp();
            long generation = await ReadGenerationAsync().ConfigureAwait(false);
            if (generation > 0)
            {
                long expiresAt = issuedAt + generationLeaseMs * Stopwatch.Frequency / 1000;
                lock (generationLeaseSync)
                {
                    // A local mutation may have adopted a newer generation while the read was in flight;
                    // the lease never goes backwards.
                    long floor = Volatile.Read(ref generationLease)?.Generation ?? 0;
                    Volatile.Write(ref generationLease, new GenerationLease(Math.Max(generation, floor), expiresAt));
                }
            }

            return generation;
        }
        finally
        {
            generationLeaseSem.Release();
        }
    }

    private bool TryReadLease(out long generation)
    {
        GenerationLease? lease = Volatile.Read(ref generationLease);
        if (lease is not null && Stopwatch.GetTimestamp() < lease.ExpiresAtTimestamp)
        {
            generation = lease.Generation;
            return true;
        }

        generation = 0;
        return false;
    }

    /// <summary>
    /// The mutating half of <see cref="ReadLeasedGenerationAsync"/>: after a mutation that can make another
    /// node's cached name wrong has moved the stamp, wait until every generation lease issued before the
    /// move has lapsed, so the mutation does not complete while another node can still serve the old
    /// mapping from a fresh-looking cache. Called after <see cref="writeSem"/> is released, so it delays only
    /// the statement that made the change. The margin covers timer granularity and clock-rate skew between
    /// nodes. A no-op in standalone mode and when the lease is off.
    /// </summary>
    private Task WaitOutGenerationLeasesAsync()
    {
        if (generationLeaseMs <= 0)
            return Task.CompletedTask;

        return Task.Delay(generationLeaseMs + Math.Max(10, generationLeaseMs / 10));
    }

    /// <summary>
    /// Moves the shared registry generation so every other node's next cache hit revalidates against KV.
    /// Called after a mutation (Register/Unregister/Rename) has durably committed, so the generation only
    /// moves once the change is visible. Best-effort: if the write fails the mutation is already committed
    /// and must not be undone — coherence for that one change degrades to "observed on the next mutation or
    /// restart" rather than immediately. Returns the new generation (or the current local value on failure)
    /// so the mutating node can adopt it and avoid revalidating itself.
    ///
    /// <para>The value written is immaterial — the revision the store assigns the write is the generation —
    /// so this is a blind write with no read-modify-write and therefore no race between two nodes bumping
    /// at once: each gets its own revision, and both are above what any reader last adopted.</para>
    ///
    /// <para>Because the failure is silent, the bounded retry matters here as much as it does for id
    /// allocation: a leadership blip that skipped the bump would leave every other node serving a stale
    /// registry cache with nothing said about it anywhere.</para>
    ///
    /// <para><b>This method must never throw.</b> Every caller invokes it after its mutation has
    /// durably committed, so an escaping exception (a transport fault, a routing failure) converts a
    /// committed registration into an apparent failure. The caller's compensation then destroys state
    /// that belongs to a published database — a registered branch was left pointing at purged metadata
    /// exactly this way. A failed bump therefore degrades to the same "observed on the next mutation or
    /// restart" coherence as a non-<c>Set</c> response, and the local generation is returned unchanged.</para>
    /// </summary>
    private async Task<long> BumpGenerationAsync()
    {
        try
        {
            (KeyValueResponseType type, long revision, _) = await RetryWhileAsync(
                () => kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, GenerationKey, GenerationMarker, null, -1,
                    KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None),
                r => r.Item1 is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication,
                ValueStopwatch.StartNew()
            ).ConfigureAwait(false);

            if (type == KeyValueResponseType.Set)
                return revision + 1;
        }
        catch (Exception)
        {
            // A thrown transport fault is the same outcome as a non-Set response: the notification
            // did not land. The mutation it announces is already committed and must not be undone,
            // so the failure stays here.
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

        // This node's own change is known without a read, so a lease that predates it must not make the
        // cache look stale against itself. The expiry is kept: nothing else was learned about other nodes.
        lock (generationLeaseSync)
        {
            GenerationLease? lease = Volatile.Read(ref generationLease);
            if (lease is not null && generation > lease.Generation)
                Volatile.Write(ref generationLease, lease with { Generation = generation });
        }

        // Every local registration/unregistration/rename passes through here, which makes this the one
        // place that knows the shared background snapshot has just gone stale.
        InvalidateBackgroundSnapshot();
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
            await RevalidateFromKvLockedAsync(authoritativeGeneration).ConfigureAwait(false);
        }
        finally
        {
            writeSem.Release();
        }
    }

    /// <summary>
    /// The body of <see cref="RevalidateFromKvAsync"/>, for callers that <b>already hold</b>
    /// <see cref="writeSem"/>. Mutating paths need to reconcile before they read the cache, and they
    /// must do so without releasing the semaphore — calling the public wrapper from inside the lock
    /// would deadlock on its own re-acquisition.
    /// </summary>
    private async Task RevalidateFromKvLockedAsync(long authoritativeGeneration)
    {
        if (Volatile.Read(ref loadedGeneration) >= authoritativeGeneration)
            return; // another hit already reloaded to at least this generation

        List<DatabaseRegistryEntry> entries = [];

        KvTransaction tx = KvTransaction.CreateReadOnly();
        string namePrefix = NameKeyPrefix;

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            tx.TransactionId, RegistryBucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
        {
            if (!key.StartsWith(namePrefix, StringComparison.Ordinal) || entry.Value is null)
                continue;

            entries.Add(MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.DatabaseRegistryEntry));
        }

        ReconcileCachesLocked(entries, authoritativeGeneration);
    }

    /// <summary>
    /// Rewrites the caches to match a full set of entries just read from KV, then records that the cache
    /// reflects <paramref name="authoritativeGeneration"/>. Caller must hold <see cref="writeSem"/>.
    ///
    /// <para>Reconciles in place — upsert present, evict vanished — rather than clearing first, so a
    /// concurrent lock-free reader never observes a transiently empty cache.</para>
    ///
    /// <para>Adopting the generation is a claim about the <b>whole</b> cache, which is why only a caller
    /// that has read every entry may make it. A caller that refreshed a single name must not, however
    /// current that one name now is.</para>
    /// </summary>
    private void ReconcileCachesLocked(IReadOnlyList<DatabaseRegistryEntry> entries, long authoritativeGeneration)
    {
        HashSet<string> present = new(StringComparer.Ordinal);

        lock (cacheSync)
        {
            foreach (DatabaseRegistryEntry loaded in entries)
            {
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

            // The rewrite evicted names; a backfill that read KV before it must not add them back.
            cacheMutationEpoch++;
        }

        // Every name is now current as of this generation, so the per-name records say nothing more.
        nameValidatedAt.Clear();

        Volatile.Write(ref loadedGeneration, authoritativeGeneration);
    }

    /// <summary>
    /// Copies an entry a lock-free read took from KV into the caches — unless a local mutation ran since
    /// <paramref name="epochAtRead"/> was captured (with <see cref="CaptureCacheEpoch"/>, <b>before</b>
    /// the read). The check and the write are one unit under <see cref="cacheSync"/>, so a mutation can
    /// never slip between them. Returns whether the entry was written.
    ///
    /// <para>Skipping is always safe: a backfill only adds what KV already held, and after a local
    /// mutation the caches already hold the truth for every name that mutation touched, while any other
    /// name simply misses and re-reads KV. Writing after a mutation is not safe — it is how a name a
    /// concurrent <see cref="UnregisterAsync"/> or <see cref="RetractRegistrationAsync"/> removed came
    /// back as a phantom. A backfill never advances the epoch itself.</para>
    ///
    /// <para><paramref name="overwrite"/> selects the miss-path semantics (replace whatever is cached for
    /// the name and id, e.g. re-point <c>byId</c> after a rename seen on another node) over the scan's
    /// add-if-absent semantics.</para>
    /// </summary>
    private bool TryBackfill(DatabaseRegistryEntry entry, long epochAtRead, bool overwrite)
    {
        string normalized = Normalize(entry.Name);

        lock (cacheSync)
        {
            if (cacheMutationEpoch != epochAtRead)
                return false;

            if (overwrite)
            {
                byName[normalized] = entry;
                byId[entry.Id] = entry;
            }
            else
            {
                byName.TryAdd(normalized, entry);
                byId.TryAdd(entry.Id, entry);
            }

            return true;
        }
    }

    /// <summary>
    /// The epoch a backfill must capture before its KV read; see <see cref="TryBackfill"/>.
    /// </summary>
    private long CaptureCacheEpoch() => Volatile.Read(ref cacheMutationEpoch);

    /// <summary>
    /// Refreshes <b>one</b> name against KV and returns what it resolves to now, or null if it no longer
    /// exists. This is what a stale cache hit costs on the foreground resolve path: a single point read,
    /// not a scan of the whole registry bucket.
    ///
    /// <para><b>Why not just revalidate everything.</b> The generation stamp is deliberately coarse — any
    /// mutation anywhere invalidates every cached name — so under steady DDL churn a full rebuild would
    /// run on the resolve path of user statements, and its cost grows with the number of registered
    /// databases. A caller opening one database only needs the truth about that one name.</para>
    ///
    /// <para><b>The generation is deliberately not adopted here.</b> Adopting it would claim the entire
    /// cache is current on the strength of having checked a single key, which is exactly the stale-hit
    /// bug the stamp exists to prevent. The cost of that honesty is that other names keep revalidating
    /// until a full reconcile runs; the background snapshot performs one, so a node converges within a
    /// sweep rather than on a user's statement.</para>
    ///
    /// <para>Caller must hold <see cref="writeSem"/>: the upsert/evict below must not interleave with a
    /// mutation's own cache update.</para>
    /// </summary>
    /// <summary>
    /// A registry read that came back neither <c>Get</c> nor <c>DoesNotExist</c> — still transient past the
    /// retry budget, or errored — carries no verdict on the name. It is surfaced as the retryable code so
    /// the statement is replayed from BeginAsync, never read as an absence.
    /// </summary>
    private static void ThrowIfUnanswered(KeyValueResponseType type, string name)
    {
        if (type is KeyValueResponseType.Get or KeyValueResponseType.DoesNotExist)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.TransactionMustRetry,
            $"The registry entry for database '{name}' could not be read from Kahuna ({type}) — retry the statement from BeginAsync.");
    }

    private async Task<DatabaseRegistryEntry?> RevalidateSingleNameLockedAsync(string normalizedName)
    {
        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? kvEntry) = await KahunaRetryPolicy.RetryOnMustRetry(
            () => kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, NameKey(normalizedName), -1,
                HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None),
            CancellationToken.None
        ).ConfigureAwait(false);

        if (getType == KeyValueResponseType.Get && kvEntry?.Value is not null)
        {
            // The common case: the name is exactly what the cache already holds, and only the coarse
            // stamp moved (a registration of some other database). Nothing is written and the epoch
            // stays put. Moving it here was the reason a node could stay stale for minutes: a background
            // sweep adopts the generation only if the epoch did not move during its scan, and under load
            // every statement's re-read moved it. The same cached object is returned,
            // which is what lets the caller record the name as confirmed.
            if (byName.TryGetValue(normalizedName, out DatabaseRegistryEntry? current)
                && byId.TryGetValue(current.Id, out DatabaseRegistryEntry? currentById)
                && ReferenceEquals(current, currentById)
                && kvEntry.Value.AsSpan().SequenceEqual(
                    MetaJsonSerializer.Serialize(current, MetaJsonContext.Default.DatabaseRegistryEntry)))
                return current;

            DatabaseRegistryEntry loaded = MetaJsonSerializer.Deserialize(
                kvEntry.Value, MetaJsonContext.Default.DatabaseRegistryEntry);

            lock (cacheSync)
            {
                byName[Normalize(loaded.Name)] = loaded;
                byId[loaded.Id] = loaded;
                cacheMutationEpoch++;
            }

            return loaded;
        }

        // Only a confirmed answer may evict. A read that is still unanswered past the retry budget (the
        // name's partition between leaders) says nothing about the name: evicting on it turned a
        // transient into "database does not exist" for a database that was never dropped.
        ThrowIfUnanswered(getType, normalizedName);

        // Gone from KV: dropped, or renamed away on another node. Evict it, and drop the id mapping only
        // if it still points at this name — a rename re-points byId at the new name, and that mapping
        // must survive the old name's eviction.
        lock (cacheSync)
        {
            if (byName.TryRemove(normalizedName, out DatabaseRegistryEntry? removed)
                && byId.TryGetValue(removed.Id, out DatabaseRegistryEntry? currentById)
                && Normalize(currentById.Name) == normalizedName)
            {
                byId.TryRemove(removed.Id, out _);
            }

            cacheMutationEpoch++;
        }

        return null;
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
    public static async Task<DatabaseRegistry> OpenAsync(EmbeddedKahuna sharedNode, CamusDBOptions options, bool isClusterMode = false)
    {
        ArgumentNullException.ThrowIfNull(sharedNode);

        Func<HLCTimestamp?, HLCTimestamp> mintLocalT = (floor) =>
        {
            if (floor.HasValue && !floor.Value.IsNull())
                return sharedNode.Raft.HybridLogicalClock.ReceiveEvent(sharedNode.Raft.GetLocalNodeId(), floor.Value);
            return sharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());
        };

        KvTransactionsManager txManager = new(sharedNode.Kahuna, options, mintLocalT);
        DatabaseRegistry registry = new(sharedNode.Kahuna, options, txManager, "_system/", sharedNode.Raft.GetLocalNodeId(), isClusterMode);

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
    internal static async Task<DatabaseRegistry> OpenForTestingAsync(EmbeddedKahuna node, IKahuna kvOverride, CamusDBOptions options, bool isClusterMode = false)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(kvOverride);

        Func<HLCTimestamp?, HLCTimestamp> mintLocalT = (floor) =>
        {
            if (floor.HasValue && !floor.Value.IsNull())
                return node.Raft.HybridLogicalClock.ReceiveEvent(node.Raft.GetLocalNodeId(), floor.Value);
            return node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId());
        };

        KvTransactionsManager txManager = new(kvOverride, options, mintLocalT);
        DatabaseRegistry registry = new(kvOverride, options, txManager, "_system/", node.Raft.GetLocalNodeId(), isClusterMode);

        await node.WaitUntilStartedAsync().ConfigureAwait(false);
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
    /// partition"). The bounded retry below remains as a secondary guard against a momentary fault
    /// during the scan — a re-election, or a peer that has not finished its own boot, since the scan
    /// hash-routes and so often has to reach one. It surfaces the error only if it persists past the
    /// window, since a persistent failure is a real one; see <see cref="StartupLoadRetry"/> for why
    /// that judgement is made on the budget rather than on the exception type.</para>
    /// </summary>
    private async Task LoadAsync()
    {
        Stopwatch sw = Stopwatch.StartNew();

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
            catch (Exception ex) when (StartupLoadRetry.ShouldRetry(ex, sw.ElapsedMilliseconds))
            {
                // Still assembling: a partition coming online, an election, or a peer that has not
                // finished its own boot. Wait and re-scan from a clean slate.
                await Task.Delay(StartupLoadRetry.RetryDelayMs).ConfigureAwait(false);
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

    /// <summary>The local cache-mutation epoch, for tests that pin when it may move.</summary>
    internal long CacheMutationEpochForTesting => Volatile.Read(ref cacheMutationEpoch);

    /// <summary>
    /// Whether <paramref name="normalizedName"/> was re-read at <paramref name="generation"/> or later and
    /// the cache still holds the very entry that read returned. The reference check is what makes a local
    /// change safe without clearing the record: a rename, drop or re-registration replaces or removes the
    /// cached object, and the record stops matching.
    /// </summary>
    private bool IsValidatedAt(string normalizedName, long generation, out DatabaseRegistryEntry? entry)
    {
        if (generation > 0
            && nameValidatedAt.TryGetValue(normalizedName, out (long Generation, DatabaseRegistryEntry Entry) record)
            && record.Generation >= generation
            && byName.TryGetValue(normalizedName, out DatabaseRegistryEntry? current)
            && ReferenceEquals(current, record.Entry))
        {
            entry = current;
            return true;
        }

        entry = null;
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
            // the hit may be stale — so the answer for this one name is re-read from KV before it is
            // trusted (the name may now be gone, or repointed to a new id). The generation itself comes
            // from a lease rather than a read per statement; see ReadLeasedGenerationAsync.
            long authGen = await ReadLeasedGenerationAsync().ConfigureAwait(false);
            if (authGen == Volatile.Read(ref loadedGeneration))
                return cached;

            // This name was already re-read at this generation or a later one, and the cache still holds
            // the entry that read confirmed: current, whatever the rest of the cache is.
            if (IsValidatedAt(name, authGen, out DatabaseRegistryEntry? validated))
                return validated;

            // One key, not the whole bucket. A full rebuild here would put a scan whose cost grows with
            // the number of registered databases directly under a user statement, every time any DDL
            // anywhere moved the generation. Rebuilding is left to the background sweep.
            await writeSem.WaitAsync().ConfigureAwait(false);
            try
            {
                // Re-check under the lock: a concurrent reconcile may have brought the whole cache to
                // this generation already, in which case the cached entry is trustworthy as it stands.
                if (Volatile.Read(ref loadedGeneration) >= authGen)
                    return byName.TryGetValue(name, out DatabaseRegistryEntry? reconciled) ? reconciled : null;

                // The statements that queued behind the first re-read of this name take its answer: one
                // read per name per generation change, not one per statement serialized on this lock.
                if (IsValidatedAt(name, authGen, out validated))
                    return validated;

                DatabaseRegistryEntry? revalidated = await RevalidateSingleNameLockedAsync(name).ConfigureAwait(false);

                // Only a real stamp may vouch for a name: 0 is "unanswered", which matches nothing.
                if (revalidated is not null && authGen > 0)
                    nameValidatedAt[name] = (authGen, revalidated);
                else
                    nameValidatedAt.TryRemove(name, out _);

                return revalidated;
            }
            finally
            {
                writeSem.Release();
            }
        }

        // Cache miss — try a live point-read from the persistent KV store.
        //
        // High priority, to avoid a priority inversion: this lookup runs *underneath* a user request
        // that is already in flight (opening a database it named). Queueing it behind ordinary traffic
        // would stall an already-admitted user statement on a transaction that has not been admitted
        // yet — the caller waits, but its wait is invisible to the gate.
        //
        // Captured before the read: the backfill below may only land if no local mutation ran in between
        // (see TryBackfill), otherwise it would re-add a name that mutation just removed.
        long epochAtRead = CaptureCacheEpoch();

        KvTransaction tx = await transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            priority: TransactionPriority.High
        ).ConfigureAwait(false);
        try
        {
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? kvEntry) =
                await KahunaRetryPolicy.RetryOnMustRetry(
                    () => kahuna.LocateAndTryGetValue(
                        tx.TransactionId, NameKey(name), -1,
                        HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None),
                    CancellationToken.None
                ).ConfigureAwait(false);

            // Same rule as RevalidateSingleNameLockedAsync: "no such database" is only a confirmed answer.
            ThrowIfUnanswered(getType, name);

            if (getType != KeyValueResponseType.Get || kvEntry?.Value is null)
                return null;

            DatabaseRegistryEntry entry = MetaJsonSerializer.Deserialize(
                kvEntry.Value, MetaJsonContext.Default.DatabaseRegistryEntry);

            // Backfill the local cache so subsequent reads are fast. The answer itself is returned either
            // way — it was true when read; only the cache write is conditional.
            TryBackfill(entry, epochAtRead, overwrite: true);
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
        // High for the same reason as the point-read above — it sits underneath an in-flight user
        // request, so queueing it stalls work the gate has already admitted.
        string namePrefix = NameKeyPrefix;
        KvTransaction tx = await transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            priority: TransactionPriority.High
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
    ///
    /// <para><b>Failure contract.</b> Once this method returns, the registration is durably
    /// committed; post-commit work (cache update, generation bump) cannot fail the call. When it
    /// throws, the exception code states what a compensating caller may assume: any code other
    /// than <see cref="CamusDBErrorCodes.TransactionFinalizeUnresolved"/> means the entry is
    /// confirmed not published; <c>TransactionFinalizeUnresolved</c> means the commit outcome is
    /// unknown and the entry may be — or may later become — durably published, so destructive
    /// compensation must first prove the state, e.g. via
    /// <see cref="RetractRegistrationAsync"/>.</para>
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
            bool commitAttempted = false;
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
                commitAttempted = true;
                await transactions.CommitAsync(tx).ConfigureAwait(false);
            }
            catch (Exception registerEx)
            {
                try
                {
                    await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Never mask the original failure with a cleanup failure. An unfinalized
                    // session is reclaimed by the coordinator's own timeout.
                }

                // Failure classification the caller's compensation depends on. Before the commit
                // request, nothing can ever become durable, so the original error is rethrown as a
                // confirmed non-registration. After the commit request, only a definite coordinator
                // abort (TransactionConflict) proves the entry did not land; every other failure —
                // an unresolved finalize, a lost session, a transport fault, a cancellation — leaves
                // the outcome unknown, and the entry may be (or may still become) durably published.
                // Surface that as TransactionFinalizeUnresolved so a caller never runs destructive
                // compensation against a registration it cannot prove absent.
                if (commitAttempted && registerEx is not CamusDBException { Code: CamusDBErrorCodes.TransactionConflict })
                    throw new CamusDBException(
                        CamusDBErrorCodes.TransactionFinalizeUnresolved,
                        $"Registration of database '{name}' (id={id}) has an unknown commit outcome " +
                        $"({registerEx.GetType().Name}: {registerEx.Message}); the entry may or may not be durably published");

                throw;
            }

            lock (cacheSync)
            {
                byName[normalized] = entry;
                byId[id] = entry;
                cacheMutationEpoch++;
            }

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

        bool waitOutLeases = false;

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

            lock (cacheSync)
            {
                byName.TryRemove(name, out _);
                byId.TryRemove(entry.Id, out _);
                cacheMutationEpoch++;
            }

            // Advance the shared generation so other nodes drop their now-stale cache hit for this name.
            AdoptGeneration(await BumpGenerationAsync().ConfigureAwait(false));
            waitOutLeases = true;
        }
        finally
        {
            writeSem.Release();

            // Outside the lock, and on the success path only (the flag is set after the bump).
            if (waitOutLeases)
                await WaitOutGenerationLeasesAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Identity-checked, authoritative retraction of one registration: removes the entry for
    /// <paramref name="name"/> only when the persistent store still maps it to
    /// <paramref name="expectedId"/>. This is the compensation primitive for a failed create —
    /// unlike <see cref="UnregisterAsync"/> it consults the durable key (not the local cache, which
    /// a failed <see cref="RegisterAsync"/> may have left empty or stale), and it can never remove a
    /// replacement entry another create registered under the same name.
    ///
    /// <para>The read and the delete run under the key's exclusive lock in one transaction, so the
    /// check-then-delete cannot race a concurrent registration. On every non-throwing return the
    /// caller has proof that <paramref name="expectedId"/> is not the live owner of the name
    /// <em>now</em>; whether that proof extends to "will never be" is the caller's to establish
    /// (an unresolved registration commit can still land after an <see cref="RegistryRetraction.Absent"/>
    /// read). A throw means the state could not be established or the delete could not be
    /// confirmed — the caller must then treat the registration as possibly live.</para>
    /// </summary>
    public async Task<RegistryRetraction> RetractRegistrationAsync(string name, string expectedId)
    {
        string normalized = Normalize(name);

        bool waitOutLeases = false;

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            KvTransaction tx = await transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);

            RegistryRetraction outcome;
            bool commitAttempted = false;
            try
            {
                outcome = await DeleteRegistryKeyIfIdAsync(tx, NameKey(normalized), expectedId).ConfigureAwait(false);
                if (outcome == RegistryRetraction.Retracted)
                {
                    commitAttempted = true;
                    await transactions.CommitAsync(tx).ConfigureAwait(false);
                }
                else
                {
                    await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                }
            }
            catch (Exception retractEx)
            {
                try
                {
                    await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Never mask the original failure; the abandoned session expires server-side.
                }

                // Same classification as RegisterAsync: after the commit request, only a definite
                // coordinator abort proves the delete did not land — anything else leaves the
                // retraction unconfirmed, which the caller must treat as "possibly still live".
                if (commitAttempted && retractEx is not CamusDBException { Code: CamusDBErrorCodes.TransactionConflict })
                    throw new CamusDBException(
                        CamusDBErrorCodes.TransactionFinalizeUnresolved,
                        $"Retraction of registry entry '{name}' (id={expectedId}) has an unknown commit outcome " +
                        $"({retractEx.GetType().Name}: {retractEx.Message}); the entry may or may not still be published");

                throw;
            }

            // The durable state is established. Drop any local cache claim that still maps this
            // name to this id — including an entry a failed create's RegisterAsync left behind.
            lock (cacheSync)
            {
                if (byName.TryGetValue(normalized, out DatabaseRegistryEntry? cached)
                    && string.Equals(cached.Id, expectedId, StringComparison.Ordinal))
                    byName.TryRemove(normalized, out _);

                byId.TryRemove(expectedId, out _);
                cacheMutationEpoch++;
            }

            if (outcome == RegistryRetraction.Retracted)
            {
                AdoptGeneration(await BumpGenerationAsync().ConfigureAwait(false));
                waitOutLeases = true;
            }

            return outcome;
        }
        finally
        {
            writeSem.Release();

            // Outside the lock, and on the success path only (the flag is set after the bump).
            if (waitOutLeases)
                await WaitOutGenerationLeasesAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The in-transaction body of <see cref="RetractRegistrationAsync"/>: locks <paramref name="key"/>
    /// exclusively, reads the current owner, and deletes it only on an id match. Lock, read, and
    /// delete are one lock acquisition so the verify cannot be split from the delete — the same
    /// shape as <see cref="WriteRegistryKey"/>'s compare-and-set path.
    /// </summary>
    private async Task<RegistryRetraction> DeleteRegistryKeyIfIdAsync(KvTransaction tx, string key, string expectedId)
    {
        KeyValueResponseType lockType;
        int lockRetries = 0;

        // Stable per-operation ids reused across the retry loop (see WriteRegistryKey) so the delete
        // and its lock fold once into the coordinator working set.
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

        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? current) = await kahuna.LocateAndTryGetValue(
            tx.TransactionId, key, -1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None
        ).ConfigureAwait(false);

        if (getType == KeyValueResponseType.DoesNotExist
            || (getType == KeyValueResponseType.Get && current?.Value is null))
            return RegistryRetraction.Absent;

        if (getType != KeyValueResponseType.Get)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to read registry key '{key}': {getType}");

        DatabaseRegistryEntry entry = MetaJsonSerializer.Deserialize(
            current!.Value!, MetaJsonContext.Default.DatabaseRegistryEntry);

        if (!string.Equals(entry.Id, expectedId, StringComparison.Ordinal))
            return RegistryRetraction.OwnedByOther;

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
        return RegistryRetraction.Retracted;
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

        bool waitOutLeases = false;

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

            // Copy-then-change, never rebuild by hand: everything except the name must survive a
            // rename, and enumerating the survivors here is how the comment got dropped and how the
            // snapshot-floor hold id nearly did. Copy() owns that list.
            DatabaseRegistryEntry updated = existing.Copy();
            updated.Name = newName;

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

            lock (cacheSync)
            {
                if (!caseOnlyRename)
                    byName.TryRemove(normalizedOld, out _);
                byName[normalizedNew] = updated;
                byId[existing.Id] = updated;
                cacheMutationEpoch++;
            }

            // Advance the shared generation so other nodes stop resolving the old name and pick up the new.
            AdoptGeneration(await BumpGenerationAsync().ConfigureAwait(false));
            waitOutLeases = true;
        }
        finally
        {
            writeSem.Release();

            // Outside the lock, and on the success path only (the flag is set after the bump).
            if (waitOutLeases)
                await WaitOutGenerationLeasesAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Attaches or removes the free-text comment on a registered database (<c>COMMENT ON DATABASE</c>).
    /// A null <paramref name="comment"/> removes it; the empty string stores a present-but-empty one.
    ///
    /// <para>Unlike the other registry mutations this one <b>reconciles before it reads the cache</b>
    /// and <b>verifies before it writes</b>, because neither guard can be skipped safely here.
    /// <see cref="writeSem"/> is process-local, so on a cluster it says nothing about what another
    /// node has done. Without the reconcile, a database created elsewhere is reported as
    /// non-existent. Without the verify, the write is an unconditional <c>Set</c> on a name this node
    /// may only believe in — which would <em>resurrect</em> a key another node dropped or renamed
    /// away, leaving two names pointing at one database id.</para>
    ///
    /// <para>The reconcile deliberately calls the lock-held core rather than
    /// <c>TryResolveEntryAsync</c>: that method re-acquires <see cref="writeSem"/> on its
    /// revalidation path and would deadlock against the lock this method already holds.</para>
    /// </summary>
    /// <exception cref="CamusDBException"><c>DatabaseDoesntExist</c> when the name is not registered,
    /// or was dropped/renamed away by another node before this write landed.</exception>
    public async Task SetCommentAsync(string dbName, string? comment)
    {
        string normalized = Normalize(dbName);

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            // Standalone: this process owns the only registry, so the cache is authoritative and
            // writeSem alone orders this against every local mutation. Matches the fast path in
            // TryResolveEntryAsync.
            if (isClusterMode)
            {
                long authGen = await ReadGenerationAsync().ConfigureAwait(false);
                if (authGen != Volatile.Read(ref loadedGeneration))
                    await RevalidateFromKvLockedAsync(authGen).ConfigureAwait(false);
            }

            if (!byName.TryGetValue(normalized, out DatabaseRegistryEntry? existing))
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseDoesntExist,
                    $"Database '{dbName}' is not registered");

            DatabaseRegistryEntry updated = existing.Copy();
            updated.Comment = comment;

            byte[] updatedBytes = MetaJsonSerializer.Serialize(updated, MetaJsonContext.Default.DatabaseRegistryEntry);

            KvTransaction tx = await transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);

            bool written;
            try
            {
                // expectedId turns the write into a compare-and-set under the key's exclusive lock:
                // the key must still exist and still name this database id. A concurrent drop or
                // rename on another node fails the check instead of being undone by this write.
                written = await WriteRegistryKey(
                    tx, NameKey(normalized), updatedBytes, ifAbsent: false, expectedId: existing.Id
                ).ConfigureAwait(false);

                if (!written)
                {
                    await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                }
                else
                {
                    await transactions.CommitAsync(tx).ConfigureAwait(false);
                }
            }
            catch
            {
                await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                throw;
            }

            if (!written)
            {
                // The name vanished or was repointed underneath us. Drop the stale cache entry so the
                // next resolve re-reads KV rather than serving what we just failed to write.
                lock (cacheSync)
                {
                    byName.TryRemove(normalized, out _);
                    cacheMutationEpoch++;
                }

                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseDoesntExist,
                    $"Database '{dbName}' is not registered");
            }

            lock (cacheSync)
            {
                byName[normalized] = updated;
                byId[existing.Id] = updated;
                cacheMutationEpoch++;
            }

            AdoptGeneration(await BumpGenerationAsync().ConfigureAwait(false));
        }
        finally
        {
            writeSem.Release();
        }
    }

    /// <summary>
    /// Authoritatively resolves the live registered name for a database <paramref name="id"/> by
    /// scanning persistent KV (not the local cache), or <c>null</c> if the id is not currently
    /// registered. Used by relink to decide, under the fence, whether an id is already live (and thus
    /// whether this is an idempotent retry, a conflicting second alias, or a fresh recovery) — a
    /// decision that must reflect registrations made on other cluster nodes. Also the source-liveness
    /// gate branch-create runs after publishing its child: the drop-intent marker only covers a drop
    /// still in flight, so branch-create must additionally confirm the parent's immutable id is still
    /// registered — a read that must see an unregister performed on another node, which the local
    /// cache cannot guarantee.
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
    /// Resolves the full registry entry for a database <paramref name="id"/>: local cache first,
    /// falling back to a persistent-KV scan for entries registered on other nodes. Returns
    /// <c>null</c> when the id is not currently registered anywhere.
    ///
    /// <para>A cache hit is served without cross-node revalidation, so use this only where the
    /// fields consumed are immutable after registration — the snapshot-hold chain resolution reads
    /// <see cref="DatabaseRegistryEntry.Ancestors"/> and
    /// <see cref="DatabaseRegistryEntry.ImmediateParentHoldId"/>, both of which never change (a
    /// rename preserves them). A caller that needs the current <em>name</em> must not rely on the
    /// cached copy.</para>
    /// </summary>
    public async Task<DatabaseRegistryEntry?> TryResolveEntryByIdAsync(string id)
    {
        if (byId.TryGetValue(id, out DatabaseRegistryEntry? cached))
            return cached;

        foreach (DatabaseRegistryEntry entry in await ScanAllEntriesAsync().ConfigureAwait(false))
        {
            if (string.Equals(entry.Id, id, StringComparison.Ordinal))
                return entry;
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
    /// used by <see cref="TryResolveEntryAsync"/>. The backfill is fenced by the local cache-mutation
    /// epoch (<see cref="TryBackfill"/>): once a mutation on this registry lands mid-scan, the rest of
    /// the scan adds nothing, because its pages predate that mutation and could resurrect a name it
    /// removed. The returned list is unaffected — it is what KV held when the scan read it.</para>
    ///
    /// <para>Transient scan failures are absorbed by <see cref="RegistryKeyspace.RetryTransientScanAsync"/>; a
    /// restarted attempt re-runs the backfill, which is harmless because the cache adds are
    /// idempotent.</para>
    /// </summary>
    public Task<IReadOnlyList<DatabaseRegistryEntry>> ScanAllEntriesAsync() => RegistryKeyspace.RetryTransientScanAsync(async () =>
    {
        string namePrefix = NameKeyPrefix;
        List<DatabaseRegistryEntry> entries = [];

        // A read-only context, not the read-write transaction this once opened: nothing here writes,
        // and a read-write transaction over a full bucket scan makes every background sweep look like a
        // writer to the coordinator. The synthetic read-only identity performs read-committed per-key
        // reads with no START/ROLLBACK round-trips, matching how the catalog's own meta scans read.
        KvTransaction tx = transactions.CreateReadOnlyTransaction();

        // Captured before the scan starts: every page it yields predates any mutation that lands after
        // this point, so none of them may be written into the cache once the epoch has moved.
        long epochAtRead = CaptureCacheEpoch();
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

                // Backfill cache for entries registered on other nodes. Keyed by the normalized name,
                // like every other write to this dictionary: the raw name was silently dead for any
                // database created with an upper-case letter, because every lookup normalizes first —
                // the entry went in under a key nothing would ever ask for. Add-if-absent, and only while
                // no local mutation has run since the scan began.
                TryBackfill(loaded, epochAtRead, overwrite: false);

                entries.Add(loaded);
            }
        }
        finally
        {
            await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }

        return (IReadOnlyList<DatabaseRegistryEntry>)entries;
    });

    /// <summary>
    /// How long a background snapshot is reused before it is rescanned. Sized to coalesce the sweeps
    /// that fire together — row-level TTL discovery and its metadata reaper run inside one sweep, and
    /// the other periodic loops tick on the same order of magnitude — while staying far shorter than
    /// any of their intervals, so a database registered or dropped on another node is picked up on the
    /// next tick rather than the one after.
    /// </summary>
    private const int BackgroundSnapshotValidityMs = 10_000;

    private readonly SemaphoreSlim snapshotSem = new(1, 1);
    private IReadOnlyList<DatabaseRegistryEntry>? backgroundSnapshot;
    private long backgroundSnapshotTicks;

    /// <summary>
    /// The registry entry list for <b>background sweeps</b>: the same content as
    /// <see cref="ScanAllEntriesAsync"/>, but shared between callers for a short window instead of
    /// rescanning the whole bucket once per loop. Every periodic loop in the engine — TTL discovery and
    /// its reaper, auto-analyze discovery, the snapshot-hold renewer, the orphan reclaimer — wants the
    /// same list at roughly the same moment, and independently scanning for each turns one registry
    /// read into five that grow with the number of registered databases.
    ///
    /// <para><b>Only for work that re-confirms what it acts on.</b> The list may be up to
    /// <see cref="BackgroundSnapshotValidityMs"/> old, so a database registered a moment ago can be
    /// missing and one just dropped can still appear. That is safe for the sweeps precisely because
    /// each re-establishes its own ground truth per database — a fence acquisition, an existence
    /// re-check, or a metadata scan that simply finds nothing. A foreground statement that reports the
    /// registry to a user (<c>SHOW</c>) must call <see cref="ScanAllEntriesAsync"/> instead, and see
    /// its own writes.</para>
    /// </summary>
    public async Task<IReadOnlyList<DatabaseRegistryEntry>> GetBackgroundSnapshotAsync()
    {
        if (TryReadFreshSnapshot(out IReadOnlyList<DatabaseRegistryEntry> fresh))
            return fresh;

        // Single-flight: concurrent sweeps queue here and take the one scan's result rather than each
        // launching their own, which is the whole point of sharing.
        await snapshotSem.WaitAsync().ConfigureAwait(false);
        try
        {
            if (TryReadFreshSnapshot(out fresh))
                return fresh;

            // Read the generation BEFORE the scan. A mutation landing mid-scan then leaves this value
            // behind the authoritative one, so the cache is correctly recorded as stale and revalidates
            // again — whereas a generation read afterwards would claim currency for a change the scan
            // may not have seen.
            long generationBeforeScan = isClusterMode ? await ReadGenerationAsync().ConfigureAwait(false) : 0;
            long epochBeforeScan = CaptureCacheEpoch();

            IReadOnlyList<DatabaseRegistryEntry> scanned = await ScanAllEntriesAsync().ConfigureAwait(false);

            // The background sweep is where a full rebuild belongs, so it is also where the cache earns
            // the right to call itself current. Without this the foreground resolve path — which now
            // refreshes a single name and deliberately does not adopt the generation — would keep paying
            // a point read per open forever on a node that reads but never writes.
            //
            // A local mutation that landed mid-scan makes the scanned list older than the cache, so the
            // rewrite is skipped for this sweep: the generation check alone does not catch that case when
            // the mutation's best-effort generation bump failed. Under writeSem the epoch cannot move, so
            // the single check inside the lock is decisive.
            if (isClusterMode && Volatile.Read(ref loadedGeneration) < generationBeforeScan)
            {
                await writeSem.WaitAsync().ConfigureAwait(false);
                try
                {
                    if (Volatile.Read(ref loadedGeneration) < generationBeforeScan
                        && Volatile.Read(ref cacheMutationEpoch) == epochBeforeScan)
                        ReconcileCachesLocked(scanned, generationBeforeScan);
                }
                finally
                {
                    writeSem.Release();
                }
            }

            // List before timestamp: a reader that sees the new timestamp with the old list would serve
            // one extra round of stale data, while this ordering costs at worst one redundant scan.
            Volatile.Write(ref backgroundSnapshot, scanned);
            Volatile.Write(ref backgroundSnapshotTicks, Environment.TickCount64);

            return scanned;
        }
        finally
        {
            snapshotSem.Release();
        }
    }

    private bool TryReadFreshSnapshot(out IReadOnlyList<DatabaseRegistryEntry> entries)
    {
        IReadOnlyList<DatabaseRegistryEntry>? cached = Volatile.Read(ref backgroundSnapshot);

        if (cached is not null &&
            Environment.TickCount64 - Volatile.Read(ref backgroundSnapshotTicks) < BackgroundSnapshotValidityMs)
        {
            entries = cached;
            return true;
        }

        entries = [];
        return false;
    }

    /// <summary>
    /// Drops the shared background snapshot so the next sweep rescans. Called whenever this node
    /// registers, unregisters, or renames a database: those are exactly the changes a sweep must not
    /// keep missing, and this node knows about its own immediately. A change made on another node is
    /// still only picked up when the window lapses.
    /// </summary>
    private void InvalidateBackgroundSnapshot() => Volatile.Write(ref backgroundSnapshot, null);

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
    /// <summary>
    /// Writes a registry key inside <paramref name="tx"/>, under that key's exclusive lock.
    ///
    /// <para><paramref name="ifAbsent"/> makes the write a create (returns false when the key already
    /// exists). <paramref name="expectedId"/> makes it a compare-and-set: the key must already exist
    /// and its entry's <c>Id</c> must match, or the write is skipped and false is returned. The two
    /// are opposites and must not be combined. Passing neither is an unconditional overwrite, which
    /// is only safe when the caller has independently established that the key still represents what
    /// it thinks it does — on a cluster, a local cache is not such an establishment.</para>
    /// </summary>
    private async Task<bool> WriteRegistryKey(
        KvTransaction tx, string key, byte[] value, bool ifAbsent = false, string? expectedId = null)
    {
        if (ifAbsent && expectedId is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "WriteRegistryKey cannot both require absence and require a matching id");

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

        // Read-and-verify happens after the lock is held, so nothing can slip between the check and
        // the set: the key is pinned for the rest of this transaction.
        if (expectedId is not null)
        {
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? currentEntry) =
                await kahuna.LocateAndTryGetValue(
                    tx.TransactionId, key, -1,
                    HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);

            if (getType != KeyValueResponseType.Get || currentEntry?.Value is null)
                return false;

            DatabaseRegistryEntry current = MetaJsonSerializer.Deserialize(
                currentEntry.Value, MetaJsonContext.Default.DatabaseRegistryEntry);

            if (!string.Equals(current.Id, expectedId, StringComparison.Ordinal))
                return false;
        }

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
        await DropMarkers.DisposeAsync().ConfigureAwait(false);

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

/// <summary>
/// Outcome of <see cref="DatabaseRegistry.RetractRegistrationAsync"/>. Every value is an
/// authoritative statement about the persistent store at the moment of the locked read; a caller
/// deciding whether destructive compensation is safe must also account for whether an unresolved
/// registration commit could still land afterward (see the method's summary).
/// </summary>
public enum RegistryRetraction
{
    /// <summary>The name mapped to the expected id and the entry was deleted and committed.</summary>
    Retracted,

    /// <summary>The name was not registered at the time of the locked read.</summary>
    Absent,

    /// <summary>
    /// The name is registered to a different id. The expected id's registration is not live, and the
    /// other create's entry was deliberately left untouched.
    /// </summary>
    OwnedByOther,
}
