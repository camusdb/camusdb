
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using System.Collections.Concurrent;
using System.Text;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Scoped handle returned by <see cref="DatabaseDescriptor.Use"/>.
/// Holds a use-reference on the descriptor; the reference is released when
/// <see cref="Dispose"/> is called (i.e. at the end of a <c>using</c> block).
/// </summary>
internal sealed class DatabaseUseHandle : IDisposable
{
    private DatabaseDescriptor? _db;

    internal DatabaseUseHandle(DatabaseDescriptor db) => _db = db;

    public void Dispose()
    {
        DatabaseDescriptor? db = Interlocked.Exchange(ref _db, null);
        db?.Release();
    }
}

public sealed record DatabaseDescriptor : IDisposable
{
    public string Id { get; }

    /// <summary>
    /// <see cref="Id"/> as UTF-8, encoded once when the descriptor is built. Every committed
    /// schema-log entry delivered to this database's subscriber carries the owning database's id
    /// in its byte frame, and the subscriber compares the two to decide whether the entry is its
    /// own. Re-encoding the id for that comparison would allocate on every delivery, including the
    /// deliveries the subscriber exists only to drop. The array is never handed out.
    /// </summary>
    private readonly byte[] idUtf8;

    /// <summary>UTF-8 view of <see cref="Id"/> for allocation-free comparison against frame bytes.</summary>
    internal ReadOnlySpan<byte> IdUtf8 => idUtf8;

    // Schema-log entries this database's subscriber actually deserialized, as opposed to skipped
    // from the entry's byte frame. Test-only instrumentation: the skip paths are cheap precisely
    // because they never decode, and nothing but a test can observe the difference.
    private long schemaEntriesDecoded;

    /// <summary>
    /// How many schema-log entries this database decoded since it was opened. Test-only: it exists
    /// so a test can assert that a foreign-database entry, or one this node already applied, costs
    /// no decode, and that a proposer decodes its own entry once rather than once per delivery.
    /// </summary>
    internal long SchemaEntriesDecoded => Interlocked.Read(ref schemaEntriesDecoded);

    /// <summary>Counts one decode. Called by the schema replicator, never by production readers.</summary>
    internal void CountSchemaEntryDecode() => Interlocked.Increment(ref schemaEntriesDecoded);

    /// <summary>
    /// The schema version the last applied delta produced, paired with a digest of the entry bytes
    /// it was carried in. Held as one object so a reader outside the schema lock cannot observe the
    /// version of one delta next to the digest of another.
    /// </summary>
    private sealed record AppliedSchemaEntry(long ToVersion, ulong Fingerprint);

    private AppliedSchemaEntry? lastAppliedSchemaEntry;

    /// <summary>
    /// Records which delta this node most recently applied. Called from the schema apply and
    /// restore paths with the lock held, so the two fields always move together.
    /// </summary>
    internal void RecordAppliedSchemaEntry(long toVersion, ulong fingerprint)
    {
        Volatile.Write(ref lastAppliedSchemaEntry, new AppliedSchemaEntry(toVersion, fingerprint));
    }

    /// <summary>
    /// True when the given entry is the one this node last applied, so re-applying it would do
    /// nothing. The proposer of a DDL change is delivered its own entry twice — once through
    /// replication and once through the local apply that lets it observe the change before it
    /// returns — and this is what lets the second delivery be dropped without deserializing it.
    ///
    /// <para>The digest is what makes the answer safe. Two different deltas can claim the same
    /// target version when two nodes propose against the same base; that case must reach the apply
    /// path and fail as out-of-order, and it does, because its digest differs. A false answer here
    /// is therefore only ever "no", which costs a decode and nothing else.</para>
    /// </summary>
    internal bool WasSchemaEntryApplied(long toVersion, ulong fingerprint)
    {
        AppliedSchemaEntry? applied = Volatile.Read(ref lastAppliedSchemaEntry);
        return applied is not null && applied.ToVersion == toVersion && applied.Fingerprint == fingerprint;
    }

    /// <summary>
    /// Configuration of the engine this database belongs to. Carried here so per-table and per-scan
    /// code — which always has a descriptor in hand — reads its engine's settings without threading
    /// options through every call, and without reaching for a process-wide value. Swapped in place
    /// by <see cref="ApplyOptions"/> when the engine publishes a new snapshot; readers pin it once
    /// per operation, so an in-flight operation keeps the record it started with.
    /// </summary>
    public CamusDBOptions Options { get; private set; }

    /// <summary>
    /// Forwards a newly published configuration snapshot to this database and everything long-lived
    /// it owns: the transactions manager and every already-opened table store, each of which
    /// captured the record at construction. Only table descriptors whose lazy open has completed
    /// are touched — one still opening captures the descriptor's current options when it finishes,
    /// which is this same snapshot or a newer one.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next)
    {
        Options = next;
        Transactions.ApplyOptions(next);

        foreach (KeyValuePair<string, AsyncLazy<TableDescriptor>> table in TableDescriptors)
        {
            // IsStarted first: reading AsyncLazy.Task starts the factory, and a swap must never
            // force-open a table that nothing has asked for.
            if (table.Value.IsStarted && table.Value.Task.IsCompletedSuccessfully)
                table.Value.Task.Result.Store.ApplyOptions(next);
        }
    }

    private volatile string _name;

    /// <summary>
    /// The database's current <b>display</b> name — for logs, error text, and diagnostics.
    ///
    /// <para>Never use it as a lookup key. Request-scoped code that needs a name to resolve something
    /// must take it from <c>ticket.DatabaseName</c> (the name the caller actually asked for), and code
    /// that already holds this descriptor should pass the descriptor itself — e.g.
    /// <c>CommandExecutor.OpenTableWithDescriptor</c> rather than the by-name
    /// <c>CommandExecutor.OpenTable</c>. Feeding this value back into a by-name resolution is what
    /// broke INSERT after a RENAME DATABASE.</para>
    ///
    /// <para>A rename keeps this descriptor alive — the cache is keyed by the immutable id, and
    /// evicting it would orphan the running Kahuna node — and refreshes this field in place via
    /// <see cref="SetName"/>. It is <c>volatile</c> because logging and diagnostics on other threads
    /// read it while a rename is running.</para>
    /// </summary>
    public string Name => _name;

    /// <summary>
    /// Refreshes the display name after a committed RENAME DATABASE. Called only by the rename path,
    /// which has already durably swapped the registry binding, so this cannot fail the statement.
    /// Does not touch the id, the key space, or any cached table descriptor — none of which a rename
    /// changes.
    /// </summary>
    internal void SetName(string name)
    {
        _name = name;

        // A rename is a use. The path that lands here resolves the descriptor by id and never takes a
        // use-reference, so without this a database renamed moments ago would still look untouched to
        // the idle sweep.
        Touch();
    }

    public EmbeddedKahuna Kahuna { get; }

    // Lazily resolved by SchemaLogPartition. 0 means "not yet resolved" — it is the reserved
    // partition and EmbeddedKahuna.SchemaLogPartition never returns it. Benign race: concurrent
    // resolvers compute the same value.
    private int _schemaLogPartition;

    /// <summary>
    /// The single Raft partition that carries all schema-log traffic for this database, resolved
    /// once and cached. The schema-apply subscription compares every incoming entry's partition
    /// against this value to skip deserializing entries that cannot belong to this database —
    /// with many databases open, most replicated schema entries arrive on other partitions.
    /// </summary>
    public int SchemaLogPartition
    {
        get
        {
            int partition = _schemaLogPartition;
            if (partition == 0)
                _schemaLogPartition = partition = Kahuna.SchemaLogPartition(Id);
            return partition;
        }
    }

    public KvTransactionsManager Transactions { get; }

    public SemaphoreSlim SchemaDdlSemaphore { get; } = new(1, 1);

    public SemaphoreSlim SystemSchemaSemaphore { get; } = new(1, 1);

    // Set when persist-checkpoint exhausts all retries after a committed DDL.
    // Gates further DDL proposals on this node until the node recovers via restart-replay.
    private volatile int _schemaSubsystemDegraded;

    public bool SchemaSubsystemDegraded => _schemaSubsystemDegraded != 0;

    public void MarkSchemaSubsystemDegraded()
        => Interlocked.Exchange(ref _schemaSubsystemDegraded, 1);

    public void ClearSchemaSubsystemDegraded()
        => Interlocked.Exchange(ref _schemaSubsystemDegraded, 0);

    // Step-down is deferred until the in-flight DDL transaction's CommitAsync completes,
    // so the KV commit succeeds before leadership changes (important when schema and KV share a
    // single Raft partition, as in single-partition test clusters).
    private volatile int _deferredSchemaStepDown;

    public bool DeferredSchemaStepDown => _deferredSchemaStepDown != 0;

    public void RequestDeferredSchemaStepDown()
        => Interlocked.Exchange(ref _deferredSchemaStepDown, 1);

    public void ClearDeferredSchemaStepDown()
        => Interlocked.Exchange(ref _deferredSchemaStepDown, 0);

    /// <summary>
    /// If a deferred step-down was requested due to checkpoint-persist exhaustion, clears the flag and
    /// steps down schema-partition leadership. Throws on step-down failure — callers should
    /// catch and log with their own logger. No-op if no step-down was requested.
    /// </summary>
    internal async Task FireDeferredSchemaStepDownAsync()
    {
        if (!DeferredSchemaStepDown)
            return;

        ClearDeferredSchemaStepDown();
        await Kahuna.StepDownSchemaPartitionAsync(Id, CancellationToken.None).ConfigureAwait(false);
    }

    // Fence: highest schema-log entry ToVersion received by this node (committed in Raft,
    // delivered to ApplyAsync or RestoreAsync), regardless of whether it has been applied to the
    // in-memory schema yet. Monotonically increasing; updated before the schema lock is acquired.
    // The gap HeadSchemaVersion − Schema.SchemaVersion > 1 means at least two schema deltas are
    // in the apply pipeline but not yet materialised. DML is fenced until the node catches up so
    // it does not decode rows with a stale schema. See SchemaReplicator.ApplyAsync/RestoreAsync
    // and TableOpener.Open (where the fence is checked).
    private long _headSchemaVersion;

    public long HeadSchemaVersion => Volatile.Read(ref _headSchemaVersion);

    /// <summary>
    /// Records that a schema-log entry with <paramref name="entryVersion"/> has been received
    /// (committed in Raft) but may not yet be applied to <see cref="Schema"/>. Updates
    /// <see cref="HeadSchemaVersion"/> monotonically — lower values are silently ignored.
    /// Thread-safe; called from the schema apply / restore pipeline before acquiring the lock.
    /// </summary>
    internal void ObserveSchemaEntryHead(long entryVersion)
    {
        long current;
        do
        {
            current = Volatile.Read(ref _headSchemaVersion);
            if (entryVersion <= current)
                return;
        }
        while (Interlocked.CompareExchange(ref _headSchemaVersion, entryVersion, current) != current);
    }

    /// <summary>
    /// Immutable branch ancestry chain inherited from the registry entry at open time,
    /// nearest parent first.  Empty for root databases.  The full read lineage at
    /// query-execution time is <c>[(this.Id, tx.ReadTimestamp)] + Ancestors</c>; the
    /// self-level timestamp is transaction-dependent and resolved by the storage layer.
    /// </summary>
    public IReadOnlyList<DatabaseBranchAncestor> Ancestors { get; }

    public Schema Schema { get; } = new();

    public SystemSchema SystemSchema { get; set; } = new();

    /// <summary>
    /// Per-database query result cache, or null when the cache is disabled. Set by
    /// <see cref="Controllers.DatabaseOpener"/> when the database is first loaded.
    /// Passed to <see cref="KvTransactionsManager"/> so DML commit hooks can drive the
    /// <see cref="CachePublishGate"/> write protocol (mark in-flight → invalidate → commit).
    /// DDL paths call <see cref="IQueryResultCache.InvalidateByTableId"/> directly after
    /// each successful schema commit because schema meta keys do not match the row/index
    /// keyspace bucket pattern used by the automatic key-based invalidation.
    /// </summary>
    public IQueryResultCache? Cache { get; internal set; }

    /// <summary>
    /// Evicts one relation's cached statistics on this node, by relation id. Set by the engine that
    /// owns the statistics manager.
    ///
    /// <para>It exists as a hook because the replicated schema-apply callback — the one path that runs
    /// on <b>every</b> node — has the descriptor but not the statistics manager. Anything a refresh
    /// must invalidate cluster-wide has to be reachable from there; invalidating only where the
    /// statement was issued leaves every other node serving plans costed against contents that no
    /// longer exist.</para>
    /// </summary>
    public Action<string>? EvictTableStatistics { get; internal set; }

    public ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> TableDescriptors { get; }

    /// <summary>
    /// Contents generations this node's schema apply has detached from a live relation but has not yet
    /// made recoverable on disk. Drained by the checkpoint that follows the apply, and by the
    /// reconciliation that follows a WAL restore.
    ///
    /// <para>The list exists because apply may not write: it runs inside the schema partition's commit
    /// pipeline, and a KV write from there deadlocks the partition. Entries are appended in retirement
    /// order, so several generations retired between two checkpoints are persisted in that order.</para>
    /// </summary>
    private readonly List<Catalogs.Models.ContentsRetirementIntent> pendingContentsRetirements = [];

    /// <summary>Records a detached contents generation for the next durable checkpoint to write.</summary>
    internal void AddContentsRetirement(Catalogs.Models.ContentsRetirementIntent intent)
    {
        lock (pendingContentsRetirements)
            pendingContentsRetirements.Add(intent);
    }

    /// <summary>
    /// The retirements still waiting to be written, oldest first.
    /// </summary>
    /// <remarks>
    /// Deliberately a snapshot rather than a drain. The caller writes these inside a transaction that
    /// may still fail or time out, and an intent removed before its commit succeeded would leave a
    /// retired key-space with no record — unreachable and never reclaimed. The caller calls
    /// <see cref="CompleteContentsRetirements"/> only after the commit returns.
    /// </remarks>
    internal Catalogs.Models.ContentsRetirementIntent[] PendingContentsRetirements()
    {
        lock (pendingContentsRetirements)
            return [.. pendingContentsRetirements];
    }

    /// <summary>Drops the retirements whose durable record is now committed.</summary>
    internal void CompleteContentsRetirements(IReadOnlyList<Catalogs.Models.ContentsRetirementIntent> written)
    {
        lock (pendingContentsRetirements)
        {
            foreach (Catalogs.Models.ContentsRetirementIntent intent in written)
                pendingContentsRetirements.Remove(intent);
        }
    }

    /// <summary>
    /// Bound SELECT statements reusable across executions of the same cached SQL text, keyed by the
    /// parse-cache <see cref="SQLParser.NodeAst"/> instance. Lives on the descriptor so that closing,
    /// dropping, or evicting this database releases every cached binding with it — a slot must never
    /// outlive the descriptor whose tables it references. See <see cref="Queries.BoundQueryCache"/>
    /// for the validation contract each hit must pass.
    /// </summary>
    internal Queries.BoundQueryCache BoundQueries { get; } = new();

    // -----------------------------------------------------------------------
    // Drop-quiesce: atomic ref-count + drain
    //
    // MarkDropped() — called by Drop before disposing the node:
    //   Sets the dropped flag; if nobody holds a ref the drain TCS is completed
    //   immediately so Drop can proceed without waiting.
    //
    // AddRef() / Release() — called at every database-operation entry point:
    //   AddRef increments the count (or throws DatabaseDoesntExist if the
    //   database is already being dropped).  Release decrements; when the last
    //   ref is released after a drop, the TCS is completed and Drop unblocks.
    //   The dropped flag is re-checked AFTER the increment: MarkDropped can set
    //   it and observe useCount == 0 in the window between AddRef's pre-check and
    //   its CAS, which would let Drop drain and dispose while AddRef still acquired
    //   a ref. Re-checking post-increment (and backing the ref out if dropped) closes
    //   that race so a successful AddRef can never coexist with a completed drain.
    //
    // WhenDrainedAsync() — awaited by Drop after MarkDropped:
    //   Returns when all in-flight AddRef holders have called Release.
    // -----------------------------------------------------------------------
    private int _useCount;
    private volatile int _dropped;
    private readonly TaskCompletionSource _drainedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public bool IsDropped => _dropped != 0;

    /// <summary>
    /// True while at least one caller holds a use-reference. Read by idle eviction, which must never
    /// dispose a descriptor somebody is working through.
    /// </summary>
    internal bool HasLiveUses => Volatile.Read(ref _useCount) > 0;

    // Monotonic tick of the last time this descriptor was resolved or used. Environment.TickCount64
    // is deliberate: this measures how long a local object has sat unused, which is a local wall-clock
    // question and involves no cross-node ordering — an HLC timestamp would say nothing more here.
    private long _lastUsedTicks = Environment.TickCount64;

    /// <summary>
    /// Milliseconds since this descriptor was last resolved or used.
    ///
    /// <para>This is the value idle eviction is safe by. A caller that resolves a descriptor stamps it
    /// <em>before</em> it can acquire a use-reference, so the gap between "resolved" and "referenced" —
    /// a few instructions with no await in it — is always covered by a fresh stamp. An eviction that
    /// insists on minutes of idleness therefore cannot be racing a caller in that gap: if one were
    /// there, this value would be near zero.</para>
    /// </summary>
    internal long IdleMilliseconds => Environment.TickCount64 - Volatile.Read(ref _lastUsedTicks);

    /// <summary>
    /// Marks the descriptor as used right now. Called when it is resolved and whenever a use-reference
    /// is taken or released, so a long-running operation leaves it looking fresh at both ends.
    /// </summary>
    internal void Touch() => Volatile.Write(ref _lastUsedTicks, Environment.TickCount64);

    internal void MarkDropped()
    {
        Interlocked.Exchange(ref _dropped, 1);
        if (Volatile.Read(ref _useCount) == 0)
            _drainedTcs.TrySetResult();
    }

    internal void AddRef()
    {
        int current;
        do {
            current = Volatile.Read(ref _useCount);
            if (_dropped != 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseDoesntExist,
                    $"Database '{Name}' is being dropped");
        } while (Interlocked.CompareExchange(ref _useCount, current + 1, current) != current);

        // Re-check after the increment. If MarkDropped set _dropped (and possibly already
        // observed useCount == 0 and completed the drain) between the pre-check above and the
        // CAS, our ref must not stand — Drop may already be disposing the node. Back the ref
        // out, completing the drain if we were the last holder, and refuse the use.
        if (_dropped != 0)
        {
            if (Interlocked.Decrement(ref _useCount) == 0)
                _drainedTcs.TrySetResult();
            throw new CamusDBException(
                CamusDBErrorCodes.DatabaseDoesntExist,
                $"Database '{Name}' is being dropped");
        }

        Touch();
    }

    internal void Release()
    {
        // Stamped on the way out as well as in: an operation that ran for an hour leaves the
        // descriptor freshly used, not idle since the moment it started.
        Touch();

        if (Interlocked.Decrement(ref _useCount) == 0 && _dropped != 0)
            _drainedTcs.TrySetResult();
    }

    internal Task WhenDrainedAsync() => _drainedTcs.Task;

    /// <summary>
    /// Acquires a use-reference and returns an <see cref="IDisposable"/> handle
    /// that releases it on <see cref="IDisposable.Dispose"/>.  Use with <c>using</c>
    /// in every operation entry point that accesses the database.
    /// Throws <see cref="CamusDBException"/> (<c>DatabaseDoesntExist</c>) if the
    /// database is already being dropped.
    /// </summary>
    internal DatabaseUseHandle Use()
    {
        AddRef();
        return new DatabaseUseHandle(this);
    }

    private IDisposable? schemaReplicationSubscription;

    public DatabaseDescriptor(
        string id,
        string name,
        EmbeddedKahuna kahuna,
        KvTransactionsManager transactions,
        ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors,
        CamusDBOptions options,
        IReadOnlyList<DatabaseBranchAncestor>? ancestors = null
    )
    {
        Options = options;
        Id = id;
        idUtf8 = Encoding.UTF8.GetBytes(id);
        _name = name;
        Kahuna = kahuna;
        Transactions = transactions;
        TableDescriptors = tableDescriptors;
        Ancestors = ancestors ?? [];
    }

    public void SetSchemaReplicationSubscription(IDisposable subscription)
    {
        ArgumentNullException.ThrowIfNull(subscription);

        IDisposable? previous = Interlocked.Exchange(ref schemaReplicationSubscription, subscription);
        previous?.Dispose();
    }

    public void Dispose()
    {
        IDisposable? subscription = Interlocked.Exchange(ref schemaReplicationSubscription, null);
        subscription?.Dispose();

        // Cancel any outstanding range-lock heartbeat loops. Left running, each loop roots the
        // transactions manager and the Kahuna node it references, leaking the whole node.
        Transactions?.Dispose();

        Schema?.Dispose();
        SchemaDdlSemaphore?.Dispose();
        SystemSchemaSemaphore?.Dispose();
    }
}
