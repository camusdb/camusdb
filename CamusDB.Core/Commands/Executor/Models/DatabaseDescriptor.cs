
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using System.Collections.Concurrent;

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

    public string Name { get; }

    public EmbeddedKahuna Kahuna { get; }

    public KvTransactionsManager Transactions { get; }

    public SemaphoreSlim SchemaDdlSemaphore { get; } = new(1, 1);

    public SemaphoreSlim SystemSchemaSemaphore { get; } = new(1, 1);

    // F1a: set when persist-checkpoint exhausts all retries after a committed DDL.
    // Gates further DDL proposals on this node until the node recovers (F1b restart replay).
    private volatile int _schemaSubsystemDegraded;

    public bool SchemaSubsystemDegraded => _schemaSubsystemDegraded != 0;

    public void MarkSchemaSubsystemDegraded()
        => Interlocked.Exchange(ref _schemaSubsystemDegraded, 1);

    public void ClearSchemaSubsystemDegraded()
        => Interlocked.Exchange(ref _schemaSubsystemDegraded, 0);

    // F1a: step-down is deferred until the in-flight DDL transaction's CommitAsync completes,
    // so the KV commit succeeds before leadership changes (important when schema and KV share a
    // single Raft partition, as in single-partition test clusters).
    private volatile int _deferredSchemaStepDown;

    public bool DeferredSchemaStepDown => _deferredSchemaStepDown != 0;

    public void RequestDeferredSchemaStepDown()
        => Interlocked.Exchange(ref _deferredSchemaStepDown, 1);

    public void ClearDeferredSchemaStepDown()
        => Interlocked.Exchange(ref _deferredSchemaStepDown, 0);

    /// <summary>
    /// If a deferred step-down was requested (F1a persist exhaustion), clears the flag and
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

    public Schema Schema { get; } = new();

    public SystemSchema SystemSchema { get; set; } = new();

    public ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> TableDescriptors { get; }

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
    }

    internal void Release()
    {
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
        ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors
    )
    {
        Id = id;
        Name = name;
        Kahuna = kahuna;
        Transactions = transactions;
        TableDescriptors = tableDescriptors;
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
