
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using System.Collections.Concurrent;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed record DatabaseDescriptor : IDisposable
{
    public string Name { get; }

    public EmbeddedKahuna Kahuna { get; }

    /// <summary>
    /// True when this descriptor created and owns its Kahuna node (standalone mode).
    /// False when using a process-level cluster node shared across databases.
    /// </summary>
    public bool OwnsKahuna { get; }

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
        await Kahuna.StepDownSchemaPartitionAsync(Name, CancellationToken.None).ConfigureAwait(false);
    }

    public Schema Schema { get; } = new();

    public SystemSchema SystemSchema { get; set; } = new();

    public ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> TableDescriptors { get; }

    private IDisposable? schemaReplicationSubscription;

    public DatabaseDescriptor(
        string name,
        EmbeddedKahuna kahuna,
        KvTransactionsManager transactions,
        ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors,
        bool ownsKahuna = true
    )
    {
        Name = name;
        Kahuna = kahuna;
        OwnsKahuna = ownsKahuna;
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

        Schema?.Dispose();
        SchemaDdlSemaphore?.Dispose();
        SystemSchemaSemaphore?.Dispose();
    }
}
