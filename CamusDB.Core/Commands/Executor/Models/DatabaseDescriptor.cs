
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
