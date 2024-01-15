
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using CamusDB.Core.BufferPool;
using CamusDB.Core.GC;
using CamusDB.Core.Storage;
using System.Collections.Concurrent;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed record DatabaseDescriptor : IDisposable
{
    public string Name { get; }

    public StorageManager Storage { get; }

    public BufferPoolManager BufferPool { get; }

    public GCManager GC { get; }

    public SemaphoreSlim SystemSchemaSemaphore { get; } = new(1, 1);

    public Schema Schema { get; } = new();

    public SystemSchema SystemSchema { get; set; } = new();    

    public ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> TableDescriptors { get; }

    public DatabaseDescriptor(
        string name,
        StorageManager storage,
        BufferPoolManager bufferPool,
        GCManager gc,
        ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors
    )
    {
        Name = name;
        BufferPool = bufferPool;
        Storage = storage;
        GC = gc;
        TableDescriptors = tableDescriptors;
    }

    public void Dispose()
    {
        Storage?.Dispose();
        Schema?.Dispose();
        SystemSchemaSemaphore?.Dispose();
        GC?.Dispose();
    }
}
