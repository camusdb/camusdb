
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using RocksDbSharp;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed record DatabaseDescriptor : IDisposable
{
    public string Name { get; }

    public RocksDb DbHandler { get; }

    public BufferPoolHandler TableSpace { get; }

    public Schema Schema { get; } = new();

    public SystemSchema SystemSchema { get; } = new();

    public SemaphoreSlim DescriptorsSemaphore { get; } = new(1, 1);

    public Dictionary<string, TableDescriptor> TableDescriptors { get; } = new();

    public DatabaseDescriptor(
        string name,
        RocksDb dbHandler,
        BufferPoolHandler tableSpace
    )
    {
        Name = name;
        TableSpace = tableSpace;        
        DbHandler = dbHandler;
    }

    public void Dispose()
    {
        Schema?.Dispose();
        SystemSchema?.Dispose();
        DescriptorsSemaphore?.Dispose();
    }
}
