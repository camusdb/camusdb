
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.BufferPool.Controllers;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class DatabaseDescriptor
{
    public string Name { get; }

    public BufferPoolHandler TableSpace { get; }

    public BufferPoolHandler SchemaSpace { get; }

    public BufferPoolHandler SystemSpace { get; }

    public BufferPoolFlusher TableSpaceFlusher { get; }

    public Schema Schema { get; } = new();

    public JournalManager Journal { get; }

    public SystemSchema SystemSchema { get; } = new();

    public SemaphoreSlim DescriptorsSemaphore { get; } = new(1, 1);

    public Dictionary<string, TableDescriptor> TableDescriptors = new();    

    public DatabaseDescriptor(
        string name,
        BufferPoolHandler tableSpace,
        BufferPoolHandler schemaSpace,
        BufferPoolHandler systemSpace)
    {
        Name = name;
        TableSpace = tableSpace;
        SchemaSpace = schemaSpace;
        SystemSpace = systemSpace;
        Journal = new(name);
        TableSpaceFlusher = new(TableSpace, Journal);
    }
}
