
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class DatabaseDescriptor
{
    public string Name { get; set; }

    public BufferPoolHandler TableSpace { get; set; }

    public BufferPoolHandler SchemaSpace { get; set; }

    public BufferPoolHandler SystemSpace { get; set; }

    public Schema Schema { get; set; } = new();

    public JournalWriter JournalWriter { get; set; }

    public SystemSchema SystemSchema { get; set; } = new();

    public SemaphoreSlim DescriptorsSemaphore = new(1, 1);

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
        JournalWriter = new(this);
    }
}
