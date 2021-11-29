
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class DatabaseDescriptor
{
    public string? Name { get; set; }

    public BufferPoolHandler? TableSpace { get; set; }

    public BufferPoolHandler? SchemaSpace { get; set; }

    public BufferPoolHandler? SystemSpace { get; set; }

    public Schema Schema { get; set; } = new();

    public SystemSchema SystemSchema { get; set; } = new();

    public SemaphoreSlim DescriptorsSemaphore = new(1, 1);

    public Dictionary<string, TableDescriptor> TableDescriptors = new();
}
