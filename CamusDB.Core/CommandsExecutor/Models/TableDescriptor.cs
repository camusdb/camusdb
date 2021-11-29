
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class TableDescriptor
{
    public string? Name { get; set; }

    public TableSchema? Schema { get; set; }

    public BTree Rows { get; set; } = new(-1);

    public Dictionary<string, BTree> Indexes { get; set; } = new();

    //public SemaphoreSlim WriteLock { get; } = new(1, 1);
}
