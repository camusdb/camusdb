
using CamusDB.Library.Util.Trees;
using CamusDB.Library.Catalogs.Models;

namespace CamusDB.Library.CommandsExecutor.Models;

public class TableDescriptor
{
    public string? Name { get; set; }

    public BTree Rows { get; set; } = new();

    public TableSchema? Schema { get; set; }
}
