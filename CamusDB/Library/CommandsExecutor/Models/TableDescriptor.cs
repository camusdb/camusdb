
using CamusDB.Library.Util;

namespace CamusDB.Library.CommandsExecutor.Models;

public class TableDescriptor
{
    public string? Name { get; set; }

    public BTree Rows { get; set; } = new();
}
