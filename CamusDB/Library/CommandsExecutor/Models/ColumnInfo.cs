
using CamusDB.Library.Catalogs.Models;

namespace CamusDB.Library.CommandsExecutor.Models;

public sealed class ColumnInfo
{
    public string Name { get; }

    public ColumnType Type { get; }

    public bool Primary { get; }

    public bool NotNull { get; }

    public ColumnInfo(string name, ColumnType type, bool primary = false, bool notNull = false)
    {
        Name = name;
        Type = type;
        Primary = primary;
        NotNull = notNull;
    }
}
