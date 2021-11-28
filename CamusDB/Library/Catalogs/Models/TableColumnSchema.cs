
namespace CamusDB.Library.Catalogs.Models;

public sealed class TableColumnSchema
{
    public string Name { get; }

    public ColumnType Type { get; }

    public bool Primary { get; }

    public bool NotNull { get; }

    public TableColumnSchema(string name, ColumnType type, bool primary, bool notNull)
    {
        Name = name;
        Type = type;
        Primary = primary;
        NotNull = notNull;
    }
}

