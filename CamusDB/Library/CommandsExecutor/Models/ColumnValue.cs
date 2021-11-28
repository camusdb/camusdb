
using CamusDB.Library.Catalogs.Models;

namespace CamusDB.Library.CommandsExecutor.Models;

public sealed class ColumnValue
{
    public ColumnType Type { get; }

    public string Value { get; }

    public ColumnValue(ColumnType type, string value)
    {
        Type = type;
        Value = value;
    }
}
