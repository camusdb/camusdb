
namespace CamusDB.Core.CommandsExecutor.Models;

public readonly struct QueryOrderBy
{
    public string ColumnName { get; }

    public QueryOrderByType Type { get; }

    public QueryOrderBy(string columnName, QueryOrderByType type)
	{
        ColumnName = columnName;
        Type = type;
	}
}

