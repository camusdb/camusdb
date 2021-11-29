
namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public sealed class InsertTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public string[] Columns { get; }

    public ColumnValue[] Values { get; }

    public InsertTicket(string database, string name, string[] columns, ColumnValue[] values)
    {
        DatabaseName = database;
        TableName = name;
        Columns = columns;
        Values = values;
    }
}

