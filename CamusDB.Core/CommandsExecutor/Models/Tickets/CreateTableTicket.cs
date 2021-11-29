
namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public sealed class CreateTableTicket
{
    public string Database { get; }

    public string Name { get; }

    public ColumnInfo[] Columns { get; }

    public CreateTableTicket(string database, string name, ColumnInfo[] columns)
    {
        Database = database;
        Name = name;
        Columns = columns;
    }
}

