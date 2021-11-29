
namespace CamusDB.Library.CommandsExecutor.Models.Tickets;

public sealed class QueryTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public QueryTicket(string database, string name)
    {
        DatabaseName = database;
        TableName = name;
    }
}
