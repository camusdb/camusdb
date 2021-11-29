
namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public sealed class QueryByIdTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public int Id { get; }

    public QueryByIdTicket(string database, string name, int id)
    {
        DatabaseName = database;
        TableName = name;
        Id = id;
    }
}
