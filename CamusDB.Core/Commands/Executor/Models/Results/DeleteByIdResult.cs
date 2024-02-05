
namespace CamusDB.Core.CommandsExecutor.Models.Results;

public readonly struct DeleteByIdResult
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public int DeletedRows { get; }

    public DeleteByIdResult(DatabaseDescriptor database, TableDescriptor table, int deletedRows)
    {
        Database = database;
        Table = table;
        DeletedRows = deletedRows;
    }
}
