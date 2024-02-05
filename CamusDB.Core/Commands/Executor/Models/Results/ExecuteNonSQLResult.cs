
namespace CamusDB.Core.CommandsExecutor.Models.Results;

public readonly struct ExecuteNonSQLResult
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public int ModifiedRows { get; }

    public ExecuteNonSQLResult(DatabaseDescriptor database, TableDescriptor table, int modifiedRows)
    {
        Database = database;
        Table = table;
        ModifiedRows = modifiedRows;
    }
}
