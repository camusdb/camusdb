
namespace CamusDB.Core.CommandsExecutor.Models.Results;

public readonly struct InsertResult
{
    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public int InsertedRows { get; }

    public InsertResult(DatabaseDescriptor database, TableDescriptor table, int insertedRows)
    {
        Database = database;
        Table = table;
        InsertedRows = insertedRows;
    }
}
