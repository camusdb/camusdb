
using CamusDB.Core.Util.Time;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct InsertBatchTicket
{
    public HLCTimestamp TxnId { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public List<Dictionary<string, ColumnValue>> Values { get; }

    public InsertBatchTicket(
        HLCTimestamp txnId,
        string databaseName,
        string tableName,
        List<Dictionary<string, ColumnValue>> values)
    {
        TxnId = txnId;
        DatabaseName = databaseName;
        TableName = tableName;
        Values = values;
    }
}

