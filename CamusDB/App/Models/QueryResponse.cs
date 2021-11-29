
using CamusDB.Library.CommandsExecutor.Models;

namespace CamusDB.App.Models;

public sealed class QueryResponse
{
    public string Status { get; set; }

    public List<List<ColumnValue>> Rows { get; set; }

    public QueryResponse(string status, List<List<ColumnValue>> rows)
    {
        Status = status;
        Rows = rows;
    }
}
