
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Models;

public class InsertRequest
{
    public string? DatabaseName { get; set; }

    public string? TableName { get; set; }

    public string[]? Columns { get; set; }

    public ColumnValue[]? Values { get; set; }
}


