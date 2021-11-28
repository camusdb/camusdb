

namespace CamusDB.Library.CommandsExecutor.Models;

public class DatabaseObject
{
    public DatabaseObjectType Type { get; set; }

    public string? Name { get; set; }

    public int StartOffset { get; set; }
}
