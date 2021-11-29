

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class DatabaseObject
{
    public DatabaseObjectType Type { get; set; }

    public string? Name { get; set; }

    public int StartOffset { get; set; }

    public Dictionary<string, int>? Indexes { get; set; }
}
