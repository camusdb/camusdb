
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class Schema
{
    public Dictionary<string, TableSchema> Tables { get; set; } = new();

    public SemaphoreSlim Semaphore = new(1, 1);
}

