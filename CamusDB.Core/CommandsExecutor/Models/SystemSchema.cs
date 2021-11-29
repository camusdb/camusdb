
namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class SystemSchema
{
    public Dictionary<string, DatabaseObject> Objects { get; set; } = new();

    public SemaphoreSlim Semaphore = new(1, 1);
}
