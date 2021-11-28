using System;

namespace CamusDB.Library.CommandsExecutor.Models;

public class SystemSchema
{
    public Dictionary<string, DatabaseObject> Objects { get; set; } = new();

    public SemaphoreSlim Semaphore = new(1, 1);
}
