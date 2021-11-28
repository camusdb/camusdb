
using System;
using CamusDB.Library.Support;

namespace CamusDB.Library.BufferPool.Models;

public class MemoryPage
{
    public int Offset { get; }

    public byte[] Buffer { get; }

    public Guard Initialized { get; } = new();

    public DateTime LastAccessTime { get; set; }

    //public SemaphoreSlim Semaphore { get; } = new(1, 1);

    public MemoryPage(int offset, byte[] buffer)
    {
        Offset = offset;
        Buffer = buffer;
    }
}
