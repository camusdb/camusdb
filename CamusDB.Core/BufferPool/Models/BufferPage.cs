
using CamusDB.Core.Support;

namespace CamusDB.Core.BufferPool.Models;

public sealed class BufferPage
{
    public int Offset { get; }

    public byte[] Buffer { get; }    

    public DateTime LastAccessTime { get; set; }

    public SemaphoreSlim Semaphore { get; } = new(1, 1);

    public BufferPage(int offset, byte[] buffer)
    {
        Offset = offset;
        Buffer = buffer;
    }
}
