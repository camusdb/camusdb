
namespace CamusDB.Core.BufferPool.Models;

public readonly struct PageToWrite
{
    public int Offset { get; }    

    public byte[] Buffer { get; }

    public PageToWrite(int offset, byte[] buffer)
    {
        Offset = offset;
        Buffer = buffer;
    }
}

