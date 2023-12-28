using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.BufferPool.Models;

public readonly struct BufferPageOperation
{
    public BufferPageOperationType Operation { get; }

    public ObjectIdValue Offset { get; }

    public uint Sequence { get; }

    public byte[] Buffer { get; }

    public BufferPageOperation(BufferPageOperationType operation, ObjectIdValue offset, uint sequence, byte[] buffer)
    {
        Operation = operation;
        Offset = offset;
        Sequence = sequence;
        Buffer = buffer;
    }
}
