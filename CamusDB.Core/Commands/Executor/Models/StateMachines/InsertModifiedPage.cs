
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

public readonly struct InsertModifiedPage
{
    public ObjectIdValue Offset { get; }

    public uint Sequence { get; }

    public byte[] Buffer { get; }

    public InsertModifiedPage(ObjectIdValue offset, uint sequence, byte[] buffer)
    {
        Offset = offset;
        Sequence = sequence;
        Buffer = buffer;
    }
}
