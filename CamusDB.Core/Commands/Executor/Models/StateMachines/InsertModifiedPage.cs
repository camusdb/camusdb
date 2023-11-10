
namespace CamusDB.Core.CommandsExecutor.Models.StateMachines;

public readonly struct InsertModifiedPage
{
    public int Offset { get; }

    public uint Sequence { get; }

    public byte[] Buffer { get; }

    public InsertModifiedPage(int offset, uint sequence, byte[] buffer)
    {
        Offset = offset;
        Sequence = sequence;
        Buffer = buffer;
    }
}
