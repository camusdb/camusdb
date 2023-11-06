
namespace CamusDB.Core.BufferPool.Models;

public enum NextRowIdOffsetUpdateType
{
    NextId = 0,
    NextOffset = 1
}

public readonly struct NextRowIdOffsetUpdate
{
    public NextRowIdOffsetUpdateType Type { get; }

    public int Value { get; }

    public NextRowIdOffsetUpdate(NextRowIdOffsetUpdateType type, int value)
    {
        Type = type;
        Value = value;
    }
}
