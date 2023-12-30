
namespace CamusDB.Core.Util.Time;

/// <summary>
/// A message class to hold the message's timestamp
/// </summary>
public readonly struct HLClockMessage
{
    public long L { get; }

    public int C { get; }

    public HLClockMessage(long l, int c)
    {
        L = l;
        C = c;
    }
}
