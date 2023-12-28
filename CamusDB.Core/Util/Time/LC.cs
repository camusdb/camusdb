
namespace CamusDB.Core.Util.Time;

/// <summary>
/// Logical clock
/// </summary>
public sealed class LC
{
    private ulong ticks; // logical clock

    public ulong Increment()
    {
        ticks++;
        return ticks;
    }

    public ulong GetTicks()
    {
        return ticks;
    }
}
