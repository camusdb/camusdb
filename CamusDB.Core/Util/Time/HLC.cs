
using System;

namespace CamusDB.Core.Util.Time;

public class HybridLogicalClock
{
    private long l; // logical clock

    private int c;  // counter

    public HybridLogicalClock()
    {
        this.l = 0;
        this.c = 0;
    }

    // Call this method when a send or local event occurs
    public Timestamp SendOrLocalEvent()
    {
        long lPrime = l;

        l = Math.Max(l, GetPhysicalTime());

        if (l == lPrime)
            c += 1;
        else
            c = 0;        

        return new Timestamp(l, c);
    }

    // Call this method when a receive event occurs
    // `m` represents the message received, which should contain its own timestamp
    public Timestamp ReceiveEvent(Message m)
    {
        long lPrime = l;

        l = Math.Max(l, Math.Max(m.L, GetPhysicalTime()));

        if (l == lPrime && l == m.L)
            c = Math.Max(c, m.C) + 1;
        else if (l == lPrime)
            c += 1;
        else if (l == m.L)
            c = m.C + 1;
        else
            c = 0;

        return new Timestamp(l, c);
    }

    private long GetPhysicalTime()
    {
        return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}

// A message class to hold the message's timestamp
public sealed class Message
{
    public long L { get; }

    public int C { get; }

    public Message(long l, int c)
    {
        L = l;
        C = c;
    }
}

public readonly struct Timestamp : IComparable<Timestamp>
{
    public long L { get; }

    public int C { get; }

    public Timestamp(long l, int c)
    {
        L = l;
        C = c;
    }

    public int CompareTo(Timestamp other)
    {
        if (L == other.L)
        {
            if (C == other.C)
                return 0;

            if (C < other.C)
                return -1;

            if (C > other.C)
                return 1;
        }

        if (L < other.L)
            return -1;

        return 1;
    }

    public override string ToString()
    {
        return string.Format("{0}:{1}", L, C);
    }
}
