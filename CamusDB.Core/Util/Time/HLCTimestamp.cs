
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Util.Time;

/// <summary>
/// Represents a unique point in time given by the Hybrid Logical Clock (HLC)
/// </summary>
public readonly struct HLCTimestamp : IComparable<HLCTimestamp>
{
    public long L { get; }

    public uint C { get; }

    public HLCTimestamp(long l, uint c)
    {
        L = l;
        C = c;
    }

    public int CompareTo(HLCTimestamp other)
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

    public bool IsNull()
    {
        return L == 0 && C == 0;
    }

    public override string ToString()
    {
        return string.Format("HLC({0}:{1})", L, C);
    }
}
