/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Numerics;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// An exact running sum of Float64 values from which a value can be removed again. Each value enters
/// as the binary fraction it is, into a fixed-point accumulator wide enough for the whole double
/// range, and <see cref="ToDouble"/> rounds the exact total once, to the nearest double. The result
/// is therefore the correctly rounded sum of the values the accumulator holds, whatever the order of
/// the adds and removes. A running <see langword="double"/> cannot remove a value: its total carries
/// the rounding of every earlier addition, so a subtraction would leave that error behind. This is
/// what lets a sliding window frame over Float64 values cost O(1) per row instead of a re-add of the
/// whole frame.
///
/// <para><b>Layout.</b> The total is a two's-complement integer in units of 2^-1074 (the smallest
/// subnormal), kept as 32-bit chunks in <see langword="long"/> limbs: limb <c>i</c> covers bits
/// <c>[32i, 32i + 32)</c>. A limb also holds its pending carries, so an add or a remove changes at
/// most three limbs by less than 2^32 each and propagates nothing; after 2^30 operations every limb
/// still fits a long, and the carries are then pushed up in one pass. The top bit of the largest
/// double sits at offset 2097, so 70 limbs hold 2^31 such values with room to spare.</para>
///
/// <para><b>Non-finite values</b> are counted apart, with the IEEE result of a sum: any NaN gives NaN,
/// both infinities give NaN, one infinity gives that infinity.</para>
///
/// <para>Not thread-safe: one instance serves one frame computation.</para>
/// </summary>
internal sealed class ExactDoubleSum
{
    private const int LimbBits = 32;
    private const int LimbCount = 70;
    private const ulong ChunkMask = (1UL << LimbBits) - 1;

    /// <summary>An operation moves a limb by less than 2^32, so 2^30 of them stay below 2^62.</summary>
    private const int DefaultOperationsPerNormalize = 1 << 30;

    private readonly long[] limbs = new long[LimbCount];
    private readonly int operationsPerNormalize;

    /// <summary>The lowest limb an operation touched, so the conversion skips the zero limbs below it.</summary>
    private int lowestLimb = LimbCount;

    private int operations;
    private long nanCount;
    private long positiveInfinityCount;
    private long negativeInfinityCount;

    public ExactDoubleSum() : this(DefaultOperationsPerNormalize)
    {
    }

    /// <summary>
    /// Test-only: a small <paramref name="operationsPerNormalize"/> makes the carry pass run every few
    /// operations, so a short test covers it. Production code uses the parameterless constructor.
    /// </summary>
    internal ExactDoubleSum(int operationsPerNormalize) => this.operationsPerNormalize = operationsPerNormalize;

    public void Add(double value) => Accumulate(value, 1);

    /// <summary>Removes a value added before. Removing a value that was never added makes the total meaningless.</summary>
    public void Remove(double value) => Accumulate(value, -1);

    private void Accumulate(double value, int sign)
    {
        if (!double.IsFinite(value))
        {
            if (double.IsNaN(value))
                nanCount += sign;
            else if (value > 0)
                positiveInfinityCount += sign;
            else
                negativeInfinityCount += sign;

            return;
        }

        long bits = BitConverter.DoubleToInt64Bits(value);
        int biasedExponent = (int)((bits >> 52) & 0x7FF);
        ulong mantissa = (ulong)bits & ((1UL << 52) - 1);

        if (biasedExponent == 0)
        {
            // A subnormal, or a zero, which adds nothing.
            if (mantissa == 0)
                return;
        }
        else
        {
            mantissa |= 1UL << 52;
        }

        if (bits < 0)
            sign = -sign;

        // The value is mantissa × 2^(biasedExponent - 1075), or mantissa × 2^-1074 for a subnormal:
        // in units of 2^-1074 the mantissa starts at this bit offset.
        int offset = biasedExponent == 0 ? 0 : biasedExponent - 1;
        int limb = offset >> 5;
        int shift = offset & (LimbBits - 1);

        // At most 53 + 31 bits: three chunks.
        UInt128 chunks = (UInt128)mantissa << shift;

        limbs[limb] += sign * (long)(ulong)(chunks & ChunkMask);
        limbs[limb + 1] += sign * (long)(ulong)((chunks >> LimbBits) & ChunkMask);
        limbs[limb + 2] += sign * (long)(ulong)(chunks >> (2 * LimbBits));

        if (limb < lowestLimb)
            lowestLimb = limb;

        if (++operations >= operationsPerNormalize)
        {
            Normalize(limbs, lowestLimb);
            operations = 0;
        }
    }

    /// <summary>
    /// Pushes every pending carry up, so each limb but the top holds a chunk in [0, 2^32) and the top
    /// limb holds the rest, with the sign of the total.
    /// </summary>
    private static void Normalize(Span<long> target, int from)
    {
        long carry = 0;
        int top = target.Length - 1;

        for (int i = from; i < top; i++)
        {
            long value = target[i] + carry;
            carry = value >> LimbBits;
            target[i] = value - (carry << LimbBits);
        }

        target[top] += carry;
    }

    /// <summary>The total, rounded once to the nearest double (ties to even); ±infinity past the range.</summary>
    public double ToDouble()
    {
        if (nanCount != 0 || (positiveInfinityCount != 0 && negativeInfinityCount != 0))
            return double.NaN;

        if (positiveInfinityCount != 0)
            return double.PositiveInfinity;

        if (negativeInfinityCount != 0)
            return double.NegativeInfinity;

        if (lowestLimb == LimbCount)
            return 0.0;

        // On a copy, so the next add or remove continues from the exact state.
        Span<long> total = stackalloc long[LimbCount];
        total.Clear();
        limbs.AsSpan(lowestLimb).CopyTo(total[lowestLimb..]);
        Normalize(total, lowestLimb);

        bool negative = total[LimbCount - 1] < 0;

        if (negative)
        {
            for (int i = lowestLimb; i < LimbCount; i++)
                total[i] = -total[i];

            Normalize(total, lowestLimb);
        }

        int high = LimbCount - 1;

        while (high >= lowestLimb && total[high] == 0)
            high--;

        if (high < lowestLimb)
            return 0.0;

        int topBits = 64 - BitOperations.LeadingZeroCount((ulong)total[high]);
        int highestBit = high * LimbBits + topBits - 1;
        int exponent = highestBit - 1074;

        if (exponent > 1023)
            return negative ? double.NegativeInfinity : double.PositiveInfinity;

        if (exponent < -1022)
        {
            // Every bit of the total lies below 2^-1022, so the total is a count of 2^-1074 units
            // below 2^52, which a subnormal double stores exactly: nothing to round.
            ulong units = 0;

            for (int i = high; i >= 0; i--)
                units = (units << LimbBits) | (ulong)total[i];

            long resultBits = (long)units;
            return BitConverter.Int64BitsToDouble(negative ? resultBits | long.MinValue : resultBits);
        }

        // The 53 significant bits start at the highest bit. A 128-bit window that ends with limb
        // `high` holds them, the guard bit below them and part of the sticky bits; the limbs below the
        // window hold the rest of the sticky bits.
        UInt128 window = 0;

        for (int i = high; i > high - 4; i--)
            window = (window << LimbBits) | (i >= 0 ? (ulong)total[i] : 0UL);

        int windowTopBit = 3 * LimbBits + topBits - 1;
        int mantissaShift = windowTopBit - 52;
        ulong mantissa = (ulong)(window >> mantissaShift);
        bool guard = ((window >> (mantissaShift - 1)) & 1) != 0;
        bool sticky = (window & ((UInt128.One << (mantissaShift - 1)) - 1)) != 0;

        for (int i = high - 4; i >= 0 && !sticky; i--)
            sticky = total[i] != 0;

        if (guard && (sticky || (mantissa & 1) != 0))
        {
            mantissa++;

            if (mantissa == 1UL << 53)
            {
                mantissa >>= 1;
                exponent++;

                if (exponent > 1023)
                    return negative ? double.NegativeInfinity : double.PositiveInfinity;
            }
        }

        double magnitude = Math.ScaleB((double)mantissa, exponent - 52);
        return negative ? -magnitude : magnitude;
    }
}
