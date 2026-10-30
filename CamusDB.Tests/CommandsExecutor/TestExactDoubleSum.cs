/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Numerics;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor.Controllers.Queries;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The correctly rounded sum of doubles, computed by a different route than the engine's: the exact
/// total as a <see cref="BigInteger"/> in units of 2^-1074, written as an exact decimal string and
/// parsed back, which the runtime rounds correctly for a string of any length.
/// </summary>
internal static class ExactSumOracle
{
    private static readonly BigInteger FivePow1074 = BigInteger.Pow(5, 1074);

    public static double Sum(IEnumerable<double> values)
    {
        BigInteger total = BigInteger.Zero;

        foreach (double value in values)
        {
            long bits = BitConverter.DoubleToInt64Bits(value);
            int biasedExponent = (int)((bits >> 52) & 0x7FF);
            BigInteger mantissa = bits & ((1L << 52) - 1);

            if (biasedExponent != 0)
                mantissa = (mantissa | (1L << 52)) << (biasedExponent - 1);

            total += bits < 0 ? -mantissa : mantissa;
        }

        // total / 2^1074 = total × 5^1074 / 10^1074: an exact decimal with 1074 fraction digits.
        BigInteger scaled = total * FivePow1074;
        string digits = BigInteger.Abs(scaled).ToString(CultureInfo.InvariantCulture).PadLeft(1075, '0');
        string text = (scaled.Sign < 0 ? "-" : "") + digits[..^1074] + "." + digits[^1074..];

        return double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
    }
}

/// <summary>
/// <see cref="ExactDoubleSum"/> against <see cref="ExactSumOracle"/> and against the IEEE rules it
/// must follow: one rounding of the exact total, ties to even, exact removal, subnormal and infinite
/// results, and the non-finite inputs.
/// </summary>
[TestFixture]
public sealed class TestExactDoubleSum
{
    private static double SumOf(params double[] values)
    {
        ExactDoubleSum sum = new();

        foreach (double value in values)
            sum.Add(value);

        return sum.ToDouble();
    }

    [Test]
    public void Empty_IsPositiveZero()
    {
        Assert.AreEqual(0L, BitConverter.DoubleToInt64Bits(new ExactDoubleSum().ToDouble()));
        Assert.AreEqual(0L, BitConverter.DoubleToInt64Bits(SumOf(0.0, -0.0, 0.0)));
    }

    [Test]
    public void RoundsTheExactTotalOnce()
    {
        // In row order a double adds 0.1 + 0.2 = 0.30000000000000004, then + 0.3 = 0.6000000000000001.
        // The exact total of the three doubles rounds to 0.6.
        Assert.AreEqual(0.6, SumOf(0.1, 0.2, 0.3));
        Assert.AreEqual(ExactSumOracle.Sum([0.1, 0.2, 0.3]), SumOf(0.1, 0.2, 0.3));
        Assert.AreNotEqual(0.1 + 0.2 + 0.3, SumOf(0.1, 0.2, 0.3), "the row-order sum carries a rounding the exact sum does not");

        Assert.AreEqual(ExactSumOracle.Sum([-0.1, -0.2]), SumOf(-0.1, -0.2));
        Assert.AreEqual(1.0, SumOf(1e16, 1.0, -1e16), "a double sum would give 0 or 2 here");
    }

    [Test]
    public void RemovalIsExact()
    {
        ExactDoubleSum sum = new();
        sum.Add(1e300);
        sum.Add(1e-300);
        sum.Remove(1e300);
        Assert.AreEqual(1e-300, sum.ToDouble(), "the small value survives the large one exactly");

        for (int i = 0; i < 10; i++)
            sum.Add(0.1);
        for (int i = 0; i < 10; i++)
            sum.Remove(0.1);
        Assert.AreEqual(1e-300, sum.ToDouble());

        sum.Remove(1e-300);
        Assert.AreEqual(0L, BitConverter.DoubleToInt64Bits(sum.ToDouble()), "back to exactly zero");

        sum.Add(double.MaxValue);
        sum.Add(double.MaxValue);
        Assert.AreEqual(double.PositiveInfinity, sum.ToDouble());
        sum.Remove(double.MaxValue);
        Assert.AreEqual(double.MaxValue, sum.ToDouble(), "the total past the double range is still exact");
    }

    [Test]
    public void TiesRoundToEven()
    {
        double halfUlp = Math.ScaleB(1.0, -53);
        double ulp = Math.ScaleB(1.0, -52);

        Assert.AreEqual(1.0, SumOf(1.0, halfUlp), "a tie rounds to the even mantissa, down");
        Assert.AreEqual(1.0 + 2 * ulp, SumOf(1.0, ulp, halfUlp), "a tie rounds to the even mantissa, up");
        Assert.AreEqual(1.0 + ulp, SumOf(1.0, halfUlp, Math.ScaleB(1.0, -100)), "a sticky bit breaks the tie upward");

        // Below 1.0 the doubles are 2^-53 apart: 1 - 2^-54 is a tie between 1 - 2^-53 (odd) and 1.0
        // (even), and a sticky bit below the tie rounds down to the odd neighbour.
        double quarterUlp = Math.ScaleB(1.0, -54);
        Assert.AreEqual(1.0, SumOf(1.0, -quarterUlp), "a tie from below rounds to the even mantissa");
        Assert.AreEqual(1.0 - halfUlp, SumOf(1.0, -quarterUlp, -Math.ScaleB(1.0, -100)), "a sticky bit breaks the tie downward");
        Assert.AreEqual(ExactSumOracle.Sum([1.0, -quarterUlp, -Math.ScaleB(1.0, -100)]), SumOf(1.0, -quarterUlp, -Math.ScaleB(1.0, -100)));
    }

    [Test]
    public void SubnormalsOverflowAndNonFinite()
    {
        Assert.AreEqual(2 * double.Epsilon, SumOf(double.Epsilon, double.Epsilon));
        Assert.AreEqual(-3 * double.Epsilon, SumOf(-double.Epsilon, -double.Epsilon, -double.Epsilon));
        Assert.AreEqual(ExactSumOracle.Sum([1e-310, 2.5e-310, -1e-320]), SumOf(1e-310, 2.5e-310, -1e-320));

        double smallestNormal = Math.ScaleB(1.0, -1022);
        Assert.AreEqual(smallestNormal, SumOf(smallestNormal / 2, smallestNormal / 2), "two subnormals that make a normal");

        Assert.AreEqual(double.PositiveInfinity, SumOf(Math.ScaleB(1.0, 1023), Math.ScaleB(1.0, 1023)));
        Assert.AreEqual(double.NegativeInfinity, SumOf(-double.MaxValue, -double.MaxValue));
        Assert.AreEqual(double.PositiveInfinity, SumOf(double.MaxValue, Math.ScaleB(1.0, 970)), "half an ulp above the largest double rounds to infinity");
        Assert.AreEqual(double.MaxValue, SumOf(double.MaxValue, Math.ScaleB(1.0, 969)), "less than half an ulp stays");

        ExactDoubleSum sum = new();
        sum.Add(1.5);
        sum.Add(double.NaN);
        Assert.IsNaN(sum.ToDouble());
        sum.Remove(double.NaN);
        Assert.AreEqual(1.5, sum.ToDouble());
        sum.Add(double.PositiveInfinity);
        Assert.AreEqual(double.PositiveInfinity, sum.ToDouble());
        sum.Add(double.NegativeInfinity);
        Assert.IsNaN(sum.ToDouble());
        sum.Remove(double.NegativeInfinity);
        Assert.AreEqual(double.PositiveInfinity, sum.ToDouble());
        sum.Remove(double.PositiveInfinity);
        Assert.AreEqual(1.5, sum.ToDouble());
    }

    [Test]
    public void SlidingWindows_MatchTheOracle([Values(1 << 30, 3)] int operationsPerNormalize)
    {
        Random random = new(20261029);
        double[] values = new double[1500];

        for (int i = 0; i < values.Length; i++)
        {
            values[i] = (i % 7) switch
            {
                0 => random.Next(-1000, 1000),
                1 => 0.0,
                _ => Math.ScaleB(random.NextDouble() * 2 - 1, random.Next(-70, 71)),
            };
        }

        const int width = 37;
        ExactDoubleSum sum = new(operationsPerNormalize);

        for (int i = 0; i < values.Length; i++)
        {
            sum.Add(values[i]);

            if (i >= width)
                sum.Remove(values[i - width]);

            int start = Math.Max(0, i - width + 1);
            double expected = ExactSumOracle.Sum(values.Skip(start).Take(i - start + 1));

            Assert.AreEqual(expected, sum.ToDouble(), $"frame [{start}, {i}]");
        }

        for (int i = values.Length - width; i < values.Length; i++)
            sum.Remove(values[i]);

        Assert.AreEqual(0.0, sum.ToDouble(), "every value removed");
    }
}
