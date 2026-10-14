/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.PlanQuality.Tests.Data;

/// <summary>
/// A seeded SplitMix64 generator. <see cref="System.Random"/> is not used, because its sequence for
/// a seed is not part of the .NET contract and can change between runtime versions; a baseline must
/// see the same rows on every machine and every SDK.
/// </summary>
internal sealed class DeterministicRandom
{
    private ulong state;

    public DeterministicRandom(ulong seed)
    {
        state = seed;
    }

    public ulong NextUInt64()
    {
        ulong z = state += 0x9E3779B97F4A7C15UL;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }

    /// <summary>A value in [0, 1).</summary>
    public double NextDouble() => (NextUInt64() >> 11) * (1.0 / (1UL << 53));

    /// <summary>A value in [<paramref name="minInclusive"/>, <paramref name="maxExclusive"/>).</summary>
    public int Next(int minInclusive, int maxExclusive)
    {
        if (maxExclusive <= minInclusive)
            throw new ArgumentOutOfRangeException(nameof(maxExclusive));

        return minInclusive + (int)(NextUInt64() % (ulong)(maxExclusive - minInclusive));
    }

    public bool Chance(double probability) => NextDouble() < probability;

    public T Pick<T>(IReadOnlyList<T> items) => items[Next(0, items.Count)];

    /// <summary>Picks an index by weight. The weights need not add up to 1.</summary>
    public int PickWeighted(IReadOnlyList<double> weights)
    {
        double total = 0;
        for (int i = 0; i < weights.Count; i++)
            total += weights[i];

        double target = NextDouble() * total;
        for (int i = 0; i < weights.Count; i++)
        {
            target -= weights[i];
            if (target < 0)
                return i;
        }

        return weights.Count - 1;
    }
}

/// <summary>
/// Draws ranks 1..n with a Zipf distribution: rank k has a weight of 1 / k^s. A foreign key drawn
/// this way makes a few parent rows very popular, which is the skew that a uniform-data test hides
/// from the estimator.
/// </summary>
internal sealed class ZipfSampler
{
    private readonly double[] cumulative;

    public ZipfSampler(int n, double exponent)
    {
        if (n < 1)
            throw new ArgumentOutOfRangeException(nameof(n));

        cumulative = new double[n];
        double sum = 0;
        for (int k = 1; k <= n; k++)
        {
            sum += 1.0 / Math.Pow(k, exponent);
            cumulative[k - 1] = sum;
        }

        for (int i = 0; i < n; i++)
            cumulative[i] /= sum;
    }

    /// <summary>A rank in 1..n.</summary>
    public int Next(DeterministicRandom random)
    {
        double target = random.NextDouble();
        int index = Array.BinarySearch(cumulative, target);
        if (index < 0)
            index = ~index;

        return Math.Min(index, cumulative.Length - 1) + 1;
    }
}
