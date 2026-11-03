/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.SqlFuzz.Tests.Generator;

/// <summary>
/// The seeded random source of the generator: xoshiro256** seeded through SplitMix64.
///
/// <para><b>Why not <see cref="Random"/>.</b> A seed must give the same schema, data and statements
/// on every machine and on every future .NET version, because a CI finding is replayed from its seed.
/// The algorithm of <see cref="Random"/> is an implementation detail of the runtime; this one is fixed
/// here.</para>
///
/// <para>Not thread-safe. Each round owns its own instance.</para>
/// </summary>
internal sealed class FuzzRandom
{
    private ulong s0, s1, s2, s3;

    public FuzzRandom(ulong seed)
    {
        Seed = seed;
        ulong x = seed;
        s0 = SplitMix(ref x);
        s1 = SplitMix(ref x);
        s2 = SplitMix(ref x);
        s3 = SplitMix(ref x);
    }

    /// <summary>The seed this source started from.</summary>
    public ulong Seed { get; }

    /// <summary>A seed for round <paramref name="round"/> of a campaign with <paramref name="baseSeed"/>. Each round is independent, so one round replays without the rounds before it.</summary>
    public static ulong DeriveSeed(ulong baseSeed, long round)
    {
        ulong x = baseSeed ^ (0x9E3779B97F4A7C15UL * (ulong)(round + 1));
        return SplitMix(ref x);
    }

    private static ulong SplitMix(ref ulong x)
    {
        ulong z = x += 0x9E3779B97F4A7C15UL;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }

    public ulong NextULong()
    {
        ulong result = RotateLeft(s1 * 5, 7) * 9;
        ulong t = s1 << 17;
        s2 ^= s0;
        s3 ^= s1;
        s1 ^= s2;
        s0 ^= s3;
        s2 ^= t;
        s3 = RotateLeft(s3, 45);
        return result;
    }

    private static ulong RotateLeft(ulong x, int k) => (x << k) | (x >> (64 - k));

    /// <summary>A value in [0, <paramref name="maxExclusive"/>).</summary>
    public int Next(int maxExclusive)
    {
        if (maxExclusive <= 1)
            return 0;
        return (int)(NextULong() % (ulong)maxExclusive);
    }

    /// <summary>A value in [<paramref name="minInclusive"/>, <paramref name="maxInclusive"/>].</summary>
    public int Between(int minInclusive, int maxInclusive) => minInclusive + Next(maxInclusive - minInclusive + 1);

    /// <summary>True with probability <paramref name="probability"/>.</summary>
    public bool Chance(double probability) => NextDouble() < probability;

    public bool NextBool() => (NextULong() & 1) == 1;

    /// <summary>A value in [0, 1).</summary>
    public double NextDouble() => (NextULong() >> 11) * (1.0 / (1UL << 53));

    public T Pick<T>(IReadOnlyList<T> items)
    {
        if (items.Count == 0)
            throw new InvalidOperationException("Pick from an empty list");
        return items[Next(items.Count)];
    }

    /// <summary>A new source whose sequence depends on this one. A part of the generator that takes a child keeps the parent's sequence stable when the child draws more values.</summary>
    public FuzzRandom Fork() => new(NextULong());

    /// <summary>Shuffles a copy of <paramref name="items"/> and returns it.</summary>
    public List<T> Shuffled<T>(IEnumerable<T> items)
    {
        List<T> list = items.ToList();
        for (int i = list.Count - 1; i > 0; i--)
        {
            int j = Next(i + 1);
            (list[i], list[j]) = (list[j], list[i]);
        }
        return list;
    }
}
