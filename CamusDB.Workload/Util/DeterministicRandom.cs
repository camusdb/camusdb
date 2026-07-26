/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Util;

/// <summary>
/// A small, fully deterministic pseudo-random generator (SplitMix64) used everywhere the workload
/// needs reproducibility from <c>--seed</c>: operation selection, jitter, id/payload generation.
/// It is intentionally NOT <see cref="System.Random"/> / <c>Random.Shared</c> — those give no
/// cross-run or cross-platform reproducibility guarantee, and a fixed seed must reproduce the exact
/// operation mix and row sequence for a run to be replayable. Each worker derives its own stream by mixing the base
/// seed with the worker index, so two workers never share a sequence.
/// </summary>
public sealed class DeterministicRandom
{
    private ulong _state;

    public DeterministicRandom(ulong seed)
    {
        _state = seed;
    }

    /// <summary>
    /// Derives an independent stream for a given worker from a base seed. Distinct worker indexes
    /// produce well-separated streams (the index is mixed, not merely added).
    /// </summary>
    public static DeterministicRandom ForWorker(ulong baseSeed, int workerIndex)
        => new(Mix(baseSeed ^ (0x9E3779B97F4A7C15UL * (ulong)(uint)(workerIndex + 1))));

    /// <summary>Advances the state and returns the next 64-bit value.</summary>
    public ulong NextUInt64()
    {
        // SplitMix64
        _state += 0x9E3779B97F4A7C15UL;
        ulong z = _state;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }

    /// <summary>Uniform value in [0, exclusiveMax). exclusiveMax must be positive.</summary>
    public long NextLong(long exclusiveMax)
    {
        if (exclusiveMax <= 0)
            throw new ArgumentOutOfRangeException(nameof(exclusiveMax));

        // Unbiased-enough for benchmark selection; modulo skew is negligible at these magnitudes.
        return (long)(NextUInt64() % (ulong)exclusiveMax);
    }

    /// <summary>Uniform double in [0, 1).</summary>
    public double NextDouble()
        => (NextUInt64() >> 11) * (1.0 / (1UL << 53));

    private static ulong Mix(ulong z)
    {
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }
}
