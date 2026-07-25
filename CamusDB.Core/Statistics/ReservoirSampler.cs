
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Statistics;

/// <summary>
/// Fixed-capacity reservoir sampler (Vitter's Algorithm R) used by the background auto-analyze
/// collector to build a bounded, uniform sample of a column's values in a single pass.
///
/// <para><b>Why.</b> Equi-depth histograms need a sorted sample of values, but sorting every value
/// in a large table would make peak memory scale with row count — the spike auto-analyze must not
/// cause. A reservoir of capacity <c>k</c> holds at most <c>k</c> values while giving each of the
/// <c>N</c> scanned values an equal <c>k/N</c> probability of being retained, so the histogram is
/// built from a representative sample at constant memory. The manual <c>ANALYZE</c> path still
/// samples exactly; this bounds the background path.</para>
///
/// <para><b>Determinism.</b> Backed by a seeded SplitMix64 PRNG (not <c>System.Random</c>'s shared
/// instance), so the same value stream with the same seed yields the same sample — reproducible
/// tests and stable plans. Seed the sampler from a stable value (e.g. table id + snapshot).</para>
///
/// Not thread-safe: one sampler is owned by a single collecting scan.
/// </summary>
internal sealed class ReservoirSampler<T>
{
    private readonly int capacity;
    private readonly List<T> reservoir;
    private ulong state;
    private long seen;

    public ReservoirSampler(int capacity, ulong seed)
    {
        if (capacity < 1)
            throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "Reservoir capacity must be >= 1.");

        this.capacity = capacity;
        reservoir = new List<T>(capacity);
        // Avoid a zero state (SplitMix64 tolerates it, but mixing the seed keeps small/zero seeds well-dispersed).
        state = seed == 0 ? 0x9E3779B97F4A7C15UL : seed;
    }

    /// <summary>Total number of items offered to the sampler so far.</summary>
    public long Seen => seen;

    /// <summary>The retained sample (size = min(capacity, Seen)). Returned by reference; do not mutate.</summary>
    public List<T> Items => reservoir;

    /// <summary>Offers one item to the sampler, retaining it with the correct uniform probability.</summary>
    public void Add(T item)
    {
        // 0-based index of this item in the stream.
        long i = seen;
        seen++;

        if (reservoir.Count < capacity)
        {
            reservoir.Add(item);
            return;
        }

        // Replace a random slot with probability capacity/(i+1): pick j in [0, i]; keep if j < capacity.
        ulong j = NextBounded((ulong)(i + 1));
        if (j < (ulong)capacity)
            reservoir[(int)j] = item;
    }

    // SplitMix64 — one multiply-xorshift step per draw.
    private ulong Next()
    {
        state += 0x9E3779B97F4A7C15UL;
        ulong z = state;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }

    // Unbiased bounded draw in [0, bound) via rejection sampling (Lemire-style threshold).
    private ulong NextBounded(ulong bound)
    {
        if (bound <= 1)
            return 0;

        ulong threshold = (ulong)(-(long)bound) % bound; // = (2^64 - bound) % bound
        ulong r;
        do
        {
            r = Next();
        } while (r < threshold);
        return r % bound;
    }
}
