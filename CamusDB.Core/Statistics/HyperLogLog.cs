
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace CamusDB.Core.Statistics;

/// <summary>
/// A small, dependency-free HyperLogLog sketch for approximate distinct-value counting (NDV).
///
/// <para><b>Why it exists.</b> The background auto-analyze path must build NDV over a whole table
/// without holding every distinct value in memory — an exact <c>HashSet&lt;string&gt;</c> would make
/// peak memory scale with the table's cardinality, exactly the spike auto-analyze must avoid. HLL
/// bounds memory to a fixed <c>2^precision</c> register bytes (≈2 KB at the default precision 11)
/// regardless of how many distinct values are observed, trading a small, well-understood error
/// (~2–3%) for that hard ceiling. The manual <c>ANALYZE TABLE</c> path keeps exact counting; this
/// is only for the memory-bounded background collector.</para>
///
/// <para><b>Determinism.</b> Hashing is a fixed 64-bit FNV-1a + SplitMix64 finalizer over the
/// value's canonical string form, independent of the process-randomized <see cref="string.GetHashCode()"/>,
/// so a given multiset always yields the same estimate across runs (reproducible tests, stable plans).</para>
///
/// Not thread-safe: one sketch is owned by a single collecting scan.
/// </summary>
internal sealed class HyperLogLog
{
    private readonly byte[] registers;
    private readonly int precision;
    private readonly int registerCount;
    private readonly double alphaMM;

    /// <param name="precision">Number of index bits; register count is <c>2^precision</c>.
    /// Valid range 4..16. Default 11 ⇒ 2048 registers (~2 KB), standard error ≈ 2.3%.</param>
    public HyperLogLog(int precision = 11)
    {
        if (precision is < 4 or > 16)
            throw new ArgumentOutOfRangeException(nameof(precision), precision, "HLL precision must be in [4, 16].");

        this.precision = precision;
        registerCount = 1 << precision;
        registers = new byte[registerCount];

        double alpha = registerCount switch
        {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _  => 0.7213 / (1.0 + 1.079 / registerCount),
        };
        alphaMM = alpha * registerCount * registerCount;
    }

    /// <summary>Adds one value by its canonical string form (see <see cref="TableAnalyzer"/> bound keys).</summary>
    public void Add(string value)
    {
        ulong hash = Hash64(value);

        // Top `precision` bits select the register; the remaining bits provide the rank
        // (position of the leftmost set bit) which estimates run length.
        int index = (int)(hash >> (64 - precision));
        ulong remaining = (hash << precision) | (1UL << (precision - 1)); // guard bit so rank is bounded
        byte rank = (byte)(LeadingZeroCount(remaining) + 1);

        if (rank > registers[index])
            registers[index] = rank;
    }

    /// <summary>Returns the approximate number of distinct values added so far (≥ 0).</summary>
    public long Estimate()
    {
        double sum = 0.0;
        int zeroRegisters = 0;

        for (int i = 0; i < registerCount; i++)
        {
            sum += 1.0 / (1UL << registers[i]);
            if (registers[i] == 0)
                zeroRegisters++;
        }

        double estimate = alphaMM / sum;

        // Small-cardinality correction: linear counting is far more accurate than raw HLL when
        // many registers are still empty.
        if (estimate <= 2.5 * registerCount && zeroRegisters > 0)
            estimate = registerCount * Math.Log((double)registerCount / zeroRegisters);

        // 64-bit hashes make the classic large-range correction unnecessary.
        return (long)Math.Round(estimate);
    }

    private static int LeadingZeroCount(ulong value) =>
        value == 0 ? 64 : System.Numerics.BitOperations.LeadingZeroCount(value);

    // Fixed 64-bit hash: FNV-1a over UTF-8 bytes, then a SplitMix64 finalizer to disperse bits
    // uniformly (FNV alone has weak avalanche in the high bits HLL relies on for the register index).
    private static ulong Hash64(string value)
    {
        const ulong fnvOffset = 14695981039346656037UL;
        const ulong fnvPrime = 1099511628211UL;

        ulong hash = fnvOffset;
        int byteCount = Encoding.UTF8.GetByteCount(value);
        Span<byte> buffer = byteCount <= 256 ? stackalloc byte[byteCount] : new byte[byteCount];
        Encoding.UTF8.GetBytes(value, buffer);

        foreach (byte b in buffer)
        {
            hash ^= b;
            hash *= fnvPrime;
        }

        // SplitMix64 finalizer.
        hash ^= hash >> 30;
        hash *= 0xbf58476d1ce4e5b9UL;
        hash ^= hash >> 27;
        hash *= 0x94d049bb133111ebUL;
        hash ^= hash >> 31;
        return hash;
    }
}
