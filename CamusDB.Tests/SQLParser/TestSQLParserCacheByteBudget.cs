/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// The parser cache's byte budget.
///
/// <para>The entry cap bounds the cache by <em>count</em>, which does not bound its memory: a parsed
/// statement retains roughly 20 to 60 times its SQL text. A logical restore is the shape that
/// exposes the gap — every statement it sends is unique text, so the cache fills to its entry cap
/// and never returns a hit. Measured before this budget existed: 2,048 unique 500-row INSERT
/// statements retained about 1.3 GiB for the full 300 s TTL, with zero hits.</para>
///
/// <para>These tests assert the bound holds in <em>bytes</em>. An entry-count assertion alone would
/// pass against the old behavior, because 2,048 entries was always a safe count and an unsafe number
/// of bytes.</para>
/// </summary>
public sealed class TestSQLParserCacheByteBudget
{
    /// <summary>
    /// Renders a distinct multi-row INSERT, in the shape a logical dump emits. Each call returns
    /// unique text, which is the property that defeats the cache during a restore.
    /// </summary>
    private static string UniqueInsert(int seed, int rows)
    {
        StringBuilder sb = new(rows * 48);
        sb.Append("INSERT INTO bench (id, value, name) VALUES ");

        for (int i = 0; i < rows; i++)
        {
            if (i > 0)
                sb.Append(", ");

            sb.Append("('id_").Append(seed).Append('_').Append(i).Append("', ")
              .Append(seed * 100_000 + i).Append(", 'row_").Append(seed).Append('_').Append(i).Append("')");
        }

        return sb.ToString();
    }

    [Test]
    public async Task ByteBudget_RestoreShapedLoad_StaysInsideTheBudget()
    {
        const long budget = 256 * 1024;

        await using SqlParserCache cache = new(
            null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60, maxBytes: budget);

        for (int i = 0; i < 400; i++)
            SQLParserProcessor.Parse(UniqueInsert(i, rows: 20), cache);

        Assert.LessOrEqual(cache.ApproxBytes, budget,
            "the cache retained more than its byte budget after a stream of unique statements");

        // The entry cap alone would have let all 400 in; the byte budget is what stopped it.
        Assert.Less(cache.Count, 400,
            "every statement was retained, so the byte budget did not bind");

        Assert.AreEqual(0, cache.Hits, "unique statements cannot produce a hit");
    }

    [Test]
    public async Task ByteBudget_EvictsToMakeRoom_SoTheNewestStatementIsStillCached()
    {
        const long budget = 128 * 1024;

        await using SqlParserCache cache = new(
            null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60, maxBytes: budget);

        for (int i = 0; i < 200; i++)
            SQLParserProcessor.Parse(UniqueInsert(i, rows: 20), cache);

        // The statement that pushed the cache over its budget must still be cached. Refusing to
        // store it would be cheaper, but the same text is looked up more than once per request, and
        // only a cached entry makes those repeats free.
        string latest = UniqueInsert(999, rows: 20);
        SQLParserProcessor.Parse(latest, cache);

        Assert.IsTrue(cache.TryGet(latest, out _),
            "the newest statement was evicted or never stored, so a re-parse would be needed within the same request");

        Assert.LessOrEqual(cache.ApproxBytes, budget);
    }

    [Test]
    public async Task ByteBudget_StatementLargerThanTheWholeBudget_IsNotCached()
    {
        // Small enough that one 200-row statement cannot fit whatever is evicted.
        const long budget = 1024;

        await using SqlParserCache cache = new(
            null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60, maxBytes: budget);

        string huge = UniqueInsert(1, rows: 200);
        SQLParserProcessor.Parse(huge, cache);

        Assert.AreEqual(0, cache.Count, "a statement that can never fit the budget must not be stored");
        Assert.AreEqual(0, cache.ApproxBytes);
    }

    [Test]
    public async Task ByteBudget_Zero_KeepsTheEntryCapAsTheOnlyBound()
    {
        await using SqlParserCache cache = new(
            null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60, maxBytes: 0);

        for (int i = 0; i < 300; i++)
            SQLParserProcessor.Parse(UniqueInsert(i, rows: 20), cache);

        Assert.AreEqual(300, cache.Count,
            "with the byte bound disabled every statement should be retained, as before the bound existed");
    }

    [Test]
    public async Task ByteBudget_LoweredAtRuntime_TrimsThePopulation()
    {
        await using SqlParserCache cache = new(
            null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60, maxBytes: 0);

        for (int i = 0; i < 300; i++)
            SQLParserProcessor.Parse(UniqueInsert(i, rows: 20), cache);

        long before = cache.ApproxBytes;
        Assert.Greater(before, 64 * 1024);

        cache.Retune(ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60, maxBytes: 64 * 1024);

        Assert.LessOrEqual(cache.ApproxBytes, 64 * 1024,
            "lowering the budget at runtime must trim what is already retained");
    }

    /// <summary>
    /// The running byte estimate must return to zero once every entry is gone. A decrement missed on
    /// any removal path would leave the total drifting upward, and the budget would then throttle a
    /// cache that is actually empty.
    /// </summary>
    [Test]
    public async Task ApproxBytes_ReturnsToZero_WhenEveryEntryExpires()
    {
        await using SqlParserCache cache = new(
            null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60, maxBytes: 1024 * 1024);

        for (int i = 0; i < 50; i++)
            SQLParserProcessor.Parse(UniqueInsert(i, rows: 10), cache);

        Assert.Greater(cache.ApproxBytes, 0);

        // Expire everything, then sweep.
        cache.Retune(ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60, maxBytes: 1024 * 1024);
        for (int i = 0; i < 50; i++)
            cache.Store(UniqueInsert(i, rows: 10), SQLParserProcessor.Parse(UniqueInsert(i, rows: 10)), ttlMs: 1);

        await Task.Delay(50);
        cache.Sweep();

        Assert.AreEqual(0, cache.Count);
        Assert.AreEqual(0, cache.ApproxBytes,
            "the byte estimate drifted: a removal path did not decrement it");
    }

    /// <summary>
    /// A cached statement must still be served from the cache, so the engine parses one statement
    /// once however large it is. This is what makes the byte budget free: the budget evicts to make
    /// room instead of refusing, so a second lookup of the same text inside one request stays a hit.
    ///
    /// <para>The evidence is per instance: the cache's own hit and miss counters, and the identity
    /// of the returned tree, which only a hit can preserve. The process-wide
    /// <see cref="SQLParserProcessor.TotalParses"/> counter is not usable here, because fixtures run
    /// in parallel and every other fixture that parses SQL advances it.</para>
    /// </summary>
    [Test]
    public async Task CachedStatement_IsParsedOnce_HoweverLargeItIs()
    {
        await using SqlParserCache cache = new(
            null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60, maxBytes: 64 * 1024 * 1024);

        string sql = UniqueInsert(7, rows: 500);

        NodeAst first = SQLParserProcessor.Parse(sql, cache);
        NodeAst second = SQLParserProcessor.Parse(sql, cache);
        NodeAst third = SQLParserProcessor.Parse(sql, cache);

        Assert.AreEqual(1, cache.Misses,
            "the first lookup of the large statement must be the only miss");

        Assert.AreEqual(2, cache.Hits,
            "a repeated lookup of the same large statement re-parsed instead of hitting the cache");

        Assert.AreEqual(1, cache.Count, "one statement must occupy exactly one entry");

        // A hit returns the cached instance; a re-parse would build a new tree.
        Assert.AreSame(first, second, "the second lookup returned a freshly parsed tree");
        Assert.AreSame(first, third, "the third lookup returned a freshly parsed tree");
    }
}
