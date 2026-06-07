
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Tasks;
using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// PC1.1 acceptance tests for the SQL parser AST cache.
/// Each test owns its own <see cref="SqlParserCache"/> instance, so tests are fully
/// independent — no shared state, no [NonParallelizable] required.
/// </summary>
[TestFixture]
public class TestSQLParserCache
{
    private static SqlParserCache EnabledCache()  => new(null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60);
    private static SqlParserCache DisabledCache() => new(null, ttlSeconds: 0,   maxEntries: 2048, sweepSeconds: 60);

    // ── Cache hit: same reference ──────────────────────────────────────────────

    [Test]
    public async Task ParseSameSql_SecondCallReturnsCachedReference()
    {
        await using SqlParserCache cache = EnabledCache();
        const string sql = "SELECT id, name FROM users WHERE id = 1";

        NodeAst first  = SQLParserProcessor.Parse(sql, cache);
        NodeAst second = SQLParserProcessor.Parse(sql, cache);

        Assert.That(second, Is.SameAs(first),
            "Second parse of the same SQL must return the cached NodeAst reference");
    }

    [Test]
    public async Task ParseDifferentSql_ReturnsDifferentReferences()
    {
        await using SqlParserCache cache = EnabledCache();

        NodeAst a = SQLParserProcessor.Parse("SELECT a FROM t", cache);
        NodeAst b = SQLParserProcessor.Parse("SELECT b FROM t", cache);

        Assert.That(b, Is.Not.SameAs(a),
            "Different SQL strings must produce distinct NodeAst instances");
    }

    [Test]
    public async Task CacheCount_IncreasesOnFirstParse_StaysStableOnHit()
    {
        await using SqlParserCache cache = EnabledCache();
        Assert.That(cache.Count, Is.EqualTo(0));

        SQLParserProcessor.Parse("SELECT id FROM users", cache);
        Assert.That(cache.Count, Is.EqualTo(1));

        // Second call is a cache hit — no new entry.
        SQLParserProcessor.Parse("SELECT id FROM users", cache);
        Assert.That(cache.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task ParseTwoDistinctSql_CacheCountIsTwo()
    {
        await using SqlParserCache cache = EnabledCache();

        SQLParserProcessor.Parse("SELECT a FROM t1", cache);
        SQLParserProcessor.Parse("SELECT b FROM t2", cache);

        Assert.That(cache.Count, Is.EqualTo(2));
    }

    // ── Syntax error: not cached ───────────────────────────────────────────────

    [Test]
    public async Task ParseSyntaxError_IsNotCached_AndRethrowsOnEveryCall()
    {
        await using SqlParserCache cache = EnabledCache();
        const string bad = "SELECT FROM WHERE";

        Assert.That(cache.Count, Is.EqualTo(0));

        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(bad, cache));
        Assert.That(cache.Count, Is.EqualTo(0), "Syntax errors must not be cached");

        // Second call must also throw — not silently succeed or return null.
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(bad, cache));
        Assert.That(cache.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task ParseSyntaxError_DoesNotPolluteCacheForOtherSql()
    {
        await using SqlParserCache cache = EnabledCache();
        const string bad  = "SELECT FROM WHERE";
        const string good = "SELECT id FROM users";

        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(bad, cache));

        NodeAst first  = SQLParserProcessor.Parse(good, cache);
        NodeAst second = SQLParserProcessor.Parse(good, cache);

        Assert.That(second, Is.SameAs(first));
        Assert.That(cache.Count, Is.EqualTo(1));
    }

    // ── Disabled cache ─────────────────────────────────────────────────────────

    [Test]
    public async Task CacheDisabled_NothingIsCached_ParseAlwaysReturnsNewInstance()
    {
        await using SqlParserCache cache = DisabledCache();
        const string sql = "SELECT id FROM users";

        NodeAst first  = SQLParserProcessor.Parse(sql, cache);
        NodeAst second = SQLParserProcessor.Parse(sql, cache);

        Assert.That(second, Is.Not.SameAs(first),
            "With cache disabled each parse must return a fresh instance");
        Assert.That(cache.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task CacheDisabled_IsEnabledIsFalse()
    {
        await using SqlParserCache cache = DisabledCache();

        Assert.That(cache.IsEnabled, Is.False);
        Assert.That(cache.Count, Is.EqualTo(0));
    }

    // ── Parameterized queries share a cached AST ───────────────────────────────

    [Test]
    public async Task ParameterizedQuery_SameTextDifferentValues_SharesCachedAst()
    {
        await using SqlParserCache cache = EnabledCache();
        const string sql = "SELECT id FROM users WHERE id = @id";

        NodeAst first  = SQLParserProcessor.Parse(sql, cache);
        NodeAst second = SQLParserProcessor.Parse(sql, cache);

        Assert.That(second, Is.SameAs(first),
            "Parameterized SQL must share a single cached AST across executions");
    }
}
