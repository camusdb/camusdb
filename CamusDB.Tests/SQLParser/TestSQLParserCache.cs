
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// PC1.1 acceptance tests for the SQL parser AST cache.
/// </summary>
[TestFixture]
[NonParallelizable]
public class TestSQLParserCache
{
    private int _savedTtl;

    [SetUp]
    public void SetUp()
    {
        _savedTtl = CamusDBConfig.SqlParserCacheTtlSeconds;
        // Enable caching with a long TTL for all tests unless the test overrides it.
        CamusDBConfig.SqlParserCacheTtlSeconds = 300;
        SqlParserCache.Clear();
    }

    [TearDown]
    public void TearDown()
    {
        CamusDBConfig.SqlParserCacheTtlSeconds = _savedTtl;
        SqlParserCache.Clear();
    }

    // ── Cache hit: same reference ──────────────────────────────────────────────

    [Test]
    public void ParseSameSql_SecondCallReturnsCachedReference()
    {
        const string sql = "SELECT id, name FROM users WHERE id = 1";

        NodeAst first = SQLParserProcessor.Parse(sql);
        NodeAst second = SQLParserProcessor.Parse(sql);

        Assert.That(second, Is.SameAs(first),
            "Second parse of the same SQL must return the cached NodeAst reference");
    }

    [Test]
    public void ParseDifferentSql_ReturnsDifferentReferences()
    {
        NodeAst a = SQLParserProcessor.Parse("SELECT a FROM t");
        NodeAst b = SQLParserProcessor.Parse("SELECT b FROM t");

        Assert.That(b, Is.Not.SameAs(a),
            "Different SQL strings must produce distinct NodeAst instances");
    }

    [Test]
    public void CacheCount_IncreasesOnFirstParse_StaysStableOnHit()
    {
        Assert.That(SqlParserCache.Count, Is.EqualTo(0));

        SQLParserProcessor.Parse("SELECT id FROM users");
        Assert.That(SqlParserCache.Count, Is.EqualTo(1));

        // Second call is a cache hit — no new entry.
        SQLParserProcessor.Parse("SELECT id FROM users");
        Assert.That(SqlParserCache.Count, Is.EqualTo(1));
    }

    [Test]
    public void ParseTwoDistinctSql_CacheCountIsTwo()
    {
        SQLParserProcessor.Parse("SELECT a FROM t1");
        SQLParserProcessor.Parse("SELECT b FROM t2");

        Assert.That(SqlParserCache.Count, Is.EqualTo(2));
    }

    // ── Syntax error: not cached ───────────────────────────────────────────────

    [Test]
    public void ParseSyntaxError_IsNotCached_AndRethrowsOnEveryCall()
    {
        const string bad = "SELECT FROM WHERE";

        Assert.That(SqlParserCache.Count, Is.EqualTo(0));

        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(bad));
        Assert.That(SqlParserCache.Count, Is.EqualTo(0), "Syntax errors must not be cached");

        // Second call must also throw — not silently succeed or return null.
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(bad));
        Assert.That(SqlParserCache.Count, Is.EqualTo(0));
    }

    [Test]
    public void ParseSyntaxError_DoesNotPolluteCacheForOtherSql()
    {
        const string bad = "SELECT FROM WHERE";
        const string good = "SELECT id FROM users";

        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(bad));

        NodeAst first = SQLParserProcessor.Parse(good);
        NodeAst second = SQLParserProcessor.Parse(good);

        Assert.That(second, Is.SameAs(first));
        Assert.That(SqlParserCache.Count, Is.EqualTo(1));
    }

    // ── Disabled cache ─────────────────────────────────────────────────────────

    [Test]
    public void CacheDisabled_NothingIsCached_ParseAlwaysReturnsNewInstance()
    {
        CamusDBConfig.SqlParserCacheTtlSeconds = 0;

        const string sql = "SELECT id FROM users";

        NodeAst first = SQLParserProcessor.Parse(sql);
        NodeAst second = SQLParserProcessor.Parse(sql);

        Assert.That(second, Is.Not.SameAs(first),
            "With cache disabled each parse must return a fresh instance");
        Assert.That(SqlParserCache.Count, Is.EqualTo(0));
    }

    [Test]
    public void CacheDisabledWithNegativeTtl_BehavesLikeZeroTtl()
    {
        CamusDBConfig.SqlParserCacheTtlSeconds = -1;

        const string sql = "SELECT id FROM orders";

        NodeAst first = SQLParserProcessor.Parse(sql);
        NodeAst second = SQLParserProcessor.Parse(sql);

        Assert.That(second, Is.Not.SameAs(first));
        Assert.That(SqlParserCache.Count, Is.EqualTo(0));
    }

    // ── Parameterized queries share a cached AST ───────────────────────────────

    [Test]
    public void ParameterizedQuery_SameTextDifferentValues_SharesCachedAst()
    {
        // Two executions of the same parameterized statement (the parameter value is supplied
        // outside the SQL text, at eval time). The SQL text is identical, so both hits use the
        // same cached NodeAst — the core correctness property of the cache.
        const string sql = "SELECT id FROM users WHERE id = @id";

        NodeAst first = SQLParserProcessor.Parse(sql);
        NodeAst second = SQLParserProcessor.Parse(sql);

        Assert.That(second, Is.SameAs(first),
            "Parameterized SQL must share a single cached AST across executions");
    }
}
