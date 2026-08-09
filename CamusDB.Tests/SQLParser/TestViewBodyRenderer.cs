
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// Round-trip tests for the renderer that produces a view's stored body.
///
/// <para>The property that matters is <b>idempotent re-rendering</b>: render(parse(x)) must equal
/// render(parse(render(parse(x)))). A renderer can be wrong in two ways — it can lose information
/// (the second render differs) or it can re-parse to a different tree (which shows up as a different
/// second render too). Checking the fixed point catches both, and it is the exact property the
/// stored-text design depends on, because a stored body is re-parsed on every node and rewritten in
/// place on every base-object rename.</para>
/// </summary>
public sealed class TestViewBodyRenderer
{
    private static string Render(string sql) => ViewBodyRenderer.RenderSelect(SQLParserProcessor.Parse(sql));

    /// <summary>Renders, re-parses, renders again, and asserts the two renders agree.</summary>
    private static string AssertFixedPoint(string sql)
    {
        string once = Render(sql);
        string twice = Render(once);

        Assert.AreEqual(once, twice, $"re-rendering must be a fixed point; original SQL was: {sql}");
        return once;
    }

    [Test]
    public void SimpleProjectionAndPredicate()
    {
        Assert.AreEqual("SELECT id, total FROM orders WHERE status = 'open'",
            AssertFixedPoint("SELECT id, total FROM orders WHERE status = 'open'"));
    }

    [Test]
    public void StarProjection()
    {
        Assert.AreEqual("SELECT * FROM orders", AssertFixedPoint("SELECT * FROM orders"));
    }

    [Test]
    public void AliasesAlwaysRenderWithExplicitAs()
    {
        Assert.AreEqual("SELECT total AS t FROM orders", AssertFixedPoint("SELECT total AS t FROM orders"));
    }

    [Test]
    public void PrecedenceIsPreservedByParenthesization()
    {
        // The parser's own precedence must not be able to regroup the re-parsed text.
        Assert.AreEqual("SELECT id FROM t WHERE (a OR b) AND c",
            AssertFixedPoint("SELECT id FROM t WHERE (a OR b) AND c"));

        Assert.AreEqual("SELECT id FROM t WHERE a OR (b AND c)",
            AssertFixedPoint("SELECT id FROM t WHERE a OR b AND c"));
    }

    [Test]
    public void AggregatesGroupByAndHaving()
    {
        Assert.AreEqual(
            "SELECT customer, SUM(total) AS s FROM orders GROUP BY customer HAVING SUM(total) > 100",
            AssertFixedPoint("SELECT customer, SUM(total) AS s FROM orders GROUP BY customer HAVING SUM(total) > 100"));
    }

    [Test]
    public void CountStarKeepsItsStar()
    {
        Assert.AreEqual("SELECT COUNT(*) AS n FROM orders", AssertFixedPoint("SELECT COUNT(*) AS n FROM orders"));
    }

    [Test]
    public void DistinctOrderLimitOffset()
    {
        Assert.AreEqual("SELECT DISTINCT customer FROM orders ORDER BY customer DESC LIMIT 5 OFFSET 2",
            AssertFixedPoint("SELECT DISTINCT customer FROM orders ORDER BY customer DESC LIMIT 5 OFFSET 2"));
    }

    [Test]
    public void OrderByMultipleKeysWithDirections()
    {
        Assert.AreEqual("SELECT a, b FROM t ORDER BY a ASC, b DESC",
            AssertFixedPoint("SELECT a, b FROM t ORDER BY a ASC, b DESC"));
    }

    [Test]
    public void InnerJoinWithAliases()
    {
        Assert.AreEqual(
            "SELECT o.id, c.name FROM orders AS o INNER JOIN customers AS c ON o.customer = c.id",
            AssertFixedPoint("SELECT o.id, c.name FROM orders o INNER JOIN customers c ON o.customer = c.id"));
    }

    [Test]
    public void CommaJoin()
    {
        Assert.AreEqual("SELECT a.id FROM a, b WHERE a.x = b.x",
            AssertFixedPoint("SELECT a.id FROM a, b WHERE a.x = b.x"));
    }

    [Test]
    public void DerivedTable()
    {
        Assert.AreEqual("SELECT d.n FROM (SELECT n FROM t WHERE n > 0) AS d",
            AssertFixedPoint("SELECT d.n FROM (SELECT n FROM t WHERE n > 0) AS d"));
    }

    [Test]
    public void ScalarSubqueryInProjection()
    {
        Assert.AreEqual("SELECT id, (SELECT MAX(n) FROM t2) AS m FROM t1",
            AssertFixedPoint("SELECT id, (SELECT MAX(n) FROM t2) AS m FROM t1"));
    }

    [Test]
    public void ExistsAndInSubqueries()
    {
        AssertFixedPoint("SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.a = t1.a)");
        AssertFixedPoint("SELECT id FROM t1 WHERE a IN (SELECT a FROM t2)");
        AssertFixedPoint("SELECT id FROM t1 WHERE a NOT IN (SELECT a FROM t2)");
    }

    [Test]
    public void CaseExpression()
    {
        Assert.AreEqual("SELECT CASE WHEN n > 0 THEN 'pos' ELSE 'neg' END AS s FROM t",
            AssertFixedPoint("SELECT CASE WHEN n > 0 THEN 'pos' ELSE 'neg' END AS s FROM t"));
    }

    [Test]
    public void CastAndArithmetic()
    {
        AssertFixedPoint("SELECT CAST(n AS float64) AS f, a + b * c AS x FROM t");
    }

    [Test]
    public void BetweenInListAndNullTests()
    {
        AssertFixedPoint("SELECT id FROM t WHERE n BETWEEN 1 AND 10");
        AssertFixedPoint("SELECT id FROM t WHERE s IN ('a', 'b', 'c')");
        AssertFixedPoint("SELECT id FROM t WHERE s IS NOT NULL");
    }

    [Test]
    public void LikeAndRegexOperators()
    {
        AssertFixedPoint("SELECT id FROM t WHERE s LIKE 'a%'");
        AssertFixedPoint("SELECT id FROM t WHERE s ILIKE 'a%'");
        AssertFixedPoint("SELECT id FROM t WHERE s ~ 'a.*'");
        AssertFixedPoint("SELECT id FROM t WHERE s !~* 'a.*'");
    }

    /// <summary>
    /// String literals are the case a naive renderer gets wrong: the AST holds the source form
    /// including its quotes, and the two literal dialects (plain and E'…') decode differently. The
    /// renderer must emit a canonical form that re-lexes to the same string, embedded quotes and all.
    /// </summary>
    [Test]
    public void StringLiteralsSurviveTheRoundTrip()
    {
        AssertFixedPoint("SELECT id FROM t WHERE s = 'it''s'");
        AssertFixedPoint(@"SELECT id FROM t WHERE s = E'tab\there'");
        AssertFixedPoint("SELECT id FROM t WHERE s = ''");
    }

    [Test]
    public void FromLessSelect()
    {
        AssertFixedPoint("SELECT 1");
    }

    /// <summary>
    /// An index or cache hint must be refused rather than silently dropped: dropping it would make
    /// the stored body quietly differ from what the user wrote, and keeping it would pin every future
    /// query through the view to a plan chosen once.
    /// </summary>
    [Test]
    public void IndexHintInViewBodyIsRefused()
    {
        CamusDBException? ex = Assert.Throws<CamusDBException>(
            () => Render("SELECT id FROM t@{force_index=idx_a}"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("hint", ex.Message);
    }
}
