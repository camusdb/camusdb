/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// The rules of <see cref="StatementTableCount"/> on parsed statements, with no database: what counts
/// as one relation, how derived tables, VALUES lists, CTEs and set operations add up, and the refusal
/// past the limit. View expansion needs a schema and is covered end to end in
/// <c>TestStatementTableLimit</c>.
/// </summary>
public sealed class TestStatementTableCount
{
    private static int Count(string sql) => StatementTableCount.Count(SQLParserProcessor.Parse(sql));

    [TestCase("SELECT 1", 0)]
    [TestCase("SELECT * FROM t", 1)]
    [TestCase("SELECT * FROM t AS x WHERE x.id = 1", 1)]
    [TestCase("SELECT * FROM a, b, c", 3)]
    [TestCase("SELECT * FROM a JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id CROSS JOIN d", 4)]
    [TestCase("SELECT * FROM a x JOIN a y ON x.id = y.id", 2)]
    public void EachTableInFromCountsEachTimeItIsNamed(string sql, int expected)
    {
        Assert.AreEqual(expected, Count(sql), sql);
    }

    [TestCase("SELECT * FROM a WHERE id IN (SELECT id FROM b)", 2)]
    [TestCase("SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b, c WHERE b.id = a.id)", 3)]
    [TestCase("SELECT (SELECT max(id) FROM b) AS m FROM a", 2)]
    [TestCase("SELECT * FROM a WHERE id NOT IN (SELECT id FROM b WHERE id IN (SELECT id FROM c))", 3)]
    public void TheTablesOfASubqueryCount(string sql, int expected)
    {
        Assert.AreEqual(expected, Count(sql), sql);
    }

    [TestCase("SELECT * FROM (SELECT * FROM a, b) AS d", 2)]
    [TestCase("SELECT * FROM t, (SELECT * FROM a, b) AS d", 3)]
    [TestCase("SELECT * FROM (SELECT 1 AS x) AS d", 1)]
    [TestCase("SELECT * FROM (SELECT * FROM (SELECT 1 AS x) AS d) AS e", 1)]
    [TestCase("SELECT * FROM (VALUES (1), (2), (3)) AS v (a)", 1)]
    [TestCase("SELECT * FROM t, (VALUES (1)) AS v (a), (VALUES (2)) AS w (a)", 3)]
    public void ADerivedTableCountsItsQuery_OrOneWhenItReadsNone(string sql, int expected)
    {
        Assert.AreEqual(expected, Count(sql), sql);
    }

    [TestCase("SELECT * FROM generate_series(1, 3) AS g", 1)]
    [TestCase("SELECT * FROM t, generate_series(1, t.n) AS g", 2)]
    [TestCase("SELECT * FROM t, LATERAL (SELECT * FROM a WHERE a.id = t.id) AS l", 2)]
    public void AFunctionInFromCountsAsOne_AlsoWhenItReadsTheLeftRow(string sql, int expected)
    {
        Assert.AreEqual(expected, Count(sql), sql);
    }

    [TestCase("SELECT id FROM a UNION ALL SELECT id FROM b UNION SELECT id FROM c", 3)]
    [TestCase("SELECT id FROM a EXCEPT SELECT id FROM b", 2)]
    [TestCase("SELECT * FROM (SELECT id FROM a UNION ALL SELECT id FROM b) AS u", 2)]
    public void EachBranchOfASetOperationCounts(string sql, int expected)
    {
        Assert.AreEqual(expected, Count(sql), sql);
    }

    [TestCase("WITH c AS (SELECT * FROM a, b) SELECT * FROM c", 2)]
    [TestCase("WITH c AS NOT MATERIALIZED (SELECT * FROM a, b) SELECT * FROM c x JOIN c y ON x.id = y.id", 4)]
    [TestCase("SELECT * FROM (WITH c AS (SELECT * FROM a, b) SELECT * FROM c x, c y) AS d", 4)]
    public void AnInlinedCteCountsItsQueryAtEachReference(string sql, int expected)
    {
        Assert.AreEqual(expected, Count(sql), sql);
    }

    [TestCase("WITH c AS (SELECT * FROM a, b) SELECT * FROM c x JOIN c y ON x.id = y.id", 3)]
    [TestCase("WITH c AS MATERIALIZED (SELECT * FROM a, b) SELECT * FROM c", 2)]
    [TestCase("WITH c AS (SELECT * FROM a, b) SELECT * FROM c x, c y, c z", 4)]
    public void ACteComputedOnceCountsItsQueryOnce_AndEachFurtherReferenceAsOne(string sql, int expected)
    {
        Assert.AreEqual(expected, Count(sql), sql);
    }

    [Test]
    public void ARecursiveCteCountsItsTermsAndItsWorkTable()
    {
        Assert.AreEqual(1, Count("WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT * FROM r"));
        Assert.AreEqual(3, Count(
            "WITH RECURSIVE r (id) AS (SELECT id FROM a UNION ALL SELECT b.id FROM b JOIN r ON b.parent = r.id) SELECT * FROM r"));
    }

    [TestCase("UPDATE t SET v = 1 WHERE id = 1", 1)]
    [TestCase("UPDATE t SET v = 1 WHERE id IN (SELECT id FROM a, b)", 3)]
    [TestCase("UPDATE t SET v = d.v FROM d WHERE d.id = t.id", 2)]
    [TestCase("DELETE FROM t WHERE id = 1", 1)]
    [TestCase("DELETE FROM t USING a, b WHERE a.id = t.id AND b.id = a.id", 3)]
    [TestCase("INSERT INTO t (id) VALUES (1), (2)", 0)]
    [TestCase("INSERT INTO t (id) VALUES ((SELECT max(id) FROM a))", 1)]
    [TestCase("INSERT INTO t (id) SELECT a.id FROM a, b", 2)]
    public void TheTargetOfUpdateAndDeleteCounts_AndTheTargetOfInsertDoesNot(string sql, int expected)
    {
        Assert.AreEqual(expected, Count(sql), sql);
    }

    [Test]
    public void ALongCommaList_IsCountedWithoutRecursion()
    {
        string sql = "SELECT * FROM " + string.Join(", ", Enumerable.Range(1, 300).Select(i => $"t t{i}"));

        Assert.AreEqual(300, Count(sql));
    }

    [Test]
    public void TheCountStopsAtTheFirstRelationPastTheBound()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM a, b, c, d, e");

        Assert.AreEqual(3, StatementTableCount.Count(ast, stopAbove: 2));
        Assert.AreEqual(5, StatementTableCount.Count(ast));
    }

    [Test]
    public void AStatementAtTheLimitPasses_AndOnePastItIsRefused()
    {
        StatementTableCount.Enforce(SQLParserProcessor.Parse("SELECT * FROM a, b, c"), limit: 3);

        CamusDBException error = Assert.Throws<CamusDBException>(
            () => StatementTableCount.Enforce(SQLParserProcessor.Parse("SELECT * FROM a, b, c, d"), limit: 3))!;

        Assert.AreEqual(CamusDBErrorCodes.TooManyTablesInStatement, error.Code);
        StringAssert.Contains("max_tables_per_statement", error.Message);
        StringAssert.Contains("more than 3 tables", error.Message);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void ALimitOfZeroOrLess_DisablesTheCheck(int limit)
    {
        string sql = "SELECT * FROM " + string.Join(", ", Enumerable.Range(1, 100).Select(i => $"t t{i}"));

        Assert.DoesNotThrow(() => StatementTableCount.Enforce(SQLParserProcessor.Parse(sql), limit));
    }

    [Test]
    public void ARefusedWalk_LeavesTheNextCountCorrect()
    {
        // The frame stack is reused per thread; a walk that stopped early must not leave frames behind.
        Assert.Throws<CamusDBException>(
            () => StatementTableCount.Enforce(SQLParserProcessor.Parse("SELECT * FROM a, b, (SELECT * FROM c, d) AS x"), limit: 1));

        Assert.AreEqual(2, Count("SELECT * FROM a, b"));
    }
}
