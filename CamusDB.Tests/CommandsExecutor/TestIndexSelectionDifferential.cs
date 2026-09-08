/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Differential tests for index selection on the standalone engine: a query must return the same
/// rows with and without an index. Two families of defect are covered.
///
/// <para><b>Mixed numeric literals.</b> The evaluator widens an Integer64/Float64 comparison to
/// double, but an index key is typed. An unconverted literal builds a key that addresses no entry
/// (or a bound the scan cannot compare), so <c>a = 1.0</c> on an INT column returns the row from a
/// table scan and nothing from an index.</para>
///
/// <para><b>Incomplete unique indexes.</b> A unique index has no entry for a row with NULL in any
/// key column. Reading a composite unique index over a prefix, or end to end for streaming
/// DISTINCT/GROUP BY, an EXISTS seek or a merge join, drops those rows. A residual filter cannot
/// repair that: the row never enters the pipeline.</para>
///
/// Every case asserts the expected rows on both sides, not only that the two sides agree.
/// </summary>
[NonParallelizable]
public class TestIndexSelectionDifferential : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> Setup(bool extraRowInGroupOne = false)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await IndexDifferentialProbe.CreateFixture(executor, database, dbname, extraRowInGroupOne);
        return (dbname, database, executor);
    }

    private async Task RunCase(string query, string indexDdl, string expected, bool extraRowInGroupOne = false)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup(extraRowInGroupOne);
        await IndexDifferentialProbe.AssertSameRowsBeforeAndAfterIndex(executor, database, dbname, query, indexDdl, expected);
    }

    private static void AssertPlanUses(string[] nodes, string nodePrefix, string query)
    {
        Assert.IsTrue(nodes.Any(n => n.StartsWith(nodePrefix, System.StringComparison.Ordinal)),
            $"Expected a `{nodePrefix}` node for `{query}`; got: {string.Join(", ", nodes)}");
    }

    // ── Mixed numeric literals ────────────────────────────────────────────────

    [Test]
    public async Task FloatLiteral_EqualityOnIntColumn_UniqueIndex_KeepsRowAndLookup()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();
        const string query = "SELECT a FROM probe WHERE a = 1.0";

        await IndexDifferentialProbe.AssertSameRowsBeforeAndAfterIndex(
            executor, database, dbname, query, "CREATE UNIQUE INDEX ai ON probe (a)", "a=1");

        // The literal must be rewritten into the column's type, not demoted to a table scan.
        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, database, dbname, query);
        AssertPlanUses(nodes, "index-lookup", query);
    }

    [Test]
    public async Task FloatLiteral_EqualityOnIntColumn_NonUniqueIndex_KeepsRowAndRangeScan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();
        const string query = "SELECT a FROM probe WHERE a = 1.0";

        await IndexDifferentialProbe.AssertSameRowsBeforeAndAfterIndex(
            executor, database, dbname, query, "CREATE INDEX ai ON probe (a)", "a=1");

        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, database, dbname, query);
        AssertPlanUses(nodes, "index-range-scan", query);
    }

    [TestCase("a < 1.5",  "a=1")]
    [TestCase("a <= 1.5", "a=1")]
    [TestCase("a < 2.0",  "a=1")]
    [TestCase("a > 0.5",  "a=1;a=2")]
    [TestCase("a >= 0.5", "a=1;a=2")]
    [TestCase("a > 1.5",  "a=2")]
    [TestCase("a >= 2.0", "a=2")]
    [TestCase("a BETWEEN 0.5 AND 1.5", "a=1")]
    public async Task FloatLiteral_RangeOnIntColumn_NonUniqueIndex_KeepsRows(string predicate, string expected)
    {
        // Before the fix the second run threw "Comparing incompatible ColumnValue: Integer64 and
        // Float64" from the index scan; QueryRendered lets that propagate, so the assertion covers
        // both the row set and the absence of an exception.
        await RunCase($"SELECT a FROM probe WHERE {predicate}", "CREATE INDEX ai ON probe (a)", expected);
    }

    [Test]
    public async Task FloatLiteral_RangeOnIntColumn_UsesTheIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();
        const string query = "SELECT a FROM probe WHERE a < 1.5";

        await IndexDifferentialProbe.AssertSameRowsBeforeAndAfterIndex(
            executor, database, dbname, query, "CREATE INDEX ai ON probe (a)", "a=1");

        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, database, dbname, query);
        AssertPlanUses(nodes, "index-range-scan", query);
    }

    [TestCase("CREATE UNIQUE INDEX fi ON probe (f)")]
    [TestCase("CREATE INDEX fi ON probe (f)")]
    public async Task IntLiteral_EqualityOnFloatColumn_KeepsRow(string indexDdl)
    {
        await RunCase("SELECT f FROM probe WHERE f = 1", indexDdl, "f=1");
    }

    [Test]
    public async Task IntLiteral_RangeOnFloatColumn_KeepsRows()
    {
        await RunCase("SELECT f FROM probe WHERE f > 1", "CREATE INDEX fi ON probe (f)", "f=2.5");
    }

    [TestCase("a = 1.5", "")]
    [TestCase("a != 1.5", "a=1;a=2")]
    public async Task FractionalLiteral_OnIntColumn_NeverRounds(string predicate, string expected)
    {
        // 1.5 equals no integer. The planner must hand the comparison to the evaluator, never
        // round it to 1 or 2 and return a row through the index.
        await RunCase($"SELECT a FROM probe WHERE {predicate}", "CREATE UNIQUE INDEX ai ON probe (a)", expected);
    }

    [Test]
    public async Task FloatLiteral_InListOnIntColumn_UniqueIndex_KeepsRows()
    {
        // IN membership widens a mixed numeric pair like `=` does, on the table scan (prepared set
        // and AST path) and on the index IN-list seek (items rewritten into the column's type).
        await RunCase("SELECT a FROM probe WHERE a IN (1.0, 2.0)", "CREATE UNIQUE INDEX ai ON probe (a)", "a=1;a=2");
    }

    [Test]
    public async Task FractionalLiteral_InListOnIntColumn_MatchesNothing()
    {
        await RunCase("SELECT a FROM probe WHERE a IN (1.5)", "CREATE UNIQUE INDEX ai ON probe (a)", "");
    }

    [TestCase("a NOT IN (1.0)", "a=2")]
    [TestCase("a NOT IN (1.5)", "a=1;a=2")]
    public async Task FloatLiteral_NotInListOnIntColumn_WidensLikeEquality(string predicate, string expected)
    {
        await RunCase($"SELECT a FROM probe WHERE {predicate}", "CREATE UNIQUE INDEX ai ON probe (a)", expected);
    }

    [TestCase("CREATE UNIQUE INDEX fi ON probe (f)")]
    [TestCase("CREATE INDEX fi ON probe (f)")]
    public async Task IntLiteral_InListOnFloatColumn_KeepsRow(string indexDdl)
    {
        await RunCase("SELECT f FROM probe WHERE f IN (1, 3)", indexDdl, "f=1");
    }

    [Test]
    public async Task InSubquery_MixedNumericTypes_WidensLikeEquality()
    {
        // The materialized IN-subquery path feeds the same membership rule: INT a against FLOAT f.
        await RunCase("SELECT a FROM probe WHERE a IN (SELECT f FROM probe)", "CREATE INDEX bi ON probe (b)", "a=1");
    }

    // ── Composite unique index with a nullable trailing column ────────────────

    [Test]
    public async Task EqualityPrefix_UniqueCompositeWithNullableTrailing_KeepsNullRow()
    {
        await RunCase("SELECT a FROM probe WHERE a = 1", "CREATE UNIQUE INDEX ab ON probe (a, b)", "a=1");
    }

    [Test]
    public async Task RangePrefix_UniqueCompositeWithNullableTrailing_KeepsNullRow()
    {
        await RunCase("SELECT a FROM probe WHERE a >= 1", "CREATE UNIQUE INDEX ab ON probe (a, b)", "a=1;a=2");
    }

    [Test]
    public async Task FullKeyEquality_UniqueCompositeWithNullableTrailing_StillUsesLookup()
    {
        // A full-key equality needs no completeness proof: a row with NULL in b cannot satisfy
        // b = 5, so the omitted entry is never a qualifying row. The lookup must stay.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();
        const string query = "SELECT a FROM probe WHERE a = 2 AND b = 5";

        await IndexDifferentialProbe.AssertSameRowsBeforeAndAfterIndex(
            executor, database, dbname, query, "CREATE UNIQUE INDEX ab ON probe (a, b)", "a=2");

        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, database, dbname, query);
        AssertPlanUses(nodes, "index-lookup", query);
    }

    [Test]
    public async Task EqualityPrefix_UniqueCompositeWithNotNullTrailing_StillUsesIndex()
    {
        // With the trailing column NOT NULL the schema proves completeness: the prefix scan must
        // stay, or the guard over-rejects and every composite unique index becomes useless.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await IndexDifferentialProbe.ExecDDL(executor, database, dbname,
            "CREATE TABLE strict (id OID PRIMARY KEY NOT NULL, a INT NOT NULL, b INT NOT NULL)");
        await IndexDifferentialProbe.ExecDML(executor, database, dbname,
            "INSERT INTO strict (id, a, b) VALUES (gen_id(), 1, 3)");
        await IndexDifferentialProbe.ExecDML(executor, database, dbname,
            "INSERT INTO strict (id, a, b) VALUES (gen_id(), 2, 5)");

        const string query = "SELECT a FROM strict WHERE a = 1";
        await IndexDifferentialProbe.AssertSameRowsBeforeAndAfterIndex(
            executor, database, dbname, query, "CREATE UNIQUE INDEX ab ON strict (a, b)", "a=1");

        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, database, dbname, query);
        AssertPlanUses(nodes, "index-range-scan", query);
    }

    [Test]
    public async Task EqualityPrefixWithRangeOnNullableTrailing_UniqueComposite_StillUsesIndex()
    {
        // b is nullable but constrained by the range: a NULL b fails `b > 0`, so the row the index
        // omits was never a qualifying row and the prefix-plus-range scan is complete.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();
        const string query = "SELECT a FROM probe WHERE a = 2 AND b > 0";

        await IndexDifferentialProbe.AssertSameRowsBeforeAndAfterIndex(
            executor, database, dbname, query, "CREATE UNIQUE INDEX ab ON probe (a, b)", "a=2");

        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, database, dbname, query);
        AssertPlanUses(nodes, "index-range-scan", query);
    }

    [Test]
    public async Task EqualityPrefix_UniqueCompositeWithNullableTrailing_FallsBackToTableScan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();
        const string query = "SELECT a FROM probe WHERE a = 1";

        await IndexDifferentialProbe.AssertSameRowsBeforeAndAfterIndex(
            executor, database, dbname, query, "CREATE UNIQUE INDEX ab ON probe (a, b)", "a=1");

        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, database, dbname, query);
        AssertPlanUses(nodes, "table-scan", query);
        Assert.IsFalse(nodes.Any(n => n.Contains("index=ab", System.StringComparison.Ordinal)),
            $"The incomplete unique index must not be used for `{query}`; got: {string.Join(", ", nodes)}");
    }

    [Test]
    public async Task StreamingDistinct_UniqueCompositeWithNullableTrailing_KeepsAllValues()
    {
        await RunCase("SELECT DISTINCT a FROM probe", "CREATE UNIQUE INDEX ab ON probe (a, b)", "a=1;a=2");
    }

    [Test]
    public async Task StreamingGroupBy_UniqueCompositeWithNullableTrailing_KeepsGroupsAndCounts()
    {
        // Group a=1 holds one NULL-b row and one non-NULL-b row, so an index-driven scan that
        // omits the NULL row shows up as an undercount (n=1), not only as a missing group.
        await RunCase(
            "SELECT a, count(*) AS n FROM probe GROUP BY a",
            "CREATE UNIQUE INDEX ab ON probe (a, b)",
            "a=1,n=2;a=2,n=1",
            extraRowInGroupOne: true);
    }

    [Test]
    public async Task StreamingDistinctAndGroupBy_UniqueCompositeAllNotNull_StillStream()
    {
        // With every key column NOT NULL the unique index holds every row, so the streaming
        // paths must keep the forced index scan (the guard must not over-reject).
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await IndexDifferentialProbe.ExecDDL(executor, database, dbname,
            "CREATE TABLE strict (id OID PRIMARY KEY NOT NULL, a INT NOT NULL, b INT NOT NULL)");
        await IndexDifferentialProbe.ExecDML(executor, database, dbname,
            "INSERT INTO strict (id, a, b) VALUES (gen_id(), 1, 3)");
        await IndexDifferentialProbe.ExecDML(executor, database, dbname,
            "INSERT INTO strict (id, a, b) VALUES (gen_id(), 1, 4)");
        await IndexDifferentialProbe.ExecDML(executor, database, dbname,
            "INSERT INTO strict (id, a, b) VALUES (gen_id(), 2, 5)");
        await IndexDifferentialProbe.ExecDDL(executor, database, dbname, "CREATE UNIQUE INDEX ab ON strict (a, b)");

        const string distinct = "SELECT DISTINCT a FROM strict";
        Assert.AreEqual("a=1;a=2", await IndexDifferentialProbe.QueryRendered(executor, database, dbname, distinct));
        string[] distinctNodes = await IndexDifferentialProbe.ExplainNodes(executor, database, dbname, distinct);
        AssertPlanUses(distinctNodes, "distinct(streaming: true)", distinct);
        Assert.IsTrue(distinctNodes.Any(n => n.Contains("forced-index=ab", System.StringComparison.Ordinal)),
            $"Expected the forced index scan for `{distinct}`; got: {string.Join(", ", distinctNodes)}");

        const string groupBy = "SELECT a, count(*) AS n FROM strict GROUP BY a";
        Assert.AreEqual("a=1,n=2;a=2,n=1", await IndexDifferentialProbe.QueryRendered(executor, database, dbname, groupBy));
        string[] groupNodes = await IndexDifferentialProbe.ExplainNodes(executor, database, dbname, groupBy);
        Assert.IsTrue(groupNodes.Any(n => n.StartsWith("aggregate(", System.StringComparison.Ordinal) && n.Contains("streaming: true", System.StringComparison.Ordinal)),
            $"Expected a streaming aggregate for `{groupBy}`; got: {string.Join(", ", groupNodes)}");
    }

    [Test]
    public async Task StreamingDistinct_NonUniqueCompositeWithNullableTrailing_StillStreamsAndIsComplete()
    {
        // A non-unique index stores every row, NULL keys included, so it stays eligible.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();
        const string query = "SELECT DISTINCT a FROM probe";

        await IndexDifferentialProbe.AssertSameRowsBeforeAndAfterIndex(
            executor, database, dbname, query, "CREATE INDEX ab ON probe (a, b)", "a=1;a=2");

        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, database, dbname, query);
        AssertPlanUses(nodes, "distinct(streaming: true)", query);
    }

    [Test]
    public async Task CorrelatedExistsSeek_UniqueCompositeWithNullableTrailing_KeepsOuterRow()
    {
        await RunCase(
            "SELECT p.a FROM probe p WHERE EXISTS (SELECT 1 FROM other o WHERE o.a = p.a)",
            "CREATE UNIQUE INDEX oab ON other (a, b)",
            "a=1;a=2");
    }

    [Test]
    public async Task MergeJoin_ForcedScanOverUniqueCompositeWithNullableTrailing_KeepsJoinRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();
        executor.Statistics.ForceMergeJoinForTesting = true;

        const string query = "SELECT p.a FROM probe p JOIN other o ON o.a = p.a";

        await IndexDifferentialProbe.AssertSameRowsBeforeAndAfterIndex(
            executor, database, dbname, query, "CREATE UNIQUE INDEX oab ON other (a, b)", "a=1;a=2");

        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, database, dbname, query);
        AssertPlanUses(nodes, "merge-join", query);
    }

    [Test]
    public async Task MergeJoin_ForcedScanOverUniqueCompositeAllNotNull_StillUsesIndex()
    {
        // With every key column NOT NULL the unique index holds every row, so the merge join
        // keeps its free ordering from the index scan instead of paying for a sort.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();
        await IndexDifferentialProbe.ExecDDL(executor, database, dbname,
            "CREATE TABLE strict (id OID PRIMARY KEY NOT NULL, a INT NOT NULL, b INT NOT NULL)");
        await IndexDifferentialProbe.ExecDML(executor, database, dbname,
            "INSERT INTO strict (id, a, b) VALUES (gen_id(), 1, 3)");
        await IndexDifferentialProbe.ExecDML(executor, database, dbname,
            "INSERT INTO strict (id, a, b) VALUES (gen_id(), 2, 5)");
        executor.Statistics.ForceMergeJoinForTesting = true;

        const string query = "SELECT p.a FROM probe p JOIN strict s ON s.a = p.a";

        await IndexDifferentialProbe.AssertSameRowsBeforeAndAfterIndex(
            executor, database, dbname, query, "CREATE UNIQUE INDEX sab ON strict (a, b)", "a=1;a=2");

        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, database, dbname, query);
        AssertPlanUses(nodes, "merge-join", query);
        Assert.IsTrue(nodes.Any(n => n.Contains("forced-index=sab", System.StringComparison.Ordinal)),
            $"Expected the merge join to read `sab` directly for `{query}`; got: {string.Join(", ", nodes)}");
    }

    // ── Control cases: shapes that must keep their index ─────────────────────

    [Test]
    public async Task RangeOnNullableColumn_NonUniqueIndex_KeepsNullRow()
    {
        // A non-unique index stores every row, NULL included; the NULL row fails `b < 10` in the
        // evaluator either way. This is the shape the review found clean.
        await RunCase("SELECT b FROM probe WHERE b < 10", "CREATE INDEX bi ON probe (b)", "b=5");
    }

    // ── NULL operands: a comparison is UNKNOWN, never true ───────────────────

    [TestCase("b < 10",  "b=5")]
    [TestCase("b <= 10", "b=5")]
    [TestCase("b > 0",   "b=5")]
    [TestCase("b >= 0",  "b=5")]
    [TestCase("b != 5",  "")]
    [TestCase("b = 5",   "b=5")]
    [TestCase("NOT (b < 10)", "")]
    public async Task NullOperand_ComparisonIsUnknown_OnTableScanAndIndex(string predicate, string expected)
    {
        // Row (a=1, b=NULL): `NULL < 10` is UNKNOWN, so the row is excluded on the table scan, and
        // the index range scan with an open lower side must not admit the NULL key entry either
        // (the planner drops the absorbed conjunct from the residual filter).
        await RunCase($"SELECT b FROM probe WHERE {predicate}", "CREATE INDEX bi ON probe (b)", expected);
    }

    [TestCase("b < 10 AND a = 1", "")]
    [TestCase("b < 10 OR a = 1",  "a=1;a=2")]
    [TestCase("NOT (b < 10 AND a = 1)", "a=2")]
    [TestCase("NOT (b < 10 OR a = 1)", "")]
    public async Task NullOperand_PropagatesThroughAndOrNot(string predicate, string expected)
    {
        // Three-valued logic: UNKNOWN AND TRUE is UNKNOWN (excluded); UNKNOWN OR TRUE is TRUE;
        // NOT UNKNOWN is UNKNOWN (excluded). Row a=1 has b=NULL, row a=2 has b=5.
        await RunCase($"SELECT a FROM probe WHERE {predicate}", "CREATE INDEX bi ON probe (b)", expected);
    }

    [Test]
    public async Task NullOperand_CaseWhenComparison_TakesElseBranch()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();
        string rows = await IndexDifferentialProbe.QueryRendered(executor, database, dbname,
            "SELECT a, CASE WHEN b < 10 THEN 1 ELSE 0 END AS c FROM probe");
        Assert.AreEqual("a=1,c=0;a=2,c=1", rows);
    }

    [Test]
    public async Task NullOperand_HavingComparison_IsUnknownAndMixedNumericWidens()
    {
        // Group a=1 has b=NULL only, so MAX(b) is NULL and `MAX(b) < 10` is UNKNOWN (group excluded).
        // Group a=2 has MAX(b)=5. AVG(b) is Float64; comparing it to an integer literal must widen.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();
        Assert.AreEqual("a=2,m=5", await IndexDifferentialProbe.QueryRendered(executor, database, dbname,
            "SELECT a, MAX(b) AS m FROM probe GROUP BY a HAVING MAX(b) < 10"));
        Assert.AreEqual("a=2,m=5", await IndexDifferentialProbe.QueryRendered(executor, database, dbname,
            "SELECT a, MAX(b) AS m FROM probe GROUP BY a HAVING MAX(b) < 10 OR MAX(b) > 100"));
        Assert.AreEqual("", await IndexDifferentialProbe.QueryRendered(executor, database, dbname,
            "SELECT a, MAX(b) AS m FROM probe GROUP BY a HAVING NOT (MAX(b) < 10)"));
        Assert.AreEqual("a=2,v=5", await IndexDifferentialProbe.QueryRendered(executor, database, dbname,
            "SELECT a, AVG(b) AS v FROM probe GROUP BY a HAVING AVG(b) > 1"));
    }

    [Test]
    public async Task NullOperand_EqualityPrefixWithOpenRange_OnMultiIndex_ExcludesNullKey()
    {
        // Composite non-unique (a, b): the scan for a = 1 AND b < 10 covers the (1, NULL) entry,
        // which sorts first under the prefix; the bound on b must exclude it.
        await RunCase("SELECT a FROM probe WHERE a = 1 AND b < 10", "CREATE INDEX ab ON probe (a, b)", "");
    }

    [Test]
    public async Task OrderBy_UniqueCompositeWithNullableTrailing_AlreadyGuarded()
    {
        await RunCase("SELECT a FROM probe ORDER BY a", "CREATE UNIQUE INDEX ab ON probe (a, b)", "a=1;a=2");
    }
}
