/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Config;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>WITH RECURSIVE</c> end to end, through <see cref="ExecuteSQLTicket"/>: the work-table method
/// (a counter, a tree walk, a graph with a cycle), the two limits, the forms of the recursive term
/// that PostgreSQL refuses and the ones it accepts, the statements a recursive CTE can feed, and a
/// view whose body has one. This fixture runs in cluster mode on a shared node;
/// <see cref="TestRecursiveCommonTableExpressionsStandalone"/> runs the same tests in standalone mode.
/// </summary>
[NonParallelizable]
internal class TestRecursiveCommonTableExpressions : SharedNodeBaseTest
{
    internal static async Task Ddl(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<ExecuteNonSQLResult> Dml(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
            return result;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    internal static async Task<List<QueryResultRow>> Query(
        CommandExecutor executor, DatabaseDescriptor database, string db, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static long[] Longs(List<QueryResultRow> rows, string column) =>
        rows.Select(r => r.Row[column].LongValue).ToArray();

    private static string[] Strings(List<QueryResultRow> rows, string column) =>
        rows.Select(r => r.Row[column].StrValue!).ToArray();

    /// <summary>
    /// A tree of six nodes under one root, and a directed graph with the cycle 1 → 2 → 3 → 1 and an
    /// edge 3 → 4 out of it.
    /// </summary>
    private async Task<(string Db, DatabaseDescriptor Database, CommandExecutor Executor)> Seeded(CamusDBOptions? options = null)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options ?? Options);
        await Ddl(executor, database, db, "CREATE TABLE node (id INT64 PRIMARY KEY, parent_id INT64, name STRING)");
        await Ddl(executor, database, db, "CREATE TABLE edges (eid INT64 PRIMARY KEY, src INT64, dst INT64)");
        await Dml(executor, database, db,
            "INSERT INTO node (id, parent_id, name) VALUES (1, NULL, 'root'), (2, 1, 'a'), (3, 1, 'b'), (4, 2, 'c'), (5, 2, 'd'), (6, 5, 'e')");
        await Dml(executor, database, db,
            "INSERT INTO edges (eid, src, dst) VALUES (1, 1, 2), (2, 2, 3), (3, 3, 1), (4, 3, 4)");
        return (db, database, executor);
    }

    [Test]
    public async Task ACounterRunsUntilItsConditionFails()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 1000) " +
            "SELECT COUNT(*) AS c, MIN(n) AS lo, MAX(n) AS hi, SUM(n) AS total FROM t");

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Row["c"].LongValue, Is.EqualTo(1000));
        Assert.That(rows[0].Row["lo"].LongValue, Is.EqualTo(1));
        Assert.That(rows[0].Row["hi"].LongValue, Is.EqualTo(1000));
        Assert.That(rows[0].Row["total"].LongValue, Is.EqualTo(500500));
    }

    [Test]
    public async Task ATreeWalkBuildsDepthsAndPaths()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "WITH RECURSIVE walk (id, depth, path) AS (" +
            "    SELECT id, 0, CAST(id AS STRING) FROM node WHERE parent_id IS NULL" +
            "  UNION ALL" +
            "    SELECT n.id, w.depth + 1, w.path || '/' || CAST(n.id AS STRING)" +
            "      FROM walk w JOIN node n ON n.parent_id = w.id" +
            ") SELECT id, depth, path FROM walk ORDER BY path");

        CollectionAssert.AreEqual(new[] { "1", "1/2", "1/2/4", "1/2/5", "1/2/5/6", "1/3" }, Strings(rows, "path"));
        CollectionAssert.AreEqual(new long[] { 0, 1, 2, 2, 3, 1 }, Longs(rows, "depth"));
    }

    [Test]
    public async Task UnionEndsAWalkOverACycle()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "WITH RECURSIVE reach (node) AS (" +
            "    SELECT 1 UNION SELECT e.dst FROM reach r JOIN edges e ON e.src = r.node" +
            ") SELECT node FROM reach ORDER BY node");

        CollectionAssert.AreEqual(new long[] { 1, 2, 3, 4 }, Longs(rows, "node"));
    }

    [Test]
    public async Task UnionAllOverACycleStopsAtTheStepLimit()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await Seeded(Options with { MaxRecursiveCteIterations = 50 });

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () => await Query(executor, database, db,
            "WITH RECURSIVE reach (node) AS (" +
            "    SELECT 1 UNION ALL SELECT e.dst FROM reach r JOIN edges e ON e.src = r.node" +
            ") SELECT node FROM reach"));

        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.RecursiveQueryLimitExceeded));
        Assert.That(error.Message, Does.Contain("max_recursive_cte_iterations").And.Contain("50 steps").And.Contain("'reach'"));
    }

    [Test]
    public async Task ARecursionOfExactlyTheStepLimitEnds()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await Seeded(Options with { MaxRecursiveCteIterations = 10 });

        // Ten steps make rows (2 to 11) after the non-recursive term, and the eleventh makes none.
        List<QueryResultRow> rows = await Query(executor, database, db,
            "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 11) SELECT n FROM t ORDER BY n");

        Assert.That(Longs(rows, "n"), Is.EqualTo(Enumerable.Range(1, 11).Select(i => (long)i).ToArray()));

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () => await Query(executor, database, db,
            "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 12) SELECT n FROM t"));

        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.RecursiveQueryLimitExceeded));
    }

    [Test]
    public async Task TheRowLimitStopsARecursionThatGrows()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await Seeded(Options with { MaxRecursiveCteRows = 100 });

        // Each step makes six rows for each row of the step before.
        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () => await Query(executor, database, db,
            "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT t.n + node.id FROM t, node WHERE t.n < 1000) SELECT n FROM t"));

        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.RecursiveQueryLimitExceeded));
        Assert.That(error.Message, Does.Contain("max_recursive_cte_rows").And.Contain("100 rows"));
    }

    [Test]
    public async Task ALimitStopsARecursionWithNoEnd()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t) SELECT n FROM t LIMIT 5");

        CollectionAssert.AreEqual(new long[] { 1, 2, 3, 4, 5 }, Longs(rows, "n"));
    }

    /// <summary>
    /// The forms of a recursive query that PostgreSQL 16 refuses, with the error each one gives here. The
    /// list follows the cases that were run against PostgreSQL 16.11.
    /// </summary>
    private static readonly (string Code, string Message, string Sql)[] RefusedForms =
    [
        (CamusDBErrorCodes.InvalidInput, "does not have the form", "WITH RECURSIVE t (n) AS (SELECT n FROM t) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "does not have the form", "WITH RECURSIVE t (n) AS (SELECT 1 INTERSECT SELECT n + 1 FROM t) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "does not have the form", "WITH RECURSIVE t (n) AS (SELECT 1 EXCEPT SELECT n + 1 FROM t) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "does not have the form", "WITH RECURSIVE node AS (SELECT id FROM node) SELECT id FROM node"),
        (CamusDBErrorCodes.InvalidInput, "within its non-recursive term", "WITH RECURSIVE t (n) AS (SELECT n FROM t UNION ALL SELECT 1) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "more than once", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT a.n + 1 FROM t a, t b WHERE a.n < 3) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "within a subquery", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT id FROM node WHERE id IN (SELECT n + 1 FROM t)) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "within a subquery", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT id FROM node WHERE EXISTS (SELECT 1 FROM t WHERE t.n = node.id)) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "within a subquery", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT (SELECT MAX(n) FROM t) FROM node) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "within an outer join", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT node.id FROM node LEFT JOIN t ON t.n = node.parent_id WHERE node.id > 100) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "within an outer join", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT node.id FROM t RIGHT JOIN node ON t.n = node.parent_id WHERE node.id > 100) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "within an outer join", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT node.id FROM node LEFT JOIN (SELECT n FROM t) s ON s.n = node.parent_id WHERE node.id > 100) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "within INTERSECT ALL", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL (SELECT n + 1 FROM t WHERE n < 3 INTERSECT ALL SELECT id FROM node)) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "within EXCEPT", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL (SELECT id FROM node EXCEPT SELECT n FROM t)) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "within EXCEPT", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL (SELECT n + 1 FROM t WHERE n < 3 EXCEPT ALL SELECT id FROM node)) SELECT n FROM t"),
        (CamusDBErrorCodes.FeatureNotSupported, "ORDER BY in a recursive query", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3 ORDER BY 1) SELECT n FROM t"),
        (CamusDBErrorCodes.FeatureNotSupported, "LIMIT in a recursive query", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3 LIMIT 2) SELECT n FROM t"),
        (CamusDBErrorCodes.FeatureNotSupported, "OFFSET in a recursive query", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3 OFFSET 1) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "Aggregate functions are not allowed", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT MAX(n) + 1 FROM t WHERE n < 3) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "Aggregate functions are not allowed", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT COUNT(*) + 1 FROM t WHERE n < 3) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "Aggregate functions are not allowed", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT m + 1 FROM (SELECT MAX(n) AS m FROM t) s WHERE m < 3) SELECT n FROM t"),
        (CamusDBErrorCodes.FeatureNotSupported, "Mutual recursion", "WITH RECURSIVE a (x) AS (SELECT 1 UNION ALL SELECT x FROM b), b (x) AS (SELECT 1 UNION ALL SELECT x FROM a) SELECT x FROM a"),
        (CamusDBErrorCodes.InvalidInput, "column 1 has type Integer64 in the non-recursive term but type Float64 overall", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1.5 FROM t WHERE n < 3) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "column 1 has type String in the non-recursive term", "WITH RECURSIVE t (n) AS (SELECT NULL UNION ALL SELECT 1 FROM t WHERE n IS NULL) SELECT n FROM t"),
        (CamusDBErrorCodes.InvalidInput, "columns available", "WITH RECURSIVE t (n, m) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT n FROM t"),
    ];

    [Test]
    public async Task TheFormsPostgreSqlRefusesAreRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<string> wrong = [];

        foreach ((string code, string message, string sql) in RefusedForms)
        {
            try
            {
                await Query(executor, database, db, sql);
                wrong.Add($"accepted: {sql}");
            }
            catch (CamusDBException ex) when (ex.Code == code && ex.Message.Contains(message))
            {
            }
            catch (CamusDBException ex)
            {
                wrong.Add($"{ex.Code} '{ex.Message}': {sql}");
            }
        }

        Assert.That(wrong, Is.Empty, string.Join("\n", wrong));
    }

    /// <summary>The forms of a recursive query that PostgreSQL 16 accepts, with the rows it returns.</summary>
    private static readonly (long[] Expected, string Sql)[] AcceptedForms =
    [
        ([1, 2, 3], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM (SELECT n FROM t) s WHERE n < 3) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3, 4, 5, 6], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT node.id FROM t LEFT JOIN node ON node.parent_id = t.n WHERE node.id IS NOT NULL) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3, 4, 5, 6], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT node.id FROM node RIGHT JOIN t ON node.parent_id = t.n WHERE node.id IS NOT NULL) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3 GROUP BY n + 1) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT DISTINCT n + 1 FROM t, node WHERE n < 3) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT ROW_NUMBER() OVER () + n FROM t WHERE n < 3) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL (SELECT n + 1 FROM t WHERE n < 3 ORDER BY 1 LIMIT 1)) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT MAX(m) + 1 FROM (SELECT n AS m FROM t) s HAVING MAX(m) < 3) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL (SELECT n + 1 FROM t WHERE n < 3 UNION ALL SELECT id FROM node WHERE id < 0)) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL (SELECT n + 1 FROM t WHERE n < 3 INTERSECT SELECT id FROM node)) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL (SELECT n + 1 FROM t WHERE n < 3 EXCEPT SELECT id + 100 FROM node)) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3, 4, 5, 6], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < (SELECT MAX(id) FROM node)) SELECT n FROM t ORDER BY n"),
        ([1, 2, 11, 12], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT n + 10 FROM t WHERE n < 10) SELECT n FROM t ORDER BY n"),
        ([1], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT 1 UNION SELECT n FROM t WHERE n < 0) SELECT COUNT(*) AS n FROM t"),
        ([1, 2, 3], "WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT n FROM t ORDER BY n"),
        ([1], "WITH RECURSIVE a AS (SELECT x AS n FROM b), b AS (SELECT 1 AS x) SELECT n FROM a"),
        ([1, 2, 3], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + x FROM t, b WHERE n < 3), b AS (SELECT 1 AS x) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3], "WITH RECURSIVE t (n) AS (SELECT x FROM b UNION ALL SELECT n + 1 FROM t WHERE n < 3), b AS (SELECT 1 AS x) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3, 10, 20, 30, 100, 200, 300], "WITH RECURSIVE a (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM a WHERE n < 3), b (n) AS (SELECT n FROM a UNION ALL SELECT n * 10 FROM b WHERE n < 100) SELECT n FROM b ORDER BY n"),
        ([1, 2, 3], "WITH RECURSIVE node (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM node WHERE n < 3) SELECT n FROM node ORDER BY n"),
        ([1, 2, 3], "WITH RECURSIVE t (n) AS MATERIALIZED (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3], "WITH RECURSIVE t (n) AS NOT MATERIALIZED (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3], "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT w.n + 1 FROM t AS w WHERE w.n < 3) SELECT n FROM t ORDER BY n"),
        ([1, 2, 3], "SELECT n FROM (WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT n FROM t) s ORDER BY n"),
        ([3], "SELECT (WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT MAX(n) FROM t) AS n"),
        ([2, 3, 4], "SELECT n FROM (SELECT n + 1 AS n FROM (WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT n FROM t) a) b ORDER BY n"),
    ];

    [Test]
    public async Task TheFormsPostgreSqlAcceptsReturnItsRows()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<string> wrong = [];

        foreach ((long[] expected, string sql) in AcceptedForms)
        {
            try
            {
                long[] actual = Longs(await Query(executor, database, db, sql), "n");
                if (!actual.SequenceEqual(expected))
                    wrong.Add($"[{string.Join(", ", actual)}]: {sql}");
            }
            catch (CamusDBException ex)
            {
                wrong.Add($"{ex.Code} '{ex.Message}': {sql}");
            }
        }

        Assert.That(wrong, Is.Empty, string.Join("\n", wrong));
    }

    [Test]
    public async Task AStringColumnGrowsAndAShortColumnListKeepsTheLaterNames()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> grown = await Query(executor, database, db,
            "WITH RECURSIVE t (s) AS (SELECT 'a' UNION ALL SELECT s || 'a' FROM t WHERE LENGTH(s) < 3) SELECT s FROM t ORDER BY s");
        CollectionAssert.AreEqual(new[] { "a", "aa", "aaa" }, Strings(grown, "s"));

        List<QueryResultRow> named = await Query(executor, database, db,
            "WITH RECURSIVE t (n) AS (SELECT 1, 'x' AS label UNION ALL SELECT n + 1, label FROM t WHERE n < 3) SELECT n, label FROM t ORDER BY n");
        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, Longs(named, "n"));
        CollectionAssert.AreEqual(new[] { "x", "x", "x" }, Strings(named, "label"));
    }

    [Test]
    public async Task ACorrelatedRecursiveSubqueryRunsForEachRow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT id, (WITH RECURSIVE t (n) AS (SELECT node.id UNION ALL SELECT n + 1 FROM t WHERE n < 5) SELECT COUNT(*) FROM t) AS c " +
            "FROM node ORDER BY id");

        CollectionAssert.AreEqual(new long[] { 5, 4, 3, 2, 1, 1 }, Longs(rows, "c"));
    }

    [Test]
    public async Task ARecursiveCteReadTwiceRunsOnce()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> pairs = await Query(executor, database, db,
            "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5) " +
            "SELECT a.n FROM t a JOIN t b ON b.n = a.n + 1 ORDER BY a.n");
        CollectionAssert.AreEqual(new long[] { 1, 2, 3, 4 }, Longs(pairs, "n"));

        // A volatile value is the same for both readers, because the recursion ran one time.
        List<QueryResultRow> same = await Query(executor, database, db,
            "WITH RECURSIVE t (n, r) AS (SELECT 1, random() UNION ALL SELECT n + 1, random() FROM t WHERE n < 3) " +
            "SELECT COUNT(*) AS n FROM t a JOIN t b ON a.n = b.n AND a.r = b.r");
        CollectionAssert.AreEqual(new long[] { 3 }, Longs(same, "n"));

        List<QueryResultRow> plan = await Query(executor, database, db,
            "EXPLAIN WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5) " +
            "SELECT a.n FROM t a JOIN t b ON b.n = a.n");
        string text = string.Join("\n", plan.Select(r => string.Join(" ", r.Row.Values.Select(v => v.StrValue))));
        StringAssert.Contains("computed_once=t", text);
        StringAssert.Contains("recursive-union all, cte=t", text);
    }

    [Test]
    public async Task ExplainShowsTheRecursionAndTheWorkTable()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> plan = await Query(executor, database, db,
            "EXPLAIN WITH RECURSIVE walk (id) AS (SELECT id FROM node WHERE parent_id IS NULL " +
            "UNION SELECT n.id FROM walk w JOIN node n ON n.parent_id = w.id) SELECT id FROM walk");

        string text = string.Join("\n", plan.Select(r => string.Join(" ", r.Row.Values.Select(v => v.StrValue))));
        StringAssert.Contains("recursive-union distinct: hash, cte=walk, alias=walk", text);
        StringAssert.Contains("operand left", text);
        StringAssert.Contains("operand right", text);

        // A recursive term that reads only the work table shows its scan.
        List<QueryResultRow> counter = await Query(executor, database, db,
            "EXPLAIN WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t AS w WHERE n < 3) SELECT n FROM t");

        string counterText = string.Join("\n", counter.Select(r => string.Join(" ", r.Row.Values.Select(v => v.StrValue))));
        StringAssert.Contains("recursive-union all, cte=t, alias=t", counterText);
        StringAssert.Contains("work-table-scan cte=t, alias=w", counterText);
    }

    [Test]
    public async Task TheWriteStatementsReadARecursiveCte()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // The subtree under node 2: nodes 2, 4, 5 and 6.
        const string subtree = "WITH RECURSIVE sub (id) AS (SELECT 2 UNION ALL SELECT n.id FROM sub s JOIN node n ON n.parent_id = s.id) ";

        await Dml(executor, database, db, subtree + "UPDATE node SET name = 'moved' WHERE id IN (SELECT id FROM sub)");
        List<QueryResultRow> moved = await Query(executor, database, db, "SELECT id FROM node WHERE name = 'moved' ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 2, 4, 5, 6 }, Longs(moved, "id"));

        await Dml(executor, database, db,
            "WITH RECURSIVE t (n) AS (SELECT 100 UNION ALL SELECT n + 1 FROM t WHERE n < 104) " +
            "INSERT INTO node (id, parent_id, name) SELECT n, 6, 'gen' FROM t");
        await Dml(executor, database, db,
            "INSERT INTO node (id, parent_id, name) " +
            "WITH RECURSIVE t (n) AS (SELECT 200 UNION ALL SELECT n + 1 FROM t WHERE n < 201) SELECT n, 6, 'gen' FROM t");
        List<QueryResultRow> generated = await Query(executor, database, db, "SELECT id FROM node WHERE name = 'gen' ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 100, 101, 102, 103, 104, 200, 201 }, Longs(generated, "id"));

        // Deleting the subtree under node 5 takes the generated rows under node 6 with it.
        await Dml(executor, database, db,
            "WITH RECURSIVE sub (id) AS (SELECT 5 UNION ALL SELECT n.id FROM sub s JOIN node n ON n.parent_id = s.id) " +
            "DELETE FROM node WHERE id IN (SELECT id FROM sub)");
        List<QueryResultRow> left = await Query(executor, database, db, "SELECT id FROM node ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 1, 2, 3, 4 }, Longs(left, "id"));

        await Ddl(executor, database, db,
            "CREATE TABLE nums AS WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 4) SELECT n FROM t");
        List<QueryResultRow> nums = await Query(executor, database, db, "SELECT n FROM nums ORDER BY n");
        CollectionAssert.AreEqual(new long[] { 1, 2, 3, 4 }, Longs(nums, "n"));
    }

    [Test]
    public async Task AViewWithARecursiveBodyStoresAWithRecursiveThatParsesAgain()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await Ddl(executor, database, db,
            "CREATE VIEW subtree (id, depth) AS WITH RECURSIVE w (id, depth) AS (" +
            "SELECT id, 0 FROM node WHERE id = 2 UNION ALL SELECT n.id, w.depth + 1 FROM w JOIN node n ON n.parent_id = w.id" +
            ") SELECT id, depth FROM w");

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT id, depth FROM subtree ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 2, 4, 5, 6 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long[] { 0, 1, 1, 2 }, Longs(rows, "depth"));

        string shown = (await Query(executor, database, db, "SHOW CREATE VIEW subtree"))[0].Row["create view"].StrValue!;
        StringAssert.Contains("WITH RECURSIVE w (id, depth) AS (", shown);

        // A dump writes this text and a reimport runs it. The view it makes stores the same text.
        await Ddl(executor, database, db, "DROP VIEW subtree");
        await Ddl(executor, database, db, shown);
        string again = (await Query(executor, database, db, "SHOW CREATE VIEW subtree"))[0].Row["create view"].StrValue!;
        Assert.That(again, Is.EqualTo(shown));

        List<QueryResultRow> reread = await Query(executor, database, db, "SELECT id FROM subtree ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 2, 4, 5, 6 }, Longs(reread, "id"));
    }

    [Test]
    public async Task AViewShowsARecursiveCteUnderAFreeNameWhenItsQueryReadsATableOfItsName()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // The outer CTE x reads the table node, and the inner recursive CTE is also named node, so in a
        // text with WITH RECURSIVE node the read of x would reach the CTE instead of the table.
        const string body =
            "WITH x AS (SELECT id FROM node) SELECT n FROM (WITH RECURSIVE node (n) AS (" +
            "SELECT 1 UNION ALL SELECT node.n + 1 FROM node, x WHERE node.n < 3 AND x.id = 1) SELECT n FROM node) s";

        List<QueryResultRow> rows = await Query(executor, database, db, body + " ORDER BY n");
        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, Longs(rows, "n"));

        await Ddl(executor, database, db, "CREATE VIEW v AS " + body);
        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, Longs(await Query(executor, database, db, "SELECT n FROM v ORDER BY n"), "n"));

        string shown = (await Query(executor, database, db, "SHOW CREATE VIEW v"))[0].Row["create view"].StrValue!;
        StringAssert.Contains("WITH RECURSIVE node_1 (n) AS (", shown);
        StringAssert.Contains("FROM node_1 AS node", shown);

        // The shown text makes the same view again.
        await Ddl(executor, database, db, "DROP VIEW v");
        await Ddl(executor, database, db, shown);
        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, Longs(await Query(executor, database, db, "SELECT n FROM v ORDER BY n"), "n"));
        Assert.That((await Query(executor, database, db, "SHOW CREATE VIEW v"))[0].Row["create view"].StrValue, Is.EqualTo(shown));
    }

    [Test]
    public async Task PlaceholdersInTheTermsAndACastPlaceholderAsTheStart()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@start"] = new ColumnValue(ColumnType.Integer64, 3L),
            ["@max"] = new ColumnValue(ColumnType.Integer64, 7L),
        };

        // The recursive term reads @max at every step.
        List<QueryResultRow> counted = await Query(executor, database, db,
            "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < @max) SELECT COUNT(*) AS n FROM t", parameters);
        CollectionAssert.AreEqual(new long[] { 7 }, Longs(counted, "n"));

        // A placeholder alone in the non-recursive term has no type there, so a cast gives it one.
        List<QueryResultRow> started = await Query(executor, database, db,
            "WITH RECURSIVE t (n) AS (SELECT CAST(@start AS INT64) UNION ALL SELECT n + 1 FROM t WHERE n < @max) SELECT n FROM t ORDER BY n",
            parameters);
        CollectionAssert.AreEqual(new long[] { 3, 4, 5, 6, 7 }, Longs(started, "n"));
    }

    [Test]
    public async Task ExplainAnalyzeRunsARecursiveQuery()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> plan = await Query(executor, database, db,
            "EXPLAIN (ANALYZE) WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 50) SELECT n FROM t");

        string text = string.Join("\n", plan.Select(r => string.Join(" ", r.Row.Values.Select(v => v.StrValue))));
        StringAssert.Contains("recursive-union all, cte=t, alias=t", text);
    }

    [Test]
    public async Task AMaterializedViewWithARecursiveBodyRefreshes()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await Ddl(executor, database, db,
            "CREATE MATERIALIZED VIEW below_two (id) AS WITH RECURSIVE sub (id) AS (" +
            "SELECT 2 UNION ALL SELECT n.id FROM sub s JOIN node n ON n.parent_id = s.id) SELECT id FROM sub");

        CollectionAssert.AreEqual(new long[] { 2, 4, 5, 6 }, Longs(await Query(executor, database, db, "SELECT id FROM below_two ORDER BY id"), "id"));

        await Dml(executor, database, db, "INSERT INTO node (id, parent_id, name) VALUES (7, 6, 'f')");
        await Dml(executor, database, db, "REFRESH MATERIALIZED VIEW below_two");

        CollectionAssert.AreEqual(new long[] { 2, 4, 5, 6, 7 }, Longs(await Query(executor, database, db, "SELECT id FROM below_two ORDER BY id"), "id"));
    }

    [Test]
    public async Task TheWorkTableSpills()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await Seeded(Options with { SpillEnabled = true, ForceSpillThresholdRows = 2 });

        // Each step makes six rows, so every step's work table spills at the forced threshold of two.
        List<QueryResultRow> all = await Query(executor, database, db,
            "WITH RECURSIVE t (n, step) AS (SELECT 0, 0 UNION ALL SELECT t.n * 10 + node.id, step + 1 FROM t, node WHERE step < 3) " +
            "SELECT COUNT(*) AS c, MAX(step) AS hi FROM t");
        Assert.That(all[0].Row["c"].LongValue, Is.EqualTo(1 + 6 + 36 + 216));
        Assert.That(all[0].Row["hi"].LongValue, Is.EqualTo(3));

        List<QueryResultRow> distinct = await Query(executor, database, db,
            "WITH RECURSIVE t (n) AS (SELECT 1 UNION SELECT (n * 7) % 1000 FROM t) SELECT COUNT(*) AS c FROM t");
        Assert.That(distinct[0].Row["c"].LongValue, Is.GreaterThan(1));
    }
}

/// <summary>The tests of <see cref="TestRecursiveCommonTableExpressions"/> on an engine in standalone mode.</summary>
[NonParallelizable]
internal sealed class TestRecursiveCommonTableExpressionsStandalone : TestRecursiveCommonTableExpressions
{
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: false);
    }
}

/// <summary>
/// The two limits are runtime settings: the recursive UNION reads them from its database's options each
/// time it runs, so a published change applies to the next statement without a restart.
/// </summary>
[NonParallelizable]
internal sealed class TestRecursiveCteLimitsAreRuntimeSettings : BaseTest
{
    [Test]
    public async Task APublishedLimitAppliesToTheNextStatement()
    {
        CamusDBOptions initial = Options with { MaxRecursiveCteIterations = 5 };
        CamusDBOptionsHolder holder = new(initial);

        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabaseWith(CreateCommandExecutor(initial, holder));

        const string counter = "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 20) SELECT n FROM t";

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(
            () => TestRecursiveCommonTableExpressions.Query(executor, database, db, counter))!;
        Assert.That(refused.Code, Is.EqualTo(CamusDBErrorCodes.RecursiveQueryLimitExceeded));

        holder.Publish(initial with { MaxRecursiveCteIterations = 0 });
        Assert.That(await TestRecursiveCommonTableExpressions.Query(executor, database, db, counter), Has.Count.EqualTo(20));

        holder.Publish(initial with { MaxRecursiveCteIterations = 0, MaxRecursiveCteRows = 10 });
        CamusDBException rows = Assert.ThrowsAsync<CamusDBException>(
            () => TestRecursiveCommonTableExpressions.Query(executor, database, db, counter))!;
        StringAssert.Contains("max_recursive_cte_rows", rows.Message);
    }
}
