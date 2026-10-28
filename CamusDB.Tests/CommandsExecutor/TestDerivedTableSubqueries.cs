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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A subquery in the WHERE of a derived table's inner query, of a view body, or of a set-operation
/// operand. The binder used to bind those nested SELECTs by itself, without the semi-join, rewrite
/// and EXISTS stages a top-level SELECT runs, so the subquery node reached the per-row evaluator and
/// the statement failed with "subquery must be resolved before expression evaluation". The shapes
/// here are the ones an ORM writes for a count with a relation filter:
/// <c>SELECT COUNT(*) FROM (SELECT … WHERE [NOT] EXISTS (…)) AS sub</c>.
///
/// <para>Cluster mode here; <see cref="TestDerivedTableSubqueriesStandalone"/> runs every case in
/// standalone mode.</para>
/// </summary>
[NonParallelizable]
public class TestDerivedTableSubqueries : SharedNodeBaseTest
{
    private static async Task ExecDdl(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<long> ExecNonQuery(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        long affected = (await executor.ExecuteNonSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null))).ModifiedRows;
        await database.Transactions.CommitAsync(tx);
        return affected;
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    /// <summary>The <c>id</c> column of every row, sorted, for a result whose order is not defined.</summary>
    private static async Task<List<long>> Ids(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, sql, parameters);
        return rows.Select(r => r.Row["id"].LongValue).OrderBy(x => x).ToList();
    }

    /// <summary>The one cell of the one row of an aggregate query, whatever its output name.</summary>
    private static async Task<long> Scalar(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, sql);
        Assert.AreEqual(1, rows.Count, sql);
        Assert.AreEqual(1, rows[0].Row.Count, sql);
        return rows[0].Row.Values.First().LongValue;
    }

    /// <summary>
    /// Two tables with no index on <c>b.y</c>, so an <c>IN (SELECT y FROM b)</c> cannot become a
    /// semi-join and must be materialized, which is the stage the derived-table path never ran.
    /// <c>a.x</c> is in {1, 2} for ids 1-4, 3 for id 5, NULL for id 6; <c>b.y</c> holds {1, 2, 4, NULL}.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> Setup()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname, "CREATE TABLE a (id int64 PRIMARY KEY, x int64 NULL)");
        await ExecDdl(executor, database, dbname, "CREATE TABLE b (id int64 PRIMARY KEY, y int64 NULL)");

        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO a (id, x) VALUES (1, 1), (2, 1), (3, 1), (4, 2), (5, 3), (6, NULL)");
        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO b (id, y) VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 4), (6, NULL)");

        return (dbname, database, executor);
    }

    // ── IN / NOT IN / scalar in a derived table ────────────────────────────────

    [Test]
    public async Task InSubquery_InADerivedTable_IsMaterialized()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        Assert.AreEqual(new List<long> { 1, 2, 3, 4 },
            await Ids(executor, database, dbname,
                "SELECT s.id FROM (SELECT id FROM a WHERE x IN (SELECT y FROM b)) AS s"));

        // A NULL in the list makes NOT IN unknown for every row, so the list is filtered first.
        Assert.AreEqual(new List<long> { 5 },
            await Ids(executor, database, dbname,
                "SELECT s.id FROM (SELECT id FROM a WHERE x NOT IN (SELECT y FROM b WHERE y IS NOT NULL)) AS s"));

        Assert.AreEqual(4,
            await Scalar(executor, database, dbname,
                "SELECT COUNT(*) FROM (SELECT id FROM a WHERE x IN (SELECT y FROM b)) AS sub"));

        Assert.AreEqual(new List<long> { 1, 2, 3 },
            await Ids(executor, database, dbname,
                "SELECT s.id FROM (SELECT id FROM a WHERE x = (SELECT min(y) FROM b)) AS s"));
    }

    [Test]
    public async Task InSubquery_InADerivedTable_OverAnIndexedColumn_IsASemiJoin()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        // b.id is the primary key, so this IN is eligible for the semi-join rewrite; the spec it
        // produces must reach the inner query's ticket, or the planner scans without the predicate.
        Assert.AreEqual(new List<long> { 1, 2, 3, 4, 5 },
            await Ids(executor, database, dbname,
                "SELECT s.id FROM (SELECT id FROM a WHERE x IN (SELECT id FROM b)) AS s"));

        Assert.AreEqual(new List<long> { 1, 2, 3 },
            await Ids(executor, database, dbname,
                "SELECT s.id FROM (SELECT id FROM a WHERE x NOT IN (SELECT id FROM b WHERE id > 1)) AS s"));

        Assert.AreEqual(5,
            await Scalar(executor, database, dbname,
                "SELECT COUNT(*) FROM (SELECT id FROM a WHERE x IN (SELECT id FROM b)) AS sub"));
    }

    [Test]
    public async Task InSubquery_InADerivedTable_ReadsTheStatementParameters()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        Dictionary<string, ColumnValue> parameters = new() { ["@maxid"] = new ColumnValue(ColumnType.Integer64, 2) };

        // b rows 1 and 2 both hold y = 1.
        Assert.AreEqual(new List<long> { 1, 2, 3 },
            await Ids(executor, database, dbname,
                "SELECT s.id FROM (SELECT id FROM a WHERE x IN (SELECT y FROM b WHERE id <= @maxid)) AS s",
                parameters));
    }

    [Test]
    public async Task InSubquery_InADerivedTable_OverASetOperation()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        Assert.AreEqual(new List<long> { 1, 2, 3 },
            await Ids(executor, database, dbname,
                "SELECT s.id FROM (SELECT id FROM a WHERE x IN (SELECT y FROM b EXCEPT SELECT 2)) AS s"));
    }

    // ── EXISTS / NOT EXISTS correlated to the derived table's own table ───────

    [Test]
    public async Task CorrelatedExists_InADerivedTable_UnderCountAndUnderAProjection()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        Assert.AreEqual(new List<long> { 1, 2, 3, 4 },
            await Ids(executor, database, dbname,
                "SELECT x.id FROM (SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y = a.x)) AS x"));

        Assert.AreEqual(4,
            await Scalar(executor, database, dbname,
                "SELECT COUNT(*) FROM (SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y = a.x)) AS sub"));

        // Row 6 has x = NULL: no b row equals it, so NOT EXISTS holds there too.
        Assert.AreEqual(new List<long> { 5, 6 },
            await Ids(executor, database, dbname,
                "SELECT x.id FROM (SELECT a.id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.y = a.x)) AS x"));

        Assert.AreEqual(2,
            await Scalar(executor, database, dbname,
                "SELECT COUNT(*) FROM (SELECT a.id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.y = a.x)) AS sub"));
    }

    [Test]
    public async Task CorrelatedExists_InADerivedTable_InTheShapeAnOrmWrites()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        // Quoted identifiers, an aliased inner table, and parenthesized comparison operands.
        Assert.AreEqual(4,
            await Scalar(executor, database, dbname,
                "SELECT COUNT(*) FROM (SELECT `a`.`id` FROM `a` WHERE EXISTS(SELECT `t0`.`y` FROM `b` AS `t0` " +
                "WHERE (`a`.`x`) = (`t0`.`y`))) AS `sub`"));

        Assert.AreEqual(new List<long> { 5, 6 },
            await Ids(executor, database, dbname,
                "SELECT `sub`.`id` FROM (SELECT `a`.`id` FROM `a` WHERE NOT EXISTS(SELECT `t0`.`y` FROM `b` AS `t0` " +
                "WHERE (`a`.`x`) = (`t0`.`y`))) AS `sub`"));
    }

    [Test]
    public async Task UncorrelatedExists_InADerivedTable_FoldsToALiteral()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        Assert.AreEqual(new List<long> { 1, 2, 3, 4, 5, 6 },
            await Ids(executor, database, dbname,
                "SELECT s.id FROM (SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE y = 4)) AS s"));

        Assert.AreEqual(new List<long>(),
            await Ids(executor, database, dbname,
                "SELECT s.id FROM (SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE y = 5)) AS s"));
    }

    // ── nesting, joins and the other statement kinds ──────────────────────────

    [Test]
    public async Task Subquery_TwoDerivedTablesDeep()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        Assert.AreEqual(new List<long> { 2, 3, 4 },
            await Ids(executor, database, dbname,
                "SELECT t.id FROM (SELECT s.id FROM (SELECT id FROM a WHERE x IN (SELECT y FROM b)) AS s WHERE s.id > 1) AS t"));

        Assert.AreEqual(new List<long> { 2, 3, 4 },
            await Ids(executor, database, dbname,
                "SELECT t.id FROM (SELECT s.id FROM (SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y = a.x)) AS s " +
                "WHERE s.id > 1) AS t"));
    }

    [Test]
    public async Task Subquery_InADerivedTable_ThatIsJoined()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        Assert.AreEqual(new List<long> { 1, 2, 3, 4 },
            await Ids(executor, database, dbname,
                "SELECT s.id FROM (SELECT a.id, a.x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y = a.x)) AS s " +
                "JOIN b ON b.id = s.id"));

        Assert.AreEqual(new List<long> { 1, 2, 3, 4 },
            await Ids(executor, database, dbname,
                "SELECT s.id FROM b JOIN (SELECT id FROM a WHERE x IN (SELECT y FROM b)) AS s ON b.id = s.id"));
    }

    [Test]
    public async Task Subquery_InADerivedTable_InsideAnUncorrelatedExists()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        // a.x > 1 gives {2, 3}; b has rows with y = 2, so the EXISTS holds for every a row.
        Assert.AreEqual(new List<long> { 1, 2, 3, 4, 5, 6 },
            await Ids(executor, database, dbname,
                "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM (SELECT y FROM b WHERE y IN (SELECT x FROM a WHERE x > 1)) AS d)"));
    }

    [Test]
    public async Task Subquery_InADerivedTable_AsTheSourceOfInsertSelect()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        await ExecDdl(executor, database, dbname, "CREATE TABLE c (id int64 PRIMARY KEY)");

        Assert.AreEqual(4, await ExecNonQuery(executor, database, dbname,
            "INSERT INTO c SELECT s.id FROM (SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y = a.x)) AS s"));

        Assert.AreEqual(new List<long> { 1, 2, 3, 4 }, await Ids(executor, database, dbname, "SELECT id FROM c"));
    }

    [Test]
    public async Task Explain_OverADerivedTableWithASubquery_ProducesAPlan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        Assert.IsNotEmpty(await ExecQuery(executor, database, dbname,
            "EXPLAIN SELECT COUNT(*) FROM (SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y = a.x)) AS sub"));

        Assert.IsNotEmpty(await ExecQuery(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT COUNT(*) FROM (SELECT id FROM a WHERE x IN (SELECT y FROM b)) AS sub"));
    }

    // ── set-operation operands ────────────────────────────────────────────────

    [Test]
    public async Task CorrelatedExists_InASetOperationOperand()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        Assert.AreEqual(new List<long> { 1, 2, 3, 4, 99 },
            await Ids(executor, database, dbname,
                "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y = a.x) UNION ALL SELECT 99 AS id"));

        Assert.AreEqual(new List<long> { 5, 6 },
            await Ids(executor, database, dbname,
                "SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.y = a.x) " +
                "INTERSECT SELECT id FROM a WHERE x IS NULL OR x NOT IN (SELECT y FROM b WHERE y IS NOT NULL)"));

        // EXPLAIN (ANALYZE) plans each operand in advance with the operand's own ticket, and that
        // plan is the one that runs.
        Assert.IsNotEmpty(await ExecQuery(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y = a.x) UNION ALL SELECT 99 AS id"));
    }

    // ── through a view ────────────────────────────────────────────────────────

    [Test]
    public async Task Subquery_InAViewBody()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        await ExecDdl(executor, database, dbname,
            "CREATE VIEW p1 AS SELECT id FROM a WHERE x IN (SELECT y FROM b)");
        await ExecDdl(executor, database, dbname,
            "CREATE VIEW p2 AS SELECT a.id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.y = a.x)");
        await ExecDdl(executor, database, dbname,
            "CREATE VIEW p3 AS SELECT id FROM a WHERE x IN (SELECT y FROM b) UNION SELECT id FROM a WHERE x = 3");
        await ExecDdl(executor, database, dbname,
            "CREATE VIEW p4 AS SELECT id FROM p1 WHERE id > 1");

        Assert.AreEqual(new List<long> { 1, 2, 3, 4 }, await Ids(executor, database, dbname, "SELECT id FROM p1"));
        Assert.AreEqual(4, await Scalar(executor, database, dbname, "SELECT COUNT(*) FROM p1"));

        Assert.AreEqual(new List<long> { 5, 6 }, await Ids(executor, database, dbname, "SELECT id FROM p2"));
        Assert.AreEqual(2, await Scalar(executor, database, dbname, "SELECT COUNT(*) FROM p2"));

        Assert.AreEqual(new List<long> { 1, 2, 3, 4, 5 }, await Ids(executor, database, dbname, "SELECT id FROM p3"));

        Assert.AreEqual(new List<long> { 2, 3, 4 }, await Ids(executor, database, dbname, "SELECT id FROM p4"));

        // A view read inside a derived table, and a view joined to a table.
        Assert.AreEqual(new List<long> { 2, 3, 4 },
            await Ids(executor, database, dbname, "SELECT s.id FROM (SELECT id FROM p1 WHERE id > 1) AS s"));
        Assert.AreEqual(new List<long> { 1, 2, 3, 4 },
            await Ids(executor, database, dbname, "SELECT p1.id FROM p1 JOIN b ON b.id = p1.id"));
    }
}

/// <summary>Every <see cref="TestDerivedTableSubqueries"/> case in standalone mode.</summary>
[NonParallelizable]
public sealed class TestDerivedTableSubqueriesStandalone : TestDerivedTableSubqueries
{
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: false);
    }
}
