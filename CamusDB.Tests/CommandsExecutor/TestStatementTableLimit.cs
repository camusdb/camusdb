/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>max_tables_per_statement</c> through SQL: a statement that reads more relations than the limit
/// fails with <see cref="CamusDBErrorCodes.TooManyTablesInStatement"/> before it runs, and a statement
/// at the limit runs. The count covers comma joins, JOIN chains, subqueries, CTEs and view bodies after
/// expansion, and every statement kind that reads relations. Most cases use a limit of four, so a fifth
/// relation shows the refusal; the counting rules on parsed trees are in <c>TestStatementTableCount</c>.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestStatementTableLimit : BaseTest
{
    private const int Limit = 4;

    internal static async Task<List<QueryResultRow>> Query(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction transaction = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(transaction, db, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(transaction);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(transaction);
        }
    }

    private static async Task<int> NonQuery(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction transaction = await database.Transactions.BeginAsync();

        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(transaction, db, sql, null));
            await database.Transactions.CommitAsync(transaction);
            return result.ModifiedRows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(transaction);
        }
    }

    private static async Task Ddl(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction transaction = await database.Transactions.BeginAsync();

        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(transaction, db, sql, null));
            await database.Transactions.CommitAsync(transaction);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(transaction);
        }
    }

    private static CamusDBException Refused(Func<Task> statement, int limit = Limit)
    {
        CamusDBException error = Assert.ThrowsAsync<CamusDBException>(async () => await statement())!;

        Assert.AreEqual(CamusDBErrorCodes.TooManyTablesInStatement, error.Code, error.Message);
        StringAssert.Contains("max_tables_per_statement", error.Message);
        StringAssert.Contains($"more than {limit} tables", error.Message);
        return error;
    }

    private static long Count(List<QueryResultRow> rows) => rows.Single().Row["c"].LongValue;

    /// <summary>Tables <c>a</c> to <c>e</c> and <c>t</c>, each with the rows 1 and 2, on an engine with <paramref name="limit"/>.</summary>
    private async Task<(string db, CommandExecutor executor)> Seeded(int limit = Limit)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(Options with { MaxTablesPerStatement = limit });

        foreach (string table in new[] { "a", "b", "c", "d", "e", "t" })
        {
            await Ddl(executor, db, $"CREATE TABLE {table} (id INT64 PRIMARY KEY, v INT64)");
            await NonQuery(executor, db, $"INSERT INTO {table} (id, v) VALUES (1, 10), (2, 20)");
        }

        return (db, executor);
    }

    private static string CommaJoin(int tables) =>
        "SELECT count(*) AS c FROM " + string.Join(", ", Enumerable.Range(1, tables).Select(i => $"t t{i}")) +
        " WHERE " + string.Join(" AND ", Enumerable.Range(1, tables).Select(i => $"t{i}.id = 1"));

    private static string JoinChain(int tables) =>
        "SELECT count(*) AS c FROM t t1" +
        string.Concat(Enumerable.Range(2, tables - 1).Select(i => $" JOIN t t{i} ON t{i - 1}.id = t{i}.id"));

    [Test]
    public async Task TheDefaultLimit_RunsSixtyFourCommaJoinedTables_AndRefusesSixtyFive()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)");
        await NonQuery(executor, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)");

        Assert.AreEqual(1, Count(await Query(executor, db, CommaJoin(64))));

        Refused(() => Query(executor, db, CommaJoin(65)), limit: 64);
    }

    [Test]
    public async Task TheDefaultLimit_RunsAJoinChainOfSixtyFourTables_AndRefusesSixtyFive()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)");
        await NonQuery(executor, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)");

        Assert.AreEqual(2, Count(await Query(executor, db, JoinChain(64))));

        Refused(() => Query(executor, db, JoinChain(65)), limit: 64);
    }

    [Test]
    public async Task TheTablesOfSubqueriesCount()
    {
        (string db, CommandExecutor executor) = await Seeded();

        const string atLimit =
            "SELECT count(*) AS c FROM a WHERE a.id IN (SELECT id FROM b) AND EXISTS (SELECT 1 FROM c, d WHERE c.id = a.id AND d.id = c.id)";
        Assert.AreEqual(2, Count(await Query(executor, db, atLimit)));

        // A scalar subquery in the select list is the fifth table.
        Refused(() => Query(executor, db,
            "SELECT (SELECT max(id) FROM e) AS m FROM a WHERE a.id IN (SELECT id FROM b) AND EXISTS (SELECT 1 FROM c, d WHERE c.id = a.id)"));
    }

    [Test]
    public async Task TheTablesOfADerivedTableAndOfEachUnionBranchCount()
    {
        (string db, CommandExecutor executor) = await Seeded();

        Assert.AreEqual(4, Count(await Query(executor, db,
            "SELECT count(*) AS c FROM (SELECT a.id FROM a, b WHERE a.id = b.id UNION ALL SELECT c.id FROM c, d WHERE c.id = d.id) AS u")));

        Refused(() => Query(executor, db,
            "SELECT count(*) AS c FROM e, (SELECT a.id FROM a, b WHERE a.id = b.id UNION ALL SELECT c.id FROM c, d WHERE c.id = d.id) AS u"));
    }

    [Test]
    public async Task EachValuesListCountsAsOne()
    {
        (string db, CommandExecutor executor) = await Seeded();

        Assert.AreEqual(1, Count(await Query(executor, db,
            "SELECT count(*) AS c FROM (VALUES (1)) AS w (x), (VALUES (1)) AS x (x), (VALUES (1)) AS y (x), (VALUES (1)) AS z (x)")));

        Refused(() => Query(executor, db,
            "SELECT count(*) AS c FROM (VALUES (1)) AS v (x), (VALUES (1)) AS w (x), (VALUES (1)) AS x (x), (VALUES (1)) AS y (x), (VALUES (1)) AS z (x)"));
    }

    [Test]
    public async Task AnInlinedCteCountsAtEachReference_AndACteComputedOnceCountsOnce()
    {
        (string db, CommandExecutor executor) = await Seeded();

        // Read two times, so it is computed once: two tables for the query, one for the second reader.
        Assert.AreEqual(2, Count(await Query(executor, db,
            "WITH ab AS (SELECT a.id FROM a, b WHERE a.id = b.id) SELECT count(*) AS c FROM ab x JOIN ab y ON x.id = y.id")));

        // Inlined at each reference: four tables, then a fifth.
        Assert.AreEqual(2, Count(await Query(executor, db,
            "WITH ab AS NOT MATERIALIZED (SELECT a.id FROM a, b WHERE a.id = b.id) SELECT count(*) AS c FROM ab x JOIN ab y ON x.id = y.id")));

        Refused(() => Query(executor, db,
            "WITH ab AS NOT MATERIALIZED (SELECT a.id FROM a, b WHERE a.id = b.id) " +
            "SELECT count(*) AS c FROM ab x JOIN ab y ON x.id = y.id JOIN e ON e.id = y.id"));
    }

    [Test]
    public async Task AViewCountsTheTablesOfItsBody_AlsoThroughAnotherView()
    {
        (string db, CommandExecutor executor) = await Seeded();

        await Ddl(executor, db, "CREATE VIEW abc AS SELECT a.id FROM a, b, c WHERE a.id = b.id AND b.id = c.id");
        await Ddl(executor, db, "CREATE VIEW abcd AS SELECT abc.id FROM abc, d WHERE abc.id = d.id");

        Assert.AreEqual(2, Count(await Query(executor, db, "SELECT count(*) AS c FROM abc, d WHERE abc.id = d.id")));
        Assert.AreEqual(2, Count(await Query(executor, db, "SELECT count(*) AS c FROM abcd")));

        // The statement names two relations; the view bodies make them five.
        Refused(() => Query(executor, db, "SELECT count(*) AS c FROM abc, e WHERE abc.id = e.id AND abc.id IN (SELECT id FROM d)"));
        Refused(() => Query(executor, db, "SELECT count(*) AS c FROM abcd, e WHERE abcd.id = e.id"));
    }

    [Test]
    public async Task AViewWhoseExpandedBodyIsPastTheLimit_IsRefusedAtCreate()
    {
        (string db, CommandExecutor executor) = await Seeded();

        await Ddl(executor, db, "CREATE VIEW abc AS SELECT a.id FROM a, b, c WHERE a.id = b.id AND b.id = c.id");

        Refused(() => Ddl(executor, db, "CREATE VIEW wide AS SELECT abc.id FROM abc, d, e WHERE abc.id = d.id AND d.id = e.id"));

        CamusDBException missing = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, db, "SELECT count(*) AS c FROM wide"))!;
        Assert.AreNotEqual(CamusDBErrorCodes.TooManyTablesInStatement, missing.Code, "the refused view must not exist");

        Refused(() => Ddl(executor, db, "CREATE MATERIALIZED VIEW wide_mv AS SELECT abc.id FROM abc, d, e WHERE abc.id = d.id AND d.id = e.id"));

        missing = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, db, "SELECT count(*) AS c FROM wide_mv"))!;
        Assert.AreNotEqual(CamusDBErrorCodes.TooManyTablesInStatement, missing.Code, "the refused materialized view must not exist");
    }

    [Test]
    public async Task ExplainIsRefusedLikeTheStatement()
    {
        (string db, CommandExecutor executor) = await Seeded();

        Assert.IsNotEmpty(await Query(executor, db, "EXPLAIN SELECT a.id FROM a, b, c, d"));

        Refused(() => Query(executor, db, "EXPLAIN SELECT a.id FROM a, b, c, d, e"));
    }

    [Test]
    public async Task TheTargetOfAnUpdateOrDeleteCounts_WithItsFromOrUsingList()
    {
        (string db, CommandExecutor executor) = await Seeded();

        Assert.AreEqual(1, await NonQuery(executor, db,
            "UPDATE t SET v = 11 FROM a, b, c WHERE a.id = t.id AND b.id = a.id AND c.id = b.id AND t.id = 1"));
        Refused(() => NonQuery(executor, db,
            "UPDATE t SET v = 12 FROM a, b, c, d WHERE a.id = t.id AND b.id = a.id AND c.id = b.id AND d.id = c.id AND t.id = 1"));

        Assert.AreEqual(1, await NonQuery(executor, db,
            "DELETE FROM t USING a, b, c WHERE a.id = t.id AND b.id = a.id AND c.id = b.id AND t.id = 2"));
        Refused(() => NonQuery(executor, db,
            "DELETE FROM t USING a, b, c, d WHERE a.id = t.id AND b.id = a.id AND c.id = b.id AND d.id = c.id AND t.id = 1"));

        // The refused statements changed nothing.
        List<QueryResultRow> rows = await Query(executor, db, "SELECT id, v FROM t");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(11, rows.Single().Row["v"].LongValue);
    }

    [Test]
    public async Task TheSubqueriesOfAPlainUpdateDeleteAndInsertCount()
    {
        (string db, CommandExecutor executor) = await Seeded();

        Assert.AreEqual(2, await NonQuery(executor, db, "UPDATE t SET v = 30 WHERE id IN (SELECT a.id FROM a, b, c WHERE a.id = b.id AND b.id = c.id)"));
        Refused(() => NonQuery(executor, db, "UPDATE t SET v = 40 WHERE id IN (SELECT a.id FROM a, b, c, d WHERE a.id = b.id)"));
        Refused(() => NonQuery(executor, db, "DELETE FROM t WHERE id IN (SELECT a.id FROM a, b, c, d WHERE a.id = b.id)"));
        Refused(() => NonQuery(executor, db, "INSERT INTO t (id, v) VALUES (3, (SELECT count(*) FROM a, b, c, d, e))"));

        // A write with RETURNING enters through the query path and is counted the same way.
        Refused(() => Query(executor, db, "UPDATE t SET v = 50 WHERE id IN (SELECT a.id FROM a, b, c, d WHERE a.id = b.id) RETURNING id"));

        List<QueryResultRow> rows = await Query(executor, db, "SELECT id, v FROM t");
        Assert.AreEqual(new long[] { 30, 30 }, rows.Select(row => row.Row["v"].LongValue).ToArray());
    }

    [Test]
    public async Task AWriteThroughAView_CountsTheSubqueriesItsColumnsBringIn()
    {
        (string db, CommandExecutor executor) = await Seeded();

        // The view reads three relations. Each mention of n in a write through it becomes one copy of
        // the subquery behind n.
        await Ddl(executor, db, "CREATE VIEW tv AS SELECT id, v, (SELECT count(*) FROM a, b) AS n FROM t");

        Assert.AreEqual(2, await NonQuery(executor, db, "UPDATE tv SET v = 5 WHERE n = 4"));

        // The parsed statement names one relation. After the rewrite it reads t and two copies of a, b.
        Refused(() => NonQuery(executor, db, "UPDATE tv SET v = 6 WHERE n = 4 AND n > 0"));
        Refused(() => NonQuery(executor, db, "DELETE FROM tv WHERE n = 4 AND n > 0"));
        Refused(() => Query(executor, db, "UPDATE tv SET v = 7 WHERE n = 4 RETURNING n"));

        List<QueryResultRow> rows = await Query(executor, db, "SELECT id, v FROM t");
        Assert.AreEqual(new long[] { 5, 5 }, rows.Select(row => row.Row["v"].LongValue).ToArray());
    }

    [Test]
    public async Task TheSourceOfAnInsertSelectCounts_WithTheBodiesOfItsViews()
    {
        (string db, CommandExecutor executor) = await Seeded();

        await Ddl(executor, db, "CREATE VIEW abc AS SELECT a.id FROM a, b, c WHERE a.id = b.id AND b.id = c.id");

        Assert.AreEqual(2, await NonQuery(executor, db, "INSERT INTO e (id, v) SELECT abc.id + 10, 0 FROM abc, d WHERE abc.id = d.id"));

        // The statement names two relations, so only the count after expansion refuses it.
        Refused(() => NonQuery(executor, db, "INSERT INTO e (id, v) SELECT abc.id + 20, 0 FROM abc, d, t WHERE abc.id = d.id AND d.id = t.id"));

        Assert.AreEqual(4, Count(await Query(executor, db, "SELECT count(*) AS c FROM e")));
    }

    [Test]
    public async Task ACreateTableAsPastTheLimit_IsRefusedAndCreatesNoTable()
    {
        (string db, CommandExecutor executor) = await Seeded();

        await Ddl(executor, db, "CREATE TABLE abcd AS SELECT a.id FROM a, b, c, d WHERE a.id = b.id AND b.id = c.id AND c.id = d.id");
        Assert.AreEqual(2, Count(await Query(executor, db, "SELECT count(*) AS c FROM abcd")));

        Refused(() => Ddl(executor, db, "CREATE TABLE wide AS SELECT a.id FROM a, b, c, d, e WHERE a.id = b.id"));

        CamusDBException missing = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, db, "SELECT count(*) AS c FROM wide"))!;
        Assert.AreNotEqual(CamusDBErrorCodes.TooManyTablesInStatement, missing.Code, "the refused table must not exist");
    }

    [Test]
    public async Task ALimitOfZero_DisablesTheCheck()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(Options with { MaxTablesPerStatement = 0 });
        await Ddl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)");
        await NonQuery(executor, db, "INSERT INTO t (id, v) VALUES (1, 10)");

        Assert.AreEqual(1, Count(await Query(executor, db, CommaJoin(100))));
    }
}

/// <summary>
/// <c>max_tables_per_statement</c> is a runtime setting: the SQL entry points read it from the
/// current options for each statement, so a published change applies to the next statement without a
/// restart.
/// </summary>
[NonParallelizable]
internal sealed class TestStatementTableLimitIsARuntimeSetting : BaseTest
{
    [Test]
    public async Task APublishedLimitAppliesToTheNextStatement()
    {
        CamusDBOptions initial = Options with { MaxTablesPerStatement = 2 };
        CamusDBOptionsHolder holder = new(initial);

        (string db, _, CommandExecutor executor) = await CreateDatabaseWith(CreateCommandExecutor(initial, holder));

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)", null));
        await database.Transactions.CommitAsync(ddl);

        const string select = "SELECT count(*) AS c FROM t x, t y, t z";
        const string update = "UPDATE t SET v = 1 WHERE id IN (SELECT x.id FROM t x, t y)";

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(() => TestStatementTableLimit.Query(executor, db, select))!;
        Assert.AreEqual(CamusDBErrorCodes.TooManyTablesInStatement, refused.Code);

        KvTransaction refusedWrite = await database.Transactions.BeginAsync();
        refused = Assert.ThrowsAsync<CamusDBException>(
            () => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(refusedWrite, db, update, null)))!;
        Assert.AreEqual(CamusDBErrorCodes.TooManyTablesInStatement, refused.Code);
        await database.Transactions.RollbackIfNotCompletedAsync(refusedWrite);

        holder.Publish(initial with { MaxTablesPerStatement = 3 });

        Assert.AreEqual(0, (await TestStatementTableLimit.Query(executor, db, select)).Single().Row["c"].LongValue);

        KvTransaction write = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(write, db, update, null));
        await database.Transactions.CommitAsync(write);
    }
}
