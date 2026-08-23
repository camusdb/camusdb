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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for the bound-query cache: repeated executions of the same SQL text reuse
/// one binding, every DDL that changes the source table discards it, excluded statement shapes are
/// marked ineligible and stay correct, parameter values never leak into the shared artifacts, and
/// concurrent executions share one frozen resolver safely.
/// </summary>
[NonParallelizable]
public sealed class TestBoundQueryCache : BaseTest
{
    private async Task<(string db, DatabaseDescriptor descriptor, CommandExecutor executor)> SetupRobots(
        CamusDBOptions? options = null)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = options is null
            ? await CreateDatabase()
            : await CreateDatabase(options);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbname,
            sql: "CREATE TABLE robots (id OBJECT_ID PRIMARY KEY, name STRING, year INT64)",
            parameters: null));

        await RunNonQuery(dbname, db, executor,
            "INSERT INTO robots (id, name, year) VALUES (gen_id(), \"astro\", 1963)");
        await RunNonQuery(dbname, db, executor,
            "INSERT INTO robots (id, name, year) VALUES (gen_id(), \"bender\", 1999)");
        await RunNonQuery(dbname, db, executor,
            "INSERT INTO robots (id, name, year) VALUES (gen_id(), \"r2d2\", 1977)");

        return (dbname, db, executor);
    }

    private static async Task RunNonQuery(string dbname, DatabaseDescriptor db, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task RunDdl(string dbname, CommandExecutor executor, string sql)
    {
        // DDL that derives a relation (CREATE MATERIALIZED VIEW) binds and pins schema versions
        // through the ticket's transaction, so DDL here always carries a real one.
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> RunSelect(
        string dbname,
        CommandExecutor executor,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
        return await cursor.ToListAsync();
    }

    /// <summary>
    /// The slot the executor stored for <paramref name="sql"/>. <see cref="CommandExecutor.ParseSql"/>
    /// goes through the executor's own parser cache, so it returns the same AST instance the read
    /// path keyed the slot by.
    /// </summary>
    private static BoundQuerySlot? SlotFor(DatabaseDescriptor db, CommandExecutor executor, string sql)
        => db.BoundQueries.TryGet(executor.ParseSql(sql));

    [Test]
    public async Task RepeatedExecution_ReusesOneBinding_WithIdenticalResults()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupRobots();

        const string sql = "SELECT name FROM robots WHERE year > 1970 ORDER BY name";

        List<QueryResultRow> first = await RunSelect(dbname, executor, sql);
        BoundQuerySlot? slot1 = SlotFor(db, executor, sql);

        List<QueryResultRow> second = await RunSelect(dbname, executor, sql);
        BoundQuerySlot? slot2 = SlotFor(db, executor, sql);

        Assert.IsNotNull(slot1, "first execution must publish a slot");
        Assert.IsTrue(slot1!.Eligible);
        Assert.AreSame(slot1, slot2, "second execution must reuse the published slot");

        Assert.AreEqual(2, first.Count);
        CollectionAssert.AreEqual(
            first.Select(r => r.Row["name"].StrValue).ToList(),
            second.Select(r => r.Row["name"].StrValue).ToList());
        Assert.AreEqual("bender", first[0].Row["name"].StrValue);
        Assert.AreEqual("r2d2", first[1].Row["name"].StrValue);
    }

    [Test]
    public async Task ParameterValues_AreNotBakedIntoTheSharedBinding()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupRobots();

        const string sql = "SELECT name FROM robots WHERE year > @y";

        List<QueryResultRow> wide = await RunSelect(dbname, executor, sql,
            new() { ["@y"] = new ColumnValue(ColumnType.Integer64, 1900L) });

        List<QueryResultRow> narrow = await RunSelect(dbname, executor, sql,
            new() { ["@y"] = new ColumnValue(ColumnType.Integer64, 1990L) });

        Assert.AreEqual(3, wide.Count);
        Assert.AreEqual(1, narrow.Count);
        Assert.AreEqual("bender", narrow[0].Row["name"].StrValue);
        Assert.IsTrue(SlotFor(db, executor, sql)?.Eligible ?? false);
    }

    [Test]
    public async Task ParameterizedInList_IsRebuiltPerExecution()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupRobots();

        const string sql = "SELECT name FROM robots WHERE year IN (@a, @b) ORDER BY name";

        List<QueryResultRow> firstPair = await RunSelect(dbname, executor, sql, new()
        {
            ["@a"] = new ColumnValue(ColumnType.Integer64, 1963L),
            ["@b"] = new ColumnValue(ColumnType.Integer64, 1999L),
        });

        List<QueryResultRow> secondPair = await RunSelect(dbname, executor, sql, new()
        {
            ["@a"] = new ColumnValue(ColumnType.Integer64, 1977L),
            ["@b"] = new ColumnValue(ColumnType.Integer64, 1977L),
        });

        CollectionAssert.AreEqual(
            new[] { "astro", "bender" },
            firstPair.Select(r => r.Row["name"].StrValue).ToList());
        CollectionAssert.AreEqual(
            new[] { "r2d2" },
            secondPair.Select(r => r.Row["name"].StrValue).ToList());
    }

    [Test]
    public async Task AlterTableAddColumn_DiscardsTheSlot()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupRobots();

        const string sql = "SELECT name FROM robots WHERE year > 1970";

        _ = await RunSelect(dbname, executor, sql);
        BoundQuerySlot? before = SlotFor(db, executor, sql);
        Assert.IsTrue(before?.Eligible ?? false);

        await RunDdl(dbname, executor, "ALTER TABLE robots ADD COLUMN fuel INT64 DEFAULT (5)");

        // The stale slot must fail validation; the execution rebinds and republishes.
        List<QueryResultRow> after = await RunSelect(dbname, executor, sql);
        Assert.AreEqual(2, after.Count);

        BoundQuerySlot? rebound = SlotFor(db, executor, sql);
        Assert.IsNotNull(rebound);
        Assert.AreNotSame(before, rebound, "the pre-DDL slot must be replaced");

        // The rebound schema view includes the new column.
        List<QueryResultRow> star = await RunSelect(dbname, executor, "SELECT * FROM robots WHERE year = 1999");
        Assert.AreEqual(1, star.Count);
        Assert.IsTrue(star[0].Row.ContainsKey("fuel"), "new column must be visible after invalidation");
        Assert.AreEqual(5L, star[0].Row["fuel"].LongValue);
    }

    [Test]
    public async Task RenameColumn_CachedStatementStopsResolvingOldName()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupRobots();

        const string sql = "SELECT year FROM robots WHERE name = \"bender\"";

        List<QueryResultRow> before = await RunSelect(dbname, executor, sql);
        Assert.AreEqual(1999L, before[0].Row["year"].LongValue);

        await RunDdl(dbname, executor, "ALTER TABLE robots RENAME COLUMN year TO built");

        // The cached binding must not keep answering for the old column name.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => RunSelect(dbname, executor, sql));
        Assert.AreEqual(CamusDBErrorCodes.UnknownColumn, ex!.Code);

        List<QueryResultRow> renamed = await RunSelect(
            dbname, executor, "SELECT built FROM robots WHERE name = \"bender\"");
        Assert.AreEqual(1999L, renamed[0].Row["built"].LongValue);
    }

    [Test]
    public async Task DropAndRecreateTable_ServesTheNewTable()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupRobots();

        const string sql = "SELECT name FROM robots ORDER BY name";

        List<QueryResultRow> old = await RunSelect(dbname, executor, sql);
        Assert.AreEqual(3, old.Count);

        await RunDdl(dbname, executor, "DROP TABLE robots");
        await RunDdl(dbname, executor,
            "CREATE TABLE robots (id OBJECT_ID PRIMARY KEY, name STRING, year INT64)");
        await RunNonQuery(dbname, db, executor,
            "INSERT INTO robots (id, name, year) VALUES (gen_id(), \"wall-e\", 2008)");

        // The recreated table has a new descriptor instance, so the old slot fails validation.
        List<QueryResultRow> fresh = await RunSelect(dbname, executor, sql);
        Assert.AreEqual(1, fresh.Count);
        Assert.AreEqual("wall-e", fresh[0].Row["name"].StrValue);
    }

    [Test]
    public async Task SubqueryInWhere_IsIneligible_AndReflectsNewData()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupRobots();

        await RunDdl(dbname, executor,
            "CREATE TABLE favorites (id OBJECT_ID PRIMARY KEY, fav_year INT64)");
        await RunNonQuery(dbname, db, executor,
            "INSERT INTO favorites (id, fav_year) VALUES (gen_id(), 1999)");

        const string sql = "SELECT name FROM robots WHERE year IN (SELECT fav_year FROM favorites) ORDER BY name";

        List<QueryResultRow> first = await RunSelect(dbname, executor, sql);
        CollectionAssert.AreEqual(new[] { "bender" }, first.Select(r => r.Row["name"].StrValue).ToList());

        BoundQuerySlot? slot = SlotFor(db, executor, sql);
        Assert.IsNotNull(slot, "the shape decision must be recorded");
        Assert.IsFalse(slot!.Eligible, "a WHERE subquery bakes data values into the rewrite and must bypass the cache");

        // The subquery re-materializes per execution, so new inner data must show up.
        await RunNonQuery(dbname, db, executor,
            "INSERT INTO favorites (id, fav_year) VALUES (gen_id(), 1977)");

        List<QueryResultRow> second = await RunSelect(dbname, executor, sql);
        CollectionAssert.AreEqual(
            new[] { "bender", "r2d2" },
            second.Select(r => r.Row["name"].StrValue).ToList());
    }

    [Test]
    public async Task Join_IsIneligible_AndStaysCorrect()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupRobots();

        await RunDdl(dbname, executor,
            "CREATE TABLE eras (id OBJECT_ID PRIMARY KEY, year INT64, era STRING)");
        await RunNonQuery(dbname, db, executor,
            "INSERT INTO eras (id, year, era) VALUES (gen_id(), 1999, \"future\")");

        const string sql =
            "SELECT r.name AS rname, e.era AS rera FROM robots r INNER JOIN eras e ON r.year = e.year";

        List<QueryResultRow> first = await RunSelect(dbname, executor, sql);
        List<QueryResultRow> second = await RunSelect(dbname, executor, sql);

        Assert.AreEqual(1, first.Count);
        Assert.AreEqual("bender", first[0].Row["rname"].StrValue);
        Assert.AreEqual(first.Count, second.Count);

        BoundQuerySlot? slot = SlotFor(db, executor, sql);
        Assert.IsNotNull(slot);
        Assert.IsFalse(slot!.Eligible, "a multi-source SELECT must bypass the cache");
    }

    [Test]
    public async Task SessionFunction_IsIneligible_AndStaysCorrect()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupRobots();

        const string sql = "SELECT current_user() AS u, name FROM robots WHERE year = 1999";

        List<QueryResultRow> first = await RunSelect(dbname, executor, sql);
        List<QueryResultRow> second = await RunSelect(dbname, executor, sql);

        Assert.AreEqual(1, first.Count);
        Assert.AreEqual("bender", first[0].Row["name"].StrValue);
        Assert.AreEqual(1, second.Count);

        BoundQuerySlot? slot = SlotFor(db, executor, sql);
        Assert.IsNotNull(slot);
        Assert.IsFalse(slot!.Eligible, "session-scoped functions must bypass the cache");
    }

    [Test]
    public async Task AsOfSystemTime_IsIneligible_AndReadsHistory()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupRobots();

        // Age the inserts, then update: a '-1s' snapshot lands before the update.
        await Task.Delay(1500);
        await RunNonQuery(dbname, db, executor, "UPDATE robots SET year = 2000 WHERE name = \"bender\"");

        const string sql = "SELECT year FROM robots AS OF SYSTEM TIME '-1s' WHERE name = \"bender\"";

        List<QueryResultRow> historical = await RunSelect(dbname, executor, sql);
        Assert.AreEqual(1999L, historical[0].Row["year"].LongValue);

        BoundQuerySlot? slot = SlotFor(db, executor, sql);
        Assert.IsNotNull(slot);
        Assert.IsFalse(slot!.Eligible, "AS OF SYSTEM TIME pins a per-execution snapshot and must bypass the cache");

        List<QueryResultRow> historicalAgain = await RunSelect(dbname, executor, sql);
        Assert.AreEqual(1999L, historicalAgain[0].Row["year"].LongValue);

        List<QueryResultRow> live = await RunSelect(
            dbname, executor, "SELECT year FROM robots WHERE name = \"bender\"");
        Assert.AreEqual(2000L, live[0].Row["year"].LongValue);
    }

    [Test]
    public async Task OrderByProjectionAlias_WorksOnTheSharedBinding()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupRobots();

        const string sql = "SELECT name AS n, year FROM robots ORDER BY n";

        List<QueryResultRow> first = await RunSelect(dbname, executor, sql);
        List<QueryResultRow> second = await RunSelect(dbname, executor, sql);

        CollectionAssert.AreEqual(
            new[] { "astro", "bender", "r2d2" },
            first.Select(r => r.Row["n"].StrValue).ToList());
        CollectionAssert.AreEqual(
            first.Select(r => r.Row["n"].StrValue).ToList(),
            second.Select(r => r.Row["n"].StrValue).ToList());
        Assert.IsTrue(SlotFor(db, executor, sql)?.Eligible ?? false);
    }

    [Test]
    public async Task ConcurrentExecutions_ShareOneFrozenBinding()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupRobots();

        const string sql = "SELECT name, year FROM robots WHERE year > 1900 ORDER BY year";

        // Prime the slot so every concurrent execution below runs against the shared binding.
        _ = await RunSelect(dbname, executor, sql);
        BoundQuerySlot? slot = SlotFor(db, executor, sql);
        Assert.IsTrue(slot?.Eligible ?? false);

        Task<List<QueryResultRow>>[] tasks = Enumerable.Range(0, 16)
            .Select(async _ =>
            {
                List<QueryResultRow> rows = new();
                for (int i = 0; i < 5; i++)
                    rows = await RunSelect(dbname, executor, sql);
                return rows;
            })
            .ToArray();

        List<QueryResultRow>[] all = await Task.WhenAll(tasks);

        foreach (List<QueryResultRow> rows in all)
        {
            Assert.AreEqual(3, rows.Count);
            Assert.AreEqual("astro", rows[0].Row["name"].StrValue);
            Assert.AreEqual("r2d2", rows[1].Row["name"].StrValue);
            Assert.AreEqual("bender", rows[2].Row["name"].StrValue);
        }

        Assert.AreSame(slot, SlotFor(db, executor, sql), "the binding must not be replaced by concurrent reuse");
    }

    [Test]
    public async Task MaterializedViewRefresh_DiscardsTheSlot()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupRobots();

        await RunDdl(dbname, executor,
            "CREATE MATERIALIZED VIEW modern AS SELECT id, name, year FROM robots WHERE year > 1970");
        await RunNonQuery(dbname, db, executor, "REFRESH MATERIALIZED VIEW modern");

        const string sql = "SELECT name FROM modern ORDER BY name";

        List<QueryResultRow> first = await RunSelect(dbname, executor, sql);
        CollectionAssert.AreEqual(
            new[] { "bender", "r2d2" },
            first.Select(r => r.Row["name"].StrValue).ToList());

        BoundQuerySlot? before = SlotFor(db, executor, sql);

        await RunNonQuery(dbname, db, executor,
            "INSERT INTO robots (id, name, year) VALUES (gen_id(), \"wall-e\", 2008)");
        await RunNonQuery(dbname, db, executor, "REFRESH MATERIALIZED VIEW modern");

        List<QueryResultRow> second = await RunSelect(dbname, executor, sql);
        CollectionAssert.AreEqual(
            new[] { "bender", "r2d2", "wall-e" },
            second.Select(r => r.Row["name"].StrValue).ToList());

        if (before is { Eligible: true })
            Assert.AreNotSame(before, SlotFor(db, executor, sql), "a refresh must not serve the pre-refresh binding");
    }

    [Test]
    public async Task CacheDisabled_StoresNothing_AndStaysCorrect()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) =
            await SetupRobots(Options with { BoundQueryCacheEnabled = false });

        const string sql = "SELECT name FROM robots WHERE year > 1970 ORDER BY name";

        List<QueryResultRow> first = await RunSelect(dbname, executor, sql);
        List<QueryResultRow> second = await RunSelect(dbname, executor, sql);

        Assert.AreEqual(2, first.Count);
        Assert.AreEqual(first.Count, second.Count);
        Assert.IsNull(SlotFor(db, executor, sql), "a disabled cache must publish nothing");
    }
}
