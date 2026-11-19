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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// INSERT, UPDATE and DELETE through an auto-updatable view, and <c>WITH CHECK OPTION</c>. Every case
/// runs SQL and asserts on the base table, because what matters is which rows a write through a view
/// reaches — and that it reaches no other.
/// </summary>
[NonParallelizable]
public sealed class TestUpdatableViews : SharedNodeBaseTest
{
    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<int> Write(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
            await database.Transactions.CommitAsync(tx);
            return result.ModifiedRows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Query(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static List<long> Ids(IEnumerable<QueryResultRow> rows) => rows.Select(r => r.Row["id"].LongValue).OrderBy(i => i).ToList();

    private static void Refused(System.Func<Task> act, string code, string? contains = null)
    {
        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () => await act());
        Assert.AreEqual(code, error!.Code, error.Message);
        if (contains is not null)
            StringAssert.Contains(contains, error.Message);
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> Seed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE orders (id int64 PRIMARY KEY, customer string(32) NULL, total int64 NULL, status string(16) NULL)");
        await Write(database, executor, dbname,
            "INSERT INTO orders (id, customer, total, status) VALUES " +
            "(1, 'acme', 10, 'open'), (2, 'acme', 20, 'open'), (3, 'globex', 30, 'open'), (4, 'acme', 40, 'closed')");

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW acme_orders AS SELECT id, customer, total FROM orders WHERE customer = 'acme'");

        return (dbname, database, executor);
    }

    [Test]
    public async Task InsertThroughAViewLandsInTheTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        Assert.AreEqual(1, await Write(database, executor, dbname,
            "INSERT INTO acme_orders (id, customer, total) VALUES (10, 'acme', 5)"));

        // A column the view does not expose takes its default.
        List<QueryResultRow> row = await Query(database, executor, dbname, "SELECT id, total, status FROM orders WHERE id = 10");
        Assert.AreEqual(5, row.Single().Row["total"].LongValue);
        Assert.AreEqual(ColumnType.Null, row.Single().Row["status"].Type);

        // Without a column list, the view's columns in order.
        await Write(database, executor, dbname, "INSERT INTO acme_orders VALUES (11, 'acme', 6)");
        Assert.AreEqual(6, (await Query(database, executor, dbname, "SELECT total FROM orders WHERE id = 11")).Single().Row["total"].LongValue);

        // INSERT ... SELECT, too.
        await Write(database, executor, dbname, "INSERT INTO acme_orders (id, customer, total) SELECT id + 100, customer, total FROM orders WHERE id = 1");
        CollectionAssert.Contains(Ids(await Query(database, executor, dbname, "SELECT id FROM orders")), 101L);
    }

    [Test]
    public async Task RenamedColumnsWriteTheColumnsTheyName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW named (order_id, who, amount) AS SELECT id, customer, total FROM orders");

        await Write(database, executor, dbname, "INSERT INTO named (order_id, who, amount) VALUES (20, 'initech', 7)");
        Assert.AreEqual(1, await Write(database, executor, dbname, "UPDATE named SET amount = amount * 2 WHERE order_id = 20"));

        QueryResultRow row = (await Query(database, executor, dbname, "SELECT customer, total FROM orders WHERE id = 20")).Single();
        Assert.AreEqual("initech", row.Row["customer"].StrValue);
        Assert.AreEqual(14, row.Row["total"].LongValue);
    }

    /// <summary>An UPDATE or DELETE through a view reaches only the rows the view shows.</summary>
    [Test]
    public async Task UpdateAndDeleteReachOnlyTheRowsTheViewShows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        Assert.AreEqual(3, await Write(database, executor, dbname, "UPDATE acme_orders SET total = total + 1 WHERE TRUE"));
        Assert.AreEqual(30, (await Query(database, executor, dbname, "SELECT total FROM orders WHERE id = 3")).Single().Row["total"].LongValue,
            "globex's row is not in the view and must not change");
        Assert.AreEqual(11, (await Query(database, executor, dbname, "SELECT total FROM orders WHERE id = 1")).Single().Row["total"].LongValue);

        // A statement alias qualifies the view's columns.
        Assert.AreEqual(1, await Write(database, executor, dbname, "UPDATE acme_orders AS a SET total = 0 WHERE a.id = 2"));

        Assert.AreEqual(2, await Write(database, executor, dbname, "DELETE FROM acme_orders WHERE total < 20"));
        CollectionAssert.AreEqual(new List<long> { 3, 4 }, Ids(await Query(database, executor, dbname, "SELECT id FROM orders")));
    }

    [Test]
    public async Task AComputedColumnCanBeReadButNotWritten()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW doubled AS SELECT id, total, total * 2 AS twice FROM orders");

        Refused(() => Write(database, executor, dbname, "UPDATE doubled SET twice = 1 WHERE id = 1"),
            CamusDBErrorCodes.ViewColumnNotUpdatable, "twice");
        Refused(() => Write(database, executor, dbname, "INSERT INTO doubled (id, total, twice) VALUES (9, 1, 2)"),
            CamusDBErrorCodes.ViewColumnNotUpdatable);

        // Read in a WHERE, it is the expression it stands for.
        Assert.AreEqual(1, await Write(database, executor, dbname, "UPDATE doubled SET total = 1 WHERE twice = 60"));
        Assert.AreEqual(1, (await Query(database, executor, dbname, "SELECT total FROM orders WHERE id = 3")).Single().Row["total"].LongValue);
    }

    /// <summary>
    /// A name the view does not expose is refused. Passed through, it would name a hidden column of the
    /// table, and the row count of the write would disclose what it holds.
    /// </summary>
    [Test]
    public async Task AColumnTheViewHidesCannotBeNamed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        Refused(() => Write(database, executor, dbname, "DELETE FROM acme_orders WHERE status = 'closed'"),
            CamusDBErrorCodes.UnknownColumn, "status");
        Refused(() => Write(database, executor, dbname, "UPDATE acme_orders SET status = 'x' WHERE id = 1"),
            CamusDBErrorCodes.UnknownColumn);

        Assert.AreEqual(4, (await Query(database, executor, dbname, "SELECT id FROM orders")).Count, "nothing was written");
    }

    [Test]
    public async Task AViewOverAnUpdatableViewIsUpdatableAndAppliesBothConditions()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await ExecDdl(database, executor, dbname, "CREATE VIEW big_acme AS SELECT id, total FROM acme_orders WHERE total >= 20");

        Assert.AreEqual(2, await Write(database, executor, dbname, "UPDATE big_acme SET total = total + 100 WHERE TRUE"));
        CollectionAssert.AreEqual(new List<long> { 2, 4 },
            Ids(await Query(database, executor, dbname, "SELECT id FROM orders WHERE total > 100")));

        Assert.AreEqual(2, await Write(database, executor, dbname, "DELETE FROM big_acme WHERE id > 0"));
        CollectionAssert.AreEqual(new List<long> { 1, 3 }, Ids(await Query(database, executor, dbname, "SELECT id FROM orders")));
    }

    [Test]
    public async Task ReturningThroughAViewNamesTheViewsColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await ExecDdl(database, executor, dbname, "CREATE VIEW named (order_id, who) AS SELECT id, customer FROM orders");

        List<QueryResultRow> inserted = await Query(database, executor, dbname,
            "INSERT INTO named (order_id, who) VALUES (30, 'umbrella') RETURNING *");
        Assert.AreEqual(30, inserted.Single().Row["order_id"].LongValue);
        Assert.AreEqual("umbrella", inserted.Single().Row["who"].StrValue);

        List<QueryResultRow> updated = await Query(database, executor, dbname,
            "UPDATE named SET who = 'umbrella2' WHERE order_id = 30 RETURNING who");
        Assert.AreEqual("umbrella2", updated.Single().Row["who"].StrValue);
    }

    /// <summary>The rewritten write runs in the caller's transaction, so a rollback undoes it.</summary>
    [Test]
    public async Task AWriteThroughAViewRollsBackWithItsTransaction()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "DELETE FROM acme_orders WHERE TRUE", null));
        await database.Transactions.RollbackAsync(tx);

        Assert.AreEqual(4, (await Query(database, executor, dbname, "SELECT id FROM orders")).Count);
    }

    [TestCase("CREATE VIEW agg AS SELECT customer, SUM(total) AS spent FROM orders GROUP BY customer", "agg", "GROUP BY")]
    [TestCase("CREATE VIEW sums AS SELECT SUM(total) AS spent FROM orders", "sums", "aggregates")]
    [TestCase("CREATE VIEW dist AS SELECT DISTINCT customer FROM orders", "dist", "DISTINCT")]
    [TestCase("CREATE VIEW lim AS SELECT id FROM orders LIMIT 2", "lim", "LIMIT")]
    [TestCase("CREATE VIEW uni AS SELECT id FROM orders UNION SELECT id FROM orders", "uni", "set operation")]
    [TestCase("CREATE VIEW joined AS SELECT o.id FROM orders o INNER JOIN orders p ON o.id = p.id", "joined", "more than one relation")]
    [TestCase("CREATE VIEW sub AS SELECT id FROM orders WHERE id IN (SELECT id FROM orders WHERE total > 1)", "sub", "subquery")]
    public async Task AViewThatIsNotAutoUpdatableRefusesWritesAndSaysWhy(string create, string view, string reason)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await ExecDdl(database, executor, dbname, create);

        Refused(() => Write(database, executor, dbname, $"DELETE FROM {view} WHERE TRUE"), CamusDBErrorCodes.ViewNotUpdatable, reason);
        Assert.AreEqual(4, (await Query(database, executor, dbname, "SELECT id FROM orders")).Count);
    }

    [Test]
    public async Task AMaterializedViewAndForbiddenFormsAreStillRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await ExecDdl(database, executor, dbname, "CREATE MATERIALIZED VIEW m AS SELECT id, total FROM orders");
        await ExecDdl(database, executor, dbname, "CREATE VIEW over_m AS SELECT id, total FROM m");

        Refused(() => Write(database, executor, dbname, "DELETE FROM m WHERE TRUE"), CamusDBErrorCodes.ViewNotUpdatable);
        Refused(() => Write(database, executor, dbname, "DELETE FROM over_m WHERE TRUE"), CamusDBErrorCodes.ViewNotUpdatable, "materialized view");
        Refused(() => Write(database, executor, dbname,
            "INSERT INTO acme_orders (id, customer, total) VALUES (1, 'acme', 1) ON CONFLICT (id) DO NOTHING"),
            CamusDBErrorCodes.FeatureNotSupported);
    }

    // ── WITH CHECK OPTION ──────────────────────────────────────────────────────────────────────

    [Test]
    public async Task ACheckOptionRefusesARowTheViewWouldNotShow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW checked AS SELECT id, customer, total FROM orders WHERE customer = 'acme' WITH CHECK OPTION");

        Refused(() => Write(database, executor, dbname, "INSERT INTO checked (id, customer, total) VALUES (50, 'globex', 1)"),
            CamusDBErrorCodes.ViewCheckOptionViolated, "checked");
        Refused(() => Write(database, executor, dbname, "UPDATE checked SET customer = 'globex' WHERE id = 1"),
            CamusDBErrorCodes.ViewCheckOptionViolated);

        // NULL is not visible through the view either, unlike a CHECK constraint where NULL passes.
        Refused(() => Write(database, executor, dbname, "INSERT INTO checked (id, customer, total) VALUES (51, NULL, 1)"),
            CamusDBErrorCodes.ViewCheckOptionViolated);

        // Refused before anything was written.
        Assert.IsEmpty(await Query(database, executor, dbname, "SELECT id FROM orders WHERE id >= 50"));
        Assert.AreEqual("acme", (await Query(database, executor, dbname, "SELECT customer FROM orders WHERE id = 1")).Single().Row["customer"].StrValue);

        // A row the view shows passes.
        await Write(database, executor, dbname, "INSERT INTO checked (id, customer, total) VALUES (52, 'acme', 1)");

        // Without the option the same write is allowed, and the row is simply not visible through the view.
        await Write(database, executor, dbname, "INSERT INTO acme_orders (id, customer, total) VALUES (53, 'globex', 1)");
        CollectionAssert.DoesNotContain(Ids(await Query(database, executor, dbname, "SELECT id FROM acme_orders")), 53L);
    }

    /// <summary>
    /// CASCADED checks the view and every view beneath it, whatever their own option; LOCAL checks only
    /// its own condition and the views beneath it that ask for a check themselves.
    /// </summary>
    [Test]
    public async Task CascadedAndLocalCheckOptionsFollowPostgreSql()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await ExecDdl(database, executor, dbname, "CREATE VIEW big AS SELECT id, customer, total FROM orders WHERE total > 10");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW big_acme_cascaded AS SELECT id, customer, total FROM big WHERE customer = 'acme' WITH CASCADED CHECK OPTION");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW big_acme_local AS SELECT id, customer, total FROM big WHERE customer = 'acme' WITH LOCAL CHECK OPTION");

        // total 5 is not > 10: cascaded checks big's condition too, local does not (big asks for no check).
        Refused(() => Write(database, executor, dbname, "INSERT INTO big_acme_cascaded (id, customer, total) VALUES (60, 'acme', 5)"),
            CamusDBErrorCodes.ViewCheckOptionViolated);
        await Write(database, executor, dbname, "INSERT INTO big_acme_local (id, customer, total) VALUES (61, 'acme', 5)");

        // Both check their own condition.
        Refused(() => Write(database, executor, dbname, "INSERT INTO big_acme_local (id, customer, total) VALUES (62, 'globex', 50)"),
            CamusDBErrorCodes.ViewCheckOptionViolated);

        // Beneath a LOCAL view, a view with its own check option is still checked.
        await ExecDdl(database, executor, dbname, "CREATE VIEW checked_big AS SELECT id, customer, total FROM orders WHERE total > 10 WITH LOCAL CHECK OPTION");
        await ExecDdl(database, executor, dbname, "CREATE VIEW acme_over_checked AS SELECT id, customer, total FROM checked_big WHERE customer = 'acme'");
        Refused(() => Write(database, executor, dbname, "INSERT INTO acme_over_checked (id, customer, total) VALUES (63, 'acme', 5)"),
            CamusDBErrorCodes.ViewCheckOptionViolated, "checked_big");
    }

    [Test]
    public async Task ACheckOptionNeedsAViewThatCanBeWrittenThrough()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        Refused(() => ExecDdl(database, executor, dbname,
            "CREATE VIEW agg AS SELECT customer, SUM(total) AS spent FROM orders GROUP BY customer WITH CHECK OPTION"),
            CamusDBErrorCodes.ViewNotUpdatable, "WITH CHECK OPTION");
        Assert.IsFalse(database.Schema.Views.ContainsKey("agg"));
    }
}
