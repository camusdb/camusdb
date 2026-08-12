
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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for non-materialized views: creation, the frozen-shape rule on replace,
/// dependency-aware drops, and — the part that actually matters — that reading through a view
/// returns the same rows as the inlined query it stands for, including through joins, aggregation,
/// and view-over-view nesting.
///
/// <para>These drive the real entry points (<c>ExecuteDDLSQL</c> / <c>ExecuteSQLQuery</c>) rather
/// than the controllers, because the bugs worth catching here are in the wiring: a view that
/// resolves in a unit test but is never expanded on the real query path would look correct and
/// return nothing.</para>
/// </summary>
[NonParallelizable]
public sealed class TestViews : SharedNodeBaseTest
{
    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        _ = await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql,
        QuerySchemaHolder? schemaOut = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket, null, schemaOut);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    /// <summary>Creates an `orders` table with five rows, three of them open.</summary>
    private static async Task SeedOrders(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE orders (id int64 PRIMARY KEY, customer string(64), total int64, status string(16))");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, customer, total, status) VALUES " +
            "(1, 'acme', 10, 'open'), (2, 'acme', 20, 'open'), (3, 'globex', 30, 'open'), " +
            "(4, 'globex', 40, 'closed'), (5, 'initech', 50, 'closed')");
    }

    [Test]
    public async Task SelectThroughView_ReturnsSameRowsAsInlinedQuery()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");

        List<QueryResultRow> viaView = await ExecQuery(database, executor, dbname,
            "SELECT id, customer, total FROM open_orders ORDER BY id");

        List<QueryResultRow> inlined = await ExecQuery(database, executor, dbname,
            "SELECT id, customer, total FROM orders WHERE status = 'open' ORDER BY id");

        Assert.AreEqual(3, viaView.Count);
        CollectionAssert.AreEqual(
            inlined.Select(r => r.Row["id"].StrValue ?? r.Row["id"].LongValue.ToString()).ToList(),
            viaView.Select(r => r.Row["id"].StrValue ?? r.Row["id"].LongValue.ToString()).ToList(),
            "reading through a view must return exactly what the inlined query returns");
    }

    [Test]
    public async Task ViewIsQualifiableByItsOwnName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, customer FROM orders WHERE status = 'open'");

        // The view's name must survive expansion as the derived table's alias, or a qualified
        // reference to it stops resolving.
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT open_orders.customer FROM open_orders");

        Assert.AreEqual(3, rows.Count);
    }

    [Test]
    public async Task ViewWithExplicitAlias()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, customer FROM orders WHERE status = 'open'");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT o.customer FROM open_orders o WHERE o.customer = 'acme'");

        Assert.AreEqual(2, rows.Count, "a user-supplied alias must win over the view's name");
    }

    [Test]
    public async Task AggregationOverAView()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT customer, SUM(total) AS spent FROM open_orders GROUP BY customer ORDER BY customer");

        Assert.AreEqual(2, rows.Count, "acme and globex have open orders; initech does not");
        Assert.AreEqual(30, rows[0].Row["spent"].LongValue, "acme's two open orders total 30");
    }

    [Test]
    public async Task ViewJoinedToATable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE customers (name string(64) PRIMARY KEY, region string(16))");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO customers (name, region) VALUES ('acme', 'eu'), ('globex', 'us'), ('initech', 'us')");

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT v.id, c.region FROM open_orders v INNER JOIN customers c ON v.customer = c.name ORDER BY v.id");

        Assert.AreEqual(3, rows.Count, "all three open orders join to a customer");
    }

    [Test]
    public async Task ViewOverAView()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW big_open_orders AS SELECT id, customer FROM open_orders WHERE total >= 20");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id FROM big_open_orders ORDER BY id");

        Assert.AreEqual(2, rows.Count, "orders 2 and 3 are open and >= 20");
    }

    [Test]
    public async Task StarInViewBodyIsExpandedAtCreationTime()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW all_orders AS SELECT * FROM orders");

        ViewSchema view = database.Schema.Views["all_orders"];
        Assert.IsNotNull(view.Definition);
        StringAssert.DoesNotContain("*", view.Definition!.Sql,
            "'*' must be expanded at creation, so a later ALTER TABLE ADD COLUMN cannot widen the view");
        Assert.AreEqual(4, view.Definition.Columns!.Count);

        // The frozen shape must actually hold: add a column, and the view keeps its four.
        await ExecDdl(database, executor, dbname, "ALTER TABLE orders ADD COLUMN note string(64)");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT * FROM all_orders");
        Assert.AreEqual(4, rows[0].Row.Count, "the view's shape was frozen at creation and must not have widened");
    }

    [Test]
    public async Task CreateViewOverExistingRelationNameIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE VIEW orders AS SELECT id FROM orders"));

        Assert.AreEqual(CamusDBErrorCodes.TableAlreadyExists, ex!.Code,
            "tables and views share one namespace, so a table's name is not available to a view");
    }

    [Test]
    public async Task CreateViewTwiceIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW v AS SELECT id FROM orders");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE VIEW v AS SELECT id FROM orders"));

        Assert.AreEqual(CamusDBErrorCodes.ViewAlreadyExists, ex!.Code);
    }

    [Test]
    public async Task CreateOrReplaceMayAppendColumnsButNotRenameOrDropThem()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW v AS SELECT id, customer FROM orders");

        // Appending is allowed.
        await ExecDdl(database, executor, dbname, "CREATE OR REPLACE VIEW v AS SELECT id, customer, total FROM orders");
        Assert.AreEqual(3, database.Schema.Views["v"].Definition!.Columns!.Count);

        // Dropping a column is not.
        CamusDBException? dropped = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE OR REPLACE VIEW v AS SELECT id FROM orders"));
        Assert.AreEqual(CamusDBErrorCodes.CannotChangeViewShape, dropped!.Code);

        // Neither is renaming one.
        CamusDBException? renamed = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname,
                "CREATE OR REPLACE VIEW v AS SELECT id, status AS customer2, total FROM orders"));
        Assert.AreEqual(CamusDBErrorCodes.CannotChangeViewShape, renamed!.Code);
    }

    [Test]
    public async Task ReplaceChangesWhatTheViewReturns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW v AS SELECT id FROM orders WHERE status = 'open'");
        Assert.AreEqual(3, (await ExecQuery(database, executor, dbname, "SELECT id FROM v")).Count);

        await ExecDdl(database, executor, dbname, "CREATE OR REPLACE VIEW v AS SELECT id FROM orders WHERE status = 'closed'");

        Assert.AreEqual(2, (await ExecQuery(database, executor, dbname, "SELECT id FROM v")).Count,
            "the replaced body must take effect immediately, not serve a cached parse of the old one");
    }

    [Test]
    public async Task DropViewRestrictRefusesWhenADependentExists()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW inner_v AS SELECT id, total FROM orders");
        await ExecDdl(database, executor, dbname, "CREATE VIEW outer_v AS SELECT id FROM inner_v WHERE total > 10");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "DROP VIEW inner_v"));

        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, ex!.Code);
        StringAssert.Contains("outer_v", ex.Message, "the error must name what is in the way");
    }

    [Test]
    public async Task DropViewCascadeRemovesDependents()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW inner_v AS SELECT id, total FROM orders");
        await ExecDdl(database, executor, dbname, "CREATE VIEW outer_v AS SELECT id FROM inner_v WHERE total > 10");

        await ExecDdl(database, executor, dbname, "DROP VIEW inner_v CASCADE");

        Assert.IsFalse(database.Schema.Views.ContainsKey("inner_v"));
        Assert.IsFalse(database.Schema.Views.ContainsKey("outer_v"), "CASCADE must remove the dependent too");
    }

    [Test]
    public async Task DropViewIfExistsIsQuietWhenAbsent()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        Assert.DoesNotThrowAsync(async () =>
            await ExecDdl(database, executor, dbname, "DROP VIEW IF EXISTS nope"));

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "DROP VIEW nope"));
    }

    [Test]
    public async Task DroppedViewNoLongerResolves()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW v AS SELECT id FROM orders");
        Assert.AreEqual(5, (await ExecQuery(database, executor, dbname, "SELECT id FROM v")).Count);

        await ExecDdl(database, executor, dbname, "DROP VIEW v");

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, "SELECT id FROM v"));
    }

    [Test]
    public async Task RenameView()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW v AS SELECT id FROM orders");
        await ExecDdl(database, executor, dbname, "ALTER VIEW v RENAME TO w");

        Assert.IsFalse(database.Schema.Views.ContainsKey("v"));
        Assert.AreEqual(5, (await ExecQuery(database, executor, dbname, "SELECT id FROM w")).Count);
    }

    [Test]
    public async Task UnnamedExpressionColumnIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE VIEW v AS SELECT total + 1 FROM orders"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("alias", ex.Message, "the message must say how to fix it");
    }

    [Test]
    public async Task DuplicateColumnNameIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE VIEW v AS SELECT id, total AS id FROM orders"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    [Test]
    public async Task ColumnAliasListRenamesOutputColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW v (order_id, who) AS SELECT id, customer FROM orders");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT order_id, who FROM v");
        Assert.AreEqual(5, rows.Count);
    }

    [Test]
    public async Task ColumnAliasListArityMismatchIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE VIEW v (a, b, c) AS SELECT id, customer FROM orders"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    [Test]
    public async Task TimeTravelInViewBodyIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname,
                "CREATE VIEW v AS SELECT id FROM orders AS OF SYSTEM TIME '-1s'"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("AS OF SYSTEM TIME", ex.Message);
    }

    [Test]
    public async Task ViewBodyOverAMissingTableIsRejectedAtCreateTime()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        // The point is that this fails now, not at some later reader's first SELECT.
        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE VIEW v AS SELECT id FROM no_such_table"));

        Assert.IsFalse(database.Schema.Views.ContainsKey("v"));
    }

    [Test]
    public async Task ViewSurvivesDatabaseReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, customer FROM orders WHERE status = 'open'");

        // Reloading metadata from KV is what a restart does; if the view were only in memory it would
        // vanish here and the query below would fail.
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));

        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        Assert.IsTrue(reopened.Schema.Views.ContainsKey("open_orders"), "the view must have been persisted");

        Assert.AreEqual(3, (await ExecQuery(reopened, executor, dbname, "SELECT id FROM open_orders")).Count);
    }

    [Test]
    public async Task RenamingABaseTableKeepsDependentViewsWorking()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, customer FROM orders WHERE status = 'open'");

        string bodyBeforeRename = database.Schema.Views["open_orders"].Definition!.Sql;

        await ExecDdl(database, executor, dbname, "ALTER TABLE orders RENAME TO sales");

        // The stored body binds the relation by its immutable id, so a rename has nothing to rewrite
        // and must leave the definition byte-for-byte alone.
        Assert.AreEqual(bodyBeforeRename, database.Schema.Views["open_orders"].Definition!.Sql,
            "a rename must not touch a definition that refers to the relation by id");

        Assert.AreEqual(3, (await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders")).Count,
            "renaming a base table must be transparent to a view that reads it");

        // The name comes back when the definition is shown. The original name is kept as the alias
        // on purpose: every qualified column reference in the body resolves through it, so dropping
        // it would break the very body this is meant to preserve.
        List<QueryResultRow> shown = await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW open_orders");
        StringAssert.Contains("FROM sales AS orders", shown[0].Row["create view"].StrValue!);
    }

    /// <summary>
    /// The rewrite must be an AST edit, not a textual replace. Here the table's name also appears as
    /// a column name and inside a string literal; a naive substitution corrupts both and the view
    /// then returns wrong rows rather than failing loudly.
    /// </summary>
    [Test]
    public async Task RenameRewriteDoesNotTouchColumnsOrStringLiterals()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE orders (id int64 PRIMARY KEY, orders string(64), note string(64))");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, orders, note) VALUES (1, 'x', 'orders'), (2, 'y', 'other')");

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW v AS SELECT id, orders FROM orders WHERE note = 'orders'");

        await ExecDdl(database, executor, dbname, "ALTER TABLE orders RENAME TO sales");

        string body = database.Schema.Views["v"].Definition!.Sql;
        StringAssert.Contains("'orders'", body, "the string literal must be untouched");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id, orders FROM v");
        Assert.AreEqual(1, rows.Count, "the column reference and the literal predicate must both still work");
        Assert.AreEqual("x", rows[0].Row["orders"].StrValue);
    }

    [Test]
    public async Task DropTableIsRefusedWhileAViewDependsOnIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW open_orders AS SELECT id FROM orders");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "DROP TABLE orders"));

        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, ex!.Code);
        StringAssert.Contains("open_orders", ex.Message);

        // Dropping the view first releases the table.
        await ExecDdl(database, executor, dbname, "DROP VIEW open_orders");
        Assert.DoesNotThrowAsync(async () => await ExecDdl(database, executor, dbname, "DROP TABLE orders"));
    }

    /// <summary>
    /// Views are read-only until updatable views land. The point of this test is that each write
    /// fails <i>loudly and specifically</i> — a user who tries must be told the object is a view and
    /// that writes go to the base table, not handed "table does not exist" about something they can
    /// see in SHOW VIEWS, and certainly not have the write silently go nowhere.
    /// </summary>
    [Test]
    public async Task WritesThroughAViewAreRefusedWithASpecificError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");

        foreach (string write in new[]
                 {
                     "INSERT INTO open_orders (id, customer, total) VALUES (99, 'x', 1)",
                     "UPDATE open_orders SET total = 1 WHERE id = 1",
                     "DELETE FROM open_orders WHERE id = 1",
                 })
        {
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
                async () => await ExecNonQuery(database, executor, dbname, write),
                $"'{write}' must be refused");

            Assert.AreEqual(CamusDBErrorCodes.ViewNotUpdatable, ex!.Code, $"for: {write}");
            StringAssert.Contains("view", ex.Message);
        }

        // The base table must be untouched by every one of those attempts.
        Assert.AreEqual(5, (await ExecQuery(database, executor, dbname, "SELECT id FROM orders")).Count);
    }

    [Test]
    public async Task ShowViewsListsViews()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW open_orders AS SELECT id FROM orders");
        await ExecDdl(database, executor, dbname, "CREATE VIEW closed_orders AS SELECT id FROM orders");

        List<QueryResultRow> all = await ExecQuery(database, executor, dbname, "SHOW VIEWS");
        Assert.AreEqual(2, all.Count);

        List<QueryResultRow> filtered = await ExecQuery(database, executor, dbname, "SHOW VIEWS LIKE 'open%'");
        Assert.AreEqual(1, filtered.Count);
        Assert.AreEqual("open_orders", filtered[0].Row["views"].StrValue);
    }

    [Test]
    public async Task ShowCreateViewPrintsAReparsableDefinition()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW open_orders");

        Assert.AreEqual(1, rows.Count);
        string ddl = rows[0].Row["create view"].StrValue!;
        StringAssert.Contains("CREATE VIEW", ddl);
        StringAssert.Contains("status = 'open'", ddl);

        // The printed DDL must be something the server would accept back — that is the whole point
        // of a SHOW CREATE, and a renderer that drops or mangles a clause fails here.
        await ExecDdl(database, executor, dbname, "DROP VIEW open_orders");
        Assert.DoesNotThrowAsync(async () => await ExecDdl(database, executor, dbname, ddl));
        Assert.AreEqual(3, (await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders")).Count);
    }

    [Test]
    public async Task ShowCreateViewOnAMissingViewIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW nope"));

        Assert.AreEqual(CamusDBErrorCodes.ViewDoesntExist, ex!.Code);
    }

    [Test]
    public async Task ShowColumnsWorksOnAView()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW v (order_id, who) AS SELECT id, customer FROM orders");

        List<QueryResultRow> columns = await ExecQuery(database, executor, dbname, "SHOW COLUMNS FROM v");

        Assert.AreEqual(2, columns.Count);
        Assert.AreEqual("order_id", columns[0].Row["Field"].StrValue);
        Assert.AreEqual("who", columns[1].Row["Field"].StrValue);

        // DESCRIBE is the same statement and must behave identically on a view.
        Assert.AreEqual(2, (await ExecQuery(database, executor, dbname, "DESCRIBE v")).Count);
    }

    /// <summary>
    /// The output schema a client is sent must name the columns the rows are actually keyed by. A view
    /// expands into a single derived source, which keys its rows bare — qualifying the schema as
    /// <c>view.column</c> there resolved nothing per row and shipped an all-null result set to every
    /// network client while the in-process cursor was correct.
    /// </summary>
    [Test]
    public async Task StarThroughAViewEmitsASchemaThatResolvesAgainstTheRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT * FROM orders WHERE status = 'open'");

        QuerySchemaHolder schema = new();
        List<QueryResultRow> rows = await ExecQuery(
            database, executor, dbname, "SELECT * FROM open_orders", schema);

        Assert.AreEqual(3, rows.Count);
        Assert.AreEqual(4, schema.Schema.Count);

        foreach (QueryResultRow row in rows)
        {
            foreach (DerivedColumnSchema column in schema.Schema)
            {
                Assert.IsTrue(
                    row.Row.ContainsKey(column.Name),
                    $"output column '{column.Name}' does not exist in the row, so it encodes as null on the wire");
            }
        }
    }

    /// <summary>
    /// The counterpart to the test above: a real join does key its rows <c>alias.column</c>, so the
    /// schema must keep qualifying there. Both shapes reach this code through <c>IsMultiSource</c>.
    /// </summary>
    [Test]
    public async Task StarOverAJoinedViewKeepsQualifiedColumnNames()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE customers (name string(64) PRIMARY KEY, region string(16))");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO customers (name, region) VALUES ('acme', 'eu'), ('globex', 'us'), ('initech', 'us')");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, customer FROM orders WHERE status = 'open'");

        QuerySchemaHolder schema = new();
        List<QueryResultRow> rows = await ExecQuery(
            database, executor, dbname,
            "SELECT * FROM open_orders v INNER JOIN customers c ON v.customer = c.name",
            schema);

        Assert.AreEqual(3, rows.Count);
        CollectionAssert.AreEquivalent(
            new[] { "c.name", "c.region", "v.id", "v.customer" },
            schema.Schema.Select(c => c.Name).ToList());

        foreach (QueryResultRow row in rows)
        {
            foreach (DerivedColumnSchema column in schema.Schema)
                Assert.IsTrue(row.Row.ContainsKey(column.Name), $"output column '{column.Name}' is missing from the row");
        }
    }

    [Test]
    public async Task ShowTablesDoesNotListViews()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW v AS SELECT id FROM orders");

        List<QueryResultRow> tables = await ExecQuery(database, executor, dbname, "SHOW TABLES");

        Assert.IsFalse(
            tables.Any(r => r.Row["tables"].StrValue == "v"),
            "SHOW TABLES lists tables; changing its output would break existing clients");
    }
}
