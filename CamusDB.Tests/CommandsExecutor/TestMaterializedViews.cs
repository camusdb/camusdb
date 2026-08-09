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
using Kommander.Time;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Cache;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for materialized views.
///
/// <para>They drive real SQL through <c>ExecuteDDLSQL</c> / <c>ExecuteNonSQLQuery</c> /
/// <c>ExecuteSQLQuery</c> rather than calling the controllers, because the properties worth
/// asserting are behavioral: that a materialized view keeps returning its <em>stored</em> rows after
/// the base table changes, that a refresh actually replaces them, and that the build-and-swap leaves
/// the relation queryable — including by its indexes — the instant it completes. A test that
/// inspected the schema object would pass on all of those while the data path was broken.</para>
/// </summary>
[NonParallelizable]
public sealed class TestMaterializedViews : SharedNodeBaseTest
{
    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<int> ExecNonQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
        return result.ModifiedRows;
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
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

    /// <summary>Runs a read in an autocommit snapshot so the cache is reachable at all — an explicit
    /// transaction must read live storage and bypasses it for that reason alone.</summary>
    private static async Task<List<QueryResultRow>> ExecQueryWithCacheMeta(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, CacheMetadataHolder meta)
    {
        ExecuteSQLTicket ticket = new(txnState: KvTransaction.CreateReadOnly(), database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket, meta);
        return await cursor.ToListAsync();
    }

    private static List<long> Longs(IEnumerable<QueryResultRow> rows, string column)
        => rows.Select(r => r.Row[column].LongValue).ToList();

    [Test]
    public async Task CreateWithDataPopulatesAndIsQueryable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id, customer, total FROM open_orders ORDER BY id");

        CollectionAssert.AreEqual(new List<long> { 1, 2, 3 }, Longs(rows, "id"));
        Assert.AreEqual("acme", rows[0].Row["customer"].StrValue);
        Assert.AreEqual(30, rows[2].Row["total"].LongValue);
    }

    [Test]
    public async Task CreateReportsTheRowCountItMaterialized()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        int rows = await ExecNonQuery(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        Assert.AreEqual(3, rows, "CREATE MATERIALIZED VIEW must report how many rows it stored");
    }

    /// <summary>
    /// The property that distinguishes a materialized view from a view: its contents are a snapshot,
    /// and a later change to the base table does not reach them until a REFRESH does.
    /// </summary>
    [Test]
    public async Task ContentsDoNotFollowTheBaseTableUntilRefreshed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, customer, total, status) VALUES (6, 'acme', 60, 'open')");

        List<QueryResultRow> stale = await ExecQuery(database, executor, dbname,
            "SELECT id FROM open_orders ORDER BY id");
        CollectionAssert.AreEqual(new List<long> { 1, 2, 3 }, Longs(stale, "id"),
            "a materialized view must keep returning what it stored, not what the base table now holds");

        int refreshed = await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");
        Assert.AreEqual(4, refreshed);

        List<QueryResultRow> fresh = await ExecQuery(database, executor, dbname,
            "SELECT id FROM open_orders ORDER BY id");
        CollectionAssert.AreEqual(new List<long> { 1, 2, 3, 6 }, Longs(fresh, "id"));
    }

    [Test]
    public async Task RefreshReplacesRowsRatherThanAppendingThem()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");
        await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders");
        Assert.AreEqual(3, rows.Count, "repeated refreshes must replace the contents, not accumulate them");
    }

    /// <summary>
    /// Deleting from the base table is the case a "delete only what disappeared" implementation gets
    /// wrong; a wholesale rebuild has to shrink the relation.
    /// </summary>
    [Test]
    public async Task RefreshRemovesRowsThatNoLongerQualify()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        await ExecNonQuery(database, executor, dbname, "DELETE FROM orders WHERE id = 1");
        await ExecNonQuery(database, executor, dbname, "UPDATE orders SET status = 'closed' WHERE id = 2");

        int refreshed = await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");
        Assert.AreEqual(1, refreshed);

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders");
        CollectionAssert.AreEqual(new List<long> { 3 }, Longs(rows, "id"));
    }

    [Test]
    public async Task AggregatingBodyIsMaterialized()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW customer_totals AS " +
            "SELECT customer, SUM(total) AS total_spent FROM orders GROUP BY customer");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT customer, total_spent FROM customer_totals ORDER BY customer");

        Assert.AreEqual(3, rows.Count);
        Assert.AreEqual("acme", rows[0].Row["customer"].StrValue);
        Assert.AreEqual(30, rows[0].Row["total_spent"].LongValue);
        Assert.AreEqual(70, rows[1].Row["total_spent"].LongValue, "globex");
        Assert.AreEqual(50, rows[2].Row["total_spent"].LongValue, "initech");
    }

    [Test]
    public async Task WithNoDataLeavesItUnpopulatedAndReadingItIsAnError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open' WITH NO DATA");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders"));

        Assert.AreEqual(CamusDBErrorCodes.MaterializedViewNotPopulated, error!.Code,
            "an unpopulated materialized view must raise rather than look like a correct empty answer");

        await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders");
        Assert.AreEqual(3, rows.Count, "a refresh must make it readable");
    }

    [Test]
    public async Task RefreshWithNoDataEmptiesItAgain()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders WITH NO DATA");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders"));

        Assert.AreEqual(CamusDBErrorCodes.MaterializedViewNotPopulated, error!.Code);
    }

    [Test]
    public async Task ConcurrentlyIsRefusedRatherThanTreatedAsASynonym()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW CONCURRENTLY open_orders"));

        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, error!.Code);
    }

    [Test]
    public async Task WritesAreRefusedWithASpecificError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        foreach (string sql in new[]
        {
            "INSERT INTO open_orders (id, total) VALUES (99, 1)",
            "UPDATE open_orders SET total = 1 WHERE id = 1",
            "DELETE FROM open_orders WHERE id = 1",
        })
        {
            CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecNonQuery(database, executor, dbname, sql));

            Assert.AreEqual(CamusDBErrorCodes.ViewNotUpdatable, error!.Code, sql);
        }
    }

    /// <summary>
    /// A materialized view is stored as a relation, which is exactly why the two statements have to
    /// be kept apart: DROP TABLE would otherwise silently remove one.
    /// </summary>
    [Test]
    public async Task DropTableAndDropViewBothRefuseAMaterializedView()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "DROP TABLE open_orders"));

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "DROP VIEW open_orders"));

        // Still there and still readable after both refusals.
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders");
        Assert.AreEqual(3, rows.Count);
    }

    [Test]
    public async Task DropRemovesIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");
        await ExecDdl(database, executor, dbname, "DROP MATERIALIZED VIEW open_orders");

        List<QueryResultRow> listed = await ExecQuery(database, executor, dbname, "SHOW MATERIALIZED VIEWS");
        Assert.IsEmpty(listed);

        // IF EXISTS makes the second drop a no-op rather than an error.
        await ExecDdl(database, executor, dbname, "DROP MATERIALIZED VIEW IF EXISTS open_orders");
    }

    [Test]
    public async Task NameCollidesWithTablesAndViews()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE MATERIALIZED VIEW orders AS SELECT id FROM orders"));

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id FROM orders WHERE status = 'open'");

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE VIEW open_orders AS SELECT id FROM orders"));

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE TABLE open_orders (id int64 PRIMARY KEY)"));

        // IF NOT EXISTS turns the collision into a no-op.
        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW IF NOT EXISTS open_orders AS SELECT id FROM orders");
    }

    [Test]
    public async Task ListedByShowMaterializedViewsAndNotByShowTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        List<QueryResultRow> tables = await ExecQuery(database, executor, dbname, "SHOW TABLES");
        CollectionAssert.AreEqual(
            new List<string> { "orders" },
            tables.Select(r => r.Row["tables"].StrValue!).ToList(),
            "SHOW TABLES must list tables only — not materialized views, and not the relations a refresh stages into");

        List<QueryResultRow> matViews = await ExecQuery(database, executor, dbname, "SHOW MATERIALIZED VIEWS");
        Assert.AreEqual(1, matViews.Count);
        Assert.AreEqual("open_orders", matViews[0].Row["materialized_views"].StrValue);
        Assert.IsTrue(matViews[0].Row["populated"].BoolValue);
        Assert.IsNotEmpty(matViews[0].Row["refreshed_at"].StrValue!, "a populated view reports the snapshot it holds");
    }

    [Test]
    public async Task ShowCreateMaterializedViewRendersAReproducibleStatement()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open' WITH NO DATA");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SHOW CREATE MATERIALIZED VIEW open_orders");

        string sql = rows[0].Row["create materialized view"].StrValue!;
        StringAssert.Contains("CREATE MATERIALIZED VIEW", sql);
        StringAssert.Contains("open_orders", sql);
        StringAssert.Contains("WITH NO DATA", sql,
            "an unpopulated view must render the clause that reproduces it unpopulated");
    }

    [Test]
    public async Task DescribeWorksOnAMaterializedView()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open' WITH NO DATA");

        // Unpopulated on purpose: describing a materialized view says what shape it has, which is
        // knowable — and needed — before it holds anything.
        List<QueryResultRow> columns = await ExecQuery(database, executor, dbname, "SHOW COLUMNS FROM open_orders");

        List<string> names = columns.Select(r => r.Row["Field"].StrValue!).ToList();
        CollectionAssert.Contains(names, "id");
        CollectionAssert.Contains(names, "total");
    }

    [Test]
    public async Task ColumnAliasListRenamesTheStoredColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders (order_id, amount) AS " +
            "SELECT id, total FROM orders WHERE status = 'open'");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT order_id, amount FROM open_orders ORDER BY order_id");

        CollectionAssert.AreEqual(new List<long> { 1, 2, 3 }, Longs(rows, "order_id"));
        Assert.AreEqual(10, rows[0].Row["amount"].LongValue);
    }

    /// <summary>
    /// An index built on a materialized view has to survive a refresh, because the refresh replaces
    /// the underlying relation entirely. If the rebuild did not carry the index definitions across,
    /// this query would still return the right answer — by scanning — so the test asserts the index
    /// is still declared as well as that the lookup works.
    /// </summary>
    [Test]
    public async Task IndexesSurviveARefreshAndStillResolve()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");

        await ExecDdl(database, executor, dbname, "CREATE INDEX open_orders_customer ON open_orders (customer)");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, customer, total, status) VALUES (7, 'acme', 70, 'open')");
        await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");

        List<QueryResultRow> indexes = await ExecQuery(database, executor, dbname, "SHOW INDEXES FROM open_orders");
        CollectionAssert.Contains(
            indexes.Select(r => r.Row["Key_name"].StrValue!).ToList(),
            "open_orders_customer",
            "a rebuild must carry the materialized view's indexes onto the relation it swaps in");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id FROM open_orders WHERE customer = 'acme' ORDER BY id");
        CollectionAssert.AreEqual(new List<long> { 1, 2, 7 }, Longs(rows, "id"));
    }

    [Test]
    public async Task RenameKeepsTheContentsAndTheDefinition()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        await ExecDdl(database, executor, dbname, "ALTER MATERIALIZED VIEW open_orders RENAME TO live_orders");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id FROM live_orders ORDER BY id");
        CollectionAssert.AreEqual(new List<long> { 1, 2, 3 }, Longs(rows, "id"));

        // Still a materialized view under the new name, and still refreshable.
        int refreshed = await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW live_orders");
        Assert.AreEqual(3, refreshed);
    }

    /// <summary>
    /// A plain view may read a materialized view; dropping the materialized view out from under it
    /// would leave the view as a delayed error for whoever reads it next.
    /// </summary>
    [Test]
    public async Task AViewMayReadAMaterializedViewAndBlocksItsDrop()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW acme_open AS SELECT id, total FROM open_orders WHERE customer = 'acme'");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id FROM acme_open ORDER BY id");
        CollectionAssert.AreEqual(new List<long> { 1, 2 }, Longs(rows, "id"));

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "DROP MATERIALIZED VIEW open_orders"));
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, error!.Code);

        await ExecDdl(database, executor, dbname, "DROP MATERIALIZED VIEW open_orders CASCADE");

        List<QueryResultRow> views = await ExecQuery(database, executor, dbname, "SHOW VIEWS");
        Assert.IsEmpty(views, "CASCADE must take the dependent view with it");
    }

    /// <summary>
    /// The rebuild is chunked, and a chunk boundary is exactly where a "commit per chunk" bug would
    /// lose or duplicate rows. Configured through the engine's own options because a knob set after
    /// construction is a no-op.
    /// </summary>
    [Test]
    public async Task RebuildSpanningManyChunksMaterializesEveryRowExactlyOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { MaterializedViewRefreshChunkRows = 7 });

        await ExecDdl(database, executor, dbname, "CREATE TABLE numbers (id int64 PRIMARY KEY, tag string(8))");

        const int total = 50;
        string values = string.Join(", ", Enumerable.Range(1, total).Select(i => $"({i}, 'n')"));
        await ExecNonQuery(database, executor, dbname, $"INSERT INTO numbers (id, tag) VALUES {values}");

        int materialized = await ExecNonQuery(database, executor, dbname,
            "CREATE MATERIALIZED VIEW all_numbers AS SELECT id, tag FROM numbers");

        Assert.AreEqual(total, materialized);

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id FROM all_numbers ORDER BY id");
        CollectionAssert.AreEqual(Enumerable.Range(1, total).Select(i => (long)i).ToList(), Longs(rows, "id"),
            "a rebuild split across chunk-sized transactions must produce each source row exactly once");

        // And again, so the swap's replacement path is exercised at multi-chunk size too.
        int refreshed = await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW all_numbers");
        Assert.AreEqual(total, refreshed);
        Assert.AreEqual(total, (await ExecQuery(database, executor, dbname, "SELECT id FROM all_numbers")).Count);
    }

    [Test]
    public async Task RefreshOnATableOrAViewIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW open_orders AS SELECT id FROM orders");

        foreach (string name in new[] { "orders", "open_orders", "no_such_thing" })
        {
            CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecNonQuery(database, executor, dbname, $"REFRESH MATERIALIZED VIEW {name}"));

            Assert.AreEqual(CamusDBErrorCodes.ViewDoesntExist, error!.Code, name);
        }
    }

    [Test]
    public async Task RefreshIsRefusedWhenDisabledByConfiguration()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { MaterializedViewRefreshEnabled = false });

        await SeedOrders(database, executor, dbname);

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname,
                "CREATE MATERIALIZED VIEW open_orders AS SELECT id FROM orders"));

        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, error!.Code);

        // WITH NO DATA never refreshes, so it remains available on a node that does not run rebuilds.
        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id FROM orders WITH NO DATA");
    }

    /// <summary>
    /// The relation a refresh builds into must not be reachable, or a user could query, drop or
    /// collide with an object that is about to stop existing.
    /// </summary>
    [Test]
    public async Task StagingRelationsAreNotLeftBehindAfterASuccessfulRefresh()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");
        await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");

        List<string> staged = database.Schema.Tables.Keys
            .Where(MaterializedViewNaming.IsStagingRelation)
            .ToList();

        CollectionAssert.IsEmpty(staged, "a completed refresh must leave no staging relation in the schema");
    }

    /// <summary>
    /// The swap publishes a relation with a different id, different column ids and a different
    /// keyspace than the one it replaces, and it persists that through the schema checkpoint. Nothing
    /// in memory would notice a checkpoint that wrote the wrong relation, or that failed to detach the
    /// old one — only a reopen does, which is what makes this the test that matters most for refresh.
    /// </summary>
    [Test]
    public async Task RefreshedContentsSurviveACloseAndReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");
        await ExecDdl(database, executor, dbname, "CREATE INDEX open_orders_customer ON open_orders (customer)");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, customer, total, status) VALUES (8, 'acme', 80, 'open')");
        await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        TableSchema relation = reopened.Schema.Tables["open_orders"];
        Assert.IsTrue(relation.IsMaterializedView, "it must come back as a materialized view, not a table");
        Assert.IsTrue(relation.IsPopulated);
        Assert.IsNotNull(relation.ViewDefinition, "the defining query must survive, or it could never be refreshed again");

        List<QueryResultRow> rows = await ExecQuery(reopened, executor, dbname,
            "SELECT id FROM open_orders ORDER BY id");
        CollectionAssert.AreEqual(new List<long> { 1, 2, 3, 8 }, Longs(rows, "id"),
            "a reopen must read the relation the swap published, not the one it replaced");

        // Still refreshable after the reopen: the stored definition has to be usable, not merely present.
        int refreshed = await ExecNonQuery(reopened, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");
        Assert.AreEqual(4, refreshed);

        List<QueryResultRow> indexes = await ExecQuery(reopened, executor, dbname, "SHOW INDEXES FROM open_orders");
        CollectionAssert.Contains(indexes.Select(r => r.Row["Key_name"].StrValue!).ToList(), "open_orders_customer");
    }

    /// <summary>
    /// Clients route every non-SELECT statement to whichever endpoint they use for those, so view and
    /// materialized-view DDL has to work through the non-query path and not only the DDL one. It did
    /// not: the non-query dispatcher had no arm for any of it and answered "Unknown non-query AST
    /// stmt", which to a caller is indistinguishable from the statement not being supported at all.
    /// </summary>
    [Test]
    public async Task ViewDdlWorksThroughTheNonQueryPathToo()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        // Every statement below goes through ExecuteNonSQLQuery, never ExecuteDDLSQL.
        await ExecNonQuery(database, executor, dbname, "CREATE VIEW open_orders AS SELECT id FROM orders");
        await ExecNonQuery(database, executor, dbname,
            "CREATE OR REPLACE VIEW open_orders AS SELECT id FROM orders WHERE status = 'open'");
        await ExecNonQuery(database, executor, dbname, "ALTER VIEW open_orders RENAME TO live_orders");
        // Ownership transfer needs users, which this unauthenticated fixture has none of — so what is
        // asserted here is the ROUTING: it must reach the OWNER TO implementation and be refused for a
        // missing user, not bounce off the dispatcher as an unknown statement. It was the last view
        // statement still doing the latter. The authorized behavior is covered in TestMaterializedViewAuth.
        CamusDBException? ownerTo = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "ALTER VIEW live_orders OWNER TO somebody"));

        Assert.AreEqual(CamusDBErrorCodes.UserDoesNotExist, ownerTo!.Code);
        Assert.AreNotEqual(CamusDBErrorCodes.InvalidAstStmt, ownerTo.Code);

        Assert.AreEqual(3, (await ExecQuery(database, executor, dbname, "SELECT id FROM live_orders")).Count);

        int materialized = await ExecNonQuery(database, executor, dbname,
            "CREATE MATERIALIZED VIEW order_totals AS SELECT id, total FROM orders");
        Assert.AreEqual(5, materialized, "the non-query path must report the rows a CREATE materialized");

        await ExecNonQuery(database, executor, dbname,
            "ALTER MATERIALIZED VIEW order_totals RENAME TO totals");
        Assert.AreEqual(5, (await ExecQuery(database, executor, dbname, "SELECT id FROM totals")).Count);

        await ExecNonQuery(database, executor, dbname, "DROP MATERIALIZED VIEW totals");
        await ExecNonQuery(database, executor, dbname, "DROP MATERIALIZED VIEW IF EXISTS totals");
        await ExecNonQuery(database, executor, dbname, "DROP VIEW live_orders");
        await ExecNonQuery(database, executor, dbname, "DROP VIEW IF EXISTS live_orders");

        Assert.IsEmpty(await ExecQuery(database, executor, dbname, "SHOW VIEWS"));
        Assert.IsEmpty(await ExecQuery(database, executor, dbname, "SHOW MATERIALIZED VIEWS"));
    }

    /// <summary>
    /// Table DDL reaches the non-query path for the same reason view DDL does, and used to fail there
    /// the same way. One list in <c>StatementScope</c> decides what the non-query entry point forwards,
    /// so this covers the whole family rather than the statements that happened to be reported.
    /// </summary>
    [Test]
    public async Task TableDdlWorksThroughTheNonQueryPathToo()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecNonQuery(database, executor, dbname, "CREATE TABLE probe (id int64 PRIMARY KEY, v int64)");
        await ExecNonQuery(database, executor, dbname, "CREATE TABLE IF NOT EXISTS probe (id int64 PRIMARY KEY)");
        await ExecNonQuery(database, executor, dbname, "ALTER TABLE probe ADD COLUMN note string(32)");
        await ExecNonQuery(database, executor, dbname, "CREATE INDEX probe_v ON probe (v)");
        await ExecNonQuery(database, executor, dbname, "ALTER TABLE probe RENAME COLUMN note TO memo");
        await ExecNonQuery(database, executor, dbname, "ALTER TABLE probe RENAME TO probe2");

        await ExecNonQuery(database, executor, dbname, "INSERT INTO probe2 (id, v, memo) VALUES (1, 2, 'x')");
        Assert.AreEqual(1, (await ExecQuery(database, executor, dbname, "SELECT id FROM probe2")).Count);

        await ExecNonQuery(database, executor, dbname, "ALTER TABLE probe2 DROP INDEX probe_v");
        await ExecNonQuery(database, executor, dbname, "DROP TABLE probe2");
        await ExecNonQuery(database, executor, dbname, "DROP TABLE IF EXISTS probe2");

        Assert.IsEmpty(await ExecQuery(database, executor, dbname, "SHOW TABLES"));
    }

    /// <summary>
    /// A materialized view is a physical relation, so the result cache treats it exactly as it treats
    /// a table — no special case, and the hint is accepted rather than refused.
    ///
    /// <para>A plain view is different, and the difference is worth pinning: expansion turns it into a
    /// derived table, and the cache fences one physical table's row keyspace per entry, so there is
    /// nothing to fence. The hint used to be <b>rejected outright</b> on a view, which failed the
    /// whole query; it is now carried and reported as a bypass, the same treatment a join gets. That
    /// is a truthful answer, not a fix: reading through a view is still not cached.</para>
    /// </summary>
    [Test]
    public async Task CacheHintIsAcceptedOnAMaterializedViewAndReportedAsBypassedOnAView()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE VIEW open_orders AS SELECT id, total FROM orders");
        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW order_totals AS SELECT id, total FROM orders");

        // Through a view: returns rows, and says what became of the hint.
        CacheMetadataHolder viewMeta = new();
        Assert.AreEqual(5, (await ExecQueryWithCacheMeta(
            database, executor, dbname, "SELECT id FROM open_orders {cache=c}", viewMeta)).Count,
            "a cache hint on a view must no longer fail the query");

        Assert.AreEqual("c", viewMeta.CacheName);
        Assert.AreEqual(QueryCacheStatus.Bypass, viewMeta.Status);
        Assert.AreEqual(QueryCacheBypassReason.DerivedSource, viewMeta.BypassReason,
            "a view is not a join, and reporting one would send its author looking for a join they did not write");

        // An index hint stays refused: a view has no indexes for it to name.
        CamusDBException? indexHint = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders @{force_index=id}"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, indexHint!.Code);

        // Through a materialized view: accepted, and never reported as a derived-source bypass —
        // it is a relation, so whatever the cache does with a table it does with this.
        CacheMetadataHolder matViewMeta = new();
        Assert.AreEqual(5, (await ExecQueryWithCacheMeta(
            database, executor, dbname, "SELECT id FROM order_totals {cache=c}", matViewMeta)).Count);

        Assert.AreNotEqual(QueryCacheBypassReason.DerivedSource, matViewMeta.BypassReason);
        Assert.AreNotEqual(QueryCacheBypassReason.Join, matViewMeta.BypassReason);
    }

    /// <summary>
    /// A row is never one mutation: it writes itself plus an entry in every index. Chunking purely by
    /// row count therefore promises a chunk size the transaction layer will refuse — and on stock
    /// settings it already does, because the 10,000-row default times the primary key alone reaches
    /// the 20,000-mutation default exactly, and any secondary index puts it over.
    ///
    /// <para>Configured here so the two limits collide at a size a test can reach: a 40-row chunk
    /// against a 60-mutation ceiling is impossible for a relation with two indexes (40 x 3 = 120), so
    /// the refresh must derive a smaller chunk rather than take the configured one at face value.</para>
    /// </summary>
    [Test]
    public async Task RefreshChunksByMutationCostNotRowCount()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(
            Options with { MaterializedViewRefreshChunkRows = 40, MaxMutationsPerTransaction = 60 });

        await ExecDdl(database, executor, dbname, "CREATE TABLE numbers (id int64 PRIMARY KEY, tag string(8))");

        const int total = 120;

        // Seeded in batches small enough for the same tight ceiling.
        foreach (int[] batch in Enumerable.Range(1, total).Chunk(15))
        {
            string values = string.Join(", ", batch.Select(i => $"({i}, 't{i % 7}')"));
            await ExecNonQuery(database, executor, dbname, $"INSERT INTO numbers (id, tag) VALUES {values}");
        }

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW all_numbers AS SELECT id, tag FROM numbers WITH NO DATA");

        // A second index on top of the synthesized primary key: three mutations per row.
        await ExecDdl(database, executor, dbname, "CREATE INDEX all_numbers_tag ON all_numbers (tag)");

        int materialized = await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW all_numbers");

        Assert.AreEqual(total, materialized,
            "the refresh must size its chunks from the mutation ceiling, not from the configured row count");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id FROM all_numbers ORDER BY id");
        CollectionAssert.AreEqual(Enumerable.Range(1, total).Select(i => (long)i).ToList(), Longs(rows, "id"));
    }

    /// <summary>
    /// A materialized view stores a query, so it reads things — it is a dependency consumer, not only
    /// a dependency target. Its definition lives on a relation rather than in the view map, and the
    /// dependency walk used to look only at the view map, so every edge <em>out</em> of a materialized
    /// view was invisible: its sources could be dropped from under it, leaving a view that fails at
    /// its next refresh rather than at the statement that broke it.
    /// </summary>
    [Test]
    public async Task AMaterializedViewIsADependencyConsumerToo()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW order_totals AS SELECT id, total FROM orders");

        // Dropping the base table would leave the materialized view unrefreshable.
        CamusDBException? droppingSource = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "DROP TABLE orders"));
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, droppingSource!.Code);

        // Same through a view: matview reads view reads table.
        await ExecDdl(database, executor, dbname, "CREATE VIEW open_orders AS SELECT id, total FROM orders");
        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_totals AS SELECT id, total FROM open_orders");

        CamusDBException? droppingView = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "DROP VIEW open_orders"));
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, droppingView!.Code);

        StringAssert.Contains("open_totals", droppingView.Message,
            "the materialized view must be named as the dependent that blocks the drop");
    }

    /// <summary>
    /// A snapshot hold is leased, and a rebuild can outlive the lease. Once it lapses, revision GC is
    /// free to reclaim past the pinned timestamp, so the rest of the scan reads against a moved floor
    /// and the refresh finishes holding <em>some</em> of the rows it should — a wrong answer that looks
    /// exactly like a right one. The refresh must refuse to publish instead.
    /// </summary>
    [Test]
    public async Task ARefreshThatLosesItsPinnedSnapshotRefusesToPublish()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        // Contents to protect: whatever the refresh does, these must survive a failed one.
        CollectionAssert.AreEqual(
            new List<long> { 1, 2, 3 },
            Longs(await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders ORDER BY id"), "id"));

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, customer, total, status) VALUES (9, 'acme', 90, 'open')");

        SnapshotHoldLease.LoseEveryHoldForTesting = true;
        try
        {
            CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders"));

            Assert.AreEqual(CamusDBErrorCodes.InvalidAsOfSystemTime, error!.Code);
        }
        finally
        {
            SnapshotHoldLease.LoseEveryHoldForTesting = false;
        }

        // The materialized view still holds its previous contents — not the partial rebuild.
        CollectionAssert.AreEqual(
            new List<long> { 1, 2, 3 },
            Longs(await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders ORDER BY id"), "id"),
            "a refusal must leave the previous contents exactly as they were");

        // And the abandoned build left nothing registered behind.
        CollectionAssert.IsEmpty(
            database.Schema.Tables.Keys.Where(MaterializedViewNaming.IsStagingRelation).ToList());

        // A later healthy refresh still works.
        Assert.AreEqual(4, await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders"));
    }

    /// <summary>
    /// The contents generation is what lets anything holding a result notice, since neither the
    /// relation id nor the schema version moves across a refresh.
    /// </summary>
    [Test]
    public async Task EachRefreshAdvancesTheContentsGeneration()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id FROM orders WHERE status = 'open'");

        TableSchema view = database.Schema.Tables["open_orders"];
        long afterCreate = view.ContentsGeneration;
        int schemaVersion = view.Version;

        await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");
        long afterRefresh = database.Schema.Tables["open_orders"].ContentsGeneration;

        Assert.Greater(afterRefresh, afterCreate, "a refresh must advance the contents generation");
        Assert.AreEqual(schemaVersion, database.Schema.Tables["open_orders"].Version,
            "the schema version must NOT move — that is precisely why a separate generation is needed");

        // And it survives a reopen, so a node that restarts cannot reuse a generation it already used.
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        Assert.AreEqual(afterRefresh, reopened.Schema.Tables["open_orders"].ContentsGeneration);
    }

    /// <summary>
    /// A rebuild copies the materialized view's column and index layout when it starts staging, and
    /// publishes that copy at the swap. DDL committed in between is therefore a lost update: the copy
    /// predates it, so an index created during the rebuild disappears from the schema — and its
    /// entries were never written into the new key-space either, so it is lost twice over.
    ///
    /// <para>The competing DDL is committed through the refresher's staging seam, which puts it
    /// exactly inside the window a real race would occupy — deterministically, with no threads or
    /// sleeps. The required outcome is not "the refresh wins" or "the DDL wins" but that neither is
    /// silently discarded: the refresh is refused, retryably, and the DDL stands.</para>
    /// </summary>
    [Test]
    public async Task DdlCommittedDuringARebuildIsNotSilentlyDiscardedByTheSwap()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");

        MaterializedViewRefresher.AfterStagingForTesting = async () =>
        {
            // Runs once, inside the rebuild window. Cleared immediately so the DDL's own machinery —
            // and the retry below — do not re-enter it.
            MaterializedViewRefresher.AfterStagingForTesting = null;
            await ExecDdl(database, executor, dbname,
                "CREATE INDEX open_orders_customer ON open_orders (customer)");
        };

        try
        {
            CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders"));

            Assert.AreEqual(CamusDBErrorCodes.ConcurrentSchemaChange, error!.Code);
        }
        finally
        {
            MaterializedViewRefresher.AfterStagingForTesting = null;
        }

        // The index the concurrent statement created is still there.
        List<QueryResultRow> indexes = await ExecQuery(database, executor, dbname, "SHOW INDEXES FROM open_orders");
        CollectionAssert.Contains(
            indexes.Select(r => r.Row["Key_name"].StrValue!).ToList(),
            "open_orders_customer",
            "the swap must not publish a layout that predates the index");

        // The materialized view still holds its previous contents, and no staging relation survived.
        CollectionAssert.AreEqual(
            new List<long> { 1, 2, 3 },
            Longs(await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders ORDER BY id"), "id"));

        CollectionAssert.IsEmpty(
            database.Schema.Tables.Keys.Where(MaterializedViewNaming.IsStagingRelation).ToList());

        // Retrying against the current definition succeeds, and keeps the index.
        Assert.AreEqual(3, await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders"));

        List<QueryResultRow> afterRetry = await ExecQuery(database, executor, dbname, "SHOW INDEXES FROM open_orders");
        CollectionAssert.Contains(
            afterRetry.Select(r => r.Row["Key_name"].StrValue!).ToList(), "open_orders_customer");

        List<QueryResultRow> byIndex = await ExecQuery(database, executor, dbname,
            "SELECT id FROM open_orders WHERE customer = 'acme' ORDER BY id");
        CollectionAssert.AreEqual(new List<long> { 1, 2 }, Longs(byIndex, "id"));
    }

    /// <summary>
    /// Renaming a relation rewrites the views that read it. Done as a second schema change after the
    /// rename, there is an interval in which a dependent's stored body still names the old relation —
    /// and because the rename frees that name, a relation created under it during the interval makes
    /// the stale body <em>resolve</em> to the new, unrelated relation and return its rows. Not a
    /// transient failure: a wrong answer, and a disclosure of data the view was never defined over.
    ///
    /// <para>Both must therefore land in one delta. This test asserts the observable consequence: at
    /// no point does the view read anything but the relation it was defined over.</para>
    /// </summary>
    [Test]
    public async Task RenamingARelationAndRewritingItsViewsIsOneChange()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        long schemaVersionBefore = database.Schema.SchemaVersion;

        await ExecDdl(database, executor, dbname, "ALTER TABLE orders RENAME TO sales");

        Assert.AreEqual(schemaVersionBefore + 1, database.Schema.SchemaVersion,
            "the rename and its dependent rewrites must be a single schema change, not one per view");

        // The body now names the new relation, and the view still reads the same rows.
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders ORDER BY id");
        CollectionAssert.AreEqual(new List<long> { 1, 2, 3 }, Longs(rows, "id"));

        // Now recreate the freed name with entirely different contents. The view must not see them.
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE orders (id int64 PRIMARY KEY, customer string(64), total int64, status string(16))");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, customer, total, status) VALUES (99, 'intruder', 999, 'open')");

        List<QueryResultRow> afterRecreate = await ExecQuery(database, executor, dbname,
            "SELECT id FROM open_orders ORDER BY id");

        CollectionAssert.AreEqual(new List<long> { 1, 2, 3 }, Longs(afterRecreate, "id"),
            "the view must still read the relation it was defined over, never the impostor under the freed name");

        List<QueryResultRow> shown = await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW open_orders");
        StringAssert.Contains("FROM sales", shown[0].Row["create view"].StrValue!);
    }

    /// <summary>
    /// The refresh fence must be the cluster-visible one, not a dictionary in one executor. Proven by
    /// taking the very fence the refresh uses from outside it: if refresh were still gated on process
    /// state it would proceed regardless, and two nodes could refresh the same view at once — each
    /// sweeping the other's staging relation away.
    /// </summary>
    [Test]
    public async Task RefreshIsGatedOnTheClusterFenceNotOnProcessState()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id FROM orders WHERE status = 'open'");

        string viewTableId = database.Schema.Tables["open_orders"].Id!;
        DatabaseRegistry registry = await executor.GetDatabaseRegistryAsync();
        string fenceId = DatabaseRegistry.TableFenceId(database.Id, viewTableId);

        Assert.IsTrue(await registry.AcquireDropIntentAsync(fenceId), "the fence must be free to begin with");
        try
        {
            CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders"));

            Assert.AreEqual(CamusDBErrorCodes.RefreshAlreadyInProgress, error!.Code);
        }
        finally
        {
            await registry.ReleaseDropIntentAsync(fenceId);
        }

        // Once released, the refresh proceeds — the fence gates it, it does not permanently block it.
        Assert.AreEqual(3, await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders"));
    }

    /// <summary>
    /// Staging storage must have a durable owner. Before this record existed, a process that died
    /// mid-rebuild left a registered relation full of rows that only a later refresh of the same view
    /// would ever look for — so if the view were dropped, or never refreshed again, it leaked for the
    /// life of the database.
    /// </summary>
    [Test]
    public async Task StagingStorageIsOwnedByADurableRecordAndReclaimedFromIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id FROM orders WHERE status = 'open'");

        string viewTableId = database.Schema.Tables["open_orders"].Id!;
        CatalogsManager catalogs = executor.GetCatalogsManagerForTesting();

        // The record must exist while the rebuild is in flight, and name the relation being built.
        MaterializedViewRefreshJob? inFlight = null;
        MaterializedViewRefresher.AfterStagingForTesting = async () =>
        {
            MaterializedViewRefresher.AfterStagingForTesting = null;
            inFlight = await catalogs.TryGetRefreshJobAsync(database, viewTableId);
        };

        try
        {
            await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");
        }
        finally
        {
            MaterializedViewRefresher.AfterStagingForTesting = null;
        }

        Assert.IsNotNull(inFlight, "a refresh must record durable ownership of its staging relation");
        Assert.AreEqual(viewTableId, inFlight!.ViewTableId);
        Assert.IsTrue(MaterializedViewNaming.IsStagingRelation(inFlight.StagingName));

        // And it must be gone once the staging relation has become the view.
        Assert.IsNull(await catalogs.TryGetRefreshJobAsync(database, viewTableId),
            "a completed refresh owns nothing and must leave no record");

        // Now stand in for a crashed run: a live staging relation with a record naming it, and no
        // process working on it. The next refresh must reclaim both.
        MaterializedViewRefresher.AfterStagingForTesting = null;
        string abandonedName = MaterializedViewNaming.StagingRelationName(viewTableId, "abandoned1");

        await ExecDdl(database, executor, dbname, "CREATE TABLE placeholder (id int64 PRIMARY KEY)");
        await catalogs.PersistRefreshJobAsync(database, new MaterializedViewRefreshJob
        {
            JobId = "crashed",
            ViewTableId = viewTableId,
            ViewName = "open_orders",
            StagingTableId = "abandoned1",
            StagingName = abandonedName,
        });

        Assert.IsNotNull(await catalogs.TryGetRefreshJobAsync(database, viewTableId));

        await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");

        Assert.IsNull(await catalogs.TryGetRefreshJobAsync(database, viewTableId),
            "the next refresh must reclaim an abandoned run's record");
        CollectionAssert.IsEmpty(
            database.Schema.Tables.Keys.Where(MaterializedViewNaming.IsStagingRelation).ToList());
    }

    /// <summary>
    /// Contents may only move forward. Two runs that both got past the fence — a lease lapsing under a
    /// stalled node — would otherwise be decided by whichever finished last, not by which read the
    /// newer source.
    /// </summary>
    [Test]
    public async Task AnOlderRebuildCannotOverwriteNewerPublishedContents()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id FROM orders WHERE status = 'open'");
        await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");

        TableSchema view = database.Schema.Tables["open_orders"];
        HLCTimestamp published = view.RefreshedAt!.Value;

        // A swap carrying a source snapshot older than what is already published must be refused.
        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.GetCatalogsManagerForTesting().SetMaterializedViewStateAsync(
                database,
                tableId: view.Id!,
                isPopulated: true,
                refreshedAt: new HLCTimestamp(published.N, published.L - 1000, published.C),
                swapToTableId: view.Id!,
                publishHlc: default,
                expectedMetadataGeneration: view.MetadataGeneration));

        Assert.AreEqual(CamusDBErrorCodes.ConcurrentSchemaChange, error!.Code);

        // The published contents are untouched.
        Assert.AreEqual(published, database.Schema.Tables["open_orders"].RefreshedAt);
    }

    /// <summary>
    /// The case neither of the other two reclamation paths can reach: a run abandoned while the
    /// database stays open, whose view is then never refreshed again and never dropped. Reclaiming on
    /// the next refresh only helps a view that is refreshed; reclaiming on drop only helps one that is
    /// dropped. Without a sweep, that storage stayed registered for the life of the database.
    ///
    /// <para>Eligibility is the fence, not elapsed time — a rebuild still running holds it, so being
    /// able to take it is proof the run that wrote the record is gone. The second half of this test is
    /// the one that matters: with the fence held, the sweep must leave the job alone.</para>
    /// </summary>
    [Test]
    public async Task TheBackgroundSweepReclaimsAbandonedRefreshesButNotLiveOnes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id FROM orders WHERE status = 'open'");

        string viewTableId = database.Schema.Tables["open_orders"].Id!;
        CatalogsManager catalogs = executor.GetCatalogsManagerForTesting();
        DatabaseRegistry registry = await executor.GetDatabaseRegistryAsync();
        string fenceId = DatabaseRegistry.TableFenceId(database.Id, viewTableId);

        async Task RecordAbandonedJob(string stagingId)
        {
            await catalogs.PersistRefreshJobAsync(database, new MaterializedViewRefreshJob
            {
                JobId = "crashed-" + stagingId,
                ViewTableId = viewTableId,
                ViewName = "open_orders",
                StagingTableId = stagingId,
                StagingName = MaterializedViewNaming.StagingRelationName(viewTableId, stagingId),
            });
        }

        // A refresh is running: the fence is held, and the sweep must not touch its storage.
        await RecordAbandonedJob("live1");
        Assert.IsTrue(await registry.AcquireDropIntentAsync(fenceId));
        try
        {
            Assert.AreEqual(0, await executor.ReclaimAbandonedRefreshesForTesting(dbname),
                "a job whose fence is held belongs to a live rebuild and must be left alone");

            Assert.IsNotNull(await catalogs.TryGetRefreshJobAsync(database, viewTableId));
        }
        finally
        {
            await registry.ReleaseDropIntentAsync(fenceId);
        }

        // Fence free — the run is gone, so the record and its storage are reclaimed.
        Assert.AreEqual(1, await executor.ReclaimAbandonedRefreshesForTesting(dbname));
        Assert.IsNull(await catalogs.TryGetRefreshJobAsync(database, viewTableId),
            "an abandoned run's record must be reclaimed without the view being refreshed or dropped");

        // The materialized view itself is untouched and still works.
        CollectionAssert.AreEqual(
            new List<long> { 1, 2, 3 },
            Longs(await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders ORDER BY id"), "id"));
        Assert.AreEqual(3, await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders"));
    }

    /// <summary>
    /// A refresh replaces what a materialized view <em>holds</em>, not the materialized view. Its id is
    /// the identity that privilege grants, dependency edges, cached results and statistics are all
    /// keyed by, so it has to survive — while the key-space its rows live in is exactly what changes.
    /// Asserting both together is the only way to catch a swap that got one of them backwards.
    /// </summary>
    [Test]
    public async Task RefreshKeepsTheRelationIdAndMovesOnlyItsStorage()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        TableSchema before = database.Schema.Tables["open_orders"];
        string idBefore = before.Id!;
        string storageBefore = before.EffectiveStorageId;

        await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");

        TableSchema after = database.Schema.Tables["open_orders"];
        Assert.AreEqual(idBefore, after.Id,
            "the relation id is the materialized view's identity and must not change on refresh");
        Assert.AreNotEqual(storageBefore, after.EffectiveStorageId,
            "the rebuilt contents must live in a new key-space, or nothing was actually swapped");
    }

    [Test]
    public async Task UnpopulatedStateSurvivesACloseAndReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id FROM orders WHERE status = 'open' WITH NO DATA");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(reopened, executor, dbname, "SELECT id FROM open_orders"));

        Assert.AreEqual(CamusDBErrorCodes.MaterializedViewNotPopulated, error!.Code,
            "an unpopulated materialized view must not come back from disk looking populated");
    }
}
