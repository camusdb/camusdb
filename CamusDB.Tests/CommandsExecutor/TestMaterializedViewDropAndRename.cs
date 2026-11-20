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
using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Replication;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// How <c>DROP MATERIALIZED VIEW</c>, <c>DROP VIEW … CASCADE</c> and the table rename treat a
/// materialized view: the drop is fenced against a running refresh, a cascade reaches every
/// dependent of either kind, and each kind of object is changed by its own statement.
/// </summary>
[NonParallelizable]
public sealed class TestMaterializedViewDropAndRename : SharedNodeBaseTest
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

    private static async Task SeedOrders(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE orders (id int64 PRIMARY KEY, customer string(64), total int64, status string(16))");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, customer, total, status) VALUES " +
            "(1, 'acme', 10, 'open'), (2, 'acme', 2000, 'open'), (3, 'globex', 30, 'open')");
    }

    private static void AssertNoStagingLeft(DatabaseDescriptor database)
        => CollectionAssert.IsEmpty(
            database.Schema.Tables.Keys.Where(MaterializedViewNaming.IsStagingRelation).ToList(),
            "no staging relation may outlive the statement");

    /// <summary>
    /// The refusal must name the object it refused and a statement that works. It once said "table"
    /// and suggested <c>DROP TABLE … CASCADE</c>, which is refused for a materialized view and has no
    /// <c>CASCADE</c> form at all, so a user who followed it failed twice.
    /// </summary>
    [Test]
    public async Task ARefusedDropNamesAMaterializedViewAndSuggestsAStatementThatWorks()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW customer_totals AS SELECT customer, SUM(total) AS total_spent FROM orders GROUP BY customer");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW big_spenders AS SELECT customer FROM customer_totals WHERE total_spent > 1000");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "DROP MATERIALIZED VIEW customer_totals"));

        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, error!.Code);
        Assert.AreEqual(
            "Cannot drop materialized view 'customer_totals' because other objects depend on it: big_spenders. " +
            "Use DROP MATERIALIZED VIEW ... CASCADE to drop them too.",
            error.Message);

        // The statement the message names does what it says.
        await ExecDdl(database, executor, dbname, "DROP MATERIALIZED VIEW customer_totals CASCADE");

        Assert.IsFalse(database.Schema.Tables.ContainsKey("customer_totals"));
        Assert.IsFalse(database.Schema.Views.ContainsKey("big_spenders"));
    }

    /// <summary>
    /// A cascade used to drop only the plain views that read the materialized view directly. A view
    /// over such a view was left reading something gone, and a materialized view over it made the
    /// final relation drop refuse after the plain views had already been removed.
    /// </summary>
    [Test]
    public async Task ACascadeReachesTransitiveDependentsOfBothKinds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW acme_open AS SELECT id, total FROM open_orders WHERE customer = 'acme'");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW acme_big AS SELECT id FROM acme_open WHERE total > 100");
        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_totals AS SELECT customer, SUM(total) AS spent FROM open_orders GROUP BY customer");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW top_open AS SELECT customer FROM open_totals WHERE spent > 100");

        await ExecDdl(database, executor, dbname, "DROP MATERIALIZED VIEW open_orders CASCADE");

        foreach (string gone in new[] { "acme_open", "acme_big", "top_open" })
            Assert.IsFalse(database.Schema.Views.ContainsKey(gone), $"{gone} must go with the cascade");

        foreach (string gone in new[] { "open_orders", "open_totals" })
            Assert.IsFalse(database.Schema.Tables.ContainsKey(gone), $"{gone} must go with the cascade");

        Assert.IsTrue(database.Schema.Tables.ContainsKey("orders"), "the base table is not a dependent");
        AssertNoStagingLeft(database);
    }

    /// <summary>
    /// <c>DROP VIEW … CASCADE</c> skipped a materialized view that read the view, so the view went
    /// and left the materialized view with a body that names nothing — failing at its next refresh.
    /// </summary>
    [Test]
    public async Task DropViewCascadeTakesAMaterializedViewThatReadsIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");
        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_totals AS SELECT customer, SUM(total) AS spent FROM open_orders GROUP BY customer");

        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "DROP VIEW open_orders"));
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, refused!.Code);

        await ExecDdl(database, executor, dbname, "DROP VIEW open_orders CASCADE");

        Assert.IsFalse(database.Schema.Views.ContainsKey("open_orders"));
        Assert.IsFalse(database.Schema.Tables.ContainsKey("open_totals"),
            "a materialized view that reads the view is a dependent like any other");
        AssertNoStagingLeft(database);
    }

    /// <summary>
    /// A drop while a rebuild is running is refused, not raced. Raced, the rebuild swapped onto an id
    /// no longer in the schema — a correct no-op that therefore never removed the staging relation —
    /// and then deleted its own ownership record, leaving storage nothing points at.
    /// </summary>
    [Test]
    public async Task ADropWhileARefreshIsRunningIsRefusedAndLeavesNothingBehind()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        string viewTableId = database.Schema.Tables["open_orders"].Id!;
        CamusDBException? duringRefresh = null;

        MaterializedViewRefresher.AfterStagingForTesting = async () =>
        {
            MaterializedViewRefresher.AfterStagingForTesting = null;
            duringRefresh = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecDdl(database, executor, dbname, "DROP MATERIALIZED VIEW open_orders"));
            await Task.CompletedTask;
        };

        try
        {
            await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");
        }
        finally
        {
            MaterializedViewRefresher.AfterStagingForTesting = null;
        }

        Assert.IsNotNull(duringRefresh, "the drop inside the rebuild window must have been attempted");
        Assert.AreEqual(CamusDBErrorCodes.RefreshAlreadyInProgress, duringRefresh!.Code);
        StringAssert.Contains("Cannot drop materialized view 'open_orders' while a REFRESH", duringRefresh.Message);

        // The refused drop changed nothing: the refresh published, and the view still answers.
        Assert.AreEqual(3, (await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders")).Count);

        // Once the rebuild is done the drop goes through, and nothing it built is left behind.
        await ExecDdl(database, executor, dbname, "DROP MATERIALIZED VIEW open_orders");

        Assert.IsFalse(database.Schema.Tables.ContainsKey("open_orders"));
        AssertNoStagingLeft(database);

        CatalogsManager catalogs = executor.GetCatalogsManagerForTesting();
        Assert.IsNull(await catalogs.TryGetRefreshJobAsync(database, viewTableId));
    }

    /// <summary>
    /// Runs <paramref name="statement"/> while a refresh of <paramref name="refreshed"/> is between
    /// staging and publish, and returns the error it raised.
    /// </summary>
    private static async Task<CamusDBException?> DuringRefreshOf(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string refreshed, string statement)
    {
        CamusDBException? duringRefresh = null;

        MaterializedViewRefresher.AfterStagingForTesting = async () =>
        {
            MaterializedViewRefresher.AfterStagingForTesting = null;
            duringRefresh = Assert.ThrowsAsync<CamusDBException>(async () => await ExecDdl(database, executor, dbname, statement));
            await Task.CompletedTask;
        };

        try
        {
            await ExecNonQuery(database, executor, dbname, $"REFRESH MATERIALIZED VIEW {refreshed}");
        }
        finally
        {
            MaterializedViewRefresher.AfterStagingForTesting = null;
        }

        Assert.IsNotNull(duringRefresh, "the statement inside the rebuild window must have been attempted");
        return duringRefresh;
    }

    /// <summary>
    /// A cascade used to drop the dependents first and take the refresh fence after, so a running
    /// refresh refused the statement with the dependent views already gone.
    /// </summary>
    [Test]
    public async Task ACascadeRefusedByARunningRefreshKeepsEveryDependent()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW acme_open AS SELECT id, total FROM open_orders WHERE customer = 'acme'");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW acme_big AS SELECT id FROM acme_open WHERE total > 100");

        CamusDBException? refused = await DuringRefreshOf(
            database, executor, dbname, "open_orders", "DROP MATERIALIZED VIEW open_orders CASCADE");

        Assert.AreEqual(CamusDBErrorCodes.RefreshAlreadyInProgress, refused!.Code);
        StringAssert.Contains("'open_orders'", refused.Message);

        foreach (string kept in new[] { "acme_open", "acme_big" })
            Assert.IsTrue(database.Schema.Views.ContainsKey(kept), $"{kept} must survive the refused drop");
        Assert.IsTrue(database.Schema.Tables.ContainsKey("open_orders"));
        Assert.AreEqual(1, (await ExecQuery(database, executor, dbname, "SELECT id FROM acme_big")).Count);

        await ExecDdl(database, executor, dbname, "DROP MATERIALIZED VIEW open_orders CASCADE");
        Assert.IsFalse(database.Schema.Views.ContainsKey("acme_open"));
        AssertNoStagingLeft(database);
    }

    /// <summary>
    /// The same refusal when the refresh runs on a materialized view further down the cascade, for
    /// both <c>DROP MATERIALIZED VIEW … CASCADE</c> and <c>DROP VIEW … CASCADE</c>.
    /// </summary>
    [Test]
    public async Task ACascadeThatReachesARefreshingMaterializedViewDropsNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, customer, total FROM orders WHERE status = 'open'");
        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_snapshot AS SELECT id, customer, total FROM open_orders");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW acme_snapshot AS SELECT id FROM open_snapshot WHERE customer = 'acme'");
        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_totals AS SELECT customer, SUM(total) AS spent FROM open_snapshot GROUP BY customer");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW top_open AS SELECT customer FROM open_totals WHERE spent > 100");

        string[] views = ["open_orders", "acme_snapshot", "top_open"];
        string[] materialized = ["open_snapshot", "open_totals"];

        foreach (string statement in new[] { "DROP MATERIALIZED VIEW open_snapshot CASCADE", "DROP VIEW open_orders CASCADE" })
        {
            CamusDBException? refused = await DuringRefreshOf(database, executor, dbname, "open_totals", statement);

            Assert.AreEqual(CamusDBErrorCodes.RefreshAlreadyInProgress, refused!.Code, statement);
            StringAssert.Contains("'open_totals'", refused.Message, "the refusal names the view being refreshed");

            foreach (string kept in views)
                Assert.IsTrue(database.Schema.Views.ContainsKey(kept), $"{kept} must survive {statement}");
            foreach (string kept in materialized)
                Assert.IsTrue(database.Schema.Tables.ContainsKey(kept), $"{kept} must survive {statement}");
        }

        await ExecDdl(database, executor, dbname, "DROP VIEW open_orders CASCADE");

        foreach (string gone in views)
            Assert.IsFalse(database.Schema.Views.ContainsKey(gone), $"{gone} goes once no refresh runs");
        foreach (string gone in materialized)
            Assert.IsFalse(database.Schema.Tables.ContainsKey(gone), $"{gone} goes once no refresh runs");
        AssertNoStagingLeft(database);
    }

    /// <summary>
    /// The drop checked the view's id under the refresh fence, awaited, and then dropped by name. A
    /// rename and a create in that wait made it drop the new relation, which can be an ordinary table.
    /// </summary>
    [Test]
    public async Task ADropDoesNotRemoveARelationThatTookTheNameMeanwhile()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        MaterializedViewCreator.BeforeRelationDropForTesting = async _ =>
        {
            MaterializedViewCreator.BeforeRelationDropForTesting = null;
            await ExecDdl(database, executor, dbname, "ALTER MATERIALIZED VIEW open_orders RENAME TO open_orders_old");
            await ExecDdl(database, executor, dbname, "CREATE TABLE open_orders (id int64 PRIMARY KEY, note string(16))");
        };

        CamusDBException? refused;
        try
        {
            refused = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecDdl(database, executor, dbname, "DROP MATERIALIZED VIEW open_orders"));
        }
        finally
        {
            MaterializedViewCreator.BeforeRelationDropForTesting = null;
        }

        Assert.AreEqual(CamusDBErrorCodes.ConcurrentSchemaChange, refused!.Code);

        Assert.IsTrue(database.Schema.Tables.TryGetValue("open_orders", out TableSchema? newcomer), "the new table survives");
        Assert.IsFalse(newcomer!.IsMaterializedView);
        Assert.IsTrue(database.Schema.Tables["open_orders_old"].IsMaterializedView, "and so does the renamed view");
        AssertNoStagingLeft(database);
    }

    /// <summary>
    /// A refresh that resolved the view, then waited for the fence while a drop took it, dropped the
    /// view and released it. The refresh then staged rows for the dropped view, the swap applied as a
    /// no-op, and the staging relation was left with no record pointing at it.
    /// </summary>
    [Test]
    public async Task ARefreshThatLostTheViewToADropBeforeItsFenceStagesNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");
        string viewTableId = database.Schema.Tables["open_orders"].Id!;

        MaterializedViewRefresher.BeforeRefreshFenceForTesting = async _ =>
        {
            MaterializedViewRefresher.BeforeRefreshFenceForTesting = null;
            await ExecDdl(database, executor, dbname, "DROP MATERIALIZED VIEW open_orders");
        };

        CamusDBException? refused;
        try
        {
            refused = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders"));
        }
        finally
        {
            MaterializedViewRefresher.BeforeRefreshFenceForTesting = null;
        }

        Assert.AreEqual(CamusDBErrorCodes.ViewDoesntExist, refused!.Code);
        Assert.IsFalse(database.Schema.Tables.ContainsKey("open_orders"));
        AssertNoStagingLeft(database);

        CatalogsManager catalogs = executor.GetCatalogsManagerForTesting();
        Assert.IsNull(await catalogs.TryGetRefreshJobAsync(database, viewTableId), "no record is left either");
    }

    /// <summary>
    /// The apply-time half, for a rename that commits on another node after the proposer's checks: a
    /// drop entry whose name now resolves to a different id is refused, and drops nothing.
    /// </summary>
    [Test]
    public async Task ADropEntryForAnIdTheNameNoLongerHasIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        Schema schema = SchemaReplicator.CloneSchema(database.Schema);
        long version = schema.SchemaVersion;

        SchemaChangeLogEntry entry = new()
        {
            Database = database.Id,
            FromVersion = version,
            ToVersion = version + 1,
            Op = SchemaOp.DropTable,
            Payload = SchemaChangeLogEntryCodec.EncodePayload(new SchemaDropTablePayload
            {
                TableName = "orders",
                ExpectedTableId = "not-the-orders-id",
            }),
        };

        SchemaDeltaRefusedException? refused = Assert.Throws<SchemaDeltaRefusedException>(
            () => SchemaDeltaApplier.ApplySchemaDelta(schema, database, entry));
        Assert.AreEqual(CamusDBErrorCodes.ConcurrentSchemaChange, refused!.Code);

        Assert.DoesNotThrow(() => SchemaDeltaApplier.ApplyOrRefuse(schema, database, entry));
        Assert.IsTrue(schema.Tables.ContainsKey("orders"), "a refused drop leaves the relation that has the name");
        Assert.IsTrue(SchemaDeltaApplier.WasSchemaDeltaApplied(schema, entry), "the refusal is how the entry landed");
    }

    /// <summary>
    /// <c>REFRESH MATERIALIZED VIEW</c> belongs to the non-query path, but a client that classifies it
    /// as view DDL by its first words — camus-cli did — sends it to the DDL path, which answered
    /// "Unknown DDL AST stmt". Both paths now run it through one implementation.
    /// </summary>
    [Test]
    public async Task RefreshWorksThroughTheDdlPathToo()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, customer, total, status) VALUES (4, 'acme', 5, 'open')");

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: "REFRESH MATERIALIZED VIEW open_orders", parameters: null));
        await database.Transactions.CommitAsync(tx);

        Assert.IsTrue(result.Success);
        Assert.AreEqual(4, result.ModifiedRows, "the DDL path reports the rows the refresh wrote, as the non-query path does");
        Assert.AreEqual(4, (await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders")).Count);

        await ExecDdl(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders WITH NO DATA");
        CamusDBException? unpopulated = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname, "SELECT id FROM open_orders"));
        Assert.AreEqual(CamusDBErrorCodes.MaterializedViewNotPopulated, unpopulated!.Code);

        CamusDBException? concurrently = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "REFRESH MATERIALIZED VIEW CONCURRENTLY open_orders"));
        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, concurrently!.Code);
    }

    /// <summary>
    /// <c>DROP TABLE</c> refuses a materialized view and names the right statement; the rename did not,
    /// so a materialized view could be renamed by the table statement. Both kinds of object are now
    /// changed by their own statement — through the SQL path and the ticket API alike.
    /// </summary>
    [Test]
    public async Task TheTableRenameRefusesAMaterializedView()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        CamusDBException? sql = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "ALTER TABLE open_orders RENAME TO active_orders"));
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, sql!.Code);
        Assert.AreEqual("'open_orders' is a materialized view; use ALTER MATERIALIZED VIEW ... RENAME TO", sql.Message);

        CamusDBException? ticket = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.RenameTable(new RenameTableTicket(dbname, "open_orders", "active_orders")));
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, ticket!.Code);

        Assert.IsTrue(database.Schema.Tables.ContainsKey("open_orders"), "a refused rename changes nothing");

        await ExecDdl(database, executor, dbname, "ALTER MATERIALIZED VIEW open_orders RENAME TO active_orders");

        Assert.IsTrue(database.Schema.Tables.ContainsKey("active_orders"));
        Assert.AreEqual(3, (await ExecQuery(database, executor, dbname, "SELECT id FROM active_orders")).Count);
    }
}
