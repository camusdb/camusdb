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
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>CREATE MATERIALIZED VIEW … WITH DATA</c> publishes nothing until its rows are in place. It used to
/// create the view empty and populate it in a second step, so other sessions could list, query, grant
/// on, alter or drop an object the statement had not finished — and a failed population then dropped
/// something they might already have touched.
/// </summary>
[NonParallelizable]
public sealed class TestMaterializedViewCreateWithData : SharedNodeBaseTest
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

    private static async Task Seed(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname, "CREATE TABLE orders (id int64 PRIMARY KEY, total int64)");

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO orders (id, total) VALUES (1, 10), (2, 20), (3, 30)", null));
        await database.Transactions.CommitAsync(tx);
    }

    private static void AssertNothingLeft(DatabaseDescriptor database, string viewName)
    {
        Assert.IsFalse(database.Schema.Tables.ContainsKey(viewName), $"'{viewName}' must not exist");
        CollectionAssert.IsEmpty(database.Schema.Tables.Keys.Where(MaterializedViewNaming.IsStagingRelation).ToList(),
            "no staging relation may outlive the statement");
    }

    [TearDown]
    public void ClearSeam() => MaterializedViewRefresher.AfterStagingForTesting = null;

    /// <summary>While the rows load, the view is not there for anyone: not in a listing, not to a query, a grant, an alter or a drop.</summary>
    [Test]
    public async Task WhileItsRowsLoadTheViewIsNotThereForAnyone()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        List<string> observed = [];

        MaterializedViewRefresher.AfterStagingForTesting = async () =>
        {
            MaterializedViewRefresher.AfterStagingForTesting = null;

            if ((await Query(database, executor, dbname, "SHOW MATERIALIZED VIEWS")).Any(r => r.Row["materialized_views"].StrValue == "totals"))
                observed.Add("listed");

            foreach (string sql in new[] { "SELECT * FROM totals" })
            {
                try { await Query(database, executor, dbname, sql); observed.Add(sql); }
                catch (CamusDBException) { }
            }

            foreach (string sql in new[]
            {
                "ALTER MATERIALIZED VIEW totals RENAME TO totals2",
                "DROP MATERIALIZED VIEW totals",
                "CREATE INDEX totals_spent ON totals (spent)",
                "COMMENT ON MATERIALIZED VIEW totals IS 'x'",
            })
            {
                try { await ExecDdl(database, executor, dbname, sql); observed.Add(sql); }
                catch (CamusDBException) { }
            }
        };

        await ExecDdl(database, executor, dbname, "CREATE MATERIALIZED VIEW totals AS SELECT id, total AS spent FROM orders");

        CollectionAssert.IsEmpty(observed, "nothing may observe or change the view before it is published");

        // Published whole: populated, with every row.
        Assert.IsTrue(database.Schema.Tables["totals"].IsPopulated);
        Assert.AreEqual(3, (await Query(database, executor, dbname, "SELECT id FROM totals")).Count);
        AssertNothingLeft(database, "no_such_view");
    }

    /// <summary>A population that fails leaves nothing: no view to clean up, no staging relation, no record.</summary>
    [Test]
    public async Task AFailedPopulationLeavesNothingBehind()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        MaterializedViewRefresher.AfterStagingForTesting = () =>
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "population fails");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE MATERIALIZED VIEW totals AS SELECT id FROM orders"));
        StringAssert.Contains("population fails", error!.Message);

        AssertNothingLeft(database, "totals");
        MaterializedViewRefresher.AfterStagingForTesting = null;

        // A retry starts clean.
        await ExecDdl(database, executor, dbname, "CREATE MATERIALIZED VIEW totals AS SELECT id FROM orders");
        Assert.AreEqual(3, (await Query(database, executor, dbname, "SELECT id FROM totals")).Count);
    }

    /// <summary>A relation created under the name while the rows load wins; this statement fails and leaves nothing.</summary>
    [Test]
    public async Task ANameTakenWhileTheRowsLoadFailsTheCreateCleanly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        MaterializedViewRefresher.AfterStagingForTesting = async () =>
        {
            MaterializedViewRefresher.AfterStagingForTesting = null;
            await ExecDdl(database, executor, dbname, "CREATE TABLE totals (id int64 PRIMARY KEY)");
        };

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE MATERIALIZED VIEW totals AS SELECT id FROM orders"));
        Assert.AreEqual(CamusDBErrorCodes.TableAlreadyExists, error!.Code);

        Assert.IsFalse(database.Schema.Tables["totals"].IsMaterializedView, "the table that took the name stays");
        CollectionAssert.IsEmpty(database.Schema.Tables.Keys.Where(MaterializedViewNaming.IsStagingRelation).ToList());
    }

    /// <summary>
    /// A process that dies before publishing leaves a record and a staging relation, and no view. The
    /// sweep removes both without the original process, and the retry of the statement starts clean.
    /// </summary>
    [Test]
    public async Task AnInterruptedCreateIsCleanedUpByTheSweepAndRetriesClean()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        // The state a crash between the load and the publish leaves: a record naming a view id that was
        // never published. Its staging relation is simulated by the name alone, as the refresh tests do.
        CatalogsManager catalogs = executor.GetCatalogsManagerForTesting();
        string viewId = "neverpublished";
        string stagingId = "stagingofcrash";
        string stagingName = MaterializedViewNaming.StagingRelationName(viewId, stagingId);

        await catalogs.PersistRefreshJobAsync(database, new MaterializedViewRefreshJob
        {
            JobId = "crashed-create",
            DatabaseId = database.Id,
            ViewTableId = viewId,
            ViewName = "totals",
            StagingTableId = stagingId,
            StagingName = stagingName,
        });

        Assert.AreEqual(1, await executor.ReclaimAbandonedRefreshesForTesting(dbname));
        Assert.IsNull(await catalogs.TryGetRefreshJobAsync(database, viewId));
        AssertNothingLeft(database, "totals");

        await ExecDdl(database, executor, dbname, "CREATE MATERIALIZED VIEW totals AS SELECT id FROM orders");
        Assert.AreEqual(3, (await Query(database, executor, dbname, "SELECT id FROM totals")).Count);
    }

    /// <summary>The adopted storage, the populated flag and the staging relation's removal all survive a reopen.</summary>
    [Test]
    public async Task ACreatedViewSurvivesAReopenWithoutItsStagingRelation()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "CREATE MATERIALIZED VIEW totals AS SELECT id, total FROM orders WHERE total > 10");
        await ExecDdl(database, executor, dbname, "CREATE INDEX totals_total ON totals (total)");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        Assert.IsTrue(reopened.Schema.Tables["totals"].IsPopulated);
        Assert.IsNotNull(reopened.Schema.Tables["totals"].RefreshedAt);
        CollectionAssert.IsEmpty(reopened.Schema.Tables.Keys.Where(MaterializedViewNaming.IsStagingRelation).ToList());
        Assert.AreEqual(2, (await Query(reopened, executor, dbname, "SELECT id FROM totals")).Count);
        Assert.AreEqual(1, (await Query(reopened, executor, dbname, "SELECT id FROM totals WHERE total = 30")).Count);

        // And it refreshes and drops like any other.
        KvTransaction tx = await reopened.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "REFRESH MATERIALIZED VIEW totals", null));
        await reopened.Transactions.CommitAsync(tx);
        await ExecDdl(reopened, executor, dbname, "DROP MATERIALIZED VIEW totals");
        AssertNothingLeft(reopened, "totals");
    }

    /// <summary>
    /// The adopting create used the plain CreateTable op with a new payload field. A node on an older
    /// build ignored the field and created the view populated over an empty key-space. The create now
    /// has its own op, which an older build refuses loudly instead of applying differently.
    /// </summary>
    [Test]
    public async Task AnAdoptingCreateHasItsOwnOpAndAnUnknownOpStopsTheApply()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnInfo[] columns = [new ColumnInfo("id", ColumnType.Integer64, notNull: true)];
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            SchemaChangeLogEntry plain = SchemaChangeEntryFactory.CreateTableEntry(
                database, new CreateTableTicket(dbname, "plain", columns, [], ifNotExists: false), tx, "P1");
            Assert.AreEqual(SchemaOp.CreateTable, plain.Op);

            SchemaChangeLogEntry adopting = SchemaChangeEntryFactory.CreateTableEntry(
                database,
                new CreateTableTicket(dbname, "adopting", columns, [], ifNotExists: false,
                    kind: RelationKind.MaterializedView,
                    viewDefinition: new ViewDefinition { Sql = "SELECT 1 AS id", Columns = [] })
                {
                    AdoptStagingTableId = "S1",
                },
                tx,
                "M1");
            Assert.AreEqual(SchemaOp.CreatePopulatedMaterializedView, adopting.Op);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        // An op this build does not know is not turned into a refusal that would let the log move on.
        SchemaChangeLogEntry unknown = new()
        {
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = (SchemaOp)999,
            Payload = [],
        };

        Schema clone = SchemaReplicator.CloneSchema(database.Schema);
        CamusDBException? stopped = Assert.Throws<CamusDBException>(() => SchemaDeltaApplier.ApplyOrRefuse(clone, database, unknown));
        Assert.That(stopped!.Message, Does.Contain("Unknown schema operation"));
        Assert.IsNotInstanceOf<SchemaDeltaRefusedException>(stopped);
    }
}
