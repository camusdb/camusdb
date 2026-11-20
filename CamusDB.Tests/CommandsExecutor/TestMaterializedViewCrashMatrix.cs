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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using static CamusDB.Core.CommandsExecutor.Controllers.DDL.MaterializedViewRefresher;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The crash and restart matrix of a materialized-view refresh. A process death is injected at each
/// point of a rebuild through <see cref="MaterializedViewRefresher.FaultInjectionForTesting"/> — which,
/// unlike a failure, runs none of the cleanup — and then the database is reopened, as a restart does,
/// and the background sweep runs.
///
/// <para>Every case asserts the three recovery properties: a reader sees the old contents or the new
/// ones, never a mixture and never an error; replaying what was committed changes nothing; and every
/// key-space the dead run created is eventually removed, so no registered storage is left that nothing
/// points at.</para>
///
/// <para>The schema-log commit, its apply and its checkpoint are one step here: the proposer commits,
/// applies and checkpoints before the swap call returns, and replay of a committed entry after a real
/// restart is the schema log's own contract. What this matrix adds is the point right after that step
/// (<see cref="FaultPoint.AfterPublish"/>), and the replay of the swap entry itself.</para>
/// </summary>
[NonParallelizable]
internal sealed class TestMaterializedViewCrashMatrix : SharedNodeBaseTest
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

    private static async Task NonQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<long>> Ids(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, "SELECT id FROM open_orders", null));
            List<long> ids = (await cursor.ToListAsync()).Select(r => r.Row["id"].LongValue).OrderBy(i => i).ToList();
            await database.Transactions.CommitAsync(tx);
            return ids;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static readonly List<long> OldContents = [1, 2, 3];

    private static readonly List<long> NewContents = [1, 2, 3, 6, 7, 8];

    [TearDown]
    public void ClearSeams()
    {
        FaultInjectionForTesting = null;
        AfterStagingForTesting = null;
    }

    /// <summary>A view of three rows whose source has since grown to six; small chunks, so a load spans several.</summary>
    private async Task<(string, DatabaseDescriptor, CommandExecutor)> Seed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { MaterializedViewRefreshChunkRows = 2 });

        await ExecDdl(database, executor, dbname, "CREATE TABLE orders (id int64 PRIMARY KEY, status string(16))");
        await NonQuery(database, executor, dbname,
            "INSERT INTO orders (id, status) VALUES (1, 'open'), (2, 'open'), (3, 'open'), (4, 'closed'), (5, 'closed')");
        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id FROM orders WHERE status = 'open'");
        await NonQuery(database, executor, dbname,
            "INSERT INTO orders (id, status) VALUES (6, 'open'), (7, 'open'), (8, 'open')");

        return (dbname, database, executor);
    }

    [TestCase(FaultPoint.AfterStaging)]
    [TestCase(FaultPoint.AfterChunk)]
    [TestCase(FaultPoint.AfterFinalChunk)]
    [TestCase(FaultPoint.AfterPublish)]
    [TestCase(FaultPoint.DuringCleanup)]
    public async Task ACrashAtEachPointRecoversToOldOrNewContentsAndLeavesNoStorageBehind(FaultPoint point)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        string viewId = database.Schema.Tables["open_orders"].Id!;

        FaultInjectionForTesting = at =>
        {
            // A failure first, for the cleanup case: the crash then lands inside the cleanup it starts.
            if (point == FaultPoint.DuringCleanup && at == FaultPoint.AfterChunk)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "the rebuild fails");

            if (at == point)
            {
                FaultInjectionForTesting = null;
                throw new SimulatedCrashForTesting(at);
            }

            return Task.CompletedTask;
        };

        Assert.ThrowsAsync<SimulatedCrashForTesting>(async () =>
            await NonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders"));

        bool published = point == FaultPoint.AfterPublish;

        // A reader during the outage: old or new, never a mixture, never an error.
        Assert.AreEqual(published ? NewContents : OldContents, await Ids(database, executor, dbname));

        // The restart: in-memory state goes, and the schema is read back from storage.
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        database = await executor.OpenDatabase(dbname);

        Assert.AreEqual(published ? NewContents : OldContents, await Ids(database, executor, dbname),
            "a restart must find exactly what was published before the crash");

        CatalogsManager catalogs = executor.GetCatalogsManagerForTesting();
        Assert.IsNotNull(await catalogs.TryGetRefreshJobAsync(database, viewId),
            "the dead run's record survives it: it is what finds the storage the run left");

        // The sweep finishes what the dead run started — a restarted rebuild, or, after the publish,
        // only the record's removal.
        Assert.AreEqual(1, await executor.ReclaimAbandonedRefreshesForTesting(dbname));

        Assert.AreEqual(NewContents, await Ids(database, executor, dbname));
        Assert.IsNull(await catalogs.TryGetRefreshJobAsync(database, viewId));
        CollectionAssert.IsEmpty(database.Schema.Tables.Keys.Where(MaterializedViewNaming.IsStagingRelation).ToList(),
            "no key-space of the dead run may be left with nothing pointing at it");

        // And the view keeps working.
        await NonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");
        Assert.AreEqual(NewContents, await Ids(database, executor, dbname));
    }

    /// <summary>
    /// A failure after the publish, such as the job-record delete failing, used to run the failure path:
    /// the statement reported an error for contents that were already published, and a retried
    /// <c>CREATE MATERIALIZED VIEW</c> then failed on the name the view held. Both statements now report
    /// success, and the sweep removes the record they left.
    /// </summary>
    [Test]
    public async Task AFailureAfterThePublishReportsSuccessAndLeavesTheRecordToTheSweep()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        CatalogsManager catalogs = executor.GetCatalogsManagerForTesting();

        int faults = 0;
        FaultInjectionForTesting = at =>
        {
            if (at == FaultPoint.AfterPublish)
            {
                faults++;
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "the record delete fails");
            }

            return Task.CompletedTask;
        };

        await NonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");
        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW closed_orders AS SELECT id FROM orders WHERE status = 'closed'");

        FaultInjectionForTesting = null;
        Assert.AreEqual(2, faults, "the fault ran once for each statement");

        Assert.AreEqual(NewContents, await Ids(database, executor, dbname), "the refresh published");
        Assert.IsTrue(database.Schema.Tables["closed_orders"].IsPopulated, "the create published");

        string refreshedId = database.Schema.Tables["open_orders"].Id!;
        string createdId = database.Schema.Tables["closed_orders"].Id!;
        Assert.IsNotNull(await catalogs.TryGetRefreshJobAsync(database, refreshedId));
        Assert.IsNotNull(await catalogs.TryGetRefreshJobAsync(database, createdId));

        Assert.AreEqual(2, await executor.ReclaimAbandonedRefreshesForTesting(dbname));

        Assert.IsNull(await catalogs.TryGetRefreshJobAsync(database, refreshedId), "the sweep removes the records");
        Assert.IsNull(await catalogs.TryGetRefreshJobAsync(database, createdId));
        Assert.AreEqual(NewContents, await Ids(database, executor, dbname), "and leaves the published contents");
        CollectionAssert.IsEmpty(database.Schema.Tables.Keys.Where(MaterializedViewNaming.IsStagingRelation).ToList());
    }

    /// <summary>
    /// The swap entry delivered again — as a Raft re-delivery or a WAL replay would — changes nothing: the
    /// view keeps the storage it adopted, and the relation it adopted from does not come back.
    /// </summary>
    [Test]
    public async Task ReplayingTheSwapEntryDoesNotApplyItTwice()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        TableSchema before = database.Schema.Tables["open_orders"];
        string viewId = before.Id!;
        long metadataGeneration = before.MetadataGeneration;
        string? stagingId = null;

        AfterStagingForTesting = () =>
        {
            AfterStagingForTesting = null;
            stagingId = database.Schema.Tables.Values.Single(t => MaterializedViewNaming.IsStagingRelation(t.Name!)).Id;
            return Task.CompletedTask;
        };

        await NonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW open_orders");

        TableSchema after = database.Schema.Tables["open_orders"];
        string storage = after.EffectiveStorageId;
        long contents = after.ContentsGeneration;

        // The entry the refresh published, built again from the same values.
        SchemaChangeLogEntry swap = new()
        {
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = SchemaOp.SetMaterializedViewState,
            Payload = Core.Catalogs.Replication.SchemaChangeLogEntryCodec.EncodePayload(new SchemaSetMatViewStatePayload
            {
                TableId = viewId,
                IsPopulated = true,
                SwapToTableId = stagingId,
                ExpectedMetadataGeneration = metadataGeneration,
            }),
        };

        Schema replayed = SchemaReplicator.CloneSchema(database.Schema);
        Assert.DoesNotThrow(() => SchemaDeltaApplier.ApplyOrRefuse(replayed, database, swap));

        TableSchema view = replayed.Tables["open_orders"];
        Assert.AreEqual(storage, view.EffectiveStorageId, "a replay must not move the storage again");
        Assert.AreEqual(contents, view.ContentsGeneration, "a replay must not count as another refresh");
        Assert.IsFalse(replayed.Tables.Values.Any(t => t.Id == stagingId), "the adopted relation must not come back");
    }
}
