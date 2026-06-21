
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Schema-convergence observability:
///   (a) <see cref="SchemaAckTracker.GetLaggingNodes"/> names the non-acked endpoints (so the
///       quorum-backstop / timeout warnings can list who lagged rather than "one or more nodes");
///   (b) <see cref="SchemaMetrics"/> counts <c>SchemaCatchingUp</c> fence rejections as an always-on
///       metric, not just a log line.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSchemaObservability : SharedNodeBaseTest
{
    // (a) GetLaggingNodes returns exactly the live members that have not acked the target version:
    // a member behind on version, and a member with no ack record at all.
    [Test]
    public void GetLaggingNodes_NamesBehindAndMissingMembers()
    {
        SchemaAckTracker tracker = new();
        string[] live = ["A", "B", "C", "D"];

        tracker.RecordApplied("db1", "A", 5);
        tracker.RecordApplied("db1", "B", 5);
        tracker.RecordApplied("db1", "C", 4); // behind
        // D: no record at all → lagging

        System.Collections.Generic.IReadOnlyList<string> lagging =
            tracker.GetLaggingNodes("db1", 5, live);

        CollectionAssert.AreEquivalent(new[] { "C", "D" }, lagging);
    }

    // (a) When every live member has acked the target version, there are no laggards.
    [Test]
    public void GetLaggingNodes_EmptyWhenAllAcked()
    {
        SchemaAckTracker tracker = new();
        string[] live = ["A", "B"];

        tracker.RecordApplied("db1", "A", 7);
        tracker.RecordApplied("db1", "B", 8); // newer than target counts as acked

        Assert.IsEmpty(tracker.GetLaggingNodes("db1", 7, live));
    }

    // (b) A SchemaCatchingUp fence rejection bumps the SchemaMetrics counter (global + per-db).
    [Test]
    [NonParallelizable]
    public async Task FenceRejection_IncrementsSchemaMetric()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "obs_tbl",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("val", ColumnType.Integer64)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        ));

        long applied = database.Schema.SchemaVersion;

        // Activate the fence with a large gap so it never self-heals and every retry re-rejects.
        database.ObserveSchemaEntryHead(applied + 100);

        SchemaMetrics.Reset();
        long before = SchemaMetrics.FenceRejections;

        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: tx,
                database: dbname,
                sql: "INSERT INTO obs_tbl (id, val) VALUES (gen_id(), 1)",
                parameters: null
            )));
        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.AreEqual(CamusDBErrorCodes.SchemaCatchingUp, ex!.Code);
        Assert.Greater(SchemaMetrics.FenceRejections, before,
            "A SchemaCatchingUp rejection must increment the global fence-rejection metric");
        Assert.GreaterOrEqual(SchemaMetrics.FenceRejectionsFor(dbname), 1,
            "The per-database fence-rejection counter must record the rejection for this database");
    }
}
