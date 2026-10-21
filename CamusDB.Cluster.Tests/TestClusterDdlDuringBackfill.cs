/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// The schedule that found the hold: on a three-node cluster, <c>CREATE INDEX</c> on a table is sent to
/// the schema leader, and while its backfill runs, other nodes send a <c>CREATE TABLE</c> of an
/// unrelated table and an <c>ALTER TABLE</c> of the indexed table, both forwarded to the leader. The
/// <c>CREATE TABLE</c> used to wait for the whole build behind the leader's DDL semaphore, and the
/// follower that forwarded it held its own semaphore for as long. Now the build holds only its table:
/// the <c>CREATE TABLE</c> completes during the build on every node, the <c>ALTER TABLE</c> waits for
/// the build and then runs, and the index comes out complete.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster. Concurrent clusters contend for ports and skew
// each other's Raft election timing.
[NonParallelizable]
public sealed class TestClusterDdlDuringBackfill
{
    /// <summary>Three backfill batches of 500, so the checkpoint seam fires between batches.</summary>
    private const int Rows = 1_200;

    private static readonly TimeSpan Soon = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan NotYet = TimeSpan.FromMilliseconds(1_500);

    [Test]
    public async Task CreateTableForwardedDuringAnIndexBuildCompletesWhileSameTableDdlWaits()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3, wireLeaderForwarder: true);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);
        await SeedAsync(cluster, db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node[] followers = cluster.Nodes.Where(n => n.Index != leader.Index).ToArray();

        TaskCompletionSource reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int fires = 0;

        leader.Executor.TestInterceptAfterBackfillCheckpoint = async () =>
        {
            if (Interlocked.Increment(ref fires) != 1)
                return;

            reached.TrySetResult();
            await released.Task;
        };

        Task createIndex = DdlAsync(leader, db, "CREATE INDEX acct_balance ON accounts (balance) INCLUDE (version)");
        Task? addColumn = null;

        try
        {
            Assert.That(await CompletesWithin(reached.Task, Soon), Is.True, "the backfill reaches its first checkpoint");
            Assert.That(leader.Database!.DdlGate.IsTableHeld("accounts"), Is.True, "the build holds its table on the leader");
            Assert.That(leader.Database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1), "and not the leader's DDL semaphore");

            // The CREATE TABLE of an unrelated table arrives on a follower and is forwarded to the
            // leader. It must complete while the index build is in progress.
            Task createTable = DdlAsync(followers[0], db, "CREATE TABLE holds (id INT64 PRIMARY KEY NOT NULL, account_id INT64 NOT NULL, amount INT64 NOT NULL)");
            Assert.That(await CompletesWithin(createTable, Soon), Is.True, "CREATE TABLE of another table completes during the index build");
            await createTable;
            Assert.That(followers[0].Database!.Schema.Tables.ContainsKey("holds"), Is.True, "the follower that forwarded it sees the table");
            Assert.That(leader.Database.Schema.Tables.ContainsKey("holds"), Is.True);

            // A statement that depends on the new table runs at once too; the follower it is sent to
            // holds nothing while the forwarded statement waits.
            await DmlAsync(followers[0], db, "INSERT INTO holds (id, account_id, amount) VALUES (1, 1, 10)");

            // An ALTER of the indexed table, from the other follower, waits for the build.
            addColumn = DdlAsync(followers[1], db, "ALTER TABLE accounts ADD COLUMN tier INT64 NOT NULL DEFAULT (7)");
            Assert.That(await CompletesWithin(addColumn, NotYet), Is.False, "ADD COLUMN on the indexed table waits for the build");
            Assert.That(leader.Database.Schema.Tables["accounts"].Columns!.Exists(c => c.Name == "tier"), Is.False, "it has not started");
            Assert.That(followers[1].Database!.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1), "the forwarding follower holds nothing while it waits");
        }
        finally
        {
            released.TrySetResult();
            leader.Executor.TestInterceptAfterBackfillCheckpoint = null;
        }

        await createIndex.WaitAsync(TimeSpan.FromSeconds(180));
        await addColumn!.WaitAsync(TimeSpan.FromSeconds(180));
        await cluster.WaitForSchemaConvergenceAsync(db, leader.Database!.Schema.SchemaVersion, timeout: TimeSpan.FromSeconds(60));

        Assert.That(leader.Database.DdlGate.HeldTableCount, Is.EqualTo(0), "nothing holds a table once the statements are done");

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableSchema accounts = node.Database!.Schema.Tables["accounts"];
            Assert.That(accounts.Indexes!.First(ix => ix.Name == "acct_balance").State, Is.EqualTo(SchemaElementState.Public), $"node {node.Index}: index Public");
            Assert.That(accounts.Columns!.First(c => c.Name == "tier").State, Is.EqualTo(SchemaElementState.Public), $"node {node.Index}: column Public");
            Assert.That(node.Database.Schema.Tables.ContainsKey("holds"), Is.True, $"node {node.Index}: the new table");

            List<(long balance, ObjectIdValue rowId)> entries = await IndexEntriesAsync(node, db);
            Assert.That(entries, Has.Count.EqualTo(Rows), $"node {node.Index}: one entry per row");
            Assert.That(entries.Select(e => e.rowId).Distinct().Count(), Is.EqualTo(Rows), $"node {node.Index}: no row indexed twice");

            List<QueryResultRow> tiers = await RowsAsync(node, db, "SELECT id FROM accounts WHERE tier = 7");
            Assert.That(tiers, Has.Count.EqualTo(Rows), $"node {node.Index}: every row was filled");
        }
    }

    private static async Task<bool> CompletesWithin(Task task, TimeSpan within)
        => ReferenceEquals(await Task.WhenAny(task, Task.Delay(within)), task);

    private static async Task SeedAsync(InProcessSchemaCluster cluster, string db)
    {
        long version = 0;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await DdlAsync(leader, db, "CREATE TABLE accounts (id INT64 PRIMARY KEY NOT NULL, balance INT64 NOT NULL, version INT64 NOT NULL)");

            const int rowsPerStatement = 200;
            for (int start = 1; start <= Rows; start += rowsPerStatement)
            {
                int end = Math.Min(Rows, start + rowsPerStatement - 1);
                string values = string.Join(", ", Enumerable.Range(start, end - start + 1).Select(i => $"({i}, {1_000_000 + i}, 0)"));
                await DmlAsync(leader, db, "INSERT INTO accounts (id, balance, version) VALUES " + values);
            }

            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(30));
    }

    private static Task DdlAsync(InProcessSchemaCluster.Node node, string db, string sql) =>
        node.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null));

    private static async Task DmlAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await node.Database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> RowsAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

            List<QueryResultRow> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(row);
            return rows;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>Every entry of the new index as this node's store holds it.</summary>
    private static async Task<List<(long balance, ObjectIdValue rowId)>> IndexEntriesAsync(InProcessSchemaCluster.Node node, string db)
    {
        TableIndexSchema index = node.Database!.Schema.Tables["accounts"].Indexes!.First(ix => ix.Name == "acct_balance");
        TableDescriptor table = await node.Executor.OpenTable(new OpenTableTicket(db, "accounts"));

        KvTransaction tx = await node.Database.Transactions.BeginAsync();
        try
        {
            List<(long, ObjectIdValue)> entries = [];
            await foreach ((CompositeColumnValue key, ObjectIdValue rowId, _) in table.Store.ScanIndex(tx, index.KvId, [ColumnType.Integer64], from: null, to: null, unique: false))
                entries.Add((key.Values[0].LongValue, rowId));
            return entries;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }
}
