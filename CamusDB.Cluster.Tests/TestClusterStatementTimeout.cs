/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

using Kahuna.Shared.Communication.Rest;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// <c>statement_timeout_ms</c> in cluster mode, through the gather path: a table whose keyspace is split
/// into two spans is read by span workers, local and remote, and an inline function runs on the
/// coordinator for every gathered row. A statement that calls the function per row must stop at the
/// limit on every node, a write must keep nothing, and the fragments the coordinator ships carry the
/// time its statement had left.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterStatementTimeout
{
    private const int Partitions = 2;

    private const int RowCount = 200;

    private const int TimeoutMs = 800;

    /// <summary>How long after the limit a stopped statement may return, on a slow runner under the load of the suite.</summary>
    private const int SlackMs = 4000;

    private const string Burn =
        "CREATE FUNCTION burn(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ " +
        "long x = 0; for (long i = 0; i < 3000000; i++) { x += i % 7; } return n + (x % 2); $$";

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static async Task<(InProcessSchemaCluster cluster, string db, List<string> insertedIds)> SetupAsync()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: Partitions, loggerFactory: sharedLoggerFactory, logger: logger,
            options: CamusDBOptions.Default with
            {
                KeyRangeShardingEnabled = true,
                DistributedQueryExecutionEnabled = true,
                InlineFunctionsEnabled = true,
                StatementTimeoutMs = TimeoutMs,
            });

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "readings",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("num", ColumnType.Integer64, notNull: true),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        List<string> ids = new(RowCount);
        InProcessSchemaCluster.Node writer = cluster.Nodes[0];
        KvTransaction tx = await writer.Database!.Transactions.BeginAsync();

        for (int i = 0; i < RowCount; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            ids.Add(id);
            await writer.Executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "readings",
                values: new() { new() {
                    { "id",  new(ColumnType.Id, id) },
                    { "num", new(ColumnType.Integer64, (long)i) },
                }}));
        }

        await writer.Database.Transactions.CommitAsync(tx);
        return (cluster, db, ids);
    }

    /// <summary>Splits the row keyspace at the median row and waits until every node sees two spans with known leaders.</summary>
    private static async Task SplitAtMedianAsync(InProcessSchemaCluster cluster, string db, List<string> insertedIds)
    {
        TableDescriptor table = await cluster.Nodes[0].Database!.TableDescriptors["readings"];
        string keySpace = table.Store.RowKeySpace;

        List<string> sorted = insertedIds.OrderBy(x => x, StringComparer.Ordinal).ToList();
        string splitKey = table.Store.RowPointKey(ObjectId.ToValue(sorted[RowCount / 2]));

        bool split = false;
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            KahunaSplitRangeResponse response =
                await node.Kahuna.Kahuna.SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);

            if (response.Success)
            {
                split = true;
                break;
            }
        }

        Assert.IsTrue(split, "One node (the meta-partition leader) must commit the split");

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            bool ready = false;

            for (int attempt = 0; attempt < 50 && !ready; attempt++)
            {
                node.Kahuna.InvalidatePlacement(keySpace);
                TablePlacement placement = node.Kahuna.GetPlacement(keySpace);
                ready = placement.IsKeyRange && placement.Spans.Count >= 2 && placement.Spans.All(s => s.LeaderEndpoint is not null);

                if (!ready)
                    await Task.Delay(200);
            }

            Assert.IsTrue(ready, $"Node {node.Index}: placement must report both spans with their leaders");
        }
    }

    private static async Task DdlOnLeaderAsync(InProcessSchemaCluster cluster, string db, string sql)
    {
        long version = 0;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            KvTransaction tx = await leader.Database!.Transactions.BeginAsync();

            try
            {
                await leader.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null)).WaitAsync(TimeSpan.FromSeconds(60));
                await leader.Database.Transactions.CommitAsync(tx);
            }
            finally
            {
                await leader.Database.Transactions.RollbackIfNotCompletedAsync(tx);
            }

            version = leader.Database.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(30));
    }

    private static async Task<List<QueryResultRow>> QueryAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await node.Database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<int> NonQueryAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();

        try
        {
            int modified = (await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null))).ModifiedRows;
            await node.Database.Transactions.CommitAsync(tx);
            return modified;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Stopped(Func<Task> statement, string what)
    {
        Stopwatch watch = Stopwatch.StartNew();
        CamusDBException stopped = Assert.ThrowsAsync<CamusDBException>(async () => await statement())!;
        watch.Stop();

        Assert.AreEqual(CamusDBErrorCodes.StatementTimeout, stopped.Code, $"{what}: {stopped.Message}");
        Assert.Less(watch.ElapsedMilliseconds, TimeoutMs + SlackMs, $"{what}: the statement must end soon after its limit");
    }

    [Test]
    public async Task AStatementThatCallsAnInlineFunctionPerGatheredRow_StopsAtTheLimitOnEveryNode()
    {
        (InProcessSchemaCluster cluster, string db, List<string> ids) = await SetupAsync();

        try
        {
            await SplitAtMedianAsync(cluster, db, ids);
            await DdlOnLeaderAsync(cluster, db, Burn);

            // The statement reads through the gather exchange: the inline call runs on the coordinator
            // for each row the span workers deliver.
            List<QueryResultRow> explain = await QueryAsync(cluster.Nodes[0], db, "EXPLAIN SELECT burn(num) FROM readings");
            Assert.IsTrue(
                explain.Any(r => r.Row.Values.Any(v => v.StrValue?.Contains("gather") == true)),
                "the statement must plan a gather. Got: " +
                string.Join(" | ", explain.Select(r => string.Join(",", r.Row.Values.Select(v => v.StrValue)))));

            // A statement that ends in time is not affected, and its remote fragments carry the time
            // the statement had left: the peer's own bound when the coordinator's cancel is lost.
            cluster.FragmentTransport!.ResetExecuted();

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                Assert.AreEqual(100, (await QueryAsync(node, db, "SELECT id FROM readings WHERE num >= 50 AND num < 150")).Count, $"node {node.Index}");

            IReadOnlyList<QueryFragmentRequest> shipped = cluster.FragmentTransport.ExecutedRequests;
            Assert.IsNotEmpty(shipped, "at least one span must run as a remote fragment (no node leads both spans)");
            Assert.IsTrue(
                shipped.All(r => r.StatementTimeoutRemainingMs is > 0 and <= TimeoutMs),
                "each fragment must carry the time its statement had left: " +
                string.Join(", ", shipped.Select(r => r.StatementTimeoutRemainingMs?.ToString() ?? "null")));

            long sumBefore = (await QueryAsync(cluster.Nodes[0], db, "SELECT sum(num) AS s FROM readings")).Single().Row["s"].LongValue;

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                await Stopped(() => QueryAsync(node, db, "SELECT burn(num) FROM readings"), $"select on node {node.Index}");
                await Stopped(() => QueryAsync(node, db, "SELECT id FROM readings WHERE burn(num) > 0"), $"filter on node {node.Index}");
            }

            // A write through the same scan keeps nothing.
            await Stopped(() => NonQueryAsync(cluster.Nodes[1], db, "UPDATE readings SET num = burn(num) + 1 WHERE TRUE"), "update");
            await Stopped(() => NonQueryAsync(cluster.Nodes[2], db, "DELETE FROM readings WHERE burn(num) >= 0"), "delete");

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<QueryResultRow> after = await QueryAsync(node, db, "SELECT count(*) AS c, sum(num) AS s FROM readings");
                Assert.AreEqual(RowCount, after.Single().Row["c"].LongValue, $"node {node.Index}: no row of the stopped DELETE is kept");
                Assert.AreEqual(sumBefore, after.Single().Row["s"].LongValue, $"node {node.Index}: no row of the stopped UPDATE is kept");
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
