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
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

using Kahuna.Shared.Communication.Rest;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// A join range seek on a multi-node cluster with key-range sharding and distributed execution on,
/// with the right table's rows split into two spans: from every node, the join that seeks the right
/// index for each left row returns the rows of the same join written so that no seek applies (the
/// plain nested loop), before and after the split. The seek reads index entries and then fetches the
/// rows of both spans.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterJoinRangeSeek
{
    private const int Partitions = 2;
    private const int Numbers = 200;

    private const string SeekSql = "SELECT s.id, g.n FROM spans s JOIN numbers g ON g.n BETWEEN s.lo AND s.hi ORDER BY s.id, g.n";

    private const string PlainSql = "SELECT s.id, g.n FROM spans s JOIN numbers g ON g.n + 0 BETWEEN s.lo AND s.hi ORDER BY s.id, g.n";

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static async Task<List<QueryResultRow>> RunSql(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: tx, database: db, sql: sql, parameters: null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await node.Database.Transactions.CommitAsync(tx);
            return rows;
        }
        catch
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    private static async Task RunDml(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));
            await node.Database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    private static List<(long Span, long N)> Shape(List<QueryResultRow> rows) =>
        rows.Select(r => (r.Row["id"].LongValue, r.Row["n"].LongValue)).ToList();

    private static async Task SplitRowsAtMedianAsync(InProcessSchemaCluster cluster, List<string> ids)
    {
        TableDescriptor table = await cluster.Nodes[0].Database!.TableDescriptors["numbers"];
        string keySpace = table.Store.RowKeySpace;

        List<string> sorted = ids.OrderBy(x => x, StringComparer.Ordinal).ToList();
        string splitKey = table.Store.RowPointKey(ObjectId.ToValue(sorted[sorted.Count / 2]));

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
            TablePlacement? placement = null;

            for (int attempt = 0; attempt < 50; attempt++)
            {
                node.Kahuna.InvalidatePlacement(keySpace);
                placement = node.Kahuna.GetPlacement(keySpace);
                if (placement.IsKeyRange && placement.Spans.Count >= 2 && placement.Spans.All(s => s.LeaderEndpoint is not null))
                    break;

                await Task.Delay(200);
            }

            Assert.IsTrue(placement!.IsKeyRange && placement.Spans.Count >= 2, "Every node's placement must report the split spans");
        }
    }

    [Test]
    public async Task RangeSeekJoin_MatchesThePlainLoopOnEveryNode_BeforeAndAfterASplit()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: Partitions,
            loggerFactory: sharedLoggerFactory, logger: logger,
            options: CamusDBOptions.Default with
            {
                KeyRangeShardingEnabled = true,
                DistributedQueryExecutionEnabled = true,
            });

        try
        {
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            string[] ddl =
            [
                "CREATE TABLE numbers (id OID PRIMARY KEY, n INT64 NOT NULL)",
                "CREATE INDEX ix_n ON numbers (n)",
                "CREATE TABLE spans (id INT64 PRIMARY KEY, lo INT64 NULL, hi INT64 NULL)",
            ];

            for (int i = 0; i < ddl.Length; i++)
            {
                string sql = ddl[i];
                await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.ExecuteDDLSQL(
                    new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null)).WaitAsync(TimeSpan.FromSeconds(30)));
                await cluster.WaitForSchemaConvergenceAsync(db, version: i + 1, timeout: TimeSpan.FromSeconds(30));
            }

            InProcessSchemaCluster.Node writer = cluster.Nodes[0];
            List<string> ids = new(Numbers);

            for (int start = 0; start < Numbers; start += 50)
            {
                List<string> values = [];
                for (int n = start; n < start + 50; n++)
                {
                    string id = ObjectIdGenerator.Generate().ToString();
                    ids.Add(id);
                    values.Add($"('{id}', {n})");
                }

                await RunDml(writer, db, "INSERT INTO numbers (id, n) VALUES " + string.Join(", ", values));
            }

            await RunDml(writer, db,
                "INSERT INTO spans (id, lo, hi) VALUES (1, 3, 7), (2, 10, 10), (3, 8, 5), (4, NULL, 4), (5, 190, 250), (6, -5, 2), (7, 98, 103)");

            List<string> plan = (await RunSql(writer, db, "EXPLAIN " + SeekSql)).Select(r => r.Row["detail"].StrValue!).ToList();
            Assert.That(plan.Any(d => d.Contains("range-seek=ix_n")), Is.True, string.Join("\n", plan));

            List<(long, long)> expected = Shape(await RunSql(writer, db, PlainSql));
            Assert.That(expected, Has.Count.EqualTo(5 + 1 + 10 + 3 + 6));

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                Assert.AreEqual(expected, Shape(await RunSql(node, db, SeekSql)), $"node {node.Index} before the split");

            await SplitRowsAtMedianAsync(cluster, ids);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.AreEqual(expected, Shape(await RunSql(node, db, PlainSql)), $"node {node.Index} plain loop after the split");
                Assert.AreEqual(expected, Shape(await RunSql(node, db, SeekSql)), $"node {node.Index} range seek after the split");
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
