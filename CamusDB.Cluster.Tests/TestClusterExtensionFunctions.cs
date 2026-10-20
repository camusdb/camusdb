/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

using Kahuna.Shared.Communication.Rest;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// A C# extension function in a WHERE filter of a span-fragmented (Gather) scan. Only node 0
/// loads the extension, so a peer that evaluated the call would fail. The function must therefore
/// never reach a fragment: every fragment request ships without it, the coordinator evaluates it
/// locally, and the result equals the same filter written with built-ins.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterExtensionFunctions
{
    private const int RowCount = 200;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static string FixturePath(string fixture) =>
        Path.Combine(TestContext.CurrentContext.TestDirectory, "extension-fixtures", fixture, fixture + ".dll");

    private static async Task<List<QueryResultRow>> RunSql(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));
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

    [Test]
    public async Task ExtensionFunctionInFilter_IsNeverShipped_AndMatchesTheBuiltInResult()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 2, loggerFactory: sharedLoggerFactory, logger: logger,
            options: CamusDBOptions.Default with { KeyRangeShardingEnabled = true, DistributedQueryExecutionEnabled = true },
            configureNodeOptions: (index, options) => index == 0
                ? options with { ExtensionAssemblies = [FixturePath("Fixture.Good")] }
                : options);

        try
        {
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
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false)).WaitAsync(TimeSpan.FromSeconds(20)));

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
                    values: new() { new() { { "id", new(ColumnType.Id, id) }, { "num", new(ColumnType.Integer64, (long)i) } } }));
            }

            await writer.Database.Transactions.CommitAsync(tx);

            // Split the keyspace in two spans so the scan gathers across nodes.
            TableDescriptor table = await cluster.Nodes[0].Database!.TableDescriptors["readings"];
            string keySpace = table.Store.RowKeySpace;
            string splitKey = table.Store.RowPointKey(ObjectId.ToValue(ids.OrderBy(x => x, StringComparer.Ordinal).ElementAt(RowCount / 2)));

            bool split = false;
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                KahunaSplitRangeResponse response = await node.Kahuna.Kahuna.SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);
                if (response.Success) { split = true; break; }
            }
            Assert.IsTrue(split, "one node must commit the split");

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                for (int attempt = 0; attempt < 50; attempt++)
                {
                    node.Kahuna.InvalidatePlacement(keySpace);
                    var placement = node.Kahuna.GetPlacement(keySpace);
                    if (placement.IsKeyRange && placement.Spans.Count >= 2 && placement.Spans.All(s => s.LeaderEndpoint is not null))
                        break;
                    await Task.Delay(200);
                }
            }

            // The reference: the same filter with built-ins, which ships. Asking from every node makes
            // sure at least one asks a peer, so the test proves fragments engage at all.
            cluster.FragmentTransport!.ResetExecuted();
            List<QueryResultRow> reference = [];

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                reference = await RunSql(node, db, "SELECT id FROM readings WHERE abs(num) >= 50 AND abs(num) < 150");

            Assert.AreEqual(100, reference.Count);
            Assert.IsTrue(cluster.FragmentTransport.ExecutedRequests.Any(r => r.FilterJson.Contains("abs")),
                "the built-in filter must ship to a peer, or this test proves nothing");

            cluster.FragmentTransport!.ResetExecuted();

            List<QueryResultRow> withExtension = await RunSql(cluster.Nodes[0], db,
                "SELECT id FROM readings WHERE fx_add(num, 0) >= 50 AND fx_add(num, 0) < 150");

            CollectionAssert.AreEquivalent(
                reference.Select(r => r.Row["id"].StrValue),
                withExtension.Select(r => r.Row["id"].StrValue),
                "the coordinator must evaluate the extension call and return the built-in result");

            foreach (var request in cluster.FragmentTransport.ExecutedRequests)
                StringAssert.DoesNotContain("fx_add", request.FilterJson, "an extension call must never be shipped to a peer");

            // A node that does not load the extension cannot run the statement at all: proof that a
            // shipped call would have failed on the peers.
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => RunSql(cluster.Nodes[1], db,
                "SELECT id FROM readings WHERE fx_add(num, 0) >= 50"))!;
            StringAssert.Contains("Function not found", ex.Message);
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
