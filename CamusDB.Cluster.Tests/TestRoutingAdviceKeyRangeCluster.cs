/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Routing;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Routing advice for a key-range routed table. While every key space of the table (rows, each index,
/// large values) is one range, all of them sit on the partition Kahuna seeded for the table's placement
/// group, so every parameter value reaches one leader and the statement gets <c>prefer</c> advice that
/// names it. A split of the row space or of an index space makes the destination depend on the
/// parameter, and the advice turns to <c>clear</c>.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestRoutingAdviceKeyRangeCluster
{
    private const int Partitions = 3;
    private const int RowCount = 40;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static readonly CamusDBOptions KeyRange = CamusDBOptions.Default with { KeyRangeShardingEnabled = true };

    private static async Task<(InProcessSchemaCluster cluster, string db, List<string> ids)> SetupAsync()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: Partitions, loggerFactory: sharedLoggerFactory, logger: logger, options: KeyRange);

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
                    new ColumnInfo("amount", ColumnType.Integer64),
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)]),
                    new ConstraintInfo(ConstraintType.IndexMulti, "amount_idx", [new ColumnIndexInfo("amount", OrderType.Ascending)]),
                ],
                ifNotExists: false
            )).WaitAsync(TimeSpan.FromSeconds(30)));

            await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

            InProcessSchemaCluster.Node seeder = cluster.Nodes[0];
            List<string> ids = [];
            KvTransaction tx = await seeder.Database!.Transactions.BeginAsync();
            for (int i = 0; i < RowCount; i++)
            {
                string id = ObjectIdGenerator.Generate().ToString();
                ids.Add(id);
                await seeder.Executor.Insert(new InsertTicket(
                    txnState: tx, databaseName: db, tableName: "readings",
                    values: new() { new() {
                        { "id", new(ColumnType.Id, id) },
                        { "amount", new(ColumnType.Integer64, (long)i) },
                    }}));
            }
            await seeder.Database.Transactions.CommitAsync(tx);

            // Registration is node-local: open the table everywhere so every node routes it by range.
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                await node.Executor.OpenTable(new OpenTableTicket(db, "readings"));

            return (cluster, db, ids);
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// The advice a fresh resolver (so no cached placement) gives for a point lookup run on
    /// <paramref name="node"/>. Retries while the node cannot name a leader yet: a just-started
    /// cluster reports "placement unknown" until leadership gossip reaches it.
    /// </summary>
    private static async Task<StatementRoutingAdvice> AdviceAsync(InProcessSchemaCluster.Node node, string db, string id)
    {
        StatementRoutingAdvice? advice = null;

        for (int attempt = 0; attempt < 50; attempt++)
        {
            StatementRoutingCollector collector = new();
            ExecuteSQLTicket ticket = new(
                txnState: node.Database!.Transactions.BeginReadOnly(), database: db,
                sql: $"SELECT amount FROM readings WHERE id = '{id}'", parameters: null, routing: collector);

            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(ticket);
            await foreach (QueryResultRow _ in cursor) { }

            advice = new StatementRoutingResolver(KeyRange).Resolve(node.Database, collector);
            if (advice is not { Reason: StatementRoutingAdvice.ReasonPlacementUnknown })
                return advice!;

            await Task.Delay(200);
        }

        return advice!;
    }

    [Test]
    public async Task UnsplitTable_GetsPreferAdvice_NamingTheRowRangeLeader()
    {
        (InProcessSchemaCluster cluster, string db, List<string> ids) = await SetupAsync();
        await using InProcessSchemaCluster scope = cluster;

        InProcessSchemaCluster.Node node = cluster.Nodes[0];
        StatementRoutingAdvice advice = await AdviceAsync(node, db, ids[0]);

        TableDescriptor table = await node.Executor.OpenTable(new OpenTableTicket(db, "readings"));
        TablePlacement rows = node.Kahuna.ReadPlacementUncached(table.Store.RowKeySpace, out _);

        Assert.Multiple(() =>
        {
            Assert.That(advice.Disposition, Is.EqualTo(StatementRoutingDisposition.Prefer), advice.Reason);
            Assert.That(advice.Reason, Is.EqualTo(StatementRoutingAdvice.ReasonSingleRangeTable));
            Assert.That(rows.IsKeyRange, Is.True, "the row space is key-range routed");
            Assert.That(advice.PreferredNodeId, Is.EqualTo(rows.Spans[0].LeaderEndpoint), "the advice names the row range's leader");
        });
    }

    [Test]
    public async Task AfterAnIndexSplit_TheAdviceIsCleared()
    {
        (InProcessSchemaCluster cluster, string db, List<string> ids) = await SetupAsync();
        await using InProcessSchemaCluster scope = cluster;

        InProcessSchemaCluster.Node node = cluster.Nodes[0];
        Assert.That((await AdviceAsync(node, db, ids[0])).Disposition, Is.EqualTo(StatementRoutingDisposition.Prefer));

        TableDescriptor table = await node.Executor.OpenTable(new OpenTableTicket(db, "readings"));
        string indexId = table.Indexes["amount_idx"].KvId;
        string splitKey = KeyRangeSplitHarness.IndexKeyAt(
            table, indexId, new CompositeColumnValue(new ColumnValue(ColumnType.Integer64, (long)(RowCount / 2))));
        await KeyRangeSplitHarness.SplitAtAsync(cluster, table.Store.IndexKeySpace(indexId), splitKey);

        StatementRoutingAdvice advice = await AdviceAsync(node, db, ids[0]);

        Assert.That(advice.Disposition, Is.EqualTo(StatementRoutingDisposition.Clear),
            "a divided index space makes the destination depend on the parameter");
        Assert.That(advice.Reason, Is.EqualTo(StatementRoutingAdvice.ReasonIneligible));
    }

    [Test]
    public async Task AfterARowSplit_TheAdviceIsCleared()
    {
        (InProcessSchemaCluster cluster, string db, List<string> ids) = await SetupAsync();
        await using InProcessSchemaCluster scope = cluster;

        InProcessSchemaCluster.Node node = cluster.Nodes[0];
        Assert.That((await AdviceAsync(node, db, ids[0])).Disposition, Is.EqualTo(StatementRoutingDisposition.Prefer));

        TableDescriptor table = await node.Executor.OpenTable(new OpenTableTicket(db, "readings"));
        string splitKey = KeyRangeSplitHarness.MedianRowKey(table, await KeyRangeSplitHarness.ScanRowIdsAsync(node, table));
        await KeyRangeSplitHarness.SplitAtAsync(cluster, table.Store.RowKeySpace, splitKey);

        StatementRoutingAdvice advice = await AdviceAsync(node, db, ids[0]);

        Assert.That(advice.Disposition, Is.EqualTo(StatementRoutingDisposition.Clear),
            "a divided row space makes the destination depend on the parameter");
        Assert.That(advice.Reason, Is.EqualTo(StatementRoutingAdvice.ReasonIneligible));
    }
}
