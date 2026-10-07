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
/// The left outer join on a multi-node cluster with the fact table split into two spans: every
/// node returns the rows an in-memory oracle computes (padded rows included), the broadcast probe
/// declines the outer join because a remote span cannot pad, while the inner form of the same
/// query still ships fragments.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterLeftOuterJoin
{
    private const int Partitions = 2;
    private const int FactCount = 120;
    private const int DimCount = 4;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static readonly string[] DimNames = ["zero", "one", "two", "three"];

    /// <summary>Facts i = 0..N: dim_id = i % 4, except every 10th row (i % 10 == 9) which has a NULL dim_id; val = i.</summary>
    private static long? DimOf(int i) => i % 10 == 9 ? null : i % DimCount;

    private static async Task<(InProcessSchemaCluster cluster, string db, List<string> factIds)> SetupAsync()
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: Partitions,
                loggerFactory: sharedLoggerFactory, logger: logger,
                options: CamusDBOptions.Default with
                {
                    KeyRangeShardingEnabled = true,
                    DistributedQueryExecutionEnabled = true,
                });

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db, tableName: "facts",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("dim_id", ColumnType.Integer64),
                new ColumnInfo("val", ColumnType.Integer64, notNull: true),
            ],
            constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db, tableName: "dims",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("k", ColumnType.Integer64),
                new ColumnInfo("name", ColumnType.String),
            ],
            constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 2);

        // A right table with no rows at all, for the SELECT * padding case.
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db, tableName: "nodims",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("k", ColumnType.Integer64),
                new ColumnInfo("label", ColumnType.String),
            ],
            constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 3);

        InProcessSchemaCluster.Node writer = cluster.Nodes[0];
        KvTransaction tx = await writer.Database!.Transactions.BeginAsync();

        List<string> factIds = new(FactCount);

        for (int i = 0; i < FactCount; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            factIds.Add(id);

            await writer.Executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "facts",
                values: new() { new() {
                    { "id", new(ColumnType.Id, id) },
                    { "dim_id", DimOf(i) is { } d ? new(ColumnType.Integer64, d) : ColumnValue.Null },
                    { "val", new(ColumnType.Integer64, (long)i) },
                }}));
        }

        for (int k = 0; k < DimCount; k++)
        {
            await writer.Executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "dims",
                values: new() { new() {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "k", new(ColumnType.Integer64, (long)k) },
                    { "name", new(ColumnType.String, DimNames[k]) },
                }}));
        }

        // One NULL-keyed dim: must never match a NULL-keyed fact.
        await writer.Executor.Insert(new InsertTicket(
            txnState: tx, databaseName: db, tableName: "dims",
            values: new() { new() {
                { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "k", ColumnValue.Null },
                { "name", new(ColumnType.String, "null-key") },
            }}));

        await writer.Database.Transactions.CommitAsync(tx);
        return (cluster, db, factIds);
    }

    private static async Task SplitFactsAtMedianAsync(InProcessSchemaCluster cluster, string db, List<string> factIds)
    {
        TableDescriptor table = await cluster.Nodes[0].Database!.TableDescriptors["facts"];
        string keySpace = table.Store.RowKeySpace;

        List<string> sorted = factIds.OrderBy(x => x, StringComparer.Ordinal).ToList();
        string splitKey = table.Store.RowPointKey(ObjectId.ToValue(sorted[FactCount / 2]));

        KahunaSplitRangeResponse? split = null;
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            KahunaSplitRangeResponse response =
                await node.Kahuna.Kahuna.SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);

            if (response.Success)
            {
                split = response;
                break;
            }
        }

        Assert.IsNotNull(split, "One node (the meta-partition leader) must commit the split");

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TablePlacement? placement = null;

            for (int attempt = 0; attempt < 50; attempt++)
            {
                node.Kahuna.InvalidatePlacement(keySpace);
                placement = node.Kahuna.GetPlacement(keySpace);
                if (placement.IsKeyRange && placement.Spans.Count >= 2
                    && placement.Spans.All(s => s.LeaderEndpoint is not null))
                    break;

                await Task.Delay(200);
            }

            Assert.IsTrue(placement!.IsKeyRange && placement.Spans.Count >= 2, "Every node's placement must report the split spans");
            Assert.IsTrue(placement.Spans.All(s => s.LeaderEndpoint is not null), "Every span must have a known leader");
        }
    }

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

    private static List<(long Val, string? Name)> Shape(List<QueryResultRow> rows) =>
        rows.Select(r => (Val: r.Row["val"].LongValue, Name: r.Row["name"].Type == ColumnType.Null ? null : r.Row["name"].StrValue))
            .OrderBy(r => r.Val).ThenBy(r => r.Name, StringComparer.Ordinal).ToList();

    private static List<(long Val, string? Name)> Oracle(Func<int, bool> factFilter, Func<string?, bool> nameFilter) =>
        Enumerable.Range(0, FactCount)
            .Where(factFilter)
            .Select(i => ((long)i, DimOf(i) is { } d ? DimNames[d] : null))
            .Where(r => nameFilter(r.Item2))
            .OrderBy(r => r.Item1).ThenBy(r => r.Item2, StringComparer.Ordinal).ToList();

    [Test]
    public async Task LeftOuterJoin_AfterSplit_MatchesTheOracleOnEveryNode_AndDeclinesBroadcast()
    {
        (InProcessSchemaCluster cluster, string db, List<string> factIds) = await SetupAsync();

        try
        {
            const string leftSql = "SELECT f.val, d.name FROM facts f LEFT JOIN dims d ON f.dim_id = d.k";
            const string innerSql = "SELECT f.val, d.name FROM facts f JOIN dims d ON f.dim_id = d.k";

            List<(long, string?)> everyFact = Oracle(_ => true, _ => true);
            Assert.AreEqual(FactCount, everyFact.Count, "the oracle pads every NULL-keyed fact once");
            Assert.AreEqual(everyFact, Shape(await RunSql(cluster.Nodes[0], db, leftSql)), "pre-split reference matches the oracle");

            await SplitFactsAtMedianAsync(cluster, db, factIds);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                cluster.FragmentTransport!.ResetExecuted();

                // Every fact, padded where the key is NULL.
                Assert.AreEqual(everyFact, Shape(await RunSql(node, db, leftSql)),
                    $"Node {node.Index}: the left outer join must reproduce the oracle after the split");
                Assert.AreEqual(0, cluster.FragmentTransport.ExecutedJoinCount,
                    $"Node {node.Index}: an outer join never ships a broadcast probe fragment (a remote span cannot pad)");

                // A point filter on the preserved side.
                Assert.AreEqual(Oracle(i => i == 19, _ => true), Shape(await RunSql(node, db, leftSql + " WHERE f.val = 19")),
                    $"Node {node.Index}: a preserved-side point filter keeps the padded row");

                // IS NULL after the join keeps exactly the padded rows.
                Assert.AreEqual(Oracle(_ => true, n => n is null), Shape(await RunSql(node, db, leftSql + " WHERE d.name IS NULL")),
                    $"Node {node.Index}: IS NULL on the null-extended side");

                // An equality after the join removes every padded row.
                Assert.AreEqual(Oracle(_ => true, n => n == "two"), Shape(await RunSql(node, db, leftSql + " WHERE d.name = 'two'")),
                    $"Node {node.Index}: equality on the null-extended side");

                // SELECT * over a zero-row right table: every fact padded, with the right keys present.
                List<QueryResultRow> star = await RunSql(node, db, "SELECT * FROM facts f LEFT JOIN nodims n ON f.dim_id = n.k");
                Assert.AreEqual(FactCount, star.Count, $"Node {node.Index}: every fact is padded against an empty right table");
                Assert.IsTrue(star.All(r => r.Row.ContainsKey("n.label") && r.Row["n.label"].Type == ColumnType.Null
                                             && r.Row.ContainsKey("n.k") && r.Row["n.k"].Type == ColumnType.Null),
                    $"Node {node.Index}: a padded row carries every right key with a NULL value");
            }

            // The inner form of the same join still engages the broadcast probe on some node.
            cluster.FragmentTransport!.ResetExecuted();
            List<(long, string?)> innerOracle = Oracle(_ => true, n => n is not null);
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.AreEqual(innerOracle, Shape(await RunSql(node, db, innerSql)),
                    $"Node {node.Index}: the inner join is unchanged");
            }
            Assert.Greater(cluster.FragmentTransport.ExecutedJoinCount, 0,
                "the inner form must still ship broadcast probe fragments, so the decline is specific to the outer kind");
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
