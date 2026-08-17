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
/// End-to-end coverage for the broadcast hash join: with distribution on and the fact table's
/// keyspace split into two spans, an inner equi-join with a small build side must ship the
/// build to remote probe spans (visible in the fragment transport), return exactly the
/// classic single-node join result from every node — the pre-split run of the identical query
/// is the reference sequence — including NULL-key exclusion on both sides and duplicate build
/// keys producing one output row per match, and stay exact when every remote fragment fails
/// and probe spans fall back to local scans.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterBroadcastJoin
{
    private const int Partitions = 2;
    private const int FactCount = 200;
    private const int DimCount = 5;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

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
            databaseName: db,
            tableName: "facts",
            columns:
            [
                new ColumnInfo("id",     ColumnType.Id),
                new ColumnInfo("dim_id", ColumnType.Integer64),
                new ColumnInfo("val",    ColumnType.Integer64, notNull: true),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "dims",
            columns:
            [
                new ColumnInfo("id",   ColumnType.Id),
                new ColumnInfo("k",    ColumnType.Integer64),
                new ColumnInfo("name", ColumnType.String),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 2);

        InProcessSchemaCluster.Node writer = cluster.Nodes[0];
        KvTransaction tx = await writer.Database!.Transactions.BeginAsync();

        // Facts: dim_id cycles 0..4, every 10th row has a NULL dim_id (must never join).
        List<string> factIds = new(FactCount);

        for (int i = 0; i < FactCount; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            factIds.Add(id);

            ColumnValue dimId = i % 10 == 9
                ? ColumnValue.Null
                : new ColumnValue(ColumnType.Integer64, (long)(i % DimCount));

            await writer.Executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "facts",
                values: new() { new() {
                    { "id",     new(ColumnType.Id,        id) },
                    { "dim_id", dimId },
                    { "val",    new(ColumnType.Integer64, (long)i) },
                }}));
        }

        // Dims: keys 0..4, key 2 duplicated (two build rows → two output rows per match),
        // plus one NULL-key row (excluded from the build) and one unmatched key (never joins).
        var dimRows = new (long? K, string Name)[]
        {
            (0, "zero"), (1, "one"), (2, "two-a"), (3, "three"), (4, "four"),
            (2, "two-b"), (null, "null-key"), (99, "unmatched"),
        };

        foreach ((long? k, string name) in dimRows)
        {
            await writer.Executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "dims",
                values: new() { new() {
                    { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "k",    k is { } kv ? new(ColumnType.Integer64, kv) : ColumnValue.Null },
                    { "name", new(ColumnType.String, name) },
                }}));
        }

        await writer.Database.Transactions.CommitAsync(tx);
        return (cluster, db, factIds);
    }

    private static async Task SplitFactsAtMedianAsync(
        InProcessSchemaCluster cluster, string db, List<string> factIds)
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

            Assert.IsTrue(placement!.IsKeyRange && placement.Spans.Count >= 2,
                "Every node's placement must report the split spans");
            Assert.IsTrue(placement.Spans.All(s => s.LeaderEndpoint is not null),
                "Every span must have a known leader so remote-vs-local dispatch is deterministic");
        }
    }

    private static async Task<List<QueryResultRow>> RunSql(
        InProcessSchemaCluster.Node node, string db, string sql)
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

    private static List<(string FactId, long Val, string Name)> Shape(List<QueryResultRow> rows) =>
        rows.Select(r => (
            r.Row["id"].StrValue!,
            r.Row["val"].LongValue,
            r.Row["name"].StrValue!)).ToList();

    /// <summary>
    /// Canonical order for cross-node comparison. Inner-join OUTPUT ORDER is plan-dependent
    /// (per-node statistics can flip the join order or build side, which legally reorders
    /// rows), so cross-node equality is a multiset property; only same-plan runs compare by
    /// sequence.
    /// </summary>
    private static List<(string FactId, long Val, string Name)> Canonical(List<(string FactId, long Val, string Name)> rows) =>
        rows.OrderBy(r => r.FactId, StringComparer.Ordinal).ThenBy(r => r.Name, StringComparer.Ordinal).ToList();

    [Test]
    public async Task BroadcastJoin_AfterSplit_MatchesClassicJoin_OnEveryNode_AndUnderFallback()
    {
        (InProcessSchemaCluster cluster, string db, List<string> factIds) = await SetupAsync();

        try
        {
            const string joinSql =
                "SELECT f.id, f.val, d.name FROM facts f JOIN dims d ON f.dim_id = d.k WHERE f.val >= 20 AND f.val < 180";

            // Reference: the identical query BEFORE the split — single span, classic local
            // hash join, no broadcast possible. This is the differential baseline.
            List<(string FactId, long Val, string Name)> reference = Shape(await RunSql(cluster.Nodes[0], db, joinSql));

            // Sanity on the fixture semantics: NULL dim_ids never join; dim key 2 is
            // duplicated so its matches emit two rows each; key 99 never matches.
            int matchedFacts = Enumerable.Range(20, 160).Count(i => i % 10 != 9);
            int doubledFacts = Enumerable.Range(20, 160).Count(i => i % 10 != 9 && i % DimCount == 2);
            Assert.AreEqual(matchedFacts + doubledFacts, reference.Count,
                "Reference join must reflect NULL exclusion and duplicate-key doubling");
            Assert.IsTrue(reference.All(r => r.Name != "null-key" && r.Name != "unmatched"),
                "NULL and unmatched build keys must never appear");

            await SplitFactsAtMedianAsync(cluster, db, factIds);

            cluster.FragmentTransport!.ResetExecuted();

            // The node that produced the reference re-runs the identical query with the same
            // plan: broadcast must reproduce the classic join's SEQUENCE byte-identically
            // (span-order concat + coordinator-side merges against its own build rows).
            Assert.AreEqual(reference, Shape(await RunSql(cluster.Nodes[0], db, joinSql)),
                "Node 0: broadcast join must reproduce its own classic join sequence exactly");

            // Other nodes may plan a different join order/build side (their statistics
            // differ), which legally reorders inner-join output — compare as multisets.
            List<(string FactId, long Val, string Name)> canonicalReference = Canonical(reference);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<(string FactId, long Val, string Name)> result = Shape(await RunSql(node, db, joinSql));
                Assert.AreEqual(canonicalReference, Canonical(result),
                    $"Node {node.Index}: broadcast join must reproduce the classic join rows exactly");
            }

            Assert.Greater(cluster.FragmentTransport.ExecutedJoinCount, 0,
                "At least one probe span must have executed as a broadcast-join fragment on its leader " +
                "(every node cannot lead every span)");

            // A remote probe fragment ships one frame per MATCHED probe row — never the
            // span's full row count, and never one frame per output pair.
            Assert.LessOrEqual(cluster.FragmentTransport.RowsReturned, (long)matchedFacts,
                "Remote join fragments must ship only matched probe rows, once each");

            // Unfiltered probe (no WHERE): broadcast must also engage and stay exact.
            const string unfilteredSql =
                "SELECT f.id, f.val, d.name FROM facts f JOIN dims d ON f.dim_id = d.k";

            List<(string FactId, long Val, string Name)> unfilteredReference = Canonical(Shape(await RunSql(cluster.Nodes[0], db, unfilteredSql)));

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.AreEqual(unfilteredReference, Canonical(Shape(await RunSql(node, db, unfilteredSql))),
                    $"Node {node.Index}: unfiltered broadcast join must reproduce the classic join rows exactly");
            }

            // Exactness under total remote failure: every remote attempt dies before the
            // first frame; every probe span must resume locally with no duplicates/losses.
            cluster.FragmentTransport.FailNextFragments(12);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.AreEqual(canonicalReference, Canonical(Shape(await RunSql(node, db, joinSql))),
                    $"Node {node.Index}: join must stay exact when remote fragments fail and spans fall back locally");
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
