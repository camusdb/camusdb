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
/// End-to-end coverage of the span-local top k of a grouped aggregate on a three-node cluster whose
/// table is split into three key-range spans: <c>GROUP BY g ORDER BY MAX(x) DESC LIMIT n</c> (and
/// MIN ASC on a NOT NULL column) lets each span send only its local top k groups, and the answer must
/// still be exact.
///
/// <para>Each result is checked against an oracle computed from the inserted rows. A result is valid
/// when its sequence of ordered values is the oracle's sequence at the same positions, and each group
/// carries its own oracle value. That check accepts any choice among tied groups, which SQL allows
/// when ORDER BY has no second key, and rejects a lost group, a wrong value and a wrong position.</para>
///
/// <para>The data has the cases the proof depends on: many groups that tie at the top value, a group
/// whose values are all NULL, NULL values inside other groups, and a column (<c>y</c>) whose values are
/// all distinct, so that the number of shipped rows is exact.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestClusterTopKAggregation
{
    private const int RowCount = 3_000;

    private const int Groups = 300;

    private const int Spans = 3;

    /// <summary>Groups 0-39 each get one row with x = TopValue, so 40 groups tie at the top.</summary>
    private const int TiedGroups = 40;

    private const long TopValue = 1_000;

    /// <summary>Every row of this group has a NULL x, so its MAX is NULL.</summary>
    private const int AllNullGroup = Groups - 1;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    private InProcessSchemaCluster? cluster;

    private string db = "";

    private readonly Dictionary<long, long?> maxX = new();

    private readonly Dictionary<long, long> minY = new();

    private readonly Dictionary<long, long> countAll = new();

    private InProcessSchemaCluster Cluster => cluster!;

    private InProcessSchemaCluster.RecordingFragmentTransport Transport => cluster!.FragmentTransport!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: Spans,
            loggerFactory: sharedLoggerFactory, logger: logger,
            options: CamusDBOptions.Default with
            {
                KeyRangeShardingEnabled = true,
                DistributedQueryExecutionEnabled = true,
            });

        db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "scores",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("g", ColumnType.Integer64, notNull: true),
                new ColumnInfo("x", ColumnType.Integer64),
                new ColumnInfo("y", ColumnType.Integer64, notNull: true),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        List<string> ids = await LoadAsync();
        await SplitIntoSpansAsync(ids);
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (cluster is not null)
            await cluster.DisposeAsync();
    }

    [SetUp]
    public void ResetTransport()
    {
        Transport.ResetExecuted();
        Transport.IgnoreTopK = false;
        Transport.FailNextFragments(0);
    }

    [Test]
    public async Task MaxDescending_IsExact_OnEveryCoordinator_WithTiesAtTheTop()
    {
        foreach (InProcessSchemaCluster.Node node in Cluster.Nodes)
        {
            List<QueryResultRow> rows = await RunSql(node,
                "SELECT g, MAX(x) AS m FROM scores GROUP BY g ORDER BY m DESC LIMIT 10");

            AssertValidTopK(rows, "m", maxX.ToDictionary(kv => kv.Key, kv => kv.Value), descending: true, offset: 0, limit: 10);

            // 40 groups tie at the top value, so every returned value is the top value.
            Assert.IsTrue(rows.All(r => r.Row["m"].LongValue == TopValue), $"node {node.Index}: all ten rows tie at the top");
        }

        Assert.Greater(Transport.ExecutedCount, 0, "At least one coordinator must run remote span fragments");
        Assert.IsTrue(Transport.ExecutedRequests.All(r => r.TopK is { K: 10, Descending: true, OutputName: "m" }),
            "Each fragment request must carry the span-local top k");
    }

    [Test]
    public async Task MaxDescending_WithOffset_ReturnsTheRightPositions()
    {
        // Positions 35-49 cross the end of the 40-way tie at the top.
        List<QueryResultRow> rows = await RunSql(Cluster.Nodes[0],
            "SELECT g, MAX(x) AS m FROM scores GROUP BY g ORDER BY m DESC LIMIT 15 OFFSET 35");

        AssertValidTopK(rows, "m", maxX.ToDictionary(kv => kv.Key, kv => kv.Value), descending: true, offset: 35, limit: 15);
    }

    [Test]
    public async Task MaxDescending_LimitPastTheGroupCount_ReturnsEveryGroup_NullMaxLast()
    {
        List<QueryResultRow> rows = await RunSql(Cluster.Nodes[1],
            "SELECT g, MAX(x) AS m FROM scores GROUP BY g ORDER BY m DESC LIMIT 1000");

        Assert.AreEqual(Groups, rows.Count);
        AssertValidTopK(rows, "m", maxX.ToDictionary(kv => kv.Key, kv => kv.Value), descending: true, offset: 0, limit: 1000);

        Assert.AreEqual(ColumnType.Null, rows[^1].Row["m"].Type, "The all-NULL group has a NULL MAX, and NULL sorts last in DESC");
        Assert.AreEqual((long)AllNullGroup, rows[^1].Row["g"].LongValue);
    }

    [Test]
    public async Task MinAscending_OnNotNullColumn_IsExact_AndEachSpanShipsAtMostK()
    {
        foreach (InProcessSchemaCluster.Node node in Cluster.Nodes)
        {
            Transport.ResetExecuted();

            List<QueryResultRow> rows = await RunSql(node,
                "SELECT g, MIN(y) AS lo FROM scores GROUP BY g ORDER BY lo ASC LIMIT 7");

            AssertValidTopK(rows, "lo", minY.ToDictionary(kv => kv.Key, kv => (long?)kv.Value), descending: false, offset: 0, limit: 7);

            // y values are all distinct, so no span has a tie at its 7th value.
            Assert.LessOrEqual(Transport.RowsReturned, 7L * Transport.ExecutedCount,
                $"node {node.Index}: each remote span ships at most k partial rows");
        }
    }

    [Test]
    public async Task SpanLocalTopK_ShipsFarFewerRowsThanEveryGroup()
    {
        int checkedNodes = 0;

        foreach (InProcessSchemaCluster.Node node in Cluster.Nodes)
        {
            Transport.ResetExecuted();
            await RunSql(node, "SELECT g, MIN(y) AS lo FROM scores GROUP BY g ORDER BY lo ASC LIMIT 5");
            long topKRows = Transport.RowsReturned;
            int topKFragments = Transport.ExecutedCount;

            if (topKFragments == 0)
                continue;

            Transport.ResetExecuted();
            await RunSql(node, "SELECT g, MIN(y) AS lo FROM scores GROUP BY g");
            long allRows = Transport.RowsReturned;

            Assert.LessOrEqual(topKRows, 5L * topKFragments, $"node {node.Index}: at most k rows per remote span");
            Assert.Greater(allRows, 10 * topKRows, $"node {node.Index}: without ORDER BY and LIMIT each span ships every group it has");
            checkedNodes++;
        }

        Assert.Greater(checkedNodes, 0, "At least one coordinator must have a remote span");
    }

    [Test]
    public async Task OtherAggregateProjected_ShipsEveryGroup_AndIsExact()
    {
        foreach (InProcessSchemaCluster.Node node in Cluster.Nodes)
        {
            List<QueryResultRow> rows = await RunSql(node,
                "SELECT g, MAX(x) AS m, COUNT(*) AS c FROM scores GROUP BY g ORDER BY m DESC LIMIT 10");

            AssertValidTopK(rows, "m", maxX.ToDictionary(kv => kv.Key, kv => kv.Value), descending: true, offset: 0, limit: 10);

            foreach (QueryResultRow row in rows)
                Assert.AreEqual(countAll[row.Row["g"].LongValue], row.Row["c"].LongValue, "The COUNT next to the MAX must be complete");
        }

        Assert.IsTrue(Transport.ExecutedRequests.All(r => r.TopK is null),
            "A second aggregate needs every span's groups, so no request carries a top k");
    }

    [Test]
    public async Task CountDescending_ShipsEveryGroup_AndIsExact()
    {
        foreach (InProcessSchemaCluster.Node node in Cluster.Nodes)
        {
            List<QueryResultRow> rows = await RunSql(node,
                "SELECT g, COUNT(*) AS c FROM scores GROUP BY g ORDER BY c DESC LIMIT 10");

            AssertValidTopK(rows, "c", countAll.ToDictionary(kv => kv.Key, kv => (long?)kv.Value), descending: true, offset: 0, limit: 10);
        }

        Assert.IsTrue(Transport.ExecutedRequests.All(r => r.TopK is null),
            "A COUNT adds up over the spans, so a span-local top k is not exact and is not requested");
    }

    [Test]
    public async Task RemoteFailure_RecomputesLocally_WithTheSameTopK()
    {
        Transport.FailNextFragments(12);

        foreach (InProcessSchemaCluster.Node node in Cluster.Nodes)
        {
            List<QueryResultRow> rows = await RunSql(node,
                "SELECT g, MAX(x) AS m FROM scores GROUP BY g ORDER BY m DESC LIMIT 45");

            AssertValidTopK(rows, "m", maxX.ToDictionary(kv => kv.Key, kv => kv.Value), descending: true, offset: 0, limit: 45);
        }
    }

    [Test]
    public async Task PeerThatIgnoresTopK_StillGivesTheExactAnswer()
    {
        Transport.IgnoreTopK = true;

        foreach (InProcessSchemaCluster.Node node in Cluster.Nodes)
        {
            List<QueryResultRow> rows = await RunSql(node,
                "SELECT g, MIN(y) AS lo FROM scores GROUP BY g ORDER BY lo ASC LIMIT 9");

            AssertValidTopK(rows, "lo", minY.ToDictionary(kv => kv.Key, kv => (long?)kv.Value), descending: false, offset: 0, limit: 9);
        }
    }

    [Test]
    public async Task Explain_ShowsTheTopKShape()
    {
        List<QueryResultRow> rows = await RunSql(Cluster.Nodes[0],
            "EXPLAIN SELECT g, MAX(x) AS m FROM scores GROUP BY g ORDER BY m DESC LIMIT 10");

        string plan = string.Join("\n", rows.Select(r => string.Join(" ", r.Row.Values.Select(v => v.StrValue))));

        Assert.That(plan, Does.Contain("top_k=[m DESC, k: 10]"));
    }

    /// <summary>
    /// Checks that <paramref name="rows"/> are positions <paramref name="offset"/> to
    /// <c>offset + limit</c> of a correct ordering of <paramref name="oracle"/>: distinct groups, each
    /// with its oracle value, and the value sequence equal to the oracle's at those positions.
    /// </summary>
    private static void AssertValidTopK(
        List<QueryResultRow> rows, string valueColumn, Dictionary<long, long?> oracle,
        bool descending, int offset, int limit)
    {
        List<long?> ordered = oracle.Values.ToList();
        ordered.Sort((a, b) => CompareNullSmallest(a, b) * (descending ? -1 : 1));

        List<long?> expected = ordered.Skip(offset).Take(limit).ToList();

        Assert.AreEqual(expected.Count, rows.Count, "Row count");

        HashSet<long> seen = new();

        for (int i = 0; i < rows.Count; i++)
        {
            long group = rows[i].Row["g"].LongValue;
            ColumnValue cell = rows[i].Row[valueColumn];
            long? value = cell.Type == ColumnType.Null ? null : cell.LongValue;

            Assert.IsTrue(seen.Add(group), $"Group {group} appears twice");
            Assert.AreEqual(oracle[group], value, $"Group {group} has the wrong {valueColumn}");
            Assert.AreEqual(expected[i], value, $"Position {offset + i} has the wrong value");
        }
    }

    private static int CompareNullSmallest(long? a, long? b)
    {
        if (a is null)
            return b is null ? 0 : -1;

        return b is null ? 1 : a.Value.CompareTo(b.Value);
    }

    /// <summary>
    /// Inserts the rows and fills the oracles. Row i goes to group i % Groups. x is random in
    /// 0 to TopValue - 1 with about one NULL in twenty; the first row of each of the first TiedGroups
    /// groups gets TopValue; the AllNullGroup gets only NULL. y is distinct for every row.
    /// </summary>
    private async Task<List<string>> LoadAsync()
    {
        Random random = new(7);
        List<string> ids = new(RowCount);
        InProcessSchemaCluster.Node writer = Cluster.Nodes[0];

        for (int start = 0; start < RowCount; start += 500)
        {
            List<Dictionary<string, ColumnValue>> batch = new(500);

            for (int i = start; i < Math.Min(start + 500, RowCount); i++)
            {
                long g = i % Groups;
                long? x = g == AllNullGroup ? null
                    : i < Groups && g < TiedGroups ? TopValue
                    : random.Next(20) == 0 ? null
                    : random.Next((int)TopValue);
                long y = (long)i * 7919 % 1_000_003;

                string id = ObjectIdGenerator.Generate().ToString();
                ids.Add(id);

                batch.Add(new()
                {
                    { "id", new(ColumnType.Id, id) },
                    { "g", new(ColumnType.Integer64, g) },
                    { "x", x is { } xv ? new(ColumnType.Integer64, xv) : ColumnValue.Null },
                    { "y", new(ColumnType.Integer64, y) },
                });

                countAll[g] = countAll.GetValueOrDefault(g) + 1;
                minY[g] = minY.TryGetValue(g, out long lo) ? Math.Min(lo, y) : y;

                long? current = maxX.GetValueOrDefault(g);
                maxX[g] = x is null ? current : current is null ? x : Math.Max(current.Value, x.Value);
            }

            KvTransaction tx = await writer.Database!.Transactions.BeginAsync();
            await writer.Executor.Insert(new InsertTicket(txnState: tx, databaseName: db, tableName: "scores", values: batch));
            await writer.Database.Transactions.CommitAsync(tx);
        }

        Assert.AreEqual(TiedGroups, maxX.Values.Count(v => v == TopValue),
            "The data must have exactly the planned tie at the top value");

        return ids;
    }

    private async Task SplitIntoSpansAsync(List<string> ids)
    {
        TableDescriptor table = await Cluster.Nodes[0].Database!.TableDescriptors["scores"];
        string keySpace = table.Store.RowKeySpace;
        List<string> sorted = ids.OrderBy(x => x, StringComparer.Ordinal).ToList();

        for (int s = 1; s < Spans; s++)
        {
            string splitKey = table.Store.RowPointKey(ObjectId.ToValue(sorted[RowCount * s / Spans]));
            bool committed = false;

            foreach (InProcessSchemaCluster.Node node in Cluster.Nodes)
            {
                KahunaSplitRangeResponse response =
                    await node.Kahuna.Kahuna.SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);

                if (response.Success)
                {
                    committed = true;
                    break;
                }
            }

            Assert.IsTrue(committed, $"Split {s} must be committed by the meta-partition leader");
        }

        foreach (InProcessSchemaCluster.Node node in Cluster.Nodes)
        {
            TablePlacement? placement = null;

            for (int attempt = 0; attempt < 100; attempt++)
            {
                node.Kahuna.InvalidatePlacement(keySpace);
                placement = node.Kahuna.GetPlacement(keySpace);
                if (placement.IsKeyRange && placement.Spans.Count >= Spans)
                    break;

                await Task.Delay(200);
            }

            Assert.IsTrue(placement!.IsKeyRange && placement.Spans.Count >= Spans,
                $"Node {node.Index} placement must report {Spans} spans");
        }
    }

    private async Task<List<QueryResultRow>> RunSql(InProcessSchemaCluster.Node node, string sql)
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
}
