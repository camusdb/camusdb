
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Exact nearest-neighbour search over rows that genuinely live on more than one node.
///
/// <para>Ranking happens at the coordinator, above the gather, so the danger is not an error but a
/// short answer: if a span's rows never arrive, the query still returns a confidently ordered list —
/// of the wrong rows. Each case therefore asserts <em>which</em> rows come back, and asks every node
/// in turn so a coordinator that only ranks its own spans is caught.</para>
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterVectorSearch
{
    private const int Partitions = 2;
    private const int RowCount = 60;
    private const int Dimensions = 4;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static ColumnValue Pack(params float[] elements)
    {
        byte[] bytes = new byte[elements.Length * 4];

        for (int i = 0; i < elements.Length; i++)
            BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(i * 4, 4), elements[i]);

        return new ColumnValue(bytes);
    }

    /// <summary>
    /// Row i sits at distance i from the query vector along the first axis, so the expected answer
    /// is simply the lowest-numbered rows — computed from the seed, never from a second query.
    /// </summary>
    private static ColumnValue EmbeddingFor(int i) => Pack(i, 0f, 0f, 0f);

    private static Dictionary<string, ColumnValue> Query() =>
        new() { { "@q", Pack(0f, 0f, 0f, 0f) } };

    private static async Task<(InProcessSchemaCluster cluster, string db)> SetupAsync()
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
            tableName: "docs",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("rank", ColumnType.Integer64, notNull: true),
                new ColumnInfo("embedding", ColumnType.Bytes, notNull: false, maxLength: Dimensions * 4),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        InProcessSchemaCluster.Node writer = cluster.Nodes[0];
        KvTransaction tx = await writer.Database!.Transactions.BeginAsync();

        for (int i = 0; i < RowCount; i++)
        {
            await writer.Executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "docs",
                values: new() { new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "rank", new(ColumnType.Integer64, (long)i) },
                    { "embedding", EmbeddingFor(i) },
                }}));
        }

        await writer.Database.Transactions.CommitAsync(tx);

        return (cluster, db);
    }

    private static async Task<List<long>> RunSql(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: Query()));

            List<QueryResultRow> rows = await cursor.ToListAsync();
            await node.Database.Transactions.CommitAsync(tx);
            return rows.Select(r => r.Row["rank"].LongValue).ToList();
        }
        catch
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    private const string NearestSql =
        "SELECT rank FROM docs ORDER BY l2_distance(embedding, @q) LIMIT 5";

    [Test]
    public async Task EveryNodeRanksTheWholeTableNotOnlyItsOwnSpans()
    {
        (InProcessSchemaCluster cluster, string db) = await SetupAsync();

        try
        {
            long[] expected = [0, 1, 2, 3, 4];

            for (int i = 0; i < cluster.Nodes.Length; i++)
            {
                List<long> ranks = await RunSql(cluster.Nodes[i], db, NearestSql);

                CollectionAssert.AreEqual(expected, ranks,
                    $"node {i} must rank rows from every span, not just the ones it leads");
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task UnboundedRankingAgreesWithTheBoundedAnswer()
    {
        // The bounded and unbounded paths take different operators to the same question. Across a
        // gather they also consume the spans differently, so agreement here is worth asserting.
        (InProcessSchemaCluster cluster, string db) = await SetupAsync();

        try
        {
            InProcessSchemaCluster.Node node = cluster.Nodes[1];

            List<long> bounded = await RunSql(node, db, NearestSql);
            List<long> full = await RunSql(node, db,
                "SELECT rank FROM docs ORDER BY l2_distance(embedding, @q)");

            Assert.AreEqual(RowCount, full.Count, "the unbounded query must rank every row");
            CollectionAssert.AreEqual(full.Take(5).ToArray(), bounded);
            CollectionAssert.AreEqual(Enumerable.Range(0, RowCount).Select(i => (long)i).ToArray(), full);
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
