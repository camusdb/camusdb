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
/// jsonb across nodes. A table with jsonb columns is created on the schema leader, written on one node
/// and read on every node, after a split that gives its keyspace two spans. A full scan ships the raw
/// row bytes of the remote span, so the cell decodes on another node; a GROUP BY on a jsonb column
/// ships one partial row per group per remote span, whose group keys are documents that cross the
/// value wire codec in both fragment encodings. The documents hold numbers past 2⁶⁴ and number
/// scales (1.0 and 1 are one group), so a codec that rounds or compares bytes fails.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterJsonbGather
{
    private const int Partitions = 2;
    private const int RowCount = 200;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    /// <summary>Row i: a document with a wide number, and a group key whose spelling alternates 1 and 1.0.</summary>
    private static string DocumentOf(int i) =>
        $"{{\"kind\": \"k{i % 4}\", \"n\": 1234567890123456789012345678{i:D2}.50, \"tags\": [\"t{i % 3}\"]}}";

    private static string KindOf(int i) => (i % 4) switch
    {
        0 => "1",
        1 => "1.0",
        2 => "[\"x\"]",
        _ => "{\"a\": null}",
    };

    /// <summary>
    /// Splits the row keyspace at the median row id and waits until every node sees two spans. Only
    /// the range-map meta-partition leader commits the split; the other nodes refuse it.
    /// </summary>
    private static async Task SplitAtMedianAsync(InProcessSchemaCluster cluster, IEnumerable<string> ids, string tableName = "ledger")
    {
        TableDescriptor table = await cluster.Nodes[0].Database!.TableDescriptors[tableName];
        string keySpace = table.Store.RowKeySpace;

        List<string> sorted = ids.OrderBy(x => x, StringComparer.Ordinal).ToList();
        string splitKey = table.Store.RowPointKey(ObjectId.ToValue(sorted[sorted.Count / 2]));

        // The range-map leader can still be in election right after setup, when every node answers
        // NotLeader, so the split is retried for a few seconds. The statuses go in the failure message.
        bool split = false;
        List<string> statuses = [];
        for (int attempt = 0; attempt < 20 && !split; attempt++)
        {
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                KahunaSplitRangeResponse response =
                    await node.Kahuna.Kahuna.SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);

                statuses.Add($"attempt {attempt}, node {node.Index}: {response.Status}");
                if (response.Success)
                {
                    split = true;
                    break;
                }
            }

            if (!split)
                await Task.Delay(250);
        }

        Assert.IsTrue(split, "One node (the meta-partition leader) must commit the split: " + string.Join("; ", statuses.TakeLast(6)));

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TablePlacement? placement = null;

            for (int attempt = 0; attempt < 50; attempt++)
            {
                node.Kahuna.InvalidatePlacement(keySpace);
                placement = node.Kahuna.GetPlacement(keySpace);
                if (placement.IsKeyRange && placement.Spans.Count >= 2)
                    break;

                await Task.Delay(200);
            }

            Assert.IsTrue(placement!.IsKeyRange && placement.Spans.Count >= 2,
                "Every node's placement must report the split spans");
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

    private static async Task DdlOnLeaderAsync(InProcessSchemaCluster cluster, string db, string sql)
    {
        long version = 0;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null))
                .WaitAsync(TimeSpan.FromSeconds(60));
            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(60));
    }

    private static async Task ExecAsync(InProcessSchemaCluster.Node node, string db, string sql)
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

    /// <summary>
    /// A GIN index added by CREATE INDEX on the schema leader over rows that already exist: the
    /// replicated add backfills the entries (the cluster backfill path, not the standalone one), and
    /// every node then plans the GIN scan and returns the rows a write on another node added, before
    /// and after a reopen.
    /// </summary>
    [TestCase(true, TestName = "GinIndex_BackfilledOnTheLeader_ServesEveryNode_KeyRange")]
    [TestCase(false, TestName = "GinIndex_BackfilledOnTheLeader_ServesEveryNode_Hash")]
    public async Task GinIndex_BackfilledOnTheLeader_ServesEveryNode(bool keyRangeSharding)
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: Partitions,
                loggerFactory: sharedLoggerFactory, logger: logger,
                options: CamusDBOptions.Default with { KeyRangeShardingEnabled = keyRangeSharding });

        try
        {
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            await DdlOnLeaderAsync(cluster, db, "CREATE TABLE events (id INT64 PRIMARY KEY, doc JSONB)");

            string values = string.Join(", ", Enumerable.Range(0, RowCount).Select(i => $"({i}, '{DocumentOf(i)}')"));
            await ExecAsync(cluster.Nodes[0], db, "INSERT INTO events (id, doc) VALUES " + values);

            await DdlOnLeaderAsync(cluster, db, "CREATE INDEX events_doc ON events USING GIN (doc)");

            // A write after the build, on another node, adds its entries through the insert path.
            await ExecAsync(cluster.Nodes[1], db, "INSERT INTO events (id, doc) VALUES (1000, '{\"kind\": \"k1\", \"tags\": [\"t2\"]}')");

            const string Where = "doc @> '{\"kind\": \"k1\"}' AND doc @> '{\"tags\": [\"t2\"]}'";
            long expected = Enumerable.Range(0, RowCount).Count(i => i % 4 == 1 && i % 3 == 2) + 1;

            async Task AssertEveryNodeAsync(string when)
            {
                foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                {
                    List<QueryResultRow> plan = await RunSql(node, db, "EXPLAIN SELECT id FROM events WHERE " + Where);
                    Assert.IsTrue(plan.Any(r => r.Row["node"].StrValue == "gin-scan" && r.Row["detail"].StrValue!.Contains("index=events_doc")),
                        $"Node {node.Index} {when}: the plan uses the GIN index");

                    List<QueryResultRow> rows = await RunSql(node, db, "SELECT id FROM events WHERE " + Where);
                    Assert.AreEqual(expected, rows.Count, $"Node {node.Index} {when}: matching rows");
                    Assert.IsTrue(rows.Any(r => r.Row["id"].LongValue == 1000), $"Node {node.Index} {when}: the row written after the build");
                }
            }

            await AssertEveryNodeAsync("after the build");

            for (int i = 0; i < cluster.Nodes.Length; i++)
                await cluster.ReopenDatabaseAsync(i, db);

            await AssertEveryNodeAsync("after reopen");
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task JsonbColumns_CrossNodesAndFragments_Exactly()
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: Partitions,
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

            await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "events",
                columns:
                [
                    new ColumnInfo("id",   ColumnType.Id),
                    new ColumnInfo("doc",  ColumnType.Jsonb, notNull: true),
                    new ColumnInfo("kind", ColumnType.Jsonb, notNull: true),
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            )).WaitAsync(TimeSpan.FromSeconds(20)));

            await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

            // Each id is created just before its row, so the ids interleave with the KV row ids and
            // half of the rows land below the median split key.
            Dictionary<string, string> documents = new(RowCount);
            InProcessSchemaCluster.Node writer = cluster.Nodes[0];
            KvTransaction tx = await writer.Database!.Transactions.BeginAsync();
            for (int i = 0; i < RowCount; i++)
            {
                string id = ObjectIdGenerator.Generate().ToString();
                ColumnValue doc = ColumnValue.FromJsonText(DocumentOf(i));
                documents[id] = doc.JsonValue!;
                await writer.Executor.Insert(new InsertTicket(
                    txnState: tx, databaseName: db, tableName: "events",
                    values: new() { new() {
                        { "id",   new(ColumnType.Id, id) },
                        { "doc",  doc },
                        { "kind", ColumnValue.FromJsonText(KindOf(i)) },
                    }}));
            }
            await writer.Database.Transactions.CommitAsync(tx);

            await SplitAtMedianAsync(cluster, documents.Keys, "events");

            cluster.FragmentTransport!.ResetExecuted();

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                // Rows: the remote span's row bytes decode here, with the exact digits.
                List<QueryResultRow> rows = await RunSql(node, db, "SELECT id, doc FROM events");
                Assert.AreEqual(RowCount, rows.Count, $"Node {node.Index}: every row once");
                foreach (QueryResultRow row in rows)
                {
                    Assert.AreEqual(ColumnType.Jsonb, row.Row["doc"].Type, $"Node {node.Index}: type");
                    Assert.AreEqual(documents[row.Row["id"].StrValue!], row.Row["doc"].JsonValue, $"Node {node.Index}: value");
                }

                // Partial aggregates keyed by a document: 1 and 1.0 are one group on every span.
                List<QueryResultRow> grouped = await RunSql(node, db, "SELECT kind, COUNT(*) AS c FROM events GROUP BY kind");
                Assert.AreEqual(3, grouped.Count, $"Node {node.Index}: three distinct documents");
                Assert.AreEqual((long)RowCount / 2, grouped.Single(g => g.Row["kind"].JsonValue is "1" or "1.0").Row["c"].LongValue,
                    $"Node {node.Index}: 1 and 1.0 are one group");
                Assert.AreEqual((long)RowCount / 4, grouped.Single(g => g.Row["kind"].JsonValue == "[\"x\"]").Row["c"].LongValue);

                // Operators in a statement that reads both spans.
                List<QueryResultRow> filtered = await RunSql(node, db,
                    "SELECT COUNT(*) AS c FROM events WHERE doc @> '{\"kind\": \"k1\"}' AND doc -> 'tags' ? 't2'");
                long expected = Enumerable.Range(0, RowCount).Count(i => i % 4 == 1 && i % 3 == 2);
                Assert.AreEqual(expected, filtered.Single().Row["c"].LongValue, $"Node {node.Index}: containment and existence");

                List<QueryResultRow> extracted = await RunSql(node, db,
                    "SELECT doc ->> 'kind' AS k, COUNT(*) AS c FROM events GROUP BY doc ->> 'kind'");
                Assert.AreEqual(4, extracted.Count, $"Node {node.Index}: four kinds");
            }

            // With two spans on three nodes, some coordinator reads a span another node serves.
            Assert.Greater(cluster.FragmentTransport.ExecutedCount, 0, "a remote fragment must run");

            // Reopen: every node reloads the schema and still reads the columns as jsonb.
            for (int i = 0; i < cluster.Nodes.Length; i++)
                await cluster.ReopenDatabaseAsync(i, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                TableColumnSchema column = node.Database!.Schema.Tables["events"].Columns!.Single(c => c.Name == "doc");
                Assert.AreEqual(ColumnType.Jsonb, column.Type, $"Node {node.Index}: column type after reopen");

                List<QueryResultRow> rows = await RunSql(node, db, "SELECT id, doc FROM events");
                Assert.AreEqual(RowCount, rows.Count, $"Node {node.Index}: rows after reopen");
                foreach (QueryResultRow row in rows)
                    Assert.AreEqual(documents[row.Row["id"].StrValue!], row.Row["doc"].JsonValue, $"Node {node.Index}: value after reopen");
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
