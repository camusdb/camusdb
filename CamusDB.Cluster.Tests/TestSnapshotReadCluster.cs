
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

using Kahuna;
using Kommander;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Verifies that a Serializable read-only transaction provides a consistent point-in-time
/// snapshot across multiple key-range partitions. Tests cover three dimensions:
///
///   1. Single-node, 3-partition (key-range): rows committed before snapshot are visible;
///      rows committed after are invisible.
///   2. 3-node Raft cluster, 3 partitions: same observable outcome, confirming Kahuna's
///      safe-time waiting works across Raft replicas.
///   3. Stable repeated reads: re-running the same SQL SELECT within one snapshot returns
///      the same row set even when a concurrent writer commits between the two reads.
///
/// The single-node and cluster scenarios must produce identical results, confirming
/// that the MVCC snapshot boundary is determined by the timestamp alone, not by node count.
///
/// Coverage notes (not blockers):
///
/// K3 safe-time: Kahuna's prepared-but-uncommitted safe-time wait is not directly exercised
/// here. Every write fully commits before the snapshot is opened; no test races a snapshot read
/// against a write whose commit timestamp straddles T. Covered by Kahuna's TestSnapshotSafeTime;
/// a CamusDB-level test would require prepare/commit injection hooks that don't exist today.
///
/// Single-data-partition scans: all row data for one table hashes to a single Kahuna partition
/// ({tableId}:r), so a SELECT * scan touches only one data partition. Cross-partition 2PC is
/// real (row data and PK-index entries live on different partitions) and directly asserted in
/// SingleNode_SnapshotIndexAndRowAreAtomicallyConsistent, but a scan that fans out across
/// multiple data partitions would require either a two-table join or a key-range-sharded table
/// (where rows are distributed by key value). Optional hardening for a future test.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSnapshotReadCluster
{
    private const int Partitions = 3;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    // -----------------------------------------------------------------------
    // Single-node (3-partition key-range) helpers
    // -----------------------------------------------------------------------

    private static async Task<(EmbeddedKahuna node, string dbname, CommandExecutor executor, DatabaseDescriptor database)>
        CreateSingleNodeSetupAsync()
    {
        EmbeddedKahuna node = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            NodeName = $"snap-test-{Guid.NewGuid():N}",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = Partitions
        });

        await node.StartAsync(CancellationToken.None);

        // Warm up all partition leaders.
        HashSet<int> seen = new();
        for (int i = 0; seen.Count < Partitions && i < 200; i++)
        {
            string key = $"{i}/warmup";
            int p = node.Raft.GetPartitionKey(key);
            if (seen.Add(p))
                await node.WaitForLeaderAsync(key, CancellationToken.None);
        }

        CommandValidator validator = new(CamusDBConfig.Ambient);
        CatalogsManager catalogs = new(logger);
        CommandExecutor executor = new(validator, catalogs, logger, CamusDBConfig.Ambient, sharedNode: node, isClusterMode: true);

        string dbname = Guid.NewGuid().ToString("n");
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "things",
            columns:
            [
                new ColumnInfo("id",    ColumnType.Id),
                new ColumnInfo("label", ColumnType.String, notNull: true)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        ));

        return (node, dbname, executor, database);
    }

    private static async Task InsertRowsAsync(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        int count,
        string prefix)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        for (int i = 0; i < count; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: tx,
                databaseName: dbname,
                tableName: "things",
                values:
                [
                    new Dictionary<string, ColumnValue>
                    {
                        { "id",    new ColumnValue(ColumnType.Id, Core.Util.ObjectIds.ObjectIdGenerator.Generate().ToString()) },
                        { "label", new ColumnValue(ColumnType.String, $"{prefix}-{i}") }
                    }
                ]
            ));
        }

        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> RunSelectAsync(
        CommandExecutor executor,
        string dbname,
        KvTransaction tx)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: "SELECT id, label FROM things", parameters: null));
        return await cursor.ToListAsync();
    }

    // -----------------------------------------------------------------------
    // 1. Single-node: pre-snapshot rows visible, post-snapshot rows invisible
    // -----------------------------------------------------------------------

    [Test]
    public async Task SingleNode_SnapshotSeesOnlyPreSnapshotRows()
    {
        (EmbeddedKahuna node, string dbname, CommandExecutor executor, DatabaseDescriptor database) =
            await CreateSingleNodeSetupAsync();

        await using EmbeddedKahuna _ = node;

        // Insert 5 rows before snapshot.
        await InsertRowsAsync(database, executor, dbname, 5, "before");

        // Open a Serializable read-only snapshot.
        KvTransaction snapshot = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        // Insert 3 more rows after snapshot.
        await InsertRowsAsync(database, executor, dbname, 3, "after");

        // Snapshot must see exactly the 5 pre-snapshot rows.
        List<QueryResultRow> rows = await RunSelectAsync(executor, dbname, snapshot);

        Assert.AreEqual(5, rows.Count,
            "Serializable snapshot must not see rows inserted after the snapshot timestamp");

        Assert.IsTrue(rows.All(r => r.Row["label"].StrValue!.StartsWith("before")),
            "All visible rows must carry the 'before' prefix");
    }

    // -----------------------------------------------------------------------
    // 2. Single-node: stable repeated reads (no non-repeatable read / phantom)
    // -----------------------------------------------------------------------

    [Test]
    public async Task SingleNode_SnapshotReadsAreStableAcrossConcurrentWrites()
    {
        (EmbeddedKahuna node, string dbname, CommandExecutor executor, DatabaseDescriptor database) =
            await CreateSingleNodeSetupAsync();

        await using EmbeddedKahuna _ = node;

        await InsertRowsAsync(database, executor, dbname, 4, "stable");

        KvTransaction snapshot = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        List<QueryResultRow> first = await RunSelectAsync(executor, dbname, snapshot);

        // Commit two more rows between the two reads — must not appear in the second read.
        await InsertRowsAsync(database, executor, dbname, 2, "phantom");

        List<QueryResultRow> second = await RunSelectAsync(executor, dbname, snapshot);

        Assert.AreEqual(first.Count, second.Count,
            "Row count must be identical across two reads within the same snapshot");

        IEnumerable<string> firstLabels  = first.Select(r => r.Row["label"].StrValue!).OrderBy(x => x);
        IEnumerable<string> secondLabels = second.Select(r => r.Row["label"].StrValue!).OrderBy(x => x);

        Assert.IsTrue(firstLabels.SequenceEqual(secondLabels),
            "Exact row set must be identical across two reads within the same snapshot");
    }

    // -----------------------------------------------------------------------
    // 3. 3-node cluster: pre-snapshot rows visible, post-snapshot rows invisible
    // -----------------------------------------------------------------------

    [Test]
    public async Task Cluster_SnapshotSeesOnlyPreSnapshotRows()
    {
        await using InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: Partitions);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Create table on leader.
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "things",
            columns:
            [
                new ColumnInfo("id",    ColumnType.Id),
                new ColumnInfo("label", ColumnType.String, notNull: true)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        // Insert 5 rows before snapshot.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            for (int i = 0; i < 5; i++)
            {
                KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx, database: db,
                    sql: $"INSERT INTO things (id, label) VALUES (gen_id(), 'before-{i}')",
                    parameters: null));
                await leader.Database.Transactions.CommitAsync(tx);
            }
        });

        // Open snapshot on the current leader.
        InProcessSchemaCluster.Node leaderNode = await cluster.WaitForSchemaLeaderNodeAsync(db);
        KvTransaction snapshot = await leaderNode.Database!.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        // Insert 3 more rows after snapshot.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            for (int i = 0; i < 3; i++)
            {
                KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx, database: db,
                    sql: $"INSERT INTO things (id, label) VALUES (gen_id(), 'after-{i}')",
                    parameters: null));
                await leader.Database.Transactions.CommitAsync(tx);
            }
        });

        // Snapshot must see exactly the 5 pre-snapshot rows.
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await leaderNode.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: snapshot, database: db, sql: "SELECT id, label FROM things", parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();

        Assert.AreEqual(5, rows.Count,
            "Cluster snapshot must not see rows inserted after the snapshot timestamp");

        Assert.IsTrue(rows.All(r => r.Row["label"].StrValue!.StartsWith("before")),
            "All visible cluster snapshot rows must carry the 'before' prefix");
    }

    // -----------------------------------------------------------------------
    // 4. 3-node cluster: stable repeated reads within one snapshot
    // -----------------------------------------------------------------------

    [Test]
    public async Task Cluster_SnapshotReadsAreStableAcrossConcurrentWrites()
    {
        await using InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: Partitions);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "things",
            columns:
            [
                new ColumnInfo("id",    ColumnType.Id),
                new ColumnInfo("label", ColumnType.String, notNull: true)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        // Insert 4 seed rows.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            for (int i = 0; i < 4; i++)
            {
                KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx, database: db,
                    sql: $"INSERT INTO things (id, label) VALUES (gen_id(), 'seed-{i}')",
                    parameters: null));
                await leader.Database.Transactions.CommitAsync(tx);
            }
        });

        InProcessSchemaCluster.Node leaderNode = await cluster.WaitForSchemaLeaderNodeAsync(db);
        KvTransaction snapshot = await leaderNode.Database!.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        // First read.
        (_, IAsyncEnumerable<QueryResultRow> c1) = await leaderNode.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: snapshot, database: db, sql: "SELECT id, label FROM things", parameters: null));
        List<QueryResultRow> first = await c1.ToListAsync();

        // Commit 2 phantom rows between the two snapshot reads.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            for (int i = 0; i < 2; i++)
            {
                KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx, database: db,
                    sql: $"INSERT INTO things (id, label) VALUES (gen_id(), 'phantom-{i}')",
                    parameters: null));
                await leader.Database.Transactions.CommitAsync(tx);
            }
        });

        // Second read — must match the first exactly.
        (_, IAsyncEnumerable<QueryResultRow> c2) = await leaderNode.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: snapshot, database: db, sql: "SELECT id, label FROM things", parameters: null));
        List<QueryResultRow> second = await c2.ToListAsync();

        Assert.AreEqual(first.Count, second.Count,
            "Cluster snapshot row count must be identical across two reads within the same snapshot");

        IEnumerable<string> firstLabels  = first.Select(r => r.Row["label"].StrValue!).OrderBy(x => x);
        IEnumerable<string> secondLabels = second.Select(r => r.Row["label"].StrValue!).OrderBy(x => x);

        Assert.IsTrue(firstLabels.SequenceEqual(secondLabels),
            "Cluster snapshot row set must be identical across two reads within the same snapshot");
    }

    // -----------------------------------------------------------------------
    // 5. Single-node ≡ cluster: identical visible count at N=1 and N=3
    // -----------------------------------------------------------------------

    /// <summary>
    /// Inserts the same number of pre- and post-snapshot rows in both a single-node
    /// 3-partition setup and a 3-node cluster with 3 partitions. The count of rows
    /// visible through the snapshot must be the same in both configurations, confirming
    /// that snapshot isolation is determined by the timestamp, not by topology.
    /// </summary>
    [Test]
    public async Task SingleNodeAndCluster_SnapshotVisibleRowCount_IsIdentical()
    {
        const int preCnt  = 6;
        const int postCnt = 4;

        // --- Single-node ---
        (EmbeddedKahuna node, string dbname, CommandExecutor executor, DatabaseDescriptor database) =
            await CreateSingleNodeSetupAsync();
        await using EmbeddedKahuna _ = node;

        await InsertRowsAsync(database, executor, dbname, preCnt, "pre");
        KvTransaction singleSnapshot = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        await InsertRowsAsync(database, executor, dbname, postCnt, "post");

        List<QueryResultRow> singleRows = await RunSelectAsync(executor, dbname, singleSnapshot);
        int singleVisible = singleRows.Count;

        // --- 3-node cluster ---
        await using InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: Partitions);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "things",
            columns:
            [
                new ColumnInfo("id",    ColumnType.Id),
                new ColumnInfo("label", ColumnType.String, notNull: true)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            for (int i = 0; i < preCnt; i++)
            {
                KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx, database: db,
                    sql: $"INSERT INTO things (id, label) VALUES (gen_id(), 'pre-{i}')",
                    parameters: null));
                await leader.Database.Transactions.CommitAsync(tx);
            }
        });

        InProcessSchemaCluster.Node leaderNode = await cluster.WaitForSchemaLeaderNodeAsync(db);
        KvTransaction clusterSnapshot = await leaderNode.Database!.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            for (int i = 0; i < postCnt; i++)
            {
                KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx, database: db,
                    sql: $"INSERT INTO things (id, label) VALUES (gen_id(), 'post-{i}')",
                    parameters: null));
                await leader.Database.Transactions.CommitAsync(tx);
            }
        });

        (DatabaseDescriptor __, IAsyncEnumerable<QueryResultRow> cc) = await leaderNode.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: clusterSnapshot, database: db, sql: "SELECT id, label FROM things", parameters: null));
        List<QueryResultRow> clusterRows = await cc.ToListAsync();
        int clusterVisible = clusterRows.Count;

        Assert.AreEqual(preCnt, singleVisible,
            $"Single-node snapshot must expose exactly {preCnt} pre-snapshot rows");
        Assert.AreEqual(preCnt, clusterVisible,
            $"Cluster snapshot must expose exactly {preCnt} pre-snapshot rows");
        Assert.AreEqual(singleVisible, clusterVisible,
            "Single-node and cluster must report identical visible row counts for the same pre/post split");
    }

    // -----------------------------------------------------------------------
    // 6. Cross-partition atomicity: index and row are consistent through snapshot
    // -----------------------------------------------------------------------

    /// <summary>
    /// Directly asserts that the index entry and the row data are atomically
    /// visible or invisible through a snapshot. Each INSERT in a 3-partition node
    /// is a cross-partition 2PC: the row key ({tableId}:r/…) and the PK-index key
    /// ({tableId}:i:~pk/…) hash to different partitions. If a half-applied write
    /// leaked through, the index lookup and the GetRow call would disagree.
    ///
    /// For a pre-snapshot row:
    ///   LookupUnique(snapshot) → non-null rowId  AND  GetRow(snapshot, rowId) → non-null bytes
    /// For a post-snapshot row (rowId obtained via a regular read before assertion):
    ///   LookupUnique(snapshot) → null            AND  GetRow(snapshot, rowId) → null
    /// </summary>
    [Test]
    public async Task SingleNode_SnapshotIndexAndRowAreAtomicallyConsistent()
    {
        (EmbeddedKahuna node, string dbname, CommandExecutor executor, DatabaseDescriptor database) =
            await CreateSingleNodeSetupAsync();

        await using EmbeddedKahuna _ = node;

        // Insert 3 rows before snapshot, capture their ids.
        await InsertRowsAsync(database, executor, dbname, 3, "before");

        KvTransaction collectBefore = await database.Transactions.BeginAsync();
        List<QueryResultRow> beforeRows = await RunSelectAsync(executor, dbname, collectBefore);
        await database.Transactions.CommitAsync(collectBefore);
        string beforeIdStr = beforeRows.First(r => r.Row["label"].StrValue!.StartsWith("before")).Row["id"].StrValue!;

        // Open snapshot.
        KvTransaction snapshot = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        // Insert 2 rows after snapshot.
        await InsertRowsAsync(database, executor, dbname, 2, "after");

        // Collect the after-row id via a regular (non-snapshot) read.
        KvTransaction collectAfter = await database.Transactions.BeginAsync();
        List<QueryResultRow> allRows = await RunSelectAsync(executor, dbname, collectAfter);
        await database.Transactions.CommitAsync(collectAfter);
        string afterIdStr = allRows.First(r => r.Row["label"].StrValue!.StartsWith("after")).Row["id"].StrValue!;

        // Open the table descriptor to access the PK index store directly.
        // The index name "~pk" is the KV key-space prefix — in-memory TableDescriptor.Indexes
        // entries intentionally have Id=null; callers always use the name.
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "things"));
        const string pkIndexName = "~pk";

        // Obtain the after-row's physical rowId via a non-snapshot lookup so we can
        // pass it to GetRow independently of the index.
        KvTransaction lookupTx = await database.Transactions.BeginAsync();
        ObjectIdValue? afterRowIdNullable = await table.Store.LookupUnique(
            lookupTx,
            pkIndexName,
            new CompositeColumnValue(new ColumnValue(ColumnType.Id, afterIdStr)));
        await database.Transactions.CommitAsync(lookupTx);

        Assert.IsNotNull(afterRowIdNullable,
            "After row must be findable via a regular non-snapshot lookup (sanity check)");

        ObjectIdValue afterRowId = afterRowIdNullable!.Value;

        // --- Assert through the snapshot ---

        // Pre-snapshot row: index entry visible → row data visible.
        ObjectIdValue? snapBeforeIndexResult = await table.Store.LookupUnique(
            snapshot,
            pkIndexName,
            new CompositeColumnValue(new ColumnValue(ColumnType.Id, beforeIdStr)));

        Assert.IsNotNull(snapBeforeIndexResult,
            "Pre-snapshot row's PK-index entry must be visible through the snapshot");

        ReadOnlyMemory<byte>? snapBeforeRowBytes = await table.Store.GetRow(snapshot, snapBeforeIndexResult!.Value);

        Assert.IsNotNull(snapBeforeRowBytes,
            "Pre-snapshot row data must be visible through the snapshot when the index entry is visible");

        // Post-snapshot row: index entry invisible → row data invisible even when rowId is known.
        ObjectIdValue? snapAfterIndexResult = await table.Store.LookupUnique(
            snapshot,
            pkIndexName,
            new CompositeColumnValue(new ColumnValue(ColumnType.Id, afterIdStr)));

        Assert.IsNull(snapAfterIndexResult,
            "Post-snapshot row's PK-index entry must be invisible through the snapshot");

        ReadOnlyMemory<byte>? snapAfterRowBytes = await table.Store.GetRow(snapshot, afterRowId);

        Assert.IsNull(snapAfterRowBytes,
            "Post-snapshot row data must be invisible through the snapshot even when the physical rowId is known externally");
    }
}
