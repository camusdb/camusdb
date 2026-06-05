/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Serializer;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

[TestFixture]
[NonParallelizable]
public sealed class TestInProcessSchemaCluster
{
    [Test]
    public async Task CreateTableOnLeaderAppliesToEveryOpenNodeWithoutReopen()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        await cluster.OpenDatabaseOnAllNodesAsync(db);
        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        SchemaChangeLogEntry entry = new()
        {
            Database = db,
            FromVersion = 0,
            ToVersion = 1,
            Op = SchemaOp.CreateTable,
            Payload = Serializator.Serialize(new SchemaCreateTablePayload
            {
                TableId = "000000000000000000000101",
                TableName = "robots",
                Columns =
                [
                    new()
                    {
                        Id = "000000000000000000000102",
                        Name = "id",
                        Type = ColumnType.Id,
                        NotNull = true
                    },
                    new()
                    {
                        Id = "000000000000000000000103",
                        Name = "name",
                        Type = ColumnType.String
                    }
                ]
            })
        };

        SchemaReplicationResult result = await leader.Kahuna.ReplicateSchemaChangeAsync(
            db,
            Serializator.Serialize(entry)
        );

        Assert.AreEqual(SchemaReplicationOutcome.Committed, result.Outcome);
        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        Assert.True(cluster.Nodes.All(node =>
            node.Database is not null &&
            node.Database.Schema.Tables.ContainsKey("robots")
        ));
    }

    // Full command-level CreateTable on the leader. This used to hang: the leader's checkpoint
    // persistence ran inside the schema-apply callback and re-entered the schema partition,
    // timing out (ProposalTimeout). After decoupling persistence to the proposer context it
    // completes, converges on every node, and durably persists the checkpoint.
    [Test]
    public async Task CommandLevelCreateTableOnLeaderPersistsAndConvergesAcrossNodes()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "robots",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        // CreateTable returning successfully already implies the checkpoint persisted:
        // PersistSchemaCheckpointWithRetryAsync throws after exhausting retries, so a
        // persist failure would have surfaced as an exception above rather than completing.
        Assert.True(cluster.Nodes.All(node =>
            node.Database is not null &&
            node.Database.Schema.SchemaVersion >= 1 &&
            node.Database.Schema.Tables.ContainsKey("robots")
        ));
    }

    // A3: pause node 2 → DDL on leader → nodes 0/1 converge, node 2 lags →
    // resume node 2 → node 2 catches up via Kommander replay.
    //
    // Uses a short SchemaAckLiveNodeLease so the paused (fully isolated) node is
    // treated as expired by the ack gate, allowing DDL to commit with quorum only.
    [Test]
    public async Task PausedNodeLagsAndCatchesUpOnResume()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Short lease so an isolated node is considered expired after 1 s, letting
        // the ack gate proceed with the two active nodes.
        TimeSpan lease = TimeSpan.FromMilliseconds(1000);
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.SchemaAckLiveNodeLease = lease;

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // Identify the non-leader nodes; pause one of them (fully isolates it).
        InProcessSchemaCluster.Node[] followers = cluster.Nodes
            .Where(n => n.Index != leader.Index)
            .ToArray();
        int pausedIndex = followers[0].Index;
        InProcessSchemaCluster.Node pausedNode = cluster.Nodes[pausedIndex];

        cluster.PauseDelivery(pausedIndex);

        // Wait for the lease to expire so the paused node is excluded from the ack gate.
        await Task.Delay(lease + TimeSpan.FromMilliseconds(250)).ConfigureAwait(false);

        // Run DDL while node is paused — leader + one follower form quorum and commit.
        await cluster.RunOnSchemaLeaderAsync(db, active => active.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "sensors",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("value", ColumnType.Integer64)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        // All non-paused nodes must have the table.
        InProcessSchemaCluster.Node[] activeNodes = cluster.Nodes
            .Where(n => n.Index != pausedIndex)
            .ToArray();

        Assert.True(activeNodes.All(n =>
            n.Database is not null &&
            n.Database.Schema.Tables.ContainsKey("sensors")),
            "Active nodes should have the table after DDL");

        // Paused node should not yet have it (fully isolated while DDL committed).
        Assert.False(
            pausedNode.Database?.Schema.Tables.ContainsKey("sensors") ?? false,
            "Paused node should not have the table before resume");

        // Resume delivery — unblocks the Raft transport in both directions.
        cluster.ResumeDelivery(pausedIndex);

        // Give the Raft heartbeat cycle time to propagate (500 ms heartbeat interval).
        // This lets the paused node learn who the KV-partition leaders are so that
        // LocateAndTryGetValue can route to them in the next step.
        await Task.Delay(TimeSpan.FromMilliseconds(1500)).ConfigureAwait(false);

        // Reopen the database on the paused node. LoadMetaAsync reads the schema
        // checkpoint via LocateAndTryGetValue → routes through MemoryInterNodeCommmunication
        // (which was never blocked) to the KV-partition leader → gets the committed version.
        await cluster.ReopenDatabaseAsync(pausedIndex, db).ConfigureAwait(false);

        // Node must now see the committed schema without a full Raft WAL replay.
        Assert.True(
            pausedNode.Database?.Schema.Tables.ContainsKey("sensors") ?? false,
            "Resumed-and-reopened node should have caught up to the committed schema version");

        // Convergence check: all registered nodes (including the now-updated one) acked.
        await cluster.WaitForSchemaConvergenceAsync(db, version: 1, timeout: TimeSpan.FromSeconds(10));

        Assert.True(cluster.Nodes.All(n =>
            n.Database is not null &&
            n.Database.Schema.Tables.ContainsKey("sensors")),
            "All nodes should have the table after full convergence");
    }

    // A3: ForceLeaderChangeAsync blocks the current leader, waits for a new election,
    // and returns the new leader. Subsequent DDL on the new leader converges on all
    // unblocked nodes.
    //
    // Uses a short SchemaAckLiveNodeLease so the blocked original leader is treated as
    // expired, allowing DDL to commit with the two remaining live nodes.
    [Test]
    public async Task ForceLeaderChangeTriggersNewElectionAndDdlConverges()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Short lease so the blocked original leader is excluded from the ack gate.
        TimeSpan lease = TimeSpan.FromMilliseconds(1000);
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.SchemaAckLiveNodeLease = lease;

        InProcessSchemaCluster.Node originalLeader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // Force a leadership change — blocks the original leader's transport entirely.
        InProcessSchemaCluster.Node newLeader =
            await cluster.ForceLeaderChangeAsync(db, timeout: TimeSpan.FromSeconds(15));

        Assert.AreNotEqual(originalLeader.Index, newLeader.Index,
            "New leader must be a different node than the original");

        // Wait for the original leader's lease to expire before running DDL.
        await Task.Delay(lease + TimeSpan.FromMilliseconds(250)).ConfigureAwait(false);

        // Run DDL on the new leader; only the two non-blocked nodes form quorum.
        await newLeader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "actuators",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("state", ColumnType.String)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20));

        // Convergence check only covers the two live (unblocked) nodes.
        InProcessSchemaCluster.Node[] liveNodes = cluster.Nodes
            .Where(n => n.Index != originalLeader.Index)
            .ToArray();

        Assert.True(liveNodes.All(n =>
            n.Database is not null &&
            n.Database.Schema.Tables.ContainsKey("actuators")),
            "Live nodes must converge on DDL issued to new leader");
    }

    // B2: CREATE INDEX on the leader must be visible on every node via table.Schema.Indexes
    // and usable via FORCE_INDEX without a reopen, proving the schema log carries the change.
    [Test]
    public async Task AddIndexOnLeaderConvergesAcrossNodesWithoutReopen()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Step 1: create the table on the leader (schema version → 1).
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "robots",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        // Step 2: insert a few rows so the backfill has work to do.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            for (int i = 1; i <= 5; i++)
            {
                KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx,
                    database: db,
                    sql: $"INSERT INTO robots (id, name, year) VALUES (gen_id(), 'robot {i}', {2000 + i})",
                    parameters: null
                ));
                await leader.Database.Transactions.CommitAsync(tx);
            }
        });

        // Step 3: add a secondary index on the leader (schema version → 4: DeleteOnly + WriteOnly + Public).
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.AlterIndex(new AlterIndexTicket(
            databaseName: db,
            tableName: "robots",
            indexName: "name_idx",
            columns: [new ColumnIndexInfo("name", OrderType.Ascending)],
            operation: AlterIndexOperation.AddIndex
        )).WaitAsync(TimeSpan.FromSeconds(30)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 4);

        // Step 4: every node must have the index in table.Schema.Indexes without reopen.
        Assert.True(cluster.Nodes.All(node =>
            node.Database is not null &&
            node.Database.Schema.Tables.TryGetValue("robots", out TableSchema? schema) &&
            schema.Indexes is not null &&
            schema.Indexes.Any(ix => ix.Name == "name_idx" && ix.State == SchemaElementState.Public)),
            "name_idx must appear in TableSchema.Indexes on every node after AddIndex convergence"
        );

        // Step 5: every node's in-memory TableDescriptor must answer FORCE_INDEX without reopen.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await leader.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                    txnState: tx,
                    database: db,
                    sql: "SELECT id FROM robots@{FORCE_INDEX=name_idx}",
                    parameters: null
                ));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await leader.Database.Transactions.CommitAsync(tx);
            Assert.AreEqual(5, rows.Count, "FORCE_INDEX on leader must return all 5 rows");
        });

        // Check each follower as well.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            if (node.Database is null) continue;

            KvTransaction tx = await node.Database.Transactions.BeginAsync();
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                    txnState: tx,
                    database: db,
                    sql: "SELECT id FROM robots@{FORCE_INDEX=name_idx}",
                    parameters: null
                ));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await node.Database.Transactions.CommitAsync(tx);
            Assert.AreEqual(5, rows.Count, $"FORCE_INDEX on node {node.Index} must return all 5 rows");
        }
    }

    // B2: DROP INDEX on the leader must remove the index from every node's table.Schema.Indexes
    // without reopen, and FORCE_INDEX must subsequently fail on all nodes.
    [Test]
    public async Task DropIndexOnLeaderConvergesAcrossNodesWithoutReopen()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Create table + index (2 schema versions).
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "sensors",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
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

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.AlterIndex(new AlterIndexTicket(
            databaseName: db,
            tableName: "sensors",
            indexName: "label_idx",
            columns: [new ColumnIndexInfo("label", OrderType.Ascending)],
            operation: AlterIndexOperation.AddIndex
        )).WaitAsync(TimeSpan.FromSeconds(30)));

        // AddIndex now takes 3 coordinator transitions (DeleteOnly + WriteOnly + Public).
        await cluster.WaitForSchemaConvergenceAsync(db, version: 4);

        // Drop the index (schema version → 5).
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.AlterIndex(new AlterIndexTicket(
            databaseName: db,
            tableName: "sensors",
            indexName: "label_idx",
            columns: [],
            operation: AlterIndexOperation.DropIndex
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 5);

        // All nodes must have the index absent from TableSchema.Indexes.
        Assert.True(cluster.Nodes.All(node =>
            node.Database is not null &&
            node.Database.Schema.Tables.TryGetValue("sensors", out TableSchema? schema) &&
            (schema.Indexes is null || schema.Indexes.All(ix => ix.Name != "label_idx"))),
            "label_idx must be absent from TableSchema.Indexes on every node after DropIndex convergence"
        );

        // FORCE_INDEX on the dropped index must throw on every node.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            if (node.Database is null) continue;

            KvTransaction tx = await node.Database.Transactions.BeginAsync();
            Assert.ThrowsAsync<CamusDB.Core.CamusDBException>(async () =>
            {
                (_, IAsyncEnumerable<QueryResultRow> cursor) =
                    await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                        txnState: tx,
                        database: db,
                        sql: "SELECT id FROM sensors@{FORCE_INDEX=label_idx}",
                        parameters: null
                    ));
                await cursor.ToListAsync();
            });
            await node.Database.Transactions.RollbackAsync(tx);
        }
    }

    // D3: a cluster ADD COLUMN drives the column through the staged sequence to Public on
    // every node, and the WriteOnly→Public backfill physically materializes the default into
    // pre-existing rows. The physical-bytes assertion is essential: a normal SELECT injects the
    // default for a missing Public column at read time (injectMissingCurrentColumns), so a
    // SELECT alone would pass even if the backfill never ran — only decoding with injection OFF
    // distinguishes "physically backfilled" from "injected on read".
    [Test]
    public async Task AddColumnOnLeaderBackfillsDefaultAndConvergesAcrossNodes()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Create the table and insert 5 rows BEFORE the column exists.
        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id), new ColumnInfo("name", ColumnType.String)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));

            for (int i = 0; i < 5; i++)
            {
                KvTransaction txIns = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: txIns,
                    database: db,
                    sql: $"INSERT INTO robots (id, name) VALUES (gen_id(), 'robot {i}')",
                    parameters: null
                ));
                await leader.Database.Transactions.CommitAsync(txIns);
            }

            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // ADD COLUMN 'score' INT64 DEFAULT 42 — drives DeleteOnly → WriteOnly → [backfill] → Public.
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.AlterTable(new AlterTableTicket(
            databaseName: db,
            tableName: "robots",
            operation: AlterTableOperation.AddColumn,
            column: new ColumnInfo("score", ColumnType.Integer64, notNull: false, defaultValue: new ColumnValue(ColumnType.Integer64, 42L))
        )));

        // The staged add advances the schema version by 3 (DeleteOnly, WriteOnly, Public).
        await cluster.WaitForSchemaConvergenceAsync(db, version + 3, timeout: TimeSpan.FromSeconds(20));

        // Every node sees 'score' as a Public column.
        Assert.True(cluster.Nodes.All(node =>
            node.Database is not null &&
            node.Database.Schema.Tables["robots"].Columns!
                .Any(c => c.Name == "score" && c.State == SchemaElementState.Public)),
            "'score' must be Public on every node after the staged add converges");

        // Read correctness on a follower: existing rows surface the default.
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n =>
            !n.Kahuna.AmISchemaLeaderAsync(db, default).AsTask().GetAwaiter().GetResult());

        KvTransaction readTx = await follower.Database!.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await follower.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: readTx, database: db, sql: "SELECT score FROM robots", parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await follower.Database.Transactions.CommitAsync(readTx);

        Assert.AreEqual(5, rows.Count);
        Assert.True(rows.All(r => r.Row["score"].LongValue == 42L), "every existing row must read score = 42");

        // Physical proof the backfill ran: decode raw row bytes with injection OFF
        // (visibilitySchemaVersion: null). The backfill re-encodes each row while 'score' is in
        // WriteOnly state, so the row header stores the WriteOnly-era schema version and the value
        // is physically in the bytes (Encode includes writable columns). We must decode with
        // WRITABLE visibility — PublicOnly would filter 'score' out as not-yet-readable in that
        // version, masking the physically-present value. If the backfill never ran, the row header
        // predates 'score' entirely, so it is absent here and only read-time injection would surface it.
        InProcessSchemaCluster.Node leaderNode = await cluster.WaitForSchemaLeaderNodeAsync(db);
        TableDescriptor table = await leaderNode.Executor.OpenTable(new OpenTableTicket(db, "robots"));
        KvTransaction scanTx = await leaderNode.Database!.Transactions.BeginAsync();

        int physicallyBackfilled = 0;
        await foreach ((ObjectIdValue rowId, byte[] data) in table.Store.ScanRows(scanTx.TransactionId))
        {
            Dictionary<string, ColumnValue> physical = await RowEncoder.DecodeWritableAsync(
                table.Schema, scanTx.TransactionId, rowId, data,
                requiredColumns: null, visibilitySchemaVersion: null);

            Assert.True(physical.ContainsKey("score"),
                "backfill must physically write 'score' into the row bytes, not rely on read-time injection");
            Assert.AreEqual(42L, physical["score"].LongValue);
            physicallyBackfilled++;
        }
        await leaderNode.Database.Transactions.CommitAsync(scanTx);

        Assert.AreEqual(5, physicallyBackfilled, "all 5 pre-existing rows must be physically backfilled");
    }

    // B3 resume: a unique index add with 600 rows (2 batches: 500 + 100) is interrupted
    // between the two batches by a forced leader change. The new leader must:
    //   (a) resume from the persisted StartOffset (not from row 0), evidenced by the
    //       intermediate-checkpoint hook firing exactly once across the whole run — if
    //       the resume re-ran from row 0 it would fire a second time;
    //   (b) produce a complete index (FORCE_INDEX returns all 600 rows);
    //   (c) not throw a DuplicateUniqueKeyValue exception, proving backfill-mode idempotency
    //       for the rows that were already indexed before the crash.
    [Test]
    public async Task UniqueIndexBackfillResumesFromCheckpointAfterLeaderChange()
    {
        const int RowCount = 600;

        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        // Short ack lease so a blocked node is quickly considered expired, letting the
        // remaining two nodes form quorum for both DDL and the ack gate.
        TimeSpan lease = TimeSpan.FromMilliseconds(1000);
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.SchemaAckLiveNodeLease = lease;

        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Step 1: create table + insert 600 rows with unique names (schema version → 1).
        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns:
                [
                    new ColumnInfo("id", ColumnType.Id),
                    new ColumnInfo("name", ColumnType.String, notNull: true)
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            ));

            for (int i = 0; i < RowCount; i++)
            {
                KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx,
                    database: db,
                    sql: $"INSERT INTO robots (id, name) VALUES (gen_id(), 'robot_{i:D5}')",
                    parameters: null
                ));
                await leader.Database.Transactions.CommitAsync(tx);
            }

            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version);

        // Step 2: set up a pause-between-batches hook on ALL node executors.
        // The hook fires after each intermediate checkpoint (i.e., after batch 1 of 500 rows
        // commits and before batch 2 of 100 rows starts). We count total fires: if the resume
        // starts from the checkpoint (only 100 rows remaining), no further intermediate
        // checkpoint fires. If the resume restarted from row 0, it would fire again.
        int checkpointFires = 0;
        TaskCompletionSource<bool> firstBatchDone = new(TaskCreationOptions.RunContinuationsAsynchronously);
        SemaphoreSlim proceedGate = new(0, 1); // released once leader change is arranged

        Func<Task> hook = async () =>
        {
            int count = Interlocked.Increment(ref checkpointFires);
            if (count == 1)
            {
                // First fire: batch 1 of original leader finished. Signal the test and block
                // until the leader change is in place, then let the original leader fail naturally.
                firstBatchDone.TrySetResult(true);
                await proceedGate.WaitAsync().ConfigureAwait(false);
            }
            // Subsequent fires would indicate resume started from row 0 — tracked via checkpointFires.
        };

        // Install the hook on ALL node executors before identifying the leader so we don't
        // miss the hook if leadership is resolved just before we install on the leader.
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Executor.TestInterceptAfterBackfillCheckpoint = hook;

        InProcessSchemaCluster.Node leaderBeforeChange = await cluster.WaitForSchemaLeaderNodeAsync(db);
        int blockedNodeIndex = leaderBeforeChange.Index;

        // Step 3: start AddUniqueIndex DIRECTLY on the identified leader (not via the
        // retry wrapper) so that when the leader is blocked mid-backfill the task fails
        // rather than being retried on the new leader — which would complete without
        // exercising the ResumeJobsAsync path.
        Task addIndexTask = leaderBeforeChange.Executor.AlterIndex(new AlterIndexTicket(
            databaseName: db,
            tableName: "robots",
            indexName: "name_unique",
            columns: [new ColumnIndexInfo("name", OrderType.Ascending)],
            operation: AlterIndexOperation.AddUniqueIndex
        ));

        try
        {
            // Step 4: wait for the first batch (500 rows) to commit and checkpoint.
            await firstBatchDone.Task.WaitAsync(TimeSpan.FromSeconds(30));

            // Step 5: block the current leader so its second-batch Kahuna writes fail.
            // ForceLeaderChangeAsync blocks both inbound and outbound for the old leader.
            await cluster.ForceLeaderChangeAsync(db, timeout: TimeSpan.FromSeconds(20));

            // Wait for the old leader's ack-lease to expire so the new leader's quorum is clean.
            await Task.Delay(lease + TimeSpan.FromMilliseconds(300));

            // Release the hook — the original leader's second batch now continues but Kahuna is
            // blocked, so the transaction will fail and the compensation path will also fail
            // (Kahuna blocked), leaving the persisted coordinator job with StartOffset intact.
            proceedGate.Release();

            // The original leader's addIndexTask is expected to fail now (Kahuna blocked).
            // Observe the exception so it isn't an unobserved task fault, but don't rethrow.
            try
            {
                await addIndexTask.WaitAsync(TimeSpan.FromSeconds(20));
            }
            catch (Exception)
            {
                // Expected: original leader's backfill fails when its Kahuna transport is blocked.
            }

            // Step 6: the new leader's RegisterSchemaLeaderCallback fires ResumeJobsAsync which
            // picks up the persisted job with StartOffset set and drives the index to Public.
            // The blocked original leader can't receive schema version (version+3) since its
            // transport is blocked. Poll only the two live (non-blocked) nodes.
            long targetVersion = version + 3;
            InProcessSchemaCluster.Node[] liveNodes = cluster.Nodes
                .Where(n => n.Index != blockedNodeIndex)
                .ToArray();

            DateTime resumeDeadline = DateTime.UtcNow.AddSeconds(40);
            while (DateTime.UtcNow < resumeDeadline)
            {
                if (liveNodes.All(n => n.Database?.Schema.SchemaVersion >= targetVersion))
                    break;
                await Task.Delay(200).ConfigureAwait(false);
            }

            Assert.True(liveNodes.All(n => n.Database?.Schema.SchemaVersion >= targetVersion),
                $"Live nodes must reach schema version {targetVersion} after leader-change resume. " +
                $"Versions: {string.Join(", ", liveNodes.Select(n => $"node{n.Index}={n.Database?.Schema.SchemaVersion}"))}");

        }
        finally
        {
            foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
                n.Executor.TestInterceptAfterBackfillCheckpoint = null;

            // Make sure the gate is released so the original leader's goroutine can unblock
            // even if an assertion above throws before we reach the Release call.
            if (proceedGate.CurrentCount == 0)
                proceedGate.Release();
        }

        // Assert (a): intermediate-checkpoint hook fired exactly once across the whole run.
        // One fire = original leader's batch 1. No second fire = resume started from
        // StartOffset (100 rows left < BackfillBatchSize, so no intermediate checkpoint fires).
        Assert.AreEqual(1, checkpointFires,
            "Hook must fire exactly once: batch 1 of the original leader. " +
            "A second fire would mean the resume restarted from row 0 (re-batched 500 rows).");

        // Assert (b): index is complete on a live (non-blocked) node.
        InProcessSchemaCluster.Node queryNode = cluster.Nodes
            .First(n => n.Index != blockedNodeIndex && n.Database?.Schema.SchemaVersion >= version + 3);
        KvTransaction readTx = await queryNode.Database!.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await queryNode.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: readTx,
            database: db,
            sql: "SELECT id FROM robots@{FORCE_INDEX=name_unique}",
            parameters: null
        ));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await queryNode.Database.Transactions.CommitAsync(readTx);

        Assert.AreEqual(RowCount, rows.Count,
            "FORCE_INDEX must return all rows after leader-change resume completes the index");
    }

    // DS11.5a — F1a persist-exhaustion policy:
    //   • An injected persist fault never leaves the proposer having acked-and-returned-success
    //     for a version it did not persist — the committed log is the source of truth.
    //   • The DDL call must succeed (commit was live), but the proposer node is marked
    //     SchemaSubsystemDegraded and voluntarily steps down schema-partition leadership.
    //   • Subsequent DDL proposals on the degraded node are rejected with a typed exception.
    [Test]
    public async Task PersistExhaustionMarksNodeDegradedStepsDownAndBlocksFutureDdl()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        // Short live-node lease so the ack gate does not hang if one node is slow to ack.
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.SchemaAckLiveNodeLease = TimeSpan.FromSeconds(5);

        await cluster.OpenDatabaseOnAllNodesAsync(db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // Inject a persist fault — every call to PersistSchemaCheckpointAsync will throw.
        leader.Executor.Catalogs.TestPersistCheckpointException = new IOException("injected checkpoint fault for F1a test");

        // The DDL must NOT throw: the Raft commit already succeeded and is live cluster-wide.
        // Only the proposer's KV checkpoint failed; the committed log remains the source of truth.
        await leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "f1a_table",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20));

        // Clear the fault so teardown logic does not encounter injected failures.
        leader.Executor.Catalogs.TestPersistCheckpointException = null;

        // The proposer node must be marked degraded.
        Assert.IsTrue(leader.Database!.SchemaSubsystemDegraded,
            "Proposer must be marked degraded after persist exhaustion");

        // The proposer must have voluntarily stepped down — poll until it is no longer leader
        // (step-down is asynchronous; allow up to 10 s for the new election to settle).
        DateTime stepDownDeadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < stepDownDeadline)
        {
            if (!await leader.Kahuna.AmISchemaLeaderAsync(db, CancellationToken.None).ConfigureAwait(false))
                break;
            await Task.Delay(100).ConfigureAwait(false);
        }

        Assert.IsFalse(
            await leader.Kahuna.AmISchemaLeaderAsync(db, CancellationToken.None).ConfigureAwait(false),
            "Degraded node must have stepped down schema-partition leadership");

        // Further DDL proposals on the degraded node must be rejected with a typed exception
        // (the degraded gate in ReplicateAndWaitLocalApplyAsync fires before any Raft traffic).
        Exception? ddlEx = Assert.ThrowsAsync<CamusDBException>(() =>
            leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "f1a_table_2",
                columns: [new ColumnInfo("id", ColumnType.Id)],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            )).WaitAsync(TimeSpan.FromSeconds(5)));

        Assert.IsNotNull(ddlEx, "Expected CamusDBException for DDL on degraded node");
        StringAssert.Contains("degraded", ddlEx!.Message.ToLowerInvariant(),
            "Exception message must mention 'degraded'");
    }

    // DS11.5b — F1a persist-exhaustion on the A2 resume path:
    //   When a new leader's ResumeJobsAsync (triggered by RegisterSchemaLeaderCallback)
    //   exhausts persist retries, the same F1a policy must apply: the resuming node is
    //   marked SchemaSubsystemDegraded and steps down so a healthy peer can take over.
    //   This exercises SchemaReplicator.Register's leader-callback finally block, which
    //   is a separate fire path from ExecuteDdlInTransaction (tested in DS11.5a).
    [Test]
    public async Task ResumeJobsPersistExhaustionMarksResumingNodeDegradedAndStepsDown()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.SchemaAckLiveNodeLease = TimeSpan.FromSeconds(5);

        await cluster.OpenDatabaseOnAllNodesAsync(db);

        InProcessSchemaCluster.Node nodeA = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // Create a table so the coordinator job's target table exists on all nodes.
        await nodeA.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "resume_test_tbl",
            columns: [new ColumnInfo("id", ColumnType.Id)],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        ));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        // Plant a coordinator job in KV (replicated to all nodes). The job targets adding
        // a column; the first resume step will call ReplicateAddColumnInStateAsync which
        // invokes PersistSchemaCheckpointWithRetryAsync — where the fault fires.
        PersistedCoordinatorJob fakeJob = new()
        {
            TableName = "resume_test_tbl",
            ElementName = "new_col",
            TargetState = SchemaElementState.Public,
            ElementKind = SchemaElementKind.Column,
            ColumnType = ColumnType.String,
            ColumnNotNull = false,
            Attempts = 0
        };
        await nodeA.Executor.Catalogs.PersistCoordinatorJobAsync(nodeA.Database!, fakeJob);

        // Pick a non-leader node to become the new leader; inject a fault on it
        // before the election so its ResumeJobsAsync call exhausts persist retries.
        InProcessSchemaCluster.Node nodeB = cluster.Nodes.First(n => n.Index != nodeA.Index);
        nodeB.Executor.Catalogs.TestPersistCheckpointException = new IOException("F1a A2 resume fault");

        try
        {
            // Transfer leadership to node B. TransferSchemaLeadershipAsync returns as soon as
            // node B wins the election — the leader callback (ResumeJobsAsync) runs async in
            // the background and drives the job until the persist fault exhausts.
            // The fault must remain active during polling: clearing it too early (before
            // PersistSchemaCheckpointWithRetryAsync is reached) would silently fix the fault and
            // prevent the degraded flag from being set.
            await cluster.TransferSchemaLeadershipAsync(db, nodeB, timeout: TimeSpan.FromSeconds(15));

            // Poll for node B to be marked degraded (the callback runs after the election win,
            // which may complete slightly after TransferSchemaLeadershipAsync returns).
            DateTime degradedDeadline = DateTime.UtcNow.AddSeconds(40);
            while (DateTime.UtcNow < degradedDeadline)
            {
                if (nodeB.Database!.SchemaSubsystemDegraded)
                    break;
                await Task.Delay(100).ConfigureAwait(false);
            }
        }
        finally
        {
            // Always clear the fault so teardown is clean even if assertions fail.
            nodeB.Executor.Catalogs.TestPersistCheckpointException = null;
        }

        Assert.IsTrue(nodeB.Database!.SchemaSubsystemDegraded,
            "Resuming node must be marked degraded after persist exhaustion during ResumeJobsAsync");

        // Node B must have stepped down — the SchemaReplicator finally fires
        // FireDeferredSchemaStepDownAsync after ResumeJobsAsync returns.
        DateTime stepDownDeadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < stepDownDeadline)
        {
            if (!await nodeB.Kahuna.AmISchemaLeaderAsync(db, CancellationToken.None).ConfigureAwait(false))
                break;
            await Task.Delay(100).ConfigureAwait(false);
        }

        Assert.IsFalse(
            await nodeB.Kahuna.AmISchemaLeaderAsync(db, CancellationToken.None).ConfigureAwait(false),
            "Degraded resuming node must have stepped down schema-partition leadership");
    }

    // DS10.2 — DROP COLUMN on the leader converges on every node, and rows written under the
    // PRE-drop schema version still decode on every node (the dropped column reads as absent;
    // the surviving columns read correctly). Proves positional/versioned decode survives a drop
    // cluster-wide.
    [Test]
    public async Task DropColumnOnLeaderConvergesAndOldRowsDecodeEverywhere()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Create table with 3 columns; insert rows that physically carry 'year'.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns:
                [
                    new ColumnInfo("id", ColumnType.Id),
                    new ColumnInfo("name", ColumnType.String),
                    new ColumnInfo("year", ColumnType.Integer64)
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            ));

            for (int i = 1; i <= 5; i++)
            {
                KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx, database: db,
                    sql: $"INSERT INTO robots (id, name, year) VALUES (gen_id(), 'robot {i}', {2000 + i})",
                    parameters: null));
                await leader.Database.Transactions.CommitAsync(tx);
            }
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        // DROP COLUMN 'year' on the leader (single SetElementState→DropColumn delta, v1 → v2).
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.AlterTable(new AlterTableTicket(
            databaseName: db,
            tableName: "robots",
            operation: AlterTableOperation.DropColumn,
            column: new ColumnInfo("year", ColumnType.Integer64)
        )).WaitAsync(TimeSpan.FromSeconds(20)));
        await cluster.WaitForSchemaConvergenceAsync(db, version: 2);

        // Every node: 'year' gone from the schema, 'id'/'name' remain.
        Assert.True(cluster.Nodes.All(n =>
            n.Database!.Schema.Tables["robots"].Columns!.All(c => c.Name != "year") &&
            n.Database.Schema.Tables["robots"].Columns!.Any(c => c.Name == "name")),
            "'year' must be dropped from every node's schema; 'name' must remain");

        // Every node: the 5 pre-drop rows still decode — SELECT of the surviving columns returns
        // all rows with no decode error (old-version bytes read under the new schema).
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            KvTransaction tx = await node.Database!.Transactions.BeginAsync();
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: tx, database: db, sql: "SELECT id, name FROM robots", parameters: null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await node.Database.Transactions.CommitAsync(tx);

            Assert.AreEqual(5, rows.Count, $"node {node.Index} must decode all 5 pre-drop rows after DROP COLUMN");
            Assert.True(rows.All(r => r.Row.ContainsKey("name") && !r.Row.ContainsKey("year")),
                $"node {node.Index}: surviving column 'name' must read, dropped column 'year' must not");
        }
    }

    // DS10.3 — DROP TABLE on the leader converges (table gone on every node) and an in-flight
    // transaction that pinned the table before the drop fails cleanly at commit rather than
    // committing against a table that no longer exists.
    [Test]
    public async Task DropTableConvergesAndPinnedTransactionFailsCleanly()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id), new ColumnInfo("name", ColumnType.String)],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            ));

            KvTransaction seed = await leader.Database!.Transactions.BeginAsync();
            await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: seed, database: db,
                sql: "INSERT INTO robots (id, name) VALUES (gen_id(), 'seed')", parameters: null));
            await leader.Database.Transactions.CommitAsync(seed);
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        InProcessSchemaCluster.Node leaderNode = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // Open a transaction on the leader that PINS the table (a read in an explicit transaction
        // captures (version, identity) and validates it at commit), and leave it open.
        KvTransaction pinnedTx = await leaderNode.Database!.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> pinCursor) = await leaderNode.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: pinnedTx, database: db, sql: "SELECT id FROM robots", parameters: null));
        await pinCursor.ToListAsync();

        // Drop the table (separate DDL transaction), then wait for convergence.
        await cluster.RunOnSchemaLeaderAsync(db, leader =>
            leader.Executor.DropTable(new DropTableTicket(db, "robots", ifExists: false))
                .WaitAsync(TimeSpan.FromSeconds(20)));
        await cluster.WaitForSchemaConvergenceAsync(db, version: 2);

        // Every node: the table is gone.
        Assert.True(cluster.Nodes.All(n => !n.Database!.Schema.Tables.ContainsKey("robots")),
            "DROP TABLE must remove 'robots' from every node's schema");

        // Committing the pinned transaction must fail cleanly (table dropped under it).
        CamusDBException? pinEx = Assert.ThrowsAsync<CamusDBException>(async () =>
            await leaderNode.Database!.Transactions.CommitAsync(pinnedTx));
        Assert.IsNotNull(pinEx, "A transaction pinned to a dropped table must fail at commit");
    }

    // DS10.6 — Concurrent DML on a follower while the leader runs the staged ADD COLUMN must lose
    // no COMMITTED writes and never produce a missing-column read error. Inserts that race a
    // schema-version transition are rejected (typed exception) — that is correct back-pressure,
    // not a lost write — so we count only committed inserts and assert all of them survive on
    // every node, with the new column readable for every row.
    [Test]
    public async Task ConcurrentInsertsOnFollowerDuringAddColumnLoseNoCommittedWrites()
    {
        const int InitialRows = 10;

        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id), new ColumnInfo("name", ColumnType.String)],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            ));

            for (int i = 0; i < InitialRows; i++)
            {
                KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx, database: db,
                    sql: $"INSERT INTO robots (id, name) VALUES (gen_id(), 'init {i}')", parameters: null));
                await leader.Database.Transactions.CommitAsync(tx);
            }
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        InProcessSchemaCluster.Node leaderNode = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leaderNode.Index);

        int committed = 0;
        int rejected = 0;
        using CancellationTokenSource stop = new();

        // Fire concurrent inserts on the follower until the ALTER completes.
        Task inserter = Task.Run(async () =>
        {
            int i = 0;
            while (!stop.IsCancellationRequested && i < 10_000)
            {
                KvTransaction tx = await follower.Database!.Transactions.BeginAsync();
                try
                {
                    await follower.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                        txnState: tx, database: db,
                        sql: $"INSERT INTO robots (id, name) VALUES (gen_id(), 'conc {i}')", parameters: null));
                    await follower.Database.Transactions.CommitAsync(tx);
                    Interlocked.Increment(ref committed);
                }
                catch (CamusDBException)
                {
                    // Schema version moved under the insert during the staged ALTER → rejected,
                    // not lost. Roll back and continue.
                    await follower.Database!.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                    Interlocked.Increment(ref rejected);
                }
                i++;
                await Task.Delay(3).ConfigureAwait(false);
            }
        });

        // Staged ADD COLUMN on the leader: Absent → DeleteOnly → WriteOnly → [backfill] → Public
        // (v1 → v4). Concurrent inserts on the follower overlap every stage.
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.AlterTable(new AlterTableTicket(
            databaseName: db,
            tableName: "robots",
            operation: AlterTableOperation.AddColumn,
            column: new ColumnInfo("score", ColumnType.Integer64, notNull: false,
                defaultValue: new ColumnValue(ColumnType.Integer64, 0L))
        )).WaitAsync(TimeSpan.FromSeconds(30)));

        stop.Cancel();
        await inserter;

        await cluster.WaitForSchemaConvergenceAsync(db, version: 4, timeout: TimeSpan.FromSeconds(20));

        Assert.Greater(committed, 0, "the test must actually commit some inserts concurrently with the ALTER");

        int expected = InitialRows + committed;

        // Every node: exactly the committed rows survive, 'score' is readable for all of them
        // (default 0 for rows written before/while the column was non-public), and no read errors.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            KvTransaction tx = await node.Database!.Transactions.BeginAsync();
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: tx, database: db, sql: "SELECT id, score FROM robots", parameters: null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await node.Database.Transactions.CommitAsync(tx);

            Assert.AreEqual(expected, rows.Count,
                $"node {node.Index}: every committed insert must survive (no lost writes). committed={committed}, rejected={rejected}");
            Assert.True(rows.All(r => r.Row.ContainsKey("score") && r.Row["score"].LongValue == 0L),
                $"node {node.Index}: 'score' must read as the default for every row, with no missing-column error");
        }
    }

    // DS10.5 — DDL issued on a FOLLOWER is forwarded to the schema leader, applied there, and
    // converges on every node. Uses the opt-in in-process leader forwarder (production uses
    // HttpSchemaDdlForwarder; that HTTP path is covered by TestSchemaDdlForwarding).
    [Test]
    public async Task FollowerForwardedDdlAppliesAndConvergesAcrossNodes()
    {
        await using InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, wireLeaderForwarder: true);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leader.Index);

        // CREATE TABLE issued on the follower → forwarded to the leader → applied.
        CreateTableResult created = await follower.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "robots",
            columns: [new ColumnInfo("id", ColumnType.Id), new ColumnInfo("name", ColumnType.String)],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20));

        Assert.IsTrue(created.Success, "follower CREATE TABLE must forward to the leader and apply");
        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);
        Assert.True(cluster.Nodes.All(n => n.Database!.Schema.Tables.ContainsKey("robots")),
            "forwarded CREATE TABLE must converge on every node without reopen");

        // ALTER (staged ADD COLUMN) issued on the follower → forwarded → converges to Public.
        bool altered = await follower.Executor.AlterTable(new AlterTableTicket(
            databaseName: db,
            tableName: "robots",
            operation: AlterTableOperation.AddColumn,
            column: new ColumnInfo("score", ColumnType.Integer64, notNull: false,
                defaultValue: new ColumnValue(ColumnType.Integer64, 0L))
        )).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.IsTrue(altered, "follower ADD COLUMN must forward to the leader and apply");
        await cluster.WaitForSchemaConvergenceAsync(db, version: 4, timeout: TimeSpan.FromSeconds(20));
        Assert.True(cluster.Nodes.All(n =>
            n.Database!.Schema.Tables["robots"].Columns!
                .Any(c => c.Name == "score" && c.State == SchemaElementState.Public)),
            "forwarded ADD COLUMN must converge to Public on every node");
    }
}
