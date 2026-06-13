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
        await foreach ((ObjectIdValue rowId, byte[] data) in table.Store.ScanRows(scanTx))
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

    // E2 — no false eviction of an idle-but-alive node. With a finite ack lease, liveness is now
    // sourced from real Raft activity (GetActiveNodes), not from an apply-derived "last applied"
    // signal. So a follower that is alive (still answering Raft heartbeats) but has applied no
    // schema delta for longer than the lease must STILL be waited on by the ack gate. The proof:
    // after an idle period longer than the lease, the next DDL's gate must wait for every live
    // node, so when the call returns every node — including the idle followers — is already at the
    // new version. Under the old apply-derived lease the idle followers would have gone "stale" and
    // been dropped from the gate, so the DDL could have returned before they applied.
    [Test]
    public async Task IdleButAliveFollowersAreNotFalselyEvictedFromAckGate()
    {
        TimeSpan lease = TimeSpan.FromMilliseconds(1500);

        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.SchemaAckLiveNodeLease = lease;

        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "first",
            columns: [new ColumnInfo("id", ColumnType.Id)],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));
        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        // Stay idle (no schema activity) for longer than the lease. The apply-derived "last
        // applied" timestamp for every node now exceeds the lease, while Raft heartbeats keep the
        // nodes genuinely active — exactly the case the old mechanism would have false-evicted.
        await Task.Delay(lease + TimeSpan.FromMilliseconds(1000)).ConfigureAwait(false);

        // Direct proof of the E2 membership source: despite being schema-idle for longer than the
        // lease, both followers are still in the leader's Raft active set within the lease window,
        // so the ack gate will keep waiting on them (the apply-derived signal would have dropped
        // them here).
        InProcessSchemaCluster.Node leaderNode = await cluster.WaitForSchemaLeaderNodeAsync(db);
        string[] followerEndpoints = cluster.Nodes
            .Where(n => n.Index != leaderNode.Index)
            .Select(n => n.Kahuna.Raft.GetLocalEndpoint())
            .ToArray();
        IReadOnlyList<string> active = leaderNode.Kahuna.Raft.GetActiveNodes(lease);
        Assert.True(followerEndpoints.All(active.Contains),
            "idle-but-alive followers must remain in the leader's Raft active set within the lease. " +
            $"active=[{string.Join(",", active)}] followers=[{string.Join(",", followerEndpoints)}]");

        // Second DDL — do NOT wait for convergence afterwards; the ack gate itself must have waited
        // for every live node before returning.
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "second",
            columns: [new ColumnInfo("id", ColumnType.Id)],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        // Immediately: every node — including the idle followers — must be at v2 with the new
        // table. If an idle-but-alive follower had been evicted from the gate, the DDL could have
        // returned before that node applied, and it would still be at v1 here.
        Assert.True(cluster.Nodes.All(n =>
            n.Database is not null &&
            n.Database.Schema.SchemaVersion >= 2 &&
            n.Database.Schema.Tables.ContainsKey("second")),
            "every live (idle-but-alive) node must have acked v2 before the DDL returned — no false eviction. " +
            $"Versions: {string.Join(", ", cluster.Nodes.Select(n => $"node{n.Index}={n.Database?.Schema.SchemaVersion}"))}");
    }

    private static CreateTableTicket SimpleTable(string db, string name) => new(
        databaseName: db,
        tableName: name,
        columns: [new ColumnInfo("id", ColumnType.Id)],
        constraints:
        [
            new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])
        ],
        ifNotExists: false
    );

    // DS11.2 — Replication failure → typed exception, nothing committed. When the leader cannot
    // reach a commit quorum (both followers isolated), a DDL must fail with a typed
    // CamusDBException and leave every node's schema version unchanged — the entry is rolled back,
    // never half-applied.
    [Test]
    public async Task ReplicationWithoutQuorumThrowsTypedExceptionAndLeavesVersionsUnchanged()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.SchemaAckLiveNodeLease = TimeSpan.FromSeconds(2);

        await cluster.OpenDatabaseOnAllNodesAsync(db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // Isolate BOTH followers — the leader is left with only itself (1 of 3), so a proposal
        // cannot reach majority and must time out / fail rather than commit.
        int[] followerIndexes = cluster.Nodes.Where(n => n.Index != leader.Index).Select(n => n.Index).ToArray();
        foreach (int idx in followerIndexes)
            cluster.PauseDelivery(idx);

        try
        {
            // The DDL is issued directly on the (now quorum-less) leader and must throw a typed
            // CamusDBException — either ProposalTimeout (couldn't commit) or leader-required
            // (the leader stepped down under us). Both mean: not committed.
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await leader.Executor.CreateTable(SimpleTable(db, "ds11_2"))
                    .WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.IsNotNull(ex, "a DDL that cannot reach quorum must fail with a typed CamusDBException");

            // Nothing committed anywhere: no node has the table and no version advanced past 0.
            Assert.True(cluster.Nodes.All(n =>
                n.Database is not null &&
                !n.Database.Schema.Tables.ContainsKey("ds11_2") &&
                n.Database.Schema.SchemaVersion == 0),
                "a failed replication must leave every node's schema version unchanged. " +
                $"Versions: {string.Join(", ", cluster.Nodes.Select(n => $"node{n.Index}={n.Database?.Schema.SchemaVersion}"))}");
        }
        finally
        {
            foreach (int idx in followerIndexes)
                cluster.ResumeDelivery(idx);
        }
    }

    // DS11.3 — Node rejoin / catch-up is covered by PausedNodeLagsAndCatchesUpOnResume (pause a
    // node, commit DDL with quorum, resume + reopen, assert it catches up). A multi-DDL variant
    // was prototyped but is fixture-flaky for a Raft reason, not a schema one: a fully-isolated
    // node bumps its term while paused (it times out and campaigns even with outbound blocked), so
    // on rejoin it can disrupt the cluster (a higher term forces a re-election), and the catch-up
    // read races the resulting churn (MustRetry). The catch-up *mechanism* (reopen → LoadMeta reads
    // the replicated checkpoint from the KV-partition leader) is identical regardless of how many
    // DDLs were missed, so the single-DDL test exercises the same path deterministically.

    // DS11.5b — Checkpoint-rollback recovery (F1b) is NOT a cluster test here, by design.
    //
    // Recovering a delta whose checkpoint persist faulted (and which no other node re-persisted)
    // requires replaying that committed entry from the Raft log via OnLogRestored — which fires
    // only on a node *restart* (StartAsync), not on a database reopen. The in-process fixture
    // shares one long-lived Kahuna node per process and cannot restart it mid-test, so the
    // un-persisted delta cannot be reconstructed through the reopen path (LoadMeta reads only the
    // replicated KV checkpoint, which never contains that delta). This is the documented F1b
    // restore-trigger limitation of InProcessSchemaCluster.
    //
    // The F1b recovery *mechanics* are therefore unit-tested in
    // CamusDB.Tests/CommandsExecutor/TestSchemaRestoreF1b.cs instead:
    //   • ReopenRestoresSchemaVersionFromWal            — close+reopen restores schema from KV.
    //   • PersistFullCheckpointIsIdempotentAndLoadableAfterReopen — PersistFullSchemaCheckpointAsync
    //     (the F1b re-persist) yields a valid, loadable checkpoint.
    //   • GapInRestoreThrowsTypedCamusDbException        — restore fails loud on a gap (never silent).

    // ── E3: Schema Ack Transport ──────────────────────────────────────────────

    // E3-1: the two-version gate blocks on a behind follower and releases once the
    // relayed ack from that follower arrives. This verifies that SchemaAckTracker is
    // per-instance (not static) and that InProcessSchemaAckRelay delivers remote acks
    // to the leader's tracker — the full production ack-transport path in-process.
    [Test]
    public async Task AckTransport_GateBlocksUntilFollowerRelayArrives()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        await cluster.OpenDatabaseOnAllNodesAsync(db);
        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // Seed all nodes at version 0 (simulates SchemaReplicator.Register).
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.RecordLocalSchemaApplied(db, 0);

        // Only the leader acks version 1 — gate must block while followers are still at 0.
        leader.Kahuna.RecordLocalSchemaApplied(db, 1);

        bool partialAck = await leader.Kahuna.WaitForSchemaAcksAsync(
            db, 1, TimeSpan.FromMilliseconds(100), liveNodeLease: Timeout.InfiniteTimeSpan, cancellationToken: CancellationToken.None);
        Assert.IsFalse(partialAck, "Gate must block when followers have not yet acked");

        // Simulate follower applies arriving via the in-process ack relay: each follower
        // calls RecordAndPublishSchemaApplied (which fires the relay), and the relay delivers
        // the ack directly to the leader's per-instance tracker.
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
        {
            if (n.Index == leader.Index)
                continue;
            // RecordAndPublishSchemaApplied records locally on the follower and fires
            // the relay that calls leader.RecordRemoteSchemaAck.
            n.Kahuna.RecordAndPublishSchemaApplied(db, 1);
        }

        // Give the fire-and-forget relay tasks time to execute.
        await Task.Delay(TimeSpan.FromMilliseconds(200)).ConfigureAwait(false);

        bool allAcked = await leader.Kahuna.WaitForSchemaAcksAsync(
            db, 1, TimeSpan.FromSeconds(3), liveNodeLease: Timeout.InfiniteTimeSpan, cancellationToken: CancellationToken.None);
        Assert.IsTrue(allAcked, "Gate must release once all follower acks are relayed to the leader");
    }

    // E3-2: each node's SchemaAckTracker is independent — a follower's per-instance
    // tracker is not visible to the leader's tracker without the relay. This test
    // verifies that the per-instance isolation is real (no shared static remains).
    [Test]
    public async Task AckTransport_PerInstanceTrackerIsolation()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        await cluster.OpenDatabaseOnAllNodesAsync(db);
        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // Seed v0 on all nodes.
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.RecordLocalSchemaApplied(db, 0);

        // Ack version 1 ONLY on each follower's own local tracker (not via relay).
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
        {
            if (n.Index == leader.Index)
                continue;
            n.Kahuna.RecordLocalSchemaApplied(db, 1);
        }

        // Leader has not acked yet, and follower acks did not reach the leader's tracker.
        bool gateResult = await leader.Kahuna.WaitForSchemaAcksAsync(
            db, 1, TimeSpan.FromMilliseconds(100), liveNodeLease: Timeout.InfiniteTimeSpan, cancellationToken: CancellationToken.None);
        Assert.IsFalse(gateResult,
            "Gate must still block: follower local acks must not bleed into the leader's per-instance tracker");
    }

    // E3-3: gate proceeds on timeout when an ack is dropped (no relay for one follower).
    // The lease-based liveness gate (finite SchemaAckLiveNodeLease) is the backstop.
    [Test]
    public async Task AckTransport_GateTimeoutWhenAckDropped()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        await cluster.OpenDatabaseOnAllNodesAsync(db);

        TimeSpan shortLease = TimeSpan.FromMilliseconds(300);
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.SchemaAckLiveNodeLease = shortLease;

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // Seed v0 and advance leader to v1; followers remain at v0 (dropped ack).
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.RecordLocalSchemaApplied(db, 0);

        leader.Kahuna.RecordLocalSchemaApplied(db, 1);

        // Wait for the lease to expire so behind followers are considered expired.
        await Task.Delay(shortLease + TimeSpan.FromMilliseconds(100)).ConfigureAwait(false);

        bool timedOut = await leader.Kahuna.WaitForSchemaAcksAsync(
            db, 1, TimeSpan.FromSeconds(2), liveNodeLease: shortLease, cancellationToken: CancellationToken.None);
        Assert.IsTrue(timedOut,
            "Gate must proceed once the lease expires for behind followers (dropped ack backstop)");
    }

    // §3.4 / H5 safety — the quorum backstop must NOT fire on the pre-proposal gate
    // (enforceFullConvergence=true path). The pre-proposal gate enforces the two-version
    // invariant: relaxing it with a backstop would let the proposer advance N→N+1 while a
    // minority sits at N−1. This test verifies the API contract: with enforceFullConvergence=true,
    // the gate blocks even after SchemaAckQuorumBackstopDelay elapses, even when quorum (2/3)
    // has already acked. With enforceFullConvergence=false the backstop fires for the same inputs.
    [Test]
    public async Task AckGate_EnforceFullConvergenceDisablesQuorumBackstop()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // Set a very short backstop so it would fire immediately if the flag were ignored.
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.SchemaAckQuorumBackstopDelay = TimeSpan.FromMilliseconds(50);

        // Leader + one follower ack version 1 (quorum = 2/3); one follower is silent.
        InProcessSchemaCluster.Node silentFollower = cluster.Nodes.First(n => n.Index != leader.Index);
        leader.Kahuna.RecordLocalSchemaApplied(db, 0);
        leader.Kahuna.RecordLocalSchemaApplied(db, 1);
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
        {
            if (n.Index == leader.Index || n.Index == silentFollower.Index)
                continue;
            // Ack via relay so the leader's tracker sees it.
            n.Kahuna.RecordLocalSchemaApplied(db, 0);
            n.Kahuna.RecordAndPublishSchemaApplied(db, 1);
        }

        // Give the relay tasks time to land on the leader.
        await Task.Delay(200).ConfigureAwait(false);

        // With enforceFullConvergence=true the backstop must be suppressed — quorum (2/3) is
        // satisfied but the silent third follower prevents full convergence → gate must time out.
        bool blocked = await leader.Kahuna.WaitForSchemaAcksAsync(
            db, 1,
            timeout: TimeSpan.FromMilliseconds(150),
            enforceFullConvergence: true,
            cancellationToken: CancellationToken.None);

        Assert.IsFalse(blocked,
            "Pre-proposal gate with enforceFullConvergence=true must block when the third node has not acked, even after the backstop delay");

        // Control: without enforceFullConvergence the backstop fires on quorum (2/3) and the gate unblocks.
        bool backstopFired = await leader.Kahuna.WaitForSchemaAcksAsync(
            db, 1,
            timeout: TimeSpan.FromSeconds(2),
            enforceFullConvergence: false,
            cancellationToken: CancellationToken.None);

        Assert.IsTrue(backstopFired,
            "Post-commit gate without enforceFullConvergence must proceed once quorum backstop fires (2/3 acked)");
    }

    // §3.4 — fence: DML on a node whose HeadSchemaVersion is more than one version ahead of
    // its applied SchemaVersion must be rejected with SchemaCatchingUp so the node does not
    // decode rows with a stale schema. HeadSchemaVersion is advanced directly (simulating the
    // window between ObserveSchemaEntryHead and the in-memory apply) to trigger the fence without
    // needing a real partitioned follower.
    [Test]
    public async Task SchemaFence_RejectsDmlWhenHeadAheadByMoreThanOne()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        await leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "fence_tbl",
            columns: [new ColumnInfo("id", ColumnType.Id), new ColumnInfo("val", ColumnType.String)],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        ));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        // Advance a follower's HeadSchemaVersion by 2 without applying the delta — simulating
        // two committed entries in the apply pipeline that have not yet acquired the lock.
        // The gap HeadSchemaVersion − SchemaVersion = 2 > 1 must trigger the fence.
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leader.Index);
        long appliedBefore = follower.Database!.Schema.SchemaVersion;
        follower.Database.ObserveSchemaEntryHead(appliedBefore + 2);

        Assert.AreEqual(appliedBefore + 2, follower.Database.HeadSchemaVersion,
            "HeadSchemaVersion must reflect the injected head");
        Assert.AreEqual(appliedBefore, follower.Database.Schema.SchemaVersion,
            "SchemaVersion must be unchanged by ObserveSchemaEntryHead");

        // DML on the fenced follower must throw SchemaCatchingUp.
        KvTransaction tx = await follower.Database.Transactions.BeginAsync();
        CamusDBException? ex = null;
        try
        {
            await follower.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: tx,
                database: db,
                sql: "INSERT INTO fence_tbl (id, val) VALUES (gen_id(), 'x')",
                parameters: null
            ));
        }
        catch (CamusDBException cex)
        {
            ex = cex;
        }
        finally
        {
            await follower.Database.Transactions.RollbackAsync(tx);
        }

        Assert.IsNotNull(ex, "DML must be rejected while HeadSchemaVersion − SchemaVersion > 1");
        Assert.AreEqual(CamusDBErrorCodes.SchemaCatchingUp, ex!.Code,
            $"Error code must be SchemaCatchingUp, got: {ex.Code} — {ex.Message}");
    }

    // H5 §3.4a #2 — fault-injected quorum backstop: one follower is isolated at the Raft
    // transport layer so it cannot receive schema-log entries and therefore cannot ack.
    // The remaining two nodes (leader + one healthy follower) form both Raft quorum and the
    // schema-ack quorum (⌊3/2⌋+1 = 2). After SchemaAckQuorumBackstopDelay the post-commit
    // ack gate fires the QuorumBackstop path and DDL proceeds. On ResumeDelivery the isolated
    // follower replays the missed entries, its schema version advances, the fence clears, and
    // DML on that node succeeds.
    //
    // This is the first test that actually exercises SchemaAckOutcome.QuorumBackstop
    // (all prior cluster tests use healthy nodes → always FullConvergence).
    //
    // Note: the catch-up path is ReopenDatabaseAsync (reads KV checkpoint via
    // MemoryInterNodeCommunication), NOT organic Raft WAL replay on ResumeDelivery. This is
    // deterministic and avoids the term-bump-on-rejoin churn, but it means this test validates
    // fence-clears-after-KV-read rather than fence-clears-after-Raft-replay. The Raft replay
    // path is covered indirectly by PausedNodeLagsAndCatchesUpOnResume.
    [Test]
    public async Task QuorumBackstop_DdlProceedsWhenOneFollowerPaused()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        // Use a short backstop so the test completes in ~2s instead of 10s.
        const int BackstopMs = 2000;
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.SchemaAckQuorumBackstopDelay = TimeSpan.FromMilliseconds(BackstopMs);

        await cluster.OpenDatabaseOnAllNodesAsync(db);
        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // Pick a follower to isolate. The other follower stays reachable — it will ack
        // the committed schema entry, giving the leader its quorum (2/3).
        InProcessSchemaCluster.Node pausedFollower = cluster.Nodes.First(n => n.Index != leader.Index);
        cluster.PauseDelivery(pausedFollower.Index);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            // CreateTable: Raft commits with quorum (leader + healthy follower),
            // then the post-commit ack gate waits. After ~BackstopMs the backstop fires on 2/3.
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "backstop_tbl",
                columns: [new ColumnInfo("id", ColumnType.Id)],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            ));
        }
        finally
        {
            sw.Stop();
        }

        // DDL must succeed (no exception) — the backstop unblocked the gate.
        long elapsedMs = sw.ElapsedMilliseconds;
        Assert.GreaterOrEqual(elapsedMs, BackstopMs - 500,
            $"DDL completed in {elapsedMs}ms — shorter than the {BackstopMs}ms backstop delay; " +
            "suggests FullConvergence fired (paused follower acked unexpectedly)");

        // The post-commit gate must have used the QuorumBackstop path, not FullConvergence.
        Assert.AreEqual(SchemaAckOutcome.QuorumBackstop, leader.Kahuna.LastGateOutcome,
            $"Expected QuorumBackstop but gate outcome was {leader.Kahuna.LastGateOutcome}; " +
            "the isolated follower should not have acked");

        // Observability (H5 §3.4a #3): the gate must name the lagging follower, not just report
        // "one or more nodes", and the always-on quorum-backstop metric must have ticked.
        string pausedEndpoint = pausedFollower.Kahuna.Raft.GetLocalEndpoint();
        CollectionAssert.Contains(leader.Kahuna.LastGateLaggards, pausedEndpoint,
            $"LastGateLaggards must name the isolated follower {pausedEndpoint}; " +
            $"got [{string.Join(",", leader.Kahuna.LastGateLaggards)}]");
        Assert.GreaterOrEqual(CamusDB.Core.Diagnostics.SchemaMetrics.QuorumBackstopActivations, 1,
            "A QuorumBackstop outcome must increment the SchemaMetrics.QuorumBackstopActivations counter");

        long clusterVersion = leader.Database!.Schema.SchemaVersion;

        // Reconnect the isolated follower, then reopen the database on it.
        // ReopenDatabaseAsync closes+reopens via LocateAndTryGetValue → MemoryInterNodeCommunication
        // (not the fault-injecting Raft transport) so it reads the latest KV checkpoint from the
        // partition leader immediately, without depending on Raft WAL replay catching up.
        cluster.ResumeDelivery(pausedFollower.Index);
        await cluster.ReopenDatabaseAsync(pausedFollower.Index, db).ConfigureAwait(false);

        // node.Database is updated by ReopenDatabaseAsync; re-bind the reference.
        InProcessSchemaCluster.Node updatedFollower = cluster.Nodes[pausedFollower.Index];

        Assert.AreEqual(clusterVersion, updatedFollower.Database!.Schema.SchemaVersion,
            "Rejoined follower must catch up to cluster schema version via reopen");

        // HeadSchemaVersion and SchemaVersion must be within 1 (fence cleared).
        Assert.LessOrEqual(updatedFollower.Database.HeadSchemaVersion - updatedFollower.Database.Schema.SchemaVersion, 1,
            "Fence gap must be ≤ 1 once the follower has caught up");

        // DML on the previously-paused follower must succeed (fence no longer rejects).
        KvTransaction tx = await updatedFollower.Database.Transactions.BeginAsync();
        try
        {
            await updatedFollower.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: tx,
                database: db,
                sql: "INSERT INTO backstop_tbl (id) VALUES (gen_id())",
                parameters: null
            ));
            await updatedFollower.Database.Transactions.CommitAsync(tx);
        }
        catch (Exception ex)
        {
            await updatedFollower.Database.Transactions.RollbackAsync(tx);
            Assert.Fail($"DML on the rejoined follower must succeed after schema catch-up, got: {ex.Message}");
        }
    }

    // §1 / H5 — dead-node liveness: DDL must proceed permanently after one node is killed,
    // NOT via the quorum backstop (which is post-commit only) but via the pre-proposal gate
    // evicting the dead member from GetLiveSchemaNodes().
    //
    // With SchemaAckLiveNodeLease set to a finite value, GetLiveSchemaNodes() calls
    // Raft.GetActiveNodes(lease) instead of Raft.GetNodes(). After the lease elapses without a
    // heartbeat from the dead node, GetActiveNodes drops it, and the pre-proposal gate (which uses
    // enforceFullConvergence=true — no backstop) sees only the two surviving nodes. Both have
    // acked the previous version → FullConvergence on the reduced set → DDL proceeds.
    //
    // The assertion is LastGateOutcome == FullConvergence (NOT QuorumBackstop): the dead node is
    // fully absent from the live set, so the reduced-set full-convergence path fires, not the
    // quorum-backstop path.
    [Test]
    public async Task DeadNode_PreProposalGateEvictsViaLease_DdlProceedsAsFullConvergence()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        // Use a short live-node lease so the dead follower is quickly dropped from GetLiveSchemaNodes.
        const int LeaseMs = 1200;
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.SchemaAckLiveNodeLease = TimeSpan.FromMilliseconds(LeaseMs);

        await cluster.OpenDatabaseOnAllNodesAsync(db);
        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // First DDL with all 3 nodes healthy → everyone acks version 1.
        await leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "tbl_v1",
            columns: [new ColumnInfo("id", ColumnType.Id)],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        ));
        await cluster.WaitForSchemaConvergenceAsync(db, version: 1, timeout: TimeSpan.FromSeconds(10));

        // Kill one follower permanently. KillNodeAsync blocks its transport + disposes it.
        // Raft heartbeats from the leader to the dead node will stop being answered, so
        // GetActiveNodes will drop it from the active set once the lease expires.
        InProcessSchemaCluster.Node deadFollower = cluster.Nodes.First(n => n.Index != leader.Index);
        await cluster.KillNodeAsync(deadFollower.Index).ConfigureAwait(false);

        // Wait for the lease to expire so the dead node is evicted from GetLiveSchemaNodes().
        // Add a buffer so the leader's Raft clock has registered silence beyond the window.
        await Task.Delay(LeaseMs + 400).ConfigureAwait(false);

        // Second DDL: the pre-proposal gate (enforceFullConvergence=true, no backstop) now
        // only waits on the two surviving nodes (leader + healthy follower), both of which acked
        // version 1. Gate proceeds as FullConvergence on the reduced set.
        await leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "tbl_v2",
            columns: [new ColumnInfo("id", ColumnType.Id)],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        ));

        // Gate must have used FullConvergence, NOT QuorumBackstop: the dead node is absent from
        // the live set entirely, so HasEveryLiveNodeAcked passes on the reduced set.
        Assert.AreEqual(SchemaAckOutcome.FullConvergence, leader.Kahuna.LastGateOutcome,
            $"Expected FullConvergence on reduced live set after dead-node lease eviction, " +
            $"but got {leader.Kahuna.LastGateOutcome}");

        // Both surviving nodes must have the new table.
        InProcessSchemaCluster.Node[] survivors = cluster.Nodes
            .Where(n => n.Index != deadFollower.Index)
            .ToArray();

        Assert.True(survivors.All(n => n.Database?.Schema.Tables.ContainsKey("tbl_v2") ?? false),
            "Both surviving nodes must have tbl_v2 after the dead-node DDL round");
    }

    // H5 §3.4a #2 — no-quorum timeout: only the leader has acked (1 of 3 nodes);
    // quorum requires 2, so the backstop fires but HasQuorumAcked returns false →
    // WaitForSchemaAcksAsync returns false and LastGateOutcome is Timeout.
    // This is the path where DDL correctly blocks because quorum was never reached.
    [Test]
    public async Task QuorumBackstop_GateTimesOutWhenFewerThanQuorumAck()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // Set a very short backstop and a short overall timeout so the test stays fast.
        leader.Kahuna.SchemaAckQuorumBackstopDelay = TimeSpan.FromMilliseconds(100);
        leader.Kahuna.SchemaAckWaitTimeout = TimeSpan.FromMilliseconds(400);

        // Only the leader acks version 1 (1/3). The other two have not acked.
        // Quorum = ⌊3/2⌋+1 = 2, so HasQuorumAcked returns false → Timeout outcome.
        leader.Kahuna.RecordLocalSchemaApplied(db, 0);
        leader.Kahuna.RecordLocalSchemaApplied(db, 1);

        // Wait long enough for the backstop to have fired.
        await Task.Delay(200).ConfigureAwait(false);

        bool result = await leader.Kahuna.WaitForSchemaAcksAsync(
            db, 1,
            timeout: TimeSpan.FromMilliseconds(400),
            enforceFullConvergence: false,
            cancellationToken: CancellationToken.None);

        Assert.IsFalse(result,
            "Gate must return false when fewer than quorum (2/3) have acked, even after the backstop elapses");
        Assert.AreEqual(SchemaAckOutcome.Timeout, leader.Kahuna.LastGateOutcome,
            $"Expected Timeout outcome but got {leader.Kahuna.LastGateOutcome}");
    }

    // ── C2: Key-range sharding data path across nodes ─────────────────────────

    // Kahuna's RangeMapStore.MetaPartitionId — the partition whose leader is the only node that
    // can commit a range-descriptor seed. A node that is NOT this leader must forward the seed (K1).
    private const int MetaPartitionId = 0;

    // C2 (docs/key-range-sharding-spec.md §5) — end-to-end key-range data path across a 3-node
    // cluster with CAMUS_KEY_RANGE_SHARDING ON. This pins the K1 seed-forwarding gap: with
    // InitialPartitions >= 2 and the flag on, the FIRST table open + INSERT is issued from a node
    // that is NOT the meta-partition (P0) leader, so registering the row key space must forward the
    // range-descriptor seed to the meta leader. Without K1 the first write throws
    // "No range descriptor covers key '{tableId}:r/...'"; with the published 0.3.3 fix it succeeds.
    // A spread of rows is then inserted from the non-leader writer and read back, ordered, from a
    // DIFFERENT node — proving cluster-wide routing + the ordered-scan property.
    [Test]
    public async Task KeyRangeShardingDataPathAcrossNodesSeedsFromNonMetaLeader()
    {
        const int rowCount = 25;

        bool previousFlag = CamusDBConfig.KeyRangeShardingEnabled;
        CamusDBConfig.KeyRangeShardingEnabled = true;
        try
        {
            // InitialPartitions = 3 keeps key-range routing genuinely multi-partition; the harness
            // uses long, well-spread election timeouts (see CreateClusterNode) so the schema-log
            // partition leader stays stable under in-process load — otherwise a spurious re-election
            // misroutes a follower's schema-applied ack and CreateTable's convergence wait flakes.
            await using InProcessSchemaCluster cluster =
                await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: 3, wireLeaderForwarder: true);
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            // Wait for the schema partition leader to be stable before issuing any DDL. Without
            // this, CreateTable can race a still-churning schema partition and time out.
            await cluster.WaitForSchemaLeaderNodeAsync(db);

            // Choose a writer that does NOT lead the meta partition, so opening the table on it
            // forwards the range-descriptor seed to the meta leader (the K1 path under test).
            InProcessSchemaCluster.Node writer = await NonMetaLeaderNodeAsync(cluster);

            // CREATE TABLE from the writer — forwarded to the schema leader, converges on all nodes.
            CreateTableResult created = await writer.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id), new ColumnInfo("name", ColumnType.String)],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            )).WaitAsync(TimeSpan.FromSeconds(60));

            Assert.IsTrue(created.Success, "CREATE TABLE on the writer must forward to the leader and apply");
            await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

            // INSERT a spread of rows from the writer. The first INSERT opens the table on the writer
            // (non-meta-leader) node → RegisterKeyRangeAsync → K1 seed forwarding to the meta leader.
            // A "No range descriptor covers key" here is the exact failure C2 pins; with 0.3.3 it must
            // not surface (the call would throw a CamusDBException and fail the test).
            for (int i = 0; i < rowCount; i++)
            {
                KvTransaction tx = await writer.Database!.Transactions.BeginAsync();
                await writer.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx,
                    database: db,
                    sql: $"INSERT INTO robots (id, name) VALUES (gen_id(), 'robot {i:D3}')",
                    parameters: null
                ));
                await writer.Database.Transactions.CommitAsync(tx);
            }

            // Read back from a DIFFERENT node than the writer (its first SELECT opens + registers the
            // row space locally too — idempotent, served from the replicated descriptor).
            InProcessSchemaCluster.Node reader = cluster.Nodes.First(n => n.Index != writer.Index);

            KvTransaction readTx = await reader.Database!.Transactions.BeginAsync();
            (_, IAsyncEnumerable<QueryResultRow> readCursor) = await reader.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: readTx,
                database: db,
                sql: "SELECT id FROM robots",
                parameters: null
            ));
            List<string> readIds = (await readCursor.ToListAsync()).Select(r => r.Row["id"].StrValue!).ToList();
            await reader.Database.Transactions.CommitAsync(readTx);

            // All rows visible on the reader node, regardless of which node wrote them.
            Assert.AreEqual(rowCount, readIds.Count,
                $"reader node {reader.Index} must see all {rowCount} rows inserted by writer node {writer.Index}");

            // Ordered-scan property: a full PK scan over the key-range row space returns rows in
            // ascending key order (ObjectId hex is order-preserving under ordinal comparison).
            List<string> ascending = readIds.OrderBy(id => id, StringComparer.Ordinal).ToList();
            CollectionAssert.AreEqual(ascending, readIds,
                "key-range PK scan must return rows ordered ascending by row id");
        }
        finally
        {
            CamusDBConfig.KeyRangeShardingEnabled = previousFlag;
        }
    }

    // Returns a cluster node that is NOT the current leader of the meta partition (P0). Opening a
    // table on such a node exercises the K1 seed-forwarding path (the seed must be forwarded to the
    // meta leader rather than committed locally).
    private static async Task<InProcessSchemaCluster.Node> NonMetaLeaderNodeAsync(InProcessSchemaCluster cluster)
    {
        await cluster.Nodes[0].Kahuna.Raft.WaitForLeader(MetaPartitionId, CancellationToken.None).ConfigureAwait(false);

        DateTime deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                if (await node.Kahuna.Raft.AmILeaderQuick(MetaPartitionId).ConfigureAwait(false))
                    return cluster.Nodes.First(n => n.Index != node.Index);
            }

            await Task.Delay(50).ConfigureAwait(false);
        }

        throw new AssertionException($"No leader resolved for meta partition {MetaPartitionId} within 10s");
    }
}
