
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using Kahuna;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Task 16: Multi-partition RW serializable acceptance tests.
///
/// <para>
/// The anomaly suite (Tasks 11 + cluster mirror) runs at <c>partitions: 1</c> — it varies
/// replica count, not partition count. A single-partition setup routes every key to the same
/// Kahuna Raft group, so all locks and 2PC votes are handled by one partition leader. This
/// suite proves the same serializability guarantees hold at <c>partitions: 2</c>, where:
/// </para>
///
/// <list type="bullet">
///   <item>A single INSERT or UPDATE is a cross-partition 2PC: the row data key
///   (<c>{tableId}:r/{rowId}</c>) and the PK-index key (<c>{tableId}:i:~pk/…</c>) hash to
///   different Kahuna partitions. Each partition leader independently enforces point locks.
///   </item>
///   <item>Two concurrent Serializable+RW transactions that touch the same row must each
///   acquire their locks on both partitions; a conflict on either partition aborts one of
///   them with <c>TransactionConflict</c> or <c>TransactionMustRetry</c>.</item>
///   <item>A reverse-order cross-partition deadlock (TxA holds P0 locks, wants P1;
///   TxB holds P1 locks, wants P0) resolves via the bounded-wait → abort policy
///   (<c>LockWaitDeadlineMs = 500 ms</c>) well under the 30 s range-lock TTL.
///   </item>
/// </list>
///
/// <para>Tests are organised in three groups:</para>
/// <list type="number">
///   <item>Single-node N=1, 2 partitions — confirms 2-partition 2PC serializability locally.</item>
///   <item>Cluster N=3, 2 partitions — confirms the same outcomes survive Raft replication.</item>
///   <item>N=1 ≡ N=3 equivalence — commit/abort outcomes must be identical across topologies.</item>
/// </list>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSerializableRWMultiPartitionCluster
{
    private const int TwoPartitions = 2;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    // -----------------------------------------------------------------------
    // Single-node (2-partition) helpers
    // -----------------------------------------------------------------------

    private static async Task<(EmbeddedKahuna node, string dbname, CommandExecutor executor, DatabaseDescriptor database)>
        CreateSingleNodeTwoPartitionSetupAsync()
    {
        EmbeddedKahuna node = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            NodeName = $"mp-test-{Guid.NewGuid():N}",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = TwoPartitions
        });

        await node.StartAsync(CancellationToken.None);

        // Warm up both partition leaders.
        HashSet<int> seen = [];
        for (int i = 0; seen.Count < TwoPartitions && i < 200; i++)
        {
            string key = $"{i}/warmup";
            int p = node.Raft.GetPartitionKey(key);
            if (seen.Add(p))
                await node.WaitForLeaderAsync(key, CancellationToken.None);
        }

        CommandValidator validator = new();
        CatalogsManager catalogs = new(logger);
        CommandExecutor executor = new(validator, catalogs, logger, sharedNode: node, isClusterMode: true);

        string dbname = Guid.NewGuid().ToString("n");
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "accounts",
            columns:
            [
                new ColumnInfo("id",      ColumnType.Id),
                new ColumnInfo("name",    ColumnType.String,     notNull: true),
                new ColumnInfo("balance", ColumnType.Integer64),
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

    // -----------------------------------------------------------------------
    // Cluster (N=3, 2-partition) helpers
    // -----------------------------------------------------------------------

    private static async Task<(InProcessSchemaCluster cluster, string db,
        InProcessSchemaCluster.Node leader, string aliceId, string bobId)>
        SetupClusterTwoPartitionAccountsAsync()
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: TwoPartitions,
                loggerFactory: sharedLoggerFactory, logger: logger);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "accounts",
            columns:
            [
                new ColumnInfo("id",      ColumnType.Id),
                new ColumnInfo("name",    ColumnType.String,     notNull: true),
                new ColumnInfo("balance", ColumnType.Integer64),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        string aliceId = ObjectIdGenerator.Generate().ToString();
        string bobId   = ObjectIdGenerator.Generate().ToString();

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            KvTransaction setup = await leader.Database!.Transactions.BeginAsync();
            await leader.Executor.Insert(new InsertTicket(
                txnState: setup, databaseName: db, tableName: "accounts",
                values: new() { new() {
                    { "id",      new(ColumnType.Id,        aliceId) },
                    { "name",    new(ColumnType.String,    "alice") },
                    { "balance", new(ColumnType.Integer64, 100L)   },
                }}));
            await leader.Executor.Insert(new InsertTicket(
                txnState: setup, databaseName: db, tableName: "accounts",
                values: new() { new() {
                    { "id",      new(ColumnType.Id,        bobId) },
                    { "name",    new(ColumnType.String,    "bob") },
                    { "balance", new(ColumnType.Integer64, 100L) },
                }}));
            await leader.Database.Transactions.CommitAsync(setup);
        });

        InProcessSchemaCluster.Node leaderNode = await cluster.WaitForSchemaLeaderNodeAsync(db);
        return (cluster, db, leaderNode, aliceId, bobId);
    }

    // -----------------------------------------------------------------------
    // Shared query helpers
    // -----------------------------------------------------------------------

    private static async Task<long> ReadBalanceAsync(
        string db, CommandExecutor executor, KvTransaction tx, string accountId)
    {
        QueryTicket ticket = new(
            txnState:     tx,
            databaseName: db,
            tableName:    "accounts",
            index:        null,
            projection:   null,
            where:        null,
            filters:      new() { new("id", "=", new(ColumnType.Id, accountId)) },
            orderBy:      null,
            limit:        null,
            offset:       null,
            parameters:   null
        );
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        return rows[0].Row["balance"].LongValue;
    }

    private static async Task<UpdateResult> UpdateBalanceAsync(
        string db, CommandExecutor executor, KvTransaction tx,
        string accountId, long newBalance)
    {
        return await executor.Update(new UpdateTicket(
            txnState:     tx,
            databaseName: db,
            tableName:    "accounts",
            plainValues:  new() { { "balance", new(ColumnType.Integer64, newBalance) } },
            exprValues:   null,
            where:        null,
            filters:      new() { new("id", "=", new(ColumnType.Id, accountId)) },
            parameters:   null
        ));
    }

    // -----------------------------------------------------------------------
    // 1. Single-node N=1, 2 partitions:
    //    A single INSERT crosses both partitions (row data + PK index).
    //    Serializability: TxA holds shared locks on both partitions;
    //    TxB's write is rejected regardless of which partition it hits first.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SingleNode_TwoPartitions_SerializableRW_SharedLocksAcrossBothPartitions()
    {
        (EmbeddedKahuna node, string dbname, CommandExecutor executor, DatabaseDescriptor database) =
            await CreateSingleNodeTwoPartitionSetupAsync();

        await using EmbeddedKahuna _ = node;

        string aliceId = ObjectIdGenerator.Generate().ToString();

        KvTransaction setup = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: setup, databaseName: dbname, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        aliceId) },
                { "name",    new(ColumnType.String,    "alice") },
                { "balance", new(ColumnType.Integer64, 100L)   },
            }}));
        await database.Transactions.CommitAsync(setup);

        // TxA: Serializable+RW read — acquires shared locks on alice's row partition
        // AND alice's PK-index partition (the insert was a cross-partition 2PC, so both
        // partitions recorded the entry).
        KvTransaction txA = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        long balance = await ReadBalanceAsync(dbname, executor, txA, aliceId);
        Assert.AreEqual(100L, balance);

        // TxB: tries to write alice — must conflict on the shared lock held by TxA
        // (whichever partition it hits first, the lock is there).
        KvTransaction txB = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => UpdateBalanceAsync(dbname, executor, txB, aliceId, 999L));

        Assert.IsTrue(
            ex?.Code == CamusDBErrorCodes.TransactionConflict ||
            ex?.Code == CamusDBErrorCodes.TransactionMustRetry,
            $"TxB must conflict on the shared lock across both partitions; got {ex?.Code}");

        await database.Transactions.RollbackAsync(txB);
        await database.Transactions.CommitAsync(txA);

        // After TxA commits, a subsequent writer must succeed (locks released on both partitions).
        KvTransaction txC = await database.Transactions.BeginAsync();
        Assert.DoesNotThrowAsync(
            () => UpdateBalanceAsync(dbname, executor, txC, aliceId, 200L),
            "Write after TxA commit must succeed — cross-partition locks must have been released");
        await database.Transactions.CommitAsync(txC);
    }

    // -----------------------------------------------------------------------
    // 2. Single-node N=1, 2 partitions:
    //    Write-skew prevention still works when alice and bob may hash to
    //    different partitions.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SingleNode_TwoPartitions_SerializableRW_WriteSkew_Prevented()
    {
        (EmbeddedKahuna node, string dbname, CommandExecutor executor, DatabaseDescriptor database) =
            await CreateSingleNodeTwoPartitionSetupAsync();

        await using EmbeddedKahuna _ = node;

        string aliceId = ObjectIdGenerator.Generate().ToString();
        string bobId   = ObjectIdGenerator.Generate().ToString();

        KvTransaction setup = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: setup, databaseName: dbname, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        aliceId) },
                { "name",    new(ColumnType.String,    "alice") },
                { "balance", new(ColumnType.Integer64, 100L)   },
            }}));
        await executor.Insert(new InsertTicket(
            txnState: setup, databaseName: dbname, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        bobId) },
                { "name",    new(ColumnType.String,    "bob") },
                { "balance", new(ColumnType.Integer64, 100L) },
            }}));
        await database.Transactions.CommitAsync(setup);

        KvTransaction txA = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction txB = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // Both read both rows; each acquires shared locks across both partitions.
        await ReadBalanceAsync(dbname, executor, txA, aliceId);
        await ReadBalanceAsync(dbname, executor, txA, bobId);
        await ReadBalanceAsync(dbname, executor, txB, aliceId);
        await ReadBalanceAsync(dbname, executor, txB, bobId);

        // TxA tries to write alice — blocked by TxB's shared lock.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => UpdateBalanceAsync(dbname, executor, txA, aliceId, -50L));
        Assert.IsTrue(
            ex?.Code == CamusDBErrorCodes.TransactionConflict ||
            ex?.Code == CamusDBErrorCodes.TransactionMustRetry,
            $"2-partition write-skew attempt must fail; got {ex?.Code}");

        await database.Transactions.RollbackAsync(txA);

        // TxB can still update bob and commit.
        await UpdateBalanceAsync(dbname, executor, txB, bobId, -50L);
        await database.Transactions.CommitAsync(txB);

        KvTransaction verify = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        long finalAlice = await ReadBalanceAsync(dbname, executor, verify, aliceId);
        long finalBob   = await ReadBalanceAsync(dbname, executor, verify, bobId);
        await database.Transactions.CommitAsync(verify);

        Assert.AreEqual(100L, finalAlice, "Alice's balance must be unchanged");
        Assert.AreEqual(-50L, finalBob,   "Bob's balance must reflect TxB's committed write");
        Assert.GreaterOrEqual(finalAlice + finalBob, 0L,
            "2-partition write-skew prevention must keep the total constraint >= 0");
    }

    // -----------------------------------------------------------------------
    // 3. Single-node N=1, 2 partitions:
    //    Reverse-order cross-partition deadlock resolves via bounded-wait → abort,
    //    not after the 30 s range-lock TTL.
    //
    //    TxA reads alice (S locks on alice's row+index partitions),
    //    TxB reads bob   (S locks on bob's row+index partitions).
    //    Both then try to write the OTHER party:
    //      TxA write bob  → blocked by TxB's S on bob's partitions
    //      TxB write alice → blocked by TxA's S on alice's partitions
    //    Classic reverse-order deadlock spanning ≥ 2 Kahuna partitions.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SingleNode_TwoPartitions_CrossPartitionDeadlock_ResolvedByBoundedWait()
    {
        (EmbeddedKahuna node, string dbname, CommandExecutor executor, DatabaseDescriptor database) =
            await CreateSingleNodeTwoPartitionSetupAsync();

        await using EmbeddedKahuna _ = node;

        string aliceId = ObjectIdGenerator.Generate().ToString();
        string bobId   = ObjectIdGenerator.Generate().ToString();

        KvTransaction setup = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: setup, databaseName: dbname, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        aliceId) },
                { "name",    new(ColumnType.String,    "alice") },
                { "balance", new(ColumnType.Integer64, 100L)   },
            }}));
        await executor.Insert(new InsertTicket(
            txnState: setup, databaseName: dbname, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        bobId) },
                { "name",    new(ColumnType.String,    "bob") },
                { "balance", new(ColumnType.Integer64, 100L) },
            }}));
        await database.Transactions.CommitAsync(setup);

        // Acquire shared locks: TxA on alice, TxB on bob.
        KvTransaction txA = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction txB = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        await ReadBalanceAsync(dbname, executor, txA, aliceId);
        await ReadBalanceAsync(dbname, executor, txB, bobId);

        Stopwatch sw = Stopwatch.StartNew();

        // Both try to write the other party's row concurrently — cross-partition deadlock.
        Task writeA = UpdateBalanceAsync(dbname, executor, txA, bobId,   200L);
        Task writeB = UpdateBalanceAsync(dbname, executor, txB, aliceId, 200L);

        await Task.WhenAll(
            writeA.ContinueWith(_ => { }, TaskContinuationOptions.None),
            writeB.ContinueWith(_ => { }, TaskContinuationOptions.None));

        sw.Stop();

        bool aFailed = writeA.IsFaulted &&
            writeA.Exception!.InnerException is CamusDBException exA &&
            (exA.Code == CamusDBErrorCodes.TransactionConflict || exA.Code == CamusDBErrorCodes.TransactionMustRetry);
        bool bFailed = writeB.IsFaulted &&
            writeB.Exception!.InnerException is CamusDBException exB &&
            (exB.Code == CamusDBErrorCodes.TransactionConflict || exB.Code == CamusDBErrorCodes.TransactionMustRetry);

        Assert.IsTrue(aFailed || bFailed,
            "At least one transaction must abort with TransactionConflict or TransactionMustRetry " +
            "in a cross-partition reverse-order deadlock");

        Assert.Less(sw.ElapsedMilliseconds, 3000,
            $"Cross-partition deadlock must resolve via bounded-wait (< 3 s); took {sw.ElapsedMilliseconds} ms. " +
            "Without the deadline, deadlock resolution waits for the 30 s range-lock TTL.");

        if (!aFailed) await database.Transactions.CommitAsync(txA);
        else await database.Transactions.RollbackAsync(txA);

        if (!bFailed) await database.Transactions.CommitAsync(txB);
        else await database.Transactions.RollbackAsync(txB);
    }

    // -----------------------------------------------------------------------
    // 4. Cluster N=3, 2 partitions:
    //    Serializable+RW shared locks cross-partition — concurrent writer rejected.
    //    Same observable outcome as Test 1, but with Raft replication behind each
    //    of the 2 Kahuna partitions (3 replicas each).
    // -----------------------------------------------------------------------

    [Test]
    public async Task Cluster_TwoPartitions_SerializableRW_ConflictDetectedAcrossPartitions()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader,
            string aliceId, _) = await SetupClusterTwoPartitionAccountsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        DatabaseDescriptor database = leader.Database!;
        CommandExecutor    executor  = leader.Executor;

        // TxA reads alice — acquires S locks on both of alice's Kahuna partitions.
        KvTransaction txA = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        long balance = await ReadBalanceAsync(db, executor, txA, aliceId);
        Assert.AreEqual(100L, balance);

        // TxB tries to write alice — must be rejected across the Raft-backed partitions.
        KvTransaction txB = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => UpdateBalanceAsync(db, executor, txB, aliceId, 999L));

        Assert.IsTrue(
            ex?.Code == CamusDBErrorCodes.TransactionConflict ||
            ex?.Code == CamusDBErrorCodes.TransactionMustRetry,
            $"Cluster: TxB must conflict on TxA's shared locks across both Raft-backed partitions; got {ex?.Code}");

        await database.Transactions.RollbackAsync(txB);
        await database.Transactions.CommitAsync(txA);

        // After TxA commits, a subsequent writer must succeed on both partitions.
        KvTransaction txC = await database.Transactions.BeginAsync();
        Assert.DoesNotThrowAsync(
            () => UpdateBalanceAsync(db, executor, txC, aliceId, 200L),
            "Cluster: write after TxA commit must succeed — Raft-backed cross-partition locks released");
        await database.Transactions.CommitAsync(txC);
    }

    // -----------------------------------------------------------------------
    // 5. Cluster N=3, 2 partitions:
    //    Reverse-order cross-partition deadlock resolves via bounded-wait → abort,
    //    not after the 30 s TTL — same guarantee as single-node but under Raft.
    // -----------------------------------------------------------------------

    [Test]
    public async Task Cluster_TwoPartitions_CrossPartitionDeadlock_ResolvedByBoundedWait()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader,
            string aliceId, string bobId) = await SetupClusterTwoPartitionAccountsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        DatabaseDescriptor database = leader.Database!;
        CommandExecutor    executor  = leader.Executor;

        KvTransaction txA = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction txB = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        await ReadBalanceAsync(db, executor, txA, aliceId);
        await ReadBalanceAsync(db, executor, txB, bobId);

        Stopwatch sw = Stopwatch.StartNew();

        Task writeA = UpdateBalanceAsync(db, executor, txA, bobId,   200L);
        Task writeB = UpdateBalanceAsync(db, executor, txB, aliceId, 200L);

        await Task.WhenAll(
            writeA.ContinueWith(_ => { }, TaskContinuationOptions.None),
            writeB.ContinueWith(_ => { }, TaskContinuationOptions.None));

        sw.Stop();

        bool aFailed = writeA.IsFaulted &&
            writeA.Exception!.InnerException is CamusDBException exA &&
            (exA.Code == CamusDBErrorCodes.TransactionConflict || exA.Code == CamusDBErrorCodes.TransactionMustRetry);
        bool bFailed = writeB.IsFaulted &&
            writeB.Exception!.InnerException is CamusDBException exB &&
            (exB.Code == CamusDBErrorCodes.TransactionConflict || exB.Code == CamusDBErrorCodes.TransactionMustRetry);

        Assert.IsTrue(aFailed || bFailed,
            "Cluster: at least one transaction must abort in a cross-partition reverse-order deadlock");

        Assert.Less(sw.ElapsedMilliseconds, 3000,
            $"Cluster: cross-partition deadlock must resolve via bounded-wait (< 3 s); took {sw.ElapsedMilliseconds} ms");

        if (!aFailed) await database.Transactions.CommitAsync(txA);
        else await database.Transactions.RollbackAsync(txA);

        if (!bFailed) await database.Transactions.CommitAsync(txB);
        else await database.Transactions.RollbackAsync(txB);
    }

    // -----------------------------------------------------------------------
    // 6. N=1 ≡ N=3 equivalence at 2 partitions:
    //    Write-skew abort outcome must be identical across topologies.
    //
    //    Runs the write-skew scenario at N=1 (2 partitions) and N=3 (2 partitions)
    //    and asserts: TxA aborts, TxB commits, final total >= 0 in both cases.
    //    A divergence would mean the cross-partition lock semantics differ by topology.
    // -----------------------------------------------------------------------

    [Test]
    public async Task TwoPartition_SingleNodeEqualsCluster_WriteSkewOutcome_IsIdentical()
    {
        static async Task<(bool txAAborted, bool txBCommitted, long finalTotal)>
            RunWriteSkewAsync(DatabaseDescriptor database, CommandExecutor executor,
                string db, string aliceId, string bobId)
        {
            KvTransaction txA = await database.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
            KvTransaction txB = await database.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

            await ReadBalanceAsync(db, executor, txA, aliceId);
            await ReadBalanceAsync(db, executor, txA, bobId);
            await ReadBalanceAsync(db, executor, txB, aliceId);
            await ReadBalanceAsync(db, executor, txB, bobId);

            bool txAAborted = false;
            bool txBCommitted = false;

            try
            {
                await UpdateBalanceAsync(db, executor, txA, aliceId, -50L);
                await database.Transactions.CommitAsync(txA);
            }
            catch (CamusDBException ex) when (
                ex.Code == CamusDBErrorCodes.TransactionConflict ||
                ex.Code == CamusDBErrorCodes.TransactionMustRetry)
            {
                txAAborted = true;
                await database.Transactions.RollbackAsync(txA);
            }

            try
            {
                await UpdateBalanceAsync(db, executor, txB, bobId, -50L);
                await database.Transactions.CommitAsync(txB);
                txBCommitted = true;
            }
            catch
            {
                await database.Transactions.RollbackAsync(txB);
            }

            KvTransaction verify = await database.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
            QueryTicket ticket = new(
                txnState: verify, databaseName: db, tableName: "accounts",
                index: null, projection: null, where: null, filters: null,
                orderBy: null, limit: null, offset: null, parameters: null);
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(ticket);
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(verify);

            long finalTotal = rows.Sum(r => r.Row["balance"].LongValue);
            return (txAAborted, txBCommitted, finalTotal);
        }

        // --- Single-node N=1, 2 partitions ---
        (EmbeddedKahuna singleNode, string n1Db, CommandExecutor n1Executor, DatabaseDescriptor n1Database) =
            await CreateSingleNodeTwoPartitionSetupAsync();
        await using EmbeddedKahuna __ = singleNode;

        string n1AliceId = ObjectIdGenerator.Generate().ToString();
        string n1BobId   = ObjectIdGenerator.Generate().ToString();

        KvTransaction n1Setup = await n1Database.Transactions.BeginAsync();
        await n1Executor.Insert(new InsertTicket(
            txnState: n1Setup, databaseName: n1Db, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        n1AliceId) },
                { "name",    new(ColumnType.String,    "alice")   },
                { "balance", new(ColumnType.Integer64, 100L)      },
            }}));
        await n1Executor.Insert(new InsertTicket(
            txnState: n1Setup, databaseName: n1Db, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        n1BobId) },
                { "name",    new(ColumnType.String,    "bob")   },
                { "balance", new(ColumnType.Integer64, 100L)    },
            }}));
        await n1Database.Transactions.CommitAsync(n1Setup);

        (bool n1TxAAborted, bool n1TxBCommitted, long n1FinalTotal) =
            await RunWriteSkewAsync(n1Database, n1Executor, n1Db, n1AliceId, n1BobId);

        // --- Cluster N=3, 2 partitions ---
        (InProcessSchemaCluster cluster3, string n3Db, InProcessSchemaCluster.Node n3Leader,
            string n3AliceId, string n3BobId) = await SetupClusterTwoPartitionAccountsAsync();
        await using InProcessSchemaCluster ___ = cluster3;

        (bool n3TxAAborted, bool n3TxBCommitted, long n3FinalTotal) =
            await RunWriteSkewAsync(n3Leader.Database!, n3Leader.Executor, n3Db, n3AliceId, n3BobId);

        // Assert identical outcomes.
        Assert.AreEqual(n1TxAAborted,   n3TxAAborted,
            "TxA abort/commit outcome must be identical at N=1 and N=3 with 2 partitions");
        Assert.AreEqual(n1TxBCommitted, n3TxBCommitted,
            "TxB abort/commit outcome must be identical at N=1 and N=3 with 2 partitions");
        Assert.GreaterOrEqual(n1FinalTotal, 0L,
            "N=1 (2-partition): final total must satisfy the constraint >= 0");
        Assert.GreaterOrEqual(n3FinalTotal, 0L,
            "N=3 (2-partition): final total must satisfy the constraint >= 0");
        Assert.IsTrue(n1TxAAborted,
            "TxA must abort in the 2-partition write-skew scenario (TxB holds S on alice's partition(s))");
        Assert.IsTrue(n1TxBCommitted,
            "TxB must commit after TxA aborts in the 2-partition write-skew scenario");
    }
}
