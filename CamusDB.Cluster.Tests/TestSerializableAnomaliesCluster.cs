
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
/// Serializable-isolation acceptance suite — proves serializability at N = 3 (3-node Raft cluster).
///
/// Mirrors TestSerializableAnomalies (unit, N = 1) so each anomaly is verified at both
/// topologies. The observable outcomes must be identical — any divergence is a correctness bug
/// (the single-node ≡ cluster invariant). Locking in hash mode is partition-local: the
/// Kahuna partition leader for each table bucket enforces the same range-lock semantics
/// regardless of how many Raft replicas back it.
///
/// Tests run on the schema leader node; both TxA and TxB talk to the same node so that
/// Kahuna's single-actor partition lock state is exercised exactly as in the N=1 case.
///
/// Plus one explicit N = 1 vs N = 3 equivalence test that asserts identical commit/abort
/// outcomes for the write-skew scenario across both topologies.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSerializableAnomaliesCluster
{
    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(
        builder => builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    // -----------------------------------------------------------------------
    // Cluster setup helpers
    // -----------------------------------------------------------------------

    private static async Task<(InProcessSchemaCluster cluster, string db,
        InProcessSchemaCluster.Node leader, string aliceId, string bobId)>
        SetupClusterAccountsAsync()
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: 1,
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

    // Single-node helper for the N=1 vs N=3 equivalence test.
    private static async Task<(EmbeddedKahuna node, string dbname,
        CommandExecutor executor, DatabaseDescriptor database, string aliceId, string bobId)>
        SetupSingleNodeAccountsAsync()
    {
        EmbeddedKahuna node = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            NodeName         = $"anomaly-n1-{Guid.NewGuid():N}",
            Storage          = "memory",
            WalStorage       = "memory",
            InitialPartitions = 1,
        });
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("warmup", CancellationToken.None);

        CommandValidator validator  = new(CamusDBOptions.Default);
        CatalogsManager  catalogs   = new(logger);
        CommandExecutor  executor   = new(validator, catalogs, logger, CamusDBOptions.Default,
            sharedNode: node, isClusterMode: true);

        string dbname = Guid.NewGuid().ToString("n");
        DatabaseDescriptor database = await executor.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: false));

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

        return (node, dbname, executor, database, aliceId, bobId);
    }

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

    private static async Task<List<QueryResultRow>> ScanAllAsync(
        string db, CommandExecutor executor, KvTransaction tx)
    {
        QueryTicket ticket = new(
            txnState:     tx,
            databaseName: db,
            tableName:    "accounts",
            index:        null,
            projection:   null,
            where:        null,
            filters:      null,
            orderBy:      null,
            limit:        null,
            offset:       null,
            parameters:   null
        );
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(ticket);
        return await cursor.ToListAsync();
    }

    // -----------------------------------------------------------------------
    // 1. Cluster N=3: Serializable+RO eliminates non-repeatable read
    // -----------------------------------------------------------------------

    [Test]
    public async Task Cluster_SerializableRO_NonRepeatableRead_Eliminated()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader,
            string aliceId, _) = await SetupClusterAccountsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        DatabaseDescriptor database = leader.Database!;
        CommandExecutor    executor  = leader.Executor;

        KvTransaction snapshot = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        long first = await ReadBalanceAsync(db, executor, snapshot, aliceId);
        Assert.AreEqual(100L, first);

        // Concurrent writer commits outside the snapshot.
        KvTransaction writer = await database.Transactions.BeginAsync();
        await UpdateBalanceAsync(db, executor, writer, aliceId, 999L);
        await database.Transactions.CommitAsync(writer);

        long second = await ReadBalanceAsync(db, executor, snapshot, aliceId);
        Assert.AreEqual(100L, second,
            "Cluster: Serializable+RO snapshot must not see writes committed after snapshot timestamp");

        await database.Transactions.CommitAsync(snapshot);
    }

    // -----------------------------------------------------------------------
    // 2. Cluster N=3: Serializable+RO eliminates phantom reads
    // -----------------------------------------------------------------------

    [Test]
    public async Task Cluster_SerializableRO_Phantom_Eliminated()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader,
            _, _) = await SetupClusterAccountsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        DatabaseDescriptor database = leader.Database!;
        CommandExecutor    executor  = leader.Executor;

        KvTransaction snapshot = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        List<QueryResultRow> first = await ScanAllAsync(db, executor, snapshot);
        Assert.AreEqual(2, first.Count);

        KvTransaction writer = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: writer, databaseName: db, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                { "name",    new(ColumnType.String,    "carol") },
                { "balance", new(ColumnType.Integer64, 50L) },
            }}));
        await database.Transactions.CommitAsync(writer);

        List<QueryResultRow> second = await ScanAllAsync(db, executor, snapshot);
        Assert.AreEqual(2, second.Count,
            "Cluster: Serializable+RO snapshot must not see rows inserted after snapshot timestamp");

        await database.Transactions.CommitAsync(snapshot);
    }

    // -----------------------------------------------------------------------
    // 3. Cluster N=3: Serializable+RO never blocks concurrent writers
    // -----------------------------------------------------------------------

    [Test]
    public async Task Cluster_SerializableRO_NeverBlocks_ConcurrentWriters()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader,
            string aliceId, _) = await SetupClusterAccountsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        DatabaseDescriptor database = leader.Database!;
        CommandExecutor    executor  = leader.Executor;

        KvTransaction snapshot = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        long before = await ReadBalanceAsync(db, executor, snapshot, aliceId);
        Assert.AreEqual(100L, before);

        KvTransaction writer = await database.Transactions.BeginAsync();
        Assert.DoesNotThrowAsync(
            async () =>
            {
                await UpdateBalanceAsync(db, executor, writer, aliceId, 200L);
                await database.Transactions.CommitAsync(writer);
            },
            "Cluster: a concurrent writer must not be blocked by a Serializable+ReadOnly snapshot");

        long after = await ReadBalanceAsync(db, executor, snapshot, aliceId);
        Assert.AreEqual(100L, after,
            "Cluster: snapshot must remain pinned after a concurrent commit");

        await database.Transactions.CommitAsync(snapshot);
    }

    // -----------------------------------------------------------------------
    // 4. Cluster N=3: Serializable+RW eliminates non-repeatable read
    // -----------------------------------------------------------------------

    [Test]
    public async Task Cluster_SerializableRW_NonRepeatableRead_Eliminated()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader,
            string aliceId, _) = await SetupClusterAccountsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        DatabaseDescriptor database = leader.Database!;
        CommandExecutor    executor  = leader.Executor;

        KvTransaction txA = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        long first = await ReadBalanceAsync(db, executor, txA, aliceId);
        Assert.AreEqual(100L, first);

        KvTransaction writer = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => UpdateBalanceAsync(db, executor, writer, aliceId, 999L));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex?.Code,
            "Cluster: write to a key locked by a Serializable+RW reader must fail with TransactionMustRetry");

        await database.Transactions.RollbackAsync(writer);

        long second = await ReadBalanceAsync(db, executor, txA, aliceId);
        Assert.AreEqual(100L, second,
            "Cluster: Serializable+RW shared lock must prevent the key from being written");

        await database.Transactions.CommitAsync(txA);
    }

    // -----------------------------------------------------------------------
    // 5. Cluster N=3: Serializable+RW eliminates phantom reads
    // -----------------------------------------------------------------------

    [Test]
    public async Task Cluster_SerializableRW_Phantom_Eliminated()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader,
            _, _) = await SetupClusterAccountsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        DatabaseDescriptor database = leader.Database!;
        CommandExecutor    executor  = leader.Executor;

        KvTransaction txA = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        List<QueryResultRow> first = await ScanAllAsync(db, executor, txA);
        Assert.AreEqual(2, first.Count);

        KvTransaction inserter = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.Insert(new InsertTicket(
                txnState: inserter, databaseName: db, tableName: "accounts",
                values: new() { new() {
                    { "id",      new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                    { "name",    new(ColumnType.String,    "carol") },
                    { "balance", new(ColumnType.Integer64, 50L) },
                }})));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex?.Code,
            "Cluster: phantom insert into a Serializable+RW scan range must fail with TransactionMustRetry");

        await database.Transactions.RollbackAsync(inserter);

        List<QueryResultRow> second = await ScanAllAsync(db, executor, txA);
        Assert.AreEqual(2, second.Count,
            "Cluster: Serializable+RW range lock must prevent phantom inserts");

        await database.Transactions.CommitAsync(txA);
    }

    // -----------------------------------------------------------------------
    // 6. Cluster N=3: Serializable+RW prevents write skew
    // -----------------------------------------------------------------------

    [Test]
    public async Task Cluster_SerializableRW_WriteSkew_Prevented()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader,
            string aliceId, string bobId) = await SetupClusterAccountsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        DatabaseDescriptor database = leader.Database!;
        CommandExecutor    executor  = leader.Executor;

        KvTransaction txA = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction txB = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // Both read both accounts.
        await ReadBalanceAsync(db, executor, txA, aliceId);
        await ReadBalanceAsync(db, executor, txA, bobId);
        await ReadBalanceAsync(db, executor, txB, aliceId);
        await ReadBalanceAsync(db, executor, txB, bobId);

        // TxA tries to update alice — TxB holds S on alice → AlreadyLocked.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => UpdateBalanceAsync(db, executor, txA, aliceId, -50L));
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code,
            "Cluster: TxA write-skew attempt must fail with TransactionConflict");

        await database.Transactions.RollbackAsync(txA);

        // TxB can update bob and commit.
        await UpdateBalanceAsync(db, executor, txB, bobId, -50L);
        await database.Transactions.CommitAsync(txB);

        KvTransaction verify = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        long finalAlice = await ReadBalanceAsync(db, executor, verify, aliceId);
        long finalBob   = await ReadBalanceAsync(db, executor, verify, bobId);
        await database.Transactions.CommitAsync(verify);

        Assert.AreEqual(100L, finalAlice);
        Assert.AreEqual(-50L, finalBob);
        Assert.GreaterOrEqual(finalAlice + finalBob, 0L,
            "Cluster: constraint total >= 0 must hold after write-skew prevention");
    }

    // -----------------------------------------------------------------------
    // 7. Cluster N=3: Serializable+RW prevents lost updates
    // -----------------------------------------------------------------------

    [Test]
    public async Task Cluster_SerializableRW_LostUpdate_Prevented()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader,
            string aliceId, _) = await SetupClusterAccountsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        DatabaseDescriptor database = leader.Database!;
        CommandExecutor    executor  = leader.Executor;

        KvTransaction txA = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction txB = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        await ReadBalanceAsync(db, executor, txA, aliceId);
        await ReadBalanceAsync(db, executor, txB, aliceId);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => UpdateBalanceAsync(db, executor, txA, aliceId, 999L));
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code,
            "Cluster: lost-update write must fail with TransactionConflict");

        await database.Transactions.RollbackAsync(txA);

        await UpdateBalanceAsync(db, executor, txB, aliceId, 999L);
        await database.Transactions.CommitAsync(txB);

        KvTransaction verify = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        long final = await ReadBalanceAsync(db, executor, verify, aliceId);
        await database.Transactions.CommitAsync(verify);

        Assert.AreEqual(999L, final,
            "Cluster: TxB's committed write must not be lost");
    }

    // -----------------------------------------------------------------------
    // 8. Cluster N=3: contention resolves immediately — no TTL stall
    // -----------------------------------------------------------------------

    [Test]
    public async Task Cluster_SerializableRW_ContentionResolvesImmediately()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader,
            _, _) = await SetupClusterAccountsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        DatabaseDescriptor database = leader.Database!;
        CommandExecutor    executor  = leader.Executor;

        KvTransaction txA = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        await executor.Update(new UpdateTicket(
            txnState:     txA,
            databaseName: db,
            tableName:    "accounts",
            plainValues:  new() { { "balance", new(ColumnType.Integer64, 200L) } },
            exprValues:   null,
            where:        null,
            filters:      null,
            parameters:   null
        ));

        KvTransaction txB = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        long start = System.Diagnostics.Stopwatch.GetTimestamp();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => executor.Update(new UpdateTicket(
                txnState:     txB,
                databaseName: db,
                tableName:    "accounts",
                plainValues:  new() { { "balance", new(ColumnType.Integer64, 300L) } },
                exprValues:   null,
                where:        null,
                filters:      null,
                parameters:   null
            )));

        long elapsedMs = (long)((System.Diagnostics.Stopwatch.GetTimestamp() - start)
            / (double)System.Diagnostics.Stopwatch.Frequency * 1000);

        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code,
            "Cluster: concurrent full-table UPDATE must fail immediately with TransactionConflict");
        Assert.Less(elapsedMs, 2000,
            "Cluster: conflict must be detected immediately (< 2 s), not after a 30 s TTL stall");

        await database.Transactions.RollbackAsync(txB);
        await database.Transactions.CommitAsync(txA);
    }

    // -----------------------------------------------------------------------
    // 9. Single-node ≡ cluster: write-skew outcome is identical at N=1 and N=3
    //
    // Runs the write-skew scenario twice — once on a standalone N=1 node and once
    // on a 3-node Raft cluster — and asserts the same commit/abort outcome:
    //   TxA aborts (TransactionConflict), TxB commits (total stays >= 0).
    // A divergence would mean N=1 and N=3 have different serializable guarantees.
    // -----------------------------------------------------------------------

    [Test]
    public async Task WriteSkew_SingleNodeEqualsCluster_IdenticalOutcome()
    {
        // Helper: run the write-skew scenario and return (txA aborted, txB committed, final total).
        static async Task<(bool txAAborted, bool txBCommitted, long finalTotal)>
            RunWriteSkewScenarioAsync(DatabaseDescriptor database, CommandExecutor executor,
                string db, string aliceId, string bobId)
        {
            KvTransaction txA = await database.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
            KvTransaction txB = await database.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

            // Both read both accounts.
            QueryTicket readTicketA = new(txnState: txA, databaseName: db,
                tableName: "accounts", index: null, projection: null, where: null,
                filters: new() { new("id", "=", new(ColumnType.Id, aliceId)) },
                orderBy: null, limit: null, offset: null, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursorA) = await executor.Query(readTicketA);
            await cursorA.ToListAsync();

            QueryTicket readTicketA2 = new(txnState: txA, databaseName: db,
                tableName: "accounts", index: null, projection: null, where: null,
                filters: new() { new("id", "=", new(ColumnType.Id, bobId)) },
                orderBy: null, limit: null, offset: null, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursorA2) = await executor.Query(readTicketA2);
            await cursorA2.ToListAsync();

            QueryTicket readTicketB = new(txnState: txB, databaseName: db,
                tableName: "accounts", index: null, projection: null, where: null,
                filters: new() { new("id", "=", new(ColumnType.Id, aliceId)) },
                orderBy: null, limit: null, offset: null, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursorB) = await executor.Query(readTicketB);
            await cursorB.ToListAsync();

            QueryTicket readTicketB2 = new(txnState: txB, databaseName: db,
                tableName: "accounts", index: null, projection: null, where: null,
                filters: new() { new("id", "=", new(ColumnType.Id, bobId)) },
                orderBy: null, limit: null, offset: null, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursorB2) = await executor.Query(readTicketB2);
            await cursorB2.ToListAsync();

            bool txAAborted = false;
            bool txBCommitted = false;

            try
            {
                await executor.Update(new UpdateTicket(
                    txnState: txA, databaseName: db, tableName: "accounts",
                    plainValues: new() { { "balance", new(ColumnType.Integer64, -50L) } },
                    exprValues: null, where: null,
                    filters: new() { new("id", "=", new(ColumnType.Id, aliceId)) },
                    parameters: null));
                await database.Transactions.CommitAsync(txA);
            }
            catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.TransactionConflict)
            {
                txAAborted = true;
                await database.Transactions.RollbackAsync(txA);
            }

            try
            {
                await executor.Update(new UpdateTicket(
                    txnState: txB, databaseName: db, tableName: "accounts",
                    plainValues: new() { { "balance", new(ColumnType.Integer64, -50L) } },
                    exprValues: null, where: null,
                    filters: new() { new("id", "=", new(ColumnType.Id, bobId)) },
                    parameters: null));
                await database.Transactions.CommitAsync(txB);
                txBCommitted = true;
            }
            catch
            {
                await database.Transactions.RollbackAsync(txB);
            }

            KvTransaction verify = await database.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
            QueryTicket verifyA = new(txnState: verify, databaseName: db,
                tableName: "accounts", index: null, projection: null, where: null,
                filters: null, orderBy: null, limit: null, offset: null, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> verifyCursor) = await executor.Query(verifyA);
            List<QueryResultRow> rows = await verifyCursor.ToListAsync();
            await database.Transactions.CommitAsync(verify);

            long finalTotal = rows.Sum(r => r.Row["balance"].LongValue);
            return (txAAborted, txBCommitted, finalTotal);
        }

        // Run at N=1.
        (EmbeddedKahuna singleNode, string n1Db, CommandExecutor n1Executor,
            DatabaseDescriptor n1Database, string n1AliceId, string n1BobId) =
            await SetupSingleNodeAccountsAsync();
        await using EmbeddedKahuna __ = singleNode;

        (bool n1TxAAborted, bool n1TxBCommitted, long n1FinalTotal) =
            await RunWriteSkewScenarioAsync(n1Database, n1Executor, n1Db, n1AliceId, n1BobId);

        // Run at N=3.
        (InProcessSchemaCluster cluster3, string n3Db, InProcessSchemaCluster.Node n3Leader,
            string n3AliceId, string n3BobId) = await SetupClusterAccountsAsync();
        await using InProcessSchemaCluster ___ = cluster3;

        (bool n3TxAAborted, bool n3TxBCommitted, long n3FinalTotal) =
            await RunWriteSkewScenarioAsync(n3Leader.Database!, n3Leader.Executor,
                n3Db, n3AliceId, n3BobId);

        // Assert identical outcomes.
        Assert.AreEqual(n1TxAAborted,    n3TxAAborted,
            "TxA abort/commit outcome must be identical at N=1 and N=3");
        Assert.AreEqual(n1TxBCommitted,  n3TxBCommitted,
            "TxB abort/commit outcome must be identical at N=1 and N=3");
        Assert.GreaterOrEqual(n1FinalTotal, 0L,
            "N=1: constraint total >= 0 must hold");
        Assert.GreaterOrEqual(n3FinalTotal, 0L,
            "N=3: constraint total >= 0 must hold");
        Assert.IsTrue(n1TxAAborted,
            "TxA must abort in write-skew scenario (S→X upgrade conflict when TxB holds S on same row)");
        Assert.IsTrue(n1TxBCommitted,
            "TxB must commit after TxA aborts");
    }
}
