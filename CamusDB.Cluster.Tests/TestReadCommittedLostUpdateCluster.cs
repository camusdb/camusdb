/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// An expression UPDATE that reads the column it writes must not lose a concurrent committed update, at any
/// isolation level. Elle found committed appends lost at read committed (Caraxes <c>append-read-committed</c>: G0 and
/// incompatible-order on 41 keys): two transactions ran <c>UPDATE … SET v = concat(v, …)</c> on the same row, both
/// committed, and one append was gone.
///
/// <para>Many workers append a unique token to one row concurrently, each in its own transaction. Every token whose
/// transaction committed must be in the final value exactly once. A token whose commit threw may or may not be there;
/// no token may be there twice, and no token may be there that no worker wrote.</para>
///
/// <para>Losing an update here is not the read-committed "lost update" that an application causes by reading a value
/// in one statement and writing it in another. The read and the write are one statement, and the engine does both.</para>
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster. Concurrent clusters contend for ports and skew
// each other's Raft election timing, which shows up as spurious leadership churn.
[NonParallelizable]
public sealed class TestReadCommittedLostUpdateCluster
{
    private const int Workers = 8;

    private const int AppendsPerWorker = 40;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(
        builder => builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    /// <summary>A 3-node cluster with <c>lists (k STRING PRIMARY KEY, v STRING)</c> holding one row, <c>a = 'seed'</c>.</summary>
    private static async Task<(InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node node)> SetupListsAsync()
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: 1,
                loggerFactory: sharedLoggerFactory, logger: logger);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "lists",
            columns:
            [
                new ColumnInfo("k", ColumnType.String, notNull: true),
                new ColumnInfo("v", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("k", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            KvTransaction setup = await leader.Database!.Transactions.BeginAsync();
            await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: setup, database: db,
                sql: "INSERT INTO lists (k, v) VALUES ('a', 'seed')", parameters: null));
            await leader.Database.Transactions.CommitAsync(setup);
        });

        InProcessSchemaCluster.Node node = await cluster.WaitForSchemaLeaderNodeAsync(db);
        return (cluster, db, node);
    }

    /// <summary>
    /// The deterministic form. T2 runs <c>UPDATE lists SET v = concat(v, ',t2')</c>. After T2's locate scan has
    /// read the row without a lock, and before T2's write phase locks it, T1 runs the same UPDATE with <c>',t1'</c>
    /// and commits. Then T2 continues and commits. When both commit, the final value must hold both appends. The
    /// defect was that T2 computed its new value from a row it read before it held the lock, so T1's committed
    /// append was overwritten. T2 now locks the row, reads it again, and computes from that read; the lock itself
    /// is covered by <see cref="Cluster_LockAndReadRowsForMutation_HoldsTheRowAgainstOtherWriters"/>.
    /// </summary>
    [TestCase(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic, false)]
    [TestCase(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic, false)]
    [TestCase(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic, false)]
    [TestCase(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic, true)]
    [TestCase(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic, true)]
    [TestCase(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic, true)]
    public async Task Cluster_ExpressionUpdate_CommitBetweenReadAndWrite_IsNotLost(
        CamusIsolationLevel isolation, KeyValueTransactionLocking locking, bool t1OnAnotherNode)
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node node) = await SetupListsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        DatabaseDescriptor database = node.Database!;
        CommandExecutor executor = node.Executor;
        RowUpdater updater = executor.RowUpdaterForTests;

        // With t1OnAnotherNode, T1 runs through a node that does not lead the table's partition, so its reads and
        // writes are forwarded, as they are for most requests in a multi-node deployment.
        InProcessSchemaCluster.Node t1Node = t1OnAnotherNode ? cluster.Nodes.First(n => !ReferenceEquals(n, node)) : node;

        async Task<bool> AppendAndCommit(string token, string who, InProcessSchemaCluster.Node on)
        {
            KvTransaction tx = await on.Database!.Transactions.BeginAsync(
                isolation, CamusTransactionMode.ReadWrite, locking: locking);
            try
            {
                await on.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx, database: db,
                    sql: $"UPDATE lists SET v = concat(v, ',{token}') WHERE k = 'a'", parameters: null));
                await on.Database.Transactions.CommitAsync(tx);
                return true;
            }
            catch (CamusDBException e)
            {
                TestContext.Out.WriteLine($"{who} refused: {e.Code}: {e.Message}");
                try { await on.Database.Transactions.RollbackAsync(tx); }
                catch (CamusDBException) { /* already aborted */ }
                return false;
            }
        }

        bool t1Committed = false;
        bool hookRan = false;

        // One-shot: T1's own UPDATE goes through the same controller and must not re-enter the hook.
        updater.TestBeforeWriteHook = async () =>
        {
            updater.TestBeforeWriteHook = null;
            hookRan = true;
            t1Committed = await AppendAndCommit("t1", "T1", t1Node);
        };

        bool t2Committed;
        try
        {
            t2Committed = await AppendAndCommit("t2", "T2", node);
        }
        finally
        {
            updater.TestBeforeWriteHook = null;
        }

        KvTransaction read = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: read, database: db, sql: "SELECT v FROM lists WHERE k = 'a'", parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(read);
        string final = rows[0].Row["v"].StrValue!;

        TestContext.Out.WriteLine($"{isolation}/{locking}/t1OnAnotherNode={t1OnAnotherNode}: T1 committed={t1Committed}, T2 committed={t2Committed}, final='{final}'");

        Assert.That(hookRan, Is.True, "T2 never reached the window between its read and its write");
        List<string> tokens = final.Split(',').ToList();
        if (t1Committed)
            Assert.That(tokens, Does.Contain("t1"), $"T1 committed its append, but the final value is '{final}'");
        if (t2Committed)
            Assert.That(tokens, Does.Contain("t2"), $"T2 committed its append, but the final value is '{final}'");
    }

    /// <summary>Runs one statement in its own transaction and commits it. A refusal rolls back and reports false.</summary>
    private static async Task<(bool committed, int modified)> RunAndCommitAsync(
        InProcessSchemaCluster.Node on, string db, string sql, string who,
        CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        KvTransaction tx = await on.Database!.Transactions.BeginAsync(
            isolation, CamusTransactionMode.ReadWrite, locking: locking);
        try
        {
            ExecuteNonSQLResult result = await on.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: tx, database: db, sql: sql, parameters: null));
            await on.Database.Transactions.CommitAsync(tx);
            return (true, result.ModifiedRows);
        }
        catch (CamusDBException e)
        {
            TestContext.Out.WriteLine($"{who} refused: {e.Code}: {e.Message}");
            try { await on.Database.Transactions.RollbackAsync(tx); }
            catch (CamusDBException) { /* already aborted */ }
            return (false, 0);
        }
    }

    /// <summary>The committed values of <c>lists.v</c>, read at read committed.</summary>
    private static async Task<List<string>> ReadValuesAsync(InProcessSchemaCluster.Node node, string db)
    {
        KvTransaction read = await node.Database!.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: read, database: db, sql: "SELECT v FROM lists", parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await node.Database.Transactions.CommitAsync(read);
        return rows.Select(r => r.Row["v"].StrValue!).ToList();
    }

    /// <summary>
    /// The competing commit makes the located row stop matching the UPDATE's WHERE. The write phase re-checks the
    /// predicate on the row it read under lock, so it leaves the row alone: the UPDATE reports no modified row and
    /// the other transaction's value survives. Without the re-check the UPDATE would append to a row that no longer
    /// qualifies.
    /// </summary>
    [TestCase(KeyValueTransactionLocking.Pessimistic, false)]
    [TestCase(KeyValueTransactionLocking.Pessimistic, true)]
    [TestCase(KeyValueTransactionLocking.Optimistic, true)]
    public async Task Cluster_Update_RowMovedOutOfWhereBeforeLock_IsNotUpdated(
        KeyValueTransactionLocking locking, bool t1OnAnotherNode)
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node node) = await SetupListsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        RowUpdater updater = node.Executor.RowUpdaterForTests;
        InProcessSchemaCluster.Node t1Node = t1OnAnotherNode ? cluster.Nodes.First(n => !ReferenceEquals(n, node)) : node;

        (bool committed, int modified) t1 = default;
        updater.TestBeforeWriteHook = async () =>
        {
            updater.TestBeforeWriteHook = null;
            t1 = await RunAndCommitAsync(t1Node, db, "UPDATE lists SET v = 'moved' WHERE k = 'a'", "T1",
                CamusIsolationLevel.ReadCommitted, locking);
        };

        (bool committed, int modified) t2;
        try
        {
            t2 = await RunAndCommitAsync(node, db, "UPDATE lists SET v = concat(v, ',t2') WHERE v = 'seed'", "T2",
                CamusIsolationLevel.ReadCommitted, locking);
        }
        finally
        {
            updater.TestBeforeWriteHook = null;
        }

        List<string> final = await ReadValuesAsync(node, db);
        TestContext.Out.WriteLine($"{locking}/t1OnAnotherNode={t1OnAnotherNode}: T1={t1}, T2={t2}, final=[{string.Join("|", final)}]");

        Assert.That(t1.committed, Is.True, "T1 must commit: T2 holds no lock on the row before its write phase");
        Assert.That(final, Is.EqualTo(new[] { "moved" }), "T2 updated a row that no longer matched its WHERE");
        if (t2.committed)
            Assert.That(t2.modified, Is.EqualTo(0), "T2 reported a row that it must have skipped");
    }

    /// <summary>
    /// The competing commit deletes the located row. The UPDATE finds nothing to change: it reports no modified row
    /// and does not fail with an internal error.
    /// </summary>
    [TestCase(false)]
    [TestCase(true)]
    public async Task Cluster_Update_RowDeletedBeforeLock_UpdatesNothing(bool t1OnAnotherNode)
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node node) = await SetupListsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        RowUpdater updater = node.Executor.RowUpdaterForTests;
        InProcessSchemaCluster.Node t1Node = t1OnAnotherNode ? cluster.Nodes.First(n => !ReferenceEquals(n, node)) : node;

        (bool committed, int modified) t1 = default;
        updater.TestBeforeWriteHook = async () =>
        {
            updater.TestBeforeWriteHook = null;
            t1 = await RunAndCommitAsync(t1Node, db, "DELETE FROM lists WHERE k = 'a'", "T1",
                CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic);
        };

        (bool committed, int modified) t2;
        try
        {
            t2 = await RunAndCommitAsync(node, db, "UPDATE lists SET v = concat(v, ',t2') WHERE k = 'a'", "T2",
                CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic);
        }
        finally
        {
            updater.TestBeforeWriteHook = null;
        }

        List<string> final = await ReadValuesAsync(node, db);
        TestContext.Out.WriteLine($"t1OnAnotherNode={t1OnAnotherNode}: T1={t1}, T2={t2}, final=[{string.Join("|", final)}]");

        Assert.That(t1.committed, Is.True);
        Assert.That(t2.committed, Is.True, "an UPDATE whose located row was deleted concurrently must not fail");
        Assert.That(t2.modified, Is.EqualTo(0));
        Assert.That(final, Is.Empty, "the UPDATE must not bring the deleted row back");
    }

    /// <summary>
    /// The competing commit makes the located row stop matching the DELETE's WHERE. The DELETE re-checks the
    /// predicate on the row it read under lock and keeps the row.
    /// </summary>
    [TestCase(false)]
    [TestCase(true)]
    public async Task Cluster_Delete_RowMovedOutOfWhereBeforeLock_IsKept(bool t1OnAnotherNode)
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node node) = await SetupListsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        RowDeleter deleter = node.Executor.RowDeleterForTests;
        InProcessSchemaCluster.Node t1Node = t1OnAnotherNode ? cluster.Nodes.First(n => !ReferenceEquals(n, node)) : node;

        (bool committed, int modified) t1 = default;
        deleter.TestBeforeWriteHook = async () =>
        {
            deleter.TestBeforeWriteHook = null;
            t1 = await RunAndCommitAsync(t1Node, db, "UPDATE lists SET v = 'moved' WHERE k = 'a'", "T1",
                CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic);
        };

        (bool committed, int modified) t2;
        try
        {
            t2 = await RunAndCommitAsync(node, db, "DELETE FROM lists WHERE v = 'seed'", "T2",
                CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic);
        }
        finally
        {
            deleter.TestBeforeWriteHook = null;
        }

        List<string> final = await ReadValuesAsync(node, db);
        TestContext.Out.WriteLine($"t1OnAnotherNode={t1OnAnotherNode}: T1={t1}, T2={t2}, final=[{string.Join("|", final)}]");

        Assert.That(t1.committed, Is.True);
        Assert.That(t2.committed, Is.True);
        Assert.That(t2.modified, Is.EqualTo(0), "the DELETE removed a row that no longer matched its WHERE");
        Assert.That(final, Is.EqualTo(new[] { "moved" }));
    }

    /// <summary>
    /// The contract of the write-phase read: a pessimistic read-committed transaction holds the exclusive row lock
    /// once <see cref="Core.Storage.Kv.KvTableStore.LockAndReadRowsForMutationAsync"/> returns, so a writer on any
    /// node is refused until it ends. This is what makes the value computed from that read safe to write.
    /// </summary>
    [TestCase(false)]
    [TestCase(true)]
    public async Task Cluster_LockAndReadRowsForMutation_HoldsTheRowAgainstOtherWriters(bool t1OnAnotherNode)
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node node) = await SetupListsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        InProcessSchemaCluster.Node t1Node = t1OnAnotherNode ? cluster.Nodes.First(n => !ReferenceEquals(n, node)) : node;
        TableDescriptor table = await node.Database!.TableDescriptors["lists"];

        KvTransaction t2 = await node.Database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, locking: KeyValueTransactionLocking.Pessimistic);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: t2, database: db, sql: "SELECT v FROM lists WHERE k = 'a'", parameters: null));
        List<QueryResultRow> located = await cursor.ToListAsync();
        Assert.That(located, Has.Count.EqualTo(1));

        ReadOnlyMemory<byte>?[] read = await table.Store.LockAndReadRowsForMutationAsync(t2, [located[0].RowId]);
        Assert.That(read[0], Is.Not.Null);

        (bool committed, int modified) t1 = await RunAndCommitAsync(t1Node, db, "UPDATE lists SET v = 'moved' WHERE k = 'a'", "T1",
            CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic);

        await node.Database.Transactions.RollbackAsync(t2);

        Assert.That(t1.committed, Is.False, "a writer changed a row that another transaction had locked for its write phase");
        Assert.That(await ReadValuesAsync(node, db), Is.EqualTo(new[] { "seed" }));
    }

    [TestCase(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic)]
    [TestCase(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic)]
    [TestCase(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic)]
    public async Task Cluster_ConcurrentExpressionUpdates_KeepEveryCommittedAppend(
        CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node node) = await SetupListsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        DatabaseDescriptor database = node.Database!;
        CommandExecutor executor = node.Executor;

        ConcurrentBag<string> committed = [];
        ConcurrentBag<string> uncertain = [];
        ConcurrentDictionary<string, int> refusals = new();

        async Task Worker(int worker)
        {
            for (int i = 0; i < AppendsPerWorker; i++)
            {
                string token = $"w{worker}-{i}";
                KvTransaction tx = await database.Transactions.BeginAsync(
                    isolation, CamusTransactionMode.ReadWrite, locking: locking);

                try
                {
                    ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                        txnState: tx, database: db,
                        sql: $"UPDATE lists SET v = concat(v, ',{token}') WHERE k = 'a'", parameters: null));
                    Assert.That(result.ModifiedRows, Is.EqualTo(1), $"append {token} must modify the one row");
                }
                catch (CamusDBException e)
                {
                    refusals.AddOrUpdate(e.Code, 1, (_, n) => n + 1);
                    try { await database.Transactions.RollbackAsync(tx); }
                    catch (CamusDBException) { /* already aborted */ }
                    continue;
                }

                try
                {
                    await database.Transactions.CommitAsync(tx);
                    committed.Add(token);
                }
                catch (CamusDBException e)
                {
                    // A commit that threw is counted as possibly applied, so the check can never blame an
                    // ambiguous outcome.
                    refusals.AddOrUpdate("commit:" + e.Code, 1, (_, n) => n + 1);
                    uncertain.Add(token);
                }
            }
        }

        await Task.WhenAll(Enumerable.Range(0, Workers).Select(Worker));

        KvTransaction read = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: read, database: db, sql: "SELECT v FROM lists WHERE k = 'a'", parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(read);

        Assert.That(rows, Has.Count.EqualTo(1));
        List<string> final = rows[0].Row["v"].StrValue!.Split(',').Skip(1).ToList();

        HashSet<string> finalSet = [.. final];
        List<string> lost = committed.Where(t => !finalSet.Contains(t)).OrderBy(t => t).ToList();
        List<string> duplicated = final.GroupBy(t => t).Where(g => g.Count() > 1).Select(g => g.Key).ToList();
        HashSet<string> written = [.. committed, .. uncertain];
        List<string> unknown = final.Where(t => !written.Contains(t)).ToList();

        TestContext.Out.WriteLine(
            $"{isolation}/{locking}: committed={committed.Count} uncertain={uncertain.Count} final={final.Count} " +
            $"lost={lost.Count} duplicated={duplicated.Count} unknown={unknown.Count} " +
            $"refusals=[{string.Join(", ", refusals.Select(r => $"{r.Key}:{r.Value}"))}]");

        Assert.That(committed.Count, Is.GreaterThan(0), "no append committed; the test proved nothing");
        Assert.That(lost, Is.Empty,
            $"{lost.Count} committed append(s) are missing from the final value, e.g. {string.Join(", ", lost.Take(10))}");
        Assert.That(duplicated, Is.Empty, $"appended more than once: {string.Join(", ", duplicated)}");
        Assert.That(unknown, Is.Empty, $"present but written by no worker: {string.Join(", ", unknown)}");
    }
}
