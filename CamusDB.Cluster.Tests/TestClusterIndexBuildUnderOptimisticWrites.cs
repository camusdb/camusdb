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
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kahuna.Shared.KeyValue;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// <c>CREATE INDEX</c> on a table that optimistic single-row UPDATEs and two-row transfers keep
/// changing from every node of a three-node cluster, at Read Committed: the production shape in
/// which an index came out with entries at values its rows no longer held. Writers run from before
/// the statement starts until after the index is Public, so some are in flight across the
/// <c>DeleteOnly → WriteOnly</c> step and across every backfill batch. Afterwards every node must
/// hold exactly one entry per row, at the row's current value.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster. Concurrent clusters contend for ports and skew
// each other's Raft election timing.
[NonParallelizable]
public sealed class TestClusterIndexBuildUnderOptimisticWrites
{
    private const int Rows = 600;

    private const int WritersPerNode = 2;

    private const long SeedBalance = 1_000_000;

    [Test]
    public async Task IndexBuiltUnderOptimisticWritesHoldsOneEntryPerRowAtItsCurrentValueOnEveryNode()
    {
        CamusDBOptions options = CamusDBOptions.Default with
        {
            DefaultTransactionLocking = KeyValueTransactionLocking.Optimistic,
            DefaultIsolationLevel = CamusIsolationLevel.ReadCommitted,
        };

        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3, wireLeaderForwarder: true, options: options);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);
        await SeedAsync(cluster, db);

        ConcurrentDictionary<long, long> applied = new();
        ConcurrentDictionary<long, int> unresolved = new();
        ConcurrentDictionary<string, int> refusals = new();
        using CancellationTokenSource stop = new();

        List<Task> writers = [];
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            for (int w = 0; w < WritersPerNode; w++)
            {
                int seed = node.Index * 100 + w;
                writers.Add(Task.Run(() => WriteUntilStopped(node, db, seed, applied, unresolved, refusals, stop.Token)));
            }
        }

        try
        {
            // Writers are already running when the statement starts, so some are in flight across
            // the first state steps; they keep running through the backfill and past Public.
            await Task.Delay(500);

            InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
            await DdlAsync(leader, db, "CREATE INDEX acct_balance ON accounts (balance) INCLUDE (version)");
            await cluster.WaitForSchemaConvergenceAsync(db, leader.Database!.Schema.SchemaVersion, timeout: TimeSpan.FromSeconds(60));

            await Task.Delay(500);
        }
        finally
        {
            stop.Cancel();
            await Task.WhenAll(writers);
        }

        TestContext.Progress.WriteLine($"applied {applied.Values.Sum()} updates, unresolved {unresolved.Values.Sum()}, refusals {string.Join(", ", refusals.Select(kv => $"{kv.Key}={kv.Value}"))}");
        Assert.That(applied.Values.Sum(), Is.GreaterThan(0), "the writers must have run");

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            Dictionary<ObjectIdValue, (long id, long balance)> rows = await TableRowsAsync(node, db);
            Assert.That(rows, Has.Count.EqualTo(Rows), $"node {node.Index}: every row is there");

            foreach ((long id, long balance) in rows.Values)
            {
                long expected = SeedBalance + id + applied.GetValueOrDefault(id);
                long slack = unresolved.GetValueOrDefault(id);
                Assert.That(balance, Is.InRange(expected - slack, expected + slack), $"node {node.Index}: row {id} holds every committed transfer");
            }

            List<(long balance, ObjectIdValue rowId)> entries = await IndexEntriesAsync(node, db);
            Assert.That(entries, Has.Count.EqualTo(Rows), $"node {node.Index}: one entry per row, no stale entry");
            Assert.That(entries.Select(e => e.rowId).Distinct().Count(), Is.EqualTo(Rows), $"node {node.Index}: no row is indexed twice");

            foreach ((long balance, ObjectIdValue rowId) in entries)
            {
                Assert.That(rows.ContainsKey(rowId), Is.True, $"node {node.Index}: an entry names a row that exists");
                Assert.That(balance, Is.EqualTo(rows[rowId].balance), $"node {node.Index}: the entry of row {rows[rowId].id} holds its current balance");
            }

            // And the index answers a lookup for a current value with the row, once.
            (long id, long balance) probe = rows.Values.First();
            Assert.That(await IdsAsync(node, db, $"SELECT id FROM accounts@{{FORCE_INDEX=acct_balance}} WHERE balance = {probe.balance}"), Does.Contain(probe.id), $"node {node.Index}");
        }
    }

    /// <summary>
    /// Alternates single-row increments and two-row transfers between random rows, one transaction at a
    /// time, until stopped. A retryable refusal is run again; a commit whose outcome is unknown is
    /// counted as possibly applied, on both rows of a transfer.
    /// </summary>
    private static async Task WriteUntilStopped(
        InProcessSchemaCluster.Node node, string db, int seed,
        ConcurrentDictionary<long, long> applied, ConcurrentDictionary<long, int> unresolved,
        ConcurrentDictionary<string, int> refusals, CancellationToken stop)
    {
        Random random = new(seed);
        bool transfer = false;

        while (!stop.IsCancellationRequested)
        {
            transfer = !transfer;
            long a = random.Next(1, Rows + 1);
            long b = random.Next(1, Rows + 1);
            if (transfer && a == b)
                continue;

            KvTransaction tx = await node.Database!.Transactions.BeginAsync();
            try
            {
                if (transfer)
                {
                    await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, $"UPDATE accounts SET balance = balance - 1, version = version + 1 WHERE id = {a}", null));
                    await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, $"UPDATE accounts SET balance = balance + 1, version = version + 1 WHERE id = {b}", null));
                }
                else
                {
                    await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, $"UPDATE accounts SET balance = balance + 1, version = version + 1 WHERE id = {a}", null));
                }

                await node.Database.Transactions.CommitAsync(tx);

                if (transfer)
                {
                    applied.AddOrUpdate(a, -1, static (_, n) => n - 1);
                    applied.AddOrUpdate(b, 1, static (_, n) => n + 1);
                }
                else
                {
                    applied.AddOrUpdate(a, 1, static (_, n) => n + 1);
                }
            }
            catch (CamusDBException ex) when (SerializableRetryHelper.IsRetryable(ex))
            {
                refusals.AddOrUpdate(ex.Code, 1, static (_, n) => n + 1);
                await Task.Delay(1);
            }
            catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.TransactionFinalizeUnresolved)
            {
                refusals.AddOrUpdate(ex.Code, 1, static (_, n) => n + 1);
                unresolved.AddOrUpdate(a, 1, static (_, n) => n + 1);
                if (transfer)
                    unresolved.AddOrUpdate(b, 1, static (_, n) => n + 1);
            }
            finally
            {
                await node.Database!.Transactions.RollbackIfNotCompletedAsync(tx);
            }
        }
    }

    private static async Task SeedAsync(InProcessSchemaCluster cluster, string db)
    {
        long version = 0;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await DdlAsync(leader, db, "CREATE TABLE accounts (id INT64 PRIMARY KEY NOT NULL, balance INT64 NOT NULL, version INT64 NOT NULL)");

            const int rowsPerStatement = 200;
            for (int start = 1; start <= Rows; start += rowsPerStatement)
            {
                int end = Math.Min(Rows, start + rowsPerStatement - 1);
                string values = string.Join(", ", Enumerable.Range(start, end - start + 1).Select(i => $"({i}, {SeedBalance + i}, 0)"));
                await DmlAsync(leader, db, "INSERT INTO accounts (id, balance, version) VALUES " + values);
            }

            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(30));
    }

    private static Task DdlAsync(InProcessSchemaCluster.Node node, string db, string sql) =>
        node.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null))
            .WaitAsync(TimeSpan.FromSeconds(180));

    private static async Task DmlAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));
            await node.Database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> RowsAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

            List<QueryResultRow> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(row);
            return rows;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<long>> IdsAsync(InProcessSchemaCluster.Node node, string db, string sql)
        => (await RowsAsync(node, db, sql)).Select(r => r.Row["id"].LongValue).ToList();

    /// <summary>Every row of the table as this node reads it, by row id.</summary>
    private static async Task<Dictionary<ObjectIdValue, (long id, long balance)>> TableRowsAsync(InProcessSchemaCluster.Node node, string db)
        => (await RowsAsync(node, db, "SELECT id, balance FROM accounts")).ToDictionary(r => r.RowId, r => (r.Row["id"].LongValue, r.Row["balance"].LongValue));

    /// <summary>Every entry of the new index as this node's store holds it.</summary>
    private static async Task<List<(long balance, ObjectIdValue rowId)>> IndexEntriesAsync(InProcessSchemaCluster.Node node, string db)
    {
        TableIndexSchema index = node.Database!.Schema.Tables["accounts"].Indexes!.First(ix => ix.Name == "acct_balance");
        TableDescriptor table = await node.Executor.OpenTable(new OpenTableTicket(db, "accounts"));

        KvTransaction tx = await node.Database.Transactions.BeginAsync();
        try
        {
            List<(long, ObjectIdValue)> entries = [];
            await foreach ((CompositeColumnValue key, ObjectIdValue rowId, _) in table.Store.ScanIndex(tx, index.KvId, [ColumnType.Integer64], from: null, to: null, unique: false))
                entries.Add((key.Values[0].LongValue, rowId));
            return entries;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }
}
