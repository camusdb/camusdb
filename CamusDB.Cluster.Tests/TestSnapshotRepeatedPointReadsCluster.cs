/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Stress reproduction of the Elle "internal" anomaly the Caraxes list-append workload reported: a
/// Serializable read-only transaction read one list twice and got two different states (a list that
/// grew, shrank, or disappeared inside one snapshot).
///
/// <para>The shape mirrors the workload: <c>lists (k STRING PRIMARY KEY, v STRING)</c>, appends as
/// <c>UPDATE … SET v = concat(v, ',x')</c> with an <c>INSERT</c> for a new list, read-write transactions
/// spread across every node, and read-only Serializable transactions that read a few lists with the
/// same list read more than once. Young lists (created during the run) are read soon after creation,
/// because four of the eight reported cases were lists read right after they were created.</para>
///
/// <para>The invariant: inside one read-only Serializable transaction every read of a key returns the
/// same value. A single mismatch is a failure. For each mismatch the output names the direction (grow,
/// shrink, vanish), when the missing append's commit started and returned relative to the first read,
/// and what Kahuna itself answers for the raw row key at the snapshot timestamp right away and again
/// 300 ms, 1.5 s and 4 s later. That raw probe separates a CamusDB read-path fault from a Kahuna one.</para>
///
/// <para>The defect is in Kahuna's snapshot read path (the prepared-intent overlay answers before the
/// safe-time wait, a later commit can be stamped at or below the snapshot, and the persisted-history
/// fallback lags the flush). This test stays red until those fixes ship; the deferred-settlement arm
/// fails about ten times more often than the inline arm because the overlay is the dominant cause.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSnapshotRepeatedPointReadsCluster
{
    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    private sealed record Mismatch(string Key, int Node, string SnapshotT, string? First, string? Second, int Position,
        long FirstReadStart, long FirstReadEnd, long SecondReadStart, long SecondReadEnd);

    /// <summary>When one appended value's commit started and when its commit call returned, in stopwatch ticks.</summary>
    private sealed record AppendTiming(int WriterNode, long CommitStart, long CommitEnd);

    /// <param name="deferredSettlement">Kahuna's default (true) settles a committed durable intent off the
    /// commit path, so the previous append's intent is still in the intent store while the next append
    /// commits. False settles inline, which removes that overlap; comparing the two arms isolates it.</param>
    [TestCase(true)]
    [TestCase(false)]
    public async Task Cluster_SerializableRO_RepeatedPointReads_AreStable_UnderConcurrentAppends(bool deferredSettlement)
    {
        await using InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: 3,
                loggerFactory: sharedLoggerFactory, logger: logger,
                configureNode: o => o.DurableDeferredSettlement = deferredSettlement);

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

        const int hotKeys = 8;
        const int writers = 6;
        const int readers = 6;
        TimeSpan duration = TimeSpan.FromSeconds(20);

        string[] hot = Enumerable.Range(0, hotKeys).Select(i => $"h{i}").ToArray();
        ConcurrentQueue<string> young = new();
        int youngCounter = 0;
        long appends = 0, inserts = 0, writeConflicts = 0, reads = 0, readErrors = 0;
        ConcurrentBag<Mismatch> mismatches = [];
        ConcurrentDictionary<string, AppendTiming> appendTimings = new(StringComparer.Ordinal);
        ConcurrentBag<string> probes = [];
        List<Task> probeTasks = [];
        object probeGate = new();

        // One open table descriptor per node, so a mismatch can compute the row's raw Kahuna key.
        TableDescriptor[] tables = new TableDescriptor[cluster.Nodes.Length];
        for (int i = 0; i < cluster.Nodes.Length; i++)
            tables[i] = await cluster.Nodes[i].Executor.OpenTable(new OpenTableTicket(db, "lists"));

        // Reads the raw row key straight from Kahuna at the snapshot timestamp, several times over the
        // next seconds, so the log shows which revision Kahuna serves at T and whether that answer moves.
        async Task ProbeAsync(string key, int nodeIndex, HLCTimestamp snapshotT, ObjectIdValue rowId, int firstLen, int secondLen)
        {
            string rowKey = tables[nodeIndex].Store.Keys.BuildRowKey(rowId);
            int[] delaysMs = [0, 300, 1500, 4000];
            System.Text.StringBuilder sb = new();
            sb.Append($"probe key={key} node={nodeIndex} T={snapshotT} len {firstLen}->{secondLen}:");
            long start = Stopwatch.GetTimestamp();
            foreach (int delay in delaysMs)
            {
                int elapsed = (int)((Stopwatch.GetTimestamp() - start) * 1000 / Stopwatch.Frequency);
                if (delay > elapsed)
                    await Task.Delay(delay - elapsed);
                try
                {
                    (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await cluster.Nodes[nodeIndex].Kahuna.Kahuna
                        .LocateAndTryGetValue(HLCTimestamp.Zero, rowKey, -1, snapshotT, KeyValueDurability.Persistent, CancellationToken.None);
                    sb.Append($" [+{delay}ms atT: {type} rev={entry?.Revision} lm={entry?.LastModified} bytes={entry?.Value?.Length}]");
                }
                catch (Exception e)
                {
                    sb.Append($" [+{delay}ms atT: {e.GetType().Name}]");
                }
            }
            try
            {
                (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await cluster.Nodes[nodeIndex].Kahuna.Kahuna
                    .LocateAndTryGetValue(HLCTimestamp.Zero, rowKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None);
                sb.Append($" [latest: {type} rev={entry?.Revision} lm={entry?.LastModified} bytes={entry?.Value?.Length}]");
            }
            catch (Exception e)
            {
                sb.Append($" [latest: {e.GetType().Name}]");
            }
            probes.Add(sb.ToString());
        }

        using CancellationTokenSource stop = new(duration);
        CancellationToken ct = stop.Token;

        string PickKey(Random rng)
        {
            // One in four picks a young list when there is one, so recently created lists are read
            // and appended soon after creation.
            if (rng.Next(4) == 0 && young.TryPeek(out string? y))
                return y;
            return hot[rng.Next(hot.Length)];
        }

        async Task Writer(int id)
        {
            Random rng = new(unchecked(1000 + id * 7919));
            int step = 0;
            while (!ct.IsCancellationRequested)
            {
                InProcessSchemaCluster.Node node = cluster.Nodes[rng.Next(cluster.Nodes.Length)];
                DatabaseDescriptor database = node.Database!;

                // One in ten writes starts a fresh list so young lists keep appearing during the run.
                bool fresh = rng.Next(10) == 0;
                string key = fresh ? $"y{Interlocked.Increment(ref youngCounter)}" : PickKey(rng);
                string value = $"{id}-{step++}";

                KvTransaction? tx = null;
                try
                {
                    tx = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

                    ExecuteNonSQLResult updated = await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                        txnState: tx, database: db,
                        sql: "UPDATE lists SET v = concat(v, @x) WHERE k = @k",
                        parameters: new() { ["@x"] = new(ColumnType.String, "," + value), ["@k"] = new(ColumnType.String, key) }));

                    if (updated.ModifiedRows == 0)
                    {
                        await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                            txnState: tx, database: db,
                            sql: "INSERT INTO lists (k, v) VALUES (@k, @x)",
                            parameters: new() { ["@x"] = new(ColumnType.String, value), ["@k"] = new(ColumnType.String, key) }));
                    }

                    long commitStart = Stopwatch.GetTimestamp();
                    await database.Transactions.CommitAsync(tx);
                    tx = null;
                    appendTimings[value] = new AppendTiming(node.Index, commitStart, Stopwatch.GetTimestamp());

                    if (updated.ModifiedRows == 0)
                    {
                        Interlocked.Increment(ref inserts);
                        young.Enqueue(key);
                        while (young.Count > 4 && young.TryDequeue(out _)) { }
                    }
                    else
                        Interlocked.Increment(ref appends);
                }
                catch (CamusDBException)
                {
                    Interlocked.Increment(ref writeConflicts);
                    if (tx is not null)
                    {
                        try { await database.Transactions.RollbackAsync(tx); } catch (CamusDBException) { }
                    }
                }
                catch (OperationCanceledException) { break; }
            }
        }

        async Task Reader(int id)
        {
            Random rng = new(unchecked(5000 + id * 104729));
            while (!ct.IsCancellationRequested)
            {
                InProcessSchemaCluster.Node node = cluster.Nodes[rng.Next(cluster.Nodes.Length)];
                DatabaseDescriptor database = node.Database!;

                // Two to four reads, with the first key always read again last, so every transaction
                // holds at least one repeated read.
                int count = 2 + rng.Next(3);
                string[] keys = new string[count];
                for (int i = 0; i < count - 1; i++)
                    keys[i] = PickKey(rng);
                keys[count - 1] = keys[0];

                KvTransaction? tx = null;
                try
                {
                    tx = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
                    string snapshotT = tx.ReadTimestamp.ToString();
                    Dictionary<string, (string? Value, ObjectIdValue RowId, long Start, long End)> seen = new(StringComparer.Ordinal);

                    for (int i = 0; i < count; i++)
                    {
                        long readStart = Stopwatch.GetTimestamp();
                        (string? value, ObjectIdValue rowId) = await ReadListAsync(node.Executor, db, tx, keys[i]);
                        long readEnd = Stopwatch.GetTimestamp();
                        Interlocked.Increment(ref reads);

                        if (seen.TryGetValue(keys[i], out (string? Value, ObjectIdValue RowId, long Start, long End) earlier))
                        {
                            if (!string.Equals(earlier.Value, value, StringComparison.Ordinal))
                            {
                                mismatches.Add(new Mismatch(keys[i], node.Index, snapshotT, earlier.Value, value, i,
                                    earlier.Start, earlier.End, readStart, readEnd));

                                ObjectIdValue probeRow = earlier.Value is null ? rowId : earlier.RowId;
                                int firstLen = earlier.Value is null ? 0 : earlier.Value.Count(c => c == ',') + 1;
                                int secondLen = value is null ? 0 : value.Count(c => c == ',') + 1;
                                lock (probeGate)
                                {
                                    if (probeTasks.Count < 12)
                                        probeTasks.Add(ProbeAsync(keys[i], node.Index, tx.ReadTimestamp, probeRow, firstLen, secondLen));
                                }
                            }
                        }
                        else
                            seen[keys[i]] = (value, rowId, readStart, readEnd);
                    }

                    await database.Transactions.CommitAsync(tx);
                    tx = null;
                }
                catch (CamusDBException)
                {
                    Interlocked.Increment(ref readErrors);
                    if (tx is not null)
                    {
                        try { await database.Transactions.RollbackAsync(tx); } catch (CamusDBException) { }
                    }
                }
                catch (OperationCanceledException) { break; }
            }
        }

        List<Task> tasks = [];
        for (int i = 0; i < writers; i++)
        {
            int writerId = i;
            tasks.Add(Task.Run(() => Writer(writerId)));
        }
        for (int i = 0; i < readers; i++)
        {
            int readerId = i;
            tasks.Add(Task.Run(() => Reader(readerId)));
        }
        await Task.WhenAll(tasks);

        Task[] pendingProbes;
        lock (probeGate)
            pendingProbes = probeTasks.ToArray();
        await Task.WhenAll(pendingProbes).WaitAsync(TimeSpan.FromSeconds(20));

        foreach (string probe in probes)
            TestContext.Out.WriteLine(probe);

        TestContext.Out.WriteLine(
            $"appends={appends} inserts={inserts} writeConflicts={writeConflicts} reads={reads} readErrors={readErrors} mismatches={mismatches.Count}");

        foreach (Mismatch m in mismatches.Take(40))
            TestContext.Out.WriteLine(DescribeMismatch(m, appendTimings));

        Assert.That(reads, Is.GreaterThan(0), "the readers must have read something");
        Assert.That(appends + inserts, Is.GreaterThan(0), "the writers must have committed something");
        Assert.That(mismatches, Is.Empty,
            $"{mismatches.Count} repeated reads inside one Serializable read-only transaction returned different values; first: " +
            string.Join(" | ", mismatches.Take(5).Select(m => $"{m.Key}@node{m.Node} T={m.SnapshotT}: '{m.First}' -> '{m.Second}'")));
    }

    /// <summary>
    /// One line per mismatch: the direction, the list lengths, and for a list that grew, when each extra
    /// element's commit ran relative to the first read (negative = before the first read finished).
    /// </summary>
    private static string DescribeMismatch(Mismatch m, ConcurrentDictionary<string, AppendTiming> timings)
    {
        static int Len(string? v) => string.IsNullOrEmpty(v) ? 0 : v.Count(c => c == ',') + 1;
        static double Ms(long from, long to) => (to - from) * 1000.0 / Stopwatch.Frequency;

        string direction = (m.First, m.Second) switch
        {
            (null, not null) => "appear",
            (not null, null) => "vanish",
            var (a, b) when b!.StartsWith(a!, StringComparison.Ordinal) => "grow",
            var (a, b) when a!.StartsWith(b!, StringComparison.Ordinal) => "shrink",
            _ => "other",
        };

        System.Text.StringBuilder sb = new();
        sb.Append($"mismatch {direction} key={m.Key} readNode={m.Node} T={m.SnapshotT} read#{m.Position} len {Len(m.First)} -> {Len(m.Second)}; " +
                  $"read1 took {Ms(m.FirstReadStart, m.FirstReadEnd):F2}ms, gap to read2 {Ms(m.FirstReadEnd, m.SecondReadStart):F2}ms");

        if (direction == "grow")
        {
            string extra = m.Second!.Substring(m.First!.Length).TrimStart(',');
            foreach (string element in extra.Split(',', StringSplitOptions.RemoveEmptyEntries))
            {
                if (timings.TryGetValue(element, out AppendTiming? t))
                    sb.Append($"; extra '{element}' by node{t.WriterNode}: commit started {Ms(m.FirstReadEnd, t.CommitStart):+0.00;-0.00}ms and returned {Ms(m.FirstReadEnd, t.CommitEnd):+0.00;-0.00}ms after read1 ended");
                else
                    sb.Append($"; extra '{element}': no timing recorded");
            }
        }

        return sb.ToString();
    }

    private static async Task<(string? Value, ObjectIdValue RowId)> ReadListAsync(CommandExecutor executor, string db, KvTransaction tx, string key)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: db, sql: "SELECT v FROM lists WHERE k = @k",
            parameters: new() { ["@k"] = new(ColumnType.String, key) }));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        return rows.Count == 0 ? (null, default) : (rows[0].Row["v"].StrValue, rows[0].RowId);
    }
}
