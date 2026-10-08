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
/// <para>The first defects were in Kahuna's snapshot read path (the prepared-intent overlay answered before
/// the safe-time wait, a later commit could be stamped at or below the snapshot, and the persisted-history
/// fallback lagged the flush); the deferred-settlement arm failed about ten times more often than the inline
/// arm because the overlay was the dominant cause. A later one was the clock fence itself: a snapshot more
/// than five seconds ahead of the serving node's clock, which a forward wall-clock jump on one node mints,
/// was served without the fence. The clock-jump arm and the deterministic test below cover that shape.</para>
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
    /// <param name="forwardClockJumps">True moves one node's hybrid logical clock forward by six seconds
    /// every second, each node in turn, the way a forward wall-clock jump on that node would. A snapshot
    /// minted on the jumped node then leads every other node's clock by more than Kahuna's five-second
    /// clock-fence bound until the next Raft message spreads the jump, and a read served inside that window
    /// without the fence can be followed by a commit stamped inside its snapshot. The jumps are folded into
    /// the clock directly because the wall clock of a test process cannot be moved.</param>
    [TestCase(true, false)]
    [TestCase(false, false)]
    [TestCase(true, true)]
    public async Task Cluster_SerializableRO_RepeatedPointReads_AreStable_UnderConcurrentAppends(bool deferredSettlement, bool forwardClockJumps)
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

        // Moves one node's clock forward by six seconds every second, each node in turn. Every jump leads the
        // cluster's current clock by more than the fence bound, so each one opens a new window in which a
        // snapshot minted on the jumped node is too far ahead of the other nodes to be fenced.
        async Task ClockJumper()
        {
            int jumps = 0;
            while (!ct.IsCancellationRequested)
            {
                try { await Task.Delay(1000, ct); }
                catch (OperationCanceledException) { break; }

                InProcessSchemaCluster.Node node = cluster.Nodes[jumps++ % cluster.Nodes.Length];
                Kommander.IRaft raft = node.Kahuna.Raft;
                HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
                raft.HybridLogicalClock.ReceiveEvent(raft.GetLocalNodeId(), now + 6_000);
            }
            TestContext.Out.WriteLine($"clock jumps applied: {jumps}");
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
        if (forwardClockJumps)
            tasks.Add(Task.Run(ClockJumper));
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
    /// The shape the Caraxes clock-jump scenario produced, made deterministic: a Serializable read-only
    /// snapshot whose timestamp leads every node's clock by twenty seconds, which is what a snapshot minted
    /// on a node whose wall clock jumped forward looks like to the rest of the cluster before the next Raft
    /// message spreads the jump.
    ///
    /// <para>Kahuna must not serve such a read without its clock fence. Served unfenced, the first read
    /// answers the current row while every clock still trails the snapshot, an append committed right after
    /// it is stamped below the snapshot, and the second read at the same snapshot sees the append: two reads
    /// of one key inside one snapshot disagree. The read is refused with MustRetry instead, which CamusDB's
    /// read path retries; the read is served once the jump reaches the serving node, after which an append is
    /// stamped above the snapshot. The invariant is the same either way: both reads return the same list.</para>
    ///
    /// <para>The snapshot is shifted on a copy of the transaction rather than minted on a jumped node, because
    /// the wall clock of a test process cannot be moved; the jump reaching the other nodes is modeled by folding
    /// the shifted timestamp into every node's clock, which is exactly what their next Raft message from a
    /// jumped leader does.</para>
    /// </summary>
    [Test]
    public async Task Cluster_SerializableRO_SnapshotAheadOfEveryClock_RepeatedPointReadAgreesAcrossConcurrentAppend()
    {
        await using InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: 3,
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

        InProcessSchemaCluster.Node readerNode = cluster.Nodes[0];
        InProcessSchemaCluster.Node writerNode = cluster.Nodes[1];
        const string key = "k";

        await AppendAsync(writerNode, db, key, "a");

        KvTransaction tx = await readerNode.Database!.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        try
        {
            const int leadMs = 20_000;
            KvTransaction shifted = new(
                transactionId:   tx.TransactionId,
                uniqueId:        tx.UniqueId,
                isReadOnly:      true,
                isolationLevel:  CamusIsolationLevel.Serializable,
                transactionMode: CamusTransactionMode.ReadOnly,
                readTimestamp:   tx.ReadTimestamp + leadMs,
                clientId:        tx.ClientId,
                priority:        tx.Priority);

            long start = Stopwatch.GetTimestamp();
            Task<string?> firstRead = ReadListWithRetryAsync(readerNode.Executor, db, shifted, key, TimeSpan.FromSeconds(15));

            // Give an unfenced read time to answer, then append while every clock still trails the snapshot.
            await Task.WhenAny(firstRead, Task.Delay(150));
            await AppendAsync(writerNode, db, key, "b");
            double appendDoneMs = ElapsedMs(start);

            // The jump reaches every node.
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Kommander.IRaft raft = node.Kahuna.Raft;
                raft.HybridLogicalClock.ReceiveEvent(raft.GetLocalNodeId(), shifted.ReadTimestamp);
            }

            string? first = await firstRead;
            double firstDoneMs = ElapsedMs(start);
            string? second = await ReadListWithRetryAsync(readerNode.Executor, db, shifted, key, TimeSpan.FromSeconds(15));

            TestContext.Out.WriteLine(
                $"T={tx.ReadTimestamp} shifted={shifted.ReadTimestamp}; append committed at {appendDoneMs:F0} ms; " +
                $"first read answered at {firstDoneMs:F0} ms with '{first}'; second read '{second}'");

            Assert.That(second, Is.EqualTo(first),
                "two reads of one key inside one Serializable read-only snapshot returned different lists");
        }
        finally
        {
            await readerNode.Database!.Transactions.RollbackAsync(tx);
        }
    }

    private static double ElapsedMs(long from) => (Stopwatch.GetTimestamp() - from) * 1000.0 / Stopwatch.Frequency;

    /// <summary>Appends one element to a list (or creates it) in a committed Serializable read-write transaction.</summary>
    private static async Task AppendAsync(InProcessSchemaCluster.Node node, string db, string key, string value)
    {
        DatabaseDescriptor database = node.Database!;
        KvTransaction tx = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
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

            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            try { await database.Transactions.RollbackAsync(tx); } catch (CamusDBException) { }
            throw;
        }
    }

    /// <summary>
    /// Reads one list, retrying a transient refusal (the read path's own retry budget is about a second and a
    /// half, shorter than the window this test holds the snapshot ahead of the clocks) until the deadline.
    /// </summary>
    private static async Task<string?> ReadListWithRetryAsync(CommandExecutor executor, string db, KvTransaction tx, string key, TimeSpan deadline)
    {
        long start = Stopwatch.GetTimestamp();
        while (true)
        {
            try
            {
                (string? value, _) = await ReadListAsync(executor, db, tx, key);
                return value;
            }
            catch (CamusDBException e) when (e.Code == CamusDBErrorCodes.TransactionMustRetry && ElapsedMs(start) < deadline.TotalMilliseconds)
            {
                await Task.Delay(20);
            }
        }
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
