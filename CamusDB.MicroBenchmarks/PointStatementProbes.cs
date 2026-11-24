
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Globalization;
using Kahuna;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Measures the engine's cost of the two statements of a bank transfer, with no transport: a
/// primary-key point <c>SELECT</c> and a primary-key <c>UPDATE</c>, alone and as the transfer the
/// workload sends (two of each in a Serializable optimistic transaction, then the commit), and the
/// autocommit point read.
///
/// <para>One caller runs the statements one after another against an embedded in-memory node, so the
/// time per statement is not a throughput figure. The figures to read are the bytes allocated and the
/// process CPU time for each statement: they are what a statement costs a node that has no idle CPU.
/// Both are process-wide, so they include the storage layer's share and its background work.</para>
///
/// <para>A one-shot probe. Run it with
/// <c>dotnet run -c Release -- probe pointstmt [--ops N] [--rounds N] [--only read|transfer]
/// [--no-read-reuse] [--pause seconds]</c>. <c>--no-read-reuse</c> makes each UPDATE read its row
/// again; <c>--pause</c> waits before the measured rounds so a profiler can attach.</para>
/// </summary>
internal static class PointStatementProbes
{
    private const int Rows = 2000;

    private const string ReadSql = "SELECT balance, version FROM workload_accounts WHERE id = @id";

    private const string UpdateSql = "UPDATE workload_accounts SET balance = @b, version = version + 1 WHERE id = @id";

    private static readonly ILoggerFactory LoggerFactory =
        Microsoft.Extensions.Logging.LoggerFactory.Create(b => b.AddFilter("*", LogLevel.Warning));

    private static readonly ILogger<ICamusDB> Logger = LoggerFactory.CreateLogger<ICamusDB>();

    /// <summary>The time and the bytes charged to one step of the loop.</summary>
    private sealed class Segment(string name)
    {
        public readonly string Name = name;
        public long Ticks;
        public long Bytes;
        public long Count;

        public void Reset() { Ticks = 0; Bytes = 0; Count = 0; }
    }

    public static async Task<int> RunAsync(string[] args)
    {
        int ops = IntArg(args, "--ops", 20_000);
        int rounds = IntArg(args, "--rounds", 3);
        int pauseSeconds = IntArg(args, "--pause", 0);
        int onlyAt = Array.IndexOf(args, "--only");
        string only = onlyAt >= 0 && onlyAt + 1 < args.Length ? args[onlyAt + 1] : "";

        EmbeddedKahuna node = new(new EmbeddedKahunaOptions
        {
            NodeName = "probe-node",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            // One caller commits at a time. With a linger or a hold every commit would wait for a
            // timer, and the threads that spin meanwhile would count as the statement's CPU.
            KeyValueWriteLingerMs = 0,
            KeyValueWritePostCompletionHoldMs = 0,
        });

        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("probe-warmup", CancellationToken.None);
        await node.FlushAsync();

        CamusDBOptions options = CamusDBOptions.Default with
        {
            DefaultIsolationLevel = CamusIsolationLevel.Serializable,
            DefaultTransactionLocking = KeyValueTransactionLocking.Optimistic,
            TransactionReadReuseEnabled = Array.IndexOf(args, "--no-read-reuse") < 0,
        };

        DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(node, options);
        CommandExecutor executor = new(new CommandValidator(options), new CatalogsManager(Logger), Logger, options,
            sharedNode: node, registry: registry, isClusterMode: true);

        string dbName = "p" + Guid.NewGuid().ToString("N");
        CamusDBConfig.DataDirectory = Path.Combine(Path.GetTempPath(), "camusdb-probe-" + dbName);
        Directory.CreateDirectory(CamusDBConfig.DataDirectory);

        try
        {
            DatabaseDescriptor db = await executor.CreateDatabase(new CreateDatabaseTicket(dbName, ifNotExists: false));

            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbName,
                tableName: "workload_accounts",
                columns:
                [
                    new ColumnInfo("id", ColumnType.Id),
                    new ColumnInfo("balance", ColumnType.Integer64),
                    new ColumnInfo("version", ColumnType.Integer64),
                ],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
                ifNotExists: false));

            string[] ids = new string[Rows];
            for (int start = 0; start < Rows; start += 500)
            {
                KvTransaction load = await db.Transactions.BeginAsync();
                List<Dictionary<string, ColumnValue>> rows = new(500);
                for (int i = start; i < start + 500; i++)
                {
                    ids[i] = i.ToString(CultureInfo.InvariantCulture).PadLeft(24, '0');
                    rows.Add(new Dictionary<string, ColumnValue>
                    {
                        ["id"] = new(ColumnType.Id, ids[i]),
                        ["balance"] = new(ColumnType.Integer64, 1000L),
                        ["version"] = new(ColumnType.Integer64, 0L),
                    });
                }

                await executor.Insert(new InsertTicket(txnState: load, databaseName: dbName, tableName: "workload_accounts", values: rows));
                await db.Transactions.CommitAsync(load);
            }

            Segment read = new("autocommit point read");
            Segment begin = new("transfer: begin");
            Segment select = new("transfer: SELECT (each)");
            Segment update = new("transfer: UPDATE (each)");
            Segment commit = new("transfer: commit");
            Segment[] segments = [read, begin, select, update, commit];

            // Warm-up: tiered compilation, the parse and bound-statement caches, the row-id cache.
            await RunReadsAsync(executor, db, dbName, ids, 4_000, read);
            await RunTransfersAsync(executor, db, dbName, ids, 4_000, begin, select, update, commit);

            if (pauseSeconds > 0)
            {
                // Time for a profiler to attach before the measured rounds.
                Console.WriteLine($"pid {Environment.ProcessId}, starting in {pauseSeconds} s");
                await Task.Delay(TimeSpan.FromSeconds(pauseSeconds));
            }

            Console.WriteLine($"point statements: {ops:N0} of each per round, {rounds} rounds, {Rows:N0} rows, one caller");
            Console.WriteLine($"{"step",-28} {"us",9} {"bytes",9}    (per call; process-wide allocation)");

            Process self = Process.GetCurrentProcess();
            for (int round = 1; round <= rounds; round++)
            {
                foreach (Segment s in segments)
                    s.Reset();

                GC.Collect();
                self.Refresh();
                TimeSpan cpu0 = self.TotalProcessorTime;
                if (only != "transfer")
                    await RunReadsAsync(executor, db, dbName, ids, ops, read);
                self.Refresh();
                TimeSpan cpu1 = self.TotalProcessorTime;
                if (only != "read")
                    await RunTransfersAsync(executor, db, dbName, ids, ops, begin, select, update, commit);
                self.Refresh();
                TimeSpan cpu2 = self.TotalProcessorTime;

                Console.WriteLine($"round {round}");
                foreach (Segment s in segments)
                {
                    if (s.Count == 0)
                        continue;

                    Console.WriteLine(
                        $"{s.Name,-28} {s.Ticks * 1e6 / Stopwatch.Frequency / s.Count,9:F1} {s.Bytes / (double)s.Count,9:F0}");
                }

                long transferBytes = begin.Bytes + select.Bytes + update.Bytes + commit.Bytes;
                long transferTicks = begin.Ticks + select.Ticks + update.Ticks + commit.Ticks;
                Console.WriteLine(
                    $"{"transfer, whole",-28} {transferTicks * 1e6 / Stopwatch.Frequency / ops,9:F1} {transferBytes / (double)ops,9:F0}");
                Console.WriteLine(
                    $"process CPU: {(cpu1 - cpu0).TotalMilliseconds * 1000 / ops:F1} us a read, " +
                    $"{(cpu2 - cpu1).TotalMilliseconds * 1000 / ops:F1} us a transfer");
            }

            return 0;
        }
        finally
        {
            await executor.DisposeAsync();
            await registry.DisposeAsync();
            await node.DisposeAsync();
        }
    }

    private static async Task RunReadsAsync(
        CommandExecutor executor, DatabaseDescriptor db, string dbName, string[] ids, int ops, Segment read)
    {
        for (int i = 0; i < ops; i++)
        {
            string id = ids[(i * 7919) % Rows];

            long t0 = Stopwatch.GetTimestamp();
            long b0 = GC.GetTotalAllocatedBytes();

            KvTransaction tx = db.Transactions.BeginReadOnly();
            await ReadRowAsync(executor, tx, dbName, id);
            await db.Transactions.CommitAsync(tx);

            read.Bytes += GC.GetTotalAllocatedBytes() - b0;
            read.Ticks += Stopwatch.GetTimestamp() - t0;
            read.Count++;
        }
    }

    private static async Task RunTransfersAsync(
        CommandExecutor executor, DatabaseDescriptor db, string dbName, string[] ids, int ops,
        Segment begin, Segment select, Segment update, Segment commit)
    {
        for (int i = 0; i < ops; i++)
        {
            int low = (i * 7919) % Rows;
            int high = (low + 1 + (i % (Rows - 1))) % Rows;
            if (low > high)
                (low, high) = (high, low);

            long t = Stopwatch.GetTimestamp();
            long b = GC.GetTotalAllocatedBytes();

            KvTransaction tx = await db.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite, locking: KeyValueTransactionLocking.Optimistic);
            Charge(begin, ref t, ref b);

            long lowBalance = await ReadRowAsync(executor, tx, dbName, ids[low]);
            Charge(select, ref t, ref b);
            await UpdateRowAsync(executor, tx, dbName, ids[low], lowBalance - 1);
            Charge(update, ref t, ref b);

            long highBalance = await ReadRowAsync(executor, tx, dbName, ids[high]);
            Charge(select, ref t, ref b);
            await UpdateRowAsync(executor, tx, dbName, ids[high], highBalance + 1);
            Charge(update, ref t, ref b);

            await db.Transactions.CommitAsync(tx);
            Charge(commit, ref t, ref b);
        }
    }

    private static void Charge(Segment segment, ref long ticks, ref long bytes)
    {
        long nowTicks = Stopwatch.GetTimestamp();
        long nowBytes = GC.GetTotalAllocatedBytes();
        segment.Ticks += nowTicks - ticks;
        segment.Bytes += nowBytes - bytes;
        segment.Count++;
        ticks = nowTicks;
        bytes = nowBytes;
    }

    private static async Task<long> ReadRowAsync(CommandExecutor executor, KvTransaction tx, string dbName, string id)
    {
        ExecuteSQLTicket ticket = new(tx, dbName, ReadSql,
            new Dictionary<string, ColumnValue> { ["@id"] = new(ColumnType.Id, id) });

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        long balance = long.MinValue;
        await foreach (QueryResultRow row in cursor)
        {
            if (row.Row.TryGetValue("balance", out ColumnValue? value))
                balance = value.LongValue;
        }

        if (balance == long.MinValue)
            throw new InvalidOperationException("row not found: " + id);

        return balance;
    }

    private static async Task UpdateRowAsync(CommandExecutor executor, KvTransaction tx, string dbName, string id, long balance)
    {
        ExecuteSQLTicket ticket = new(tx, dbName, UpdateSql,
            new Dictionary<string, ColumnValue>
            {
                ["@b"] = new(ColumnType.Integer64, balance),
                ["@id"] = new(ColumnType.Id, id),
            });

        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(ticket);
        if (result.ModifiedRows != 1)
            throw new InvalidOperationException($"update of {id} modified {result.ModifiedRows} rows");
    }

    private static int IntArg(string[] args, string name, int fallback)
    {
        int at = Array.IndexOf(args, name);
        return at >= 0 && at + 1 < args.Length ? int.Parse(args[at + 1], CultureInfo.InvariantCulture) : fallback;
    }
}
