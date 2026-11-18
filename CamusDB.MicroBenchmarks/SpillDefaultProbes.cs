/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Globalization;
using System.Text;
using Kahuna;
using Microsoft.Extensions.Logging;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Measures the cost of a statement with spill-to-disk on against the same statement with spill off,
/// on one data set. The cases are the operator paths that differ between the two settings below the
/// spill threshold, and the two paths that change at the default thresholds: a hash-join build of
/// 500,000 to 1,000,000 rows, and an <c>ORDER BY … LIMIT</c> above the row threshold.
///
/// <para>Each arm is its own engine over the same node and database, because an engine fixes its
/// options when it is built. One data load then serves every arm, and the rounds alternate the arms so
/// that a slow drift of the process affects each arm the same.</para>
///
/// <para>The arm <c>on-rows-1M</c> is spill on with <c>SpillThresholdRows = 1,000,000</c>. For the
/// hash join it gives the build cap <c>max(SpillThresholdRows, HashJoinMaxBuildRows)</c>. For the top-k
/// case it lets the planner keep the bounded top-k, which it skips when <c>LIMIT + OFFSET</c> is above
/// the row threshold.</para>
///
/// <para>A one-shot probe. The data load takes minutes, and BenchmarkDotNet would run it again in each
/// process. Run it with
/// <c>dotnet run -c Release -- probe spill [below|join|topk|wide|all] [--big N] [--rounds N]</c>.
/// The spill files go under a new directory in the system temp path, which the probe deletes at the
/// end.</para>
/// </summary>
internal static class SpillDefaultProbes
{
    private const string Off = "off";

    private const string On = "on";

    private const string OnRows1M = "on-rows-1M";

    private static readonly ILoggerFactory LoggerFactory =
        Microsoft.Extensions.Logging.LoggerFactory.Create(b => b.AddFilter("*", LogLevel.Warning));

    private static readonly ILogger<ICamusDB> Logger = LoggerFactory.CreateLogger<ICamusDB>();

    /// <summary>One engine under test and the database descriptor it opened.</summary>
    private sealed record Engine(string Name, CommandExecutor Executor, DatabaseDescriptor Database);

    /// <summary>One statement, the arms it runs on, and how many times it runs.</summary>
    private sealed record Case(string Group, string Name, string Sql, string[] Arms, int Warmups, int Rounds);

    /// <summary>The time, the allocated bytes and the row count of one run.</summary>
    private readonly record struct Sample(double Milliseconds, long AllocatedBytes, long Rows);

    public static async Task<int> RunAsync(string[] args)
    {
        string group = args.Length > 2 && !args[2].StartsWith("--", StringComparison.Ordinal) ? args[2] : "all";
        int bigRows = IntArg(args, "--big", 1_500_000);
        int midRows = IntArg(args, "--mid", 100_000);
        int wideRows = IntArg(args, "--wide", 36_000);
        int padChars = IntArg(args, "--pad", 4_000);
        int heavyRounds = IntArg(args, "--rounds", 5);
        int buildRows = IntArg(args, "--build", 750_000);
        int limitRows = IntArg(args, "--limit", 600_000);

        bool needBig = group is "all" or "join" or "topk";
        bool needMid = group is "all" or "below" or "wide";
        bool needWide = group is "all" or "wide";

        string dataDirectory = Path.Combine(Path.GetTempPath(), "camusdb-spill-probe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDirectory);

        CamusDBOptions baseOptions = CamusDBOptions.Default with { DataDirectory = dataDirectory };

        Dictionary<string, CamusDBOptions> armOptions = new(StringComparer.Ordinal)
        {
            [Off] = baseOptions with { SpillEnabled = false },
            [On] = baseOptions with { SpillEnabled = true },
            [OnRows1M] = baseOptions with { SpillEnabled = true, SpillThresholdRows = 1_000_000 },
        };

        List<Case> cases =
        [
            new("below", "sort by column", "SELECT k, v FROM mid ORDER BY v", [Off, On], 3, 15),
            new("below", "sort by expression", "SELECT k, v FROM mid ORDER BY v % 1000, k", [Off, On], 3, 15),
            new("below", "group by, one group per row", "SELECT v, COUNT(*) FROM mid GROUP BY v", [Off, On], 3, 15),
            new("below", "distinct, all distinct", "SELECT DISTINCT v FROM mid", [Off, On], 3, 15),
            new("below", "intersect, all distinct", "SELECT v FROM mid INTERSECT SELECT v FROM mid", [Off, On], 3, 15),
            new("below", "hash join, build of mid rows", "SELECT COUNT(*) FROM mid a JOIN mid b ON a.k = b.k", [Off, On], 3, 15),
            new("join", $"hash join, build of {buildRows:N0} rows",
                $"SELECT COUNT(*) FROM big a JOIN big b ON a.k = b.k WHERE b.k < {buildRows}", [Off, On, OnRows1M], 1, heavyRounds),
            new("topk", $"order by limit {limitRows:N0}",
                $"SELECT k, v FROM big ORDER BY v LIMIT {limitRows}", [Off, On, OnRows1M], 1, heavyRounds),
            new("wide", $"hash join, build of wide rows ({padChars:N0} chars)",
                "SELECT SUM(LENGTH(w.pad)) FROM wide w JOIN mid m ON m.k = w.k", [Off, On], 2, 10),
        ];

        cases = cases.Where(c => group == "all" || c.Group == group).ToList();

        if (cases.Count == 0)
        {
            Console.Error.WriteLine($"unknown group '{group}'; use below, join, topk, wide or all");
            return 2;
        }

        EmbeddedKahuna node = new(new EmbeddedKahunaOptions
        {
            NodeName = "spill-probe",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
        });

        DatabaseRegistry? registry = null;
        List<CommandExecutor> executors = [];

        try
        {
            await node.StartAsync(CancellationToken.None);
            await node.WaitForLeaderAsync("spill-probe-warmup", CancellationToken.None);
            await node.FlushAsync();

            registry = await DatabaseRegistry.OpenAsync(node, baseOptions);

            string dbName = "spillprobe" + Guid.NewGuid().ToString("N");

            CommandExecutor loader = NewExecutor(armOptions[Off], node, registry);
            executors.Add(loader);
            DatabaseDescriptor loaderDb = await loader.CreateDatabase(new CreateDatabaseTicket(dbName, ifNotExists: false));

            Stopwatch load = Stopwatch.StartNew();

            if (needBig)
                await LoadNarrowAsync(loader, loaderDb, dbName, "big", bigRows, withCategory: false);

            if (needMid)
                await LoadNarrowAsync(loader, loaderDb, dbName, "mid", midRows, withCategory: true);

            if (needWide)
                await LoadWideAsync(loader, loaderDb, dbName, wideRows, padChars);

            Console.WriteLine($"loaded big={(needBig ? bigRows : 0):N0} mid={(needMid ? midRows : 0):N0} " +
                              $"wide={(needWide ? wideRows : 0):N0} (pad {padChars:N0} chars) in {load.Elapsed.TotalSeconds:F1} s; " +
                              $"working set {Environment.WorkingSet / (1024 * 1024):N0} MiB");

            Dictionary<string, Engine> engines = new(StringComparer.Ordinal);

            foreach (string armName in cases.SelectMany(c => c.Arms).Distinct())
            {
                CommandExecutor executor = NewExecutor(armOptions[armName], node, registry);
                executors.Add(executor);
                DatabaseDescriptor db = await executor.OpenDatabase(dbName);
                engines[armName] = new Engine(armName, executor, db);

                // Statistics are cached per engine, so each engine collects its own.
                foreach (string table in new[] { needBig ? "big" : null, needMid ? "mid" : null, needWide ? "wide" : null })
                {
                    if (table is not null)
                        await RunAsync(engines[armName], "ANALYZE " + table);
                }
            }

            Console.WriteLine($"spill directory: {dataDirectory}");
            Console.WriteLine();

            foreach (Case probe in cases)
                await RunCaseAsync(probe, engines);

            return 0;
        }
        finally
        {
            foreach (CommandExecutor executor in executors)
                await executor.DisposeAsync();

            if (registry is not null)
                await registry.DisposeAsync();

            await node.DisposeAsync();

            try
            {
                Directory.Delete(dataDirectory, recursive: true);
            }
            catch (IOException ex)
            {
                Console.Error.WriteLine($"could not delete {dataDirectory}: {ex.Message}");
            }
        }
    }

    private static CommandExecutor NewExecutor(CamusDBOptions options, EmbeddedKahuna node, DatabaseRegistry registry) =>
        new(new CommandValidator(options), new CatalogsManager(Logger), Logger, options,
            sharedNode: node, registry: registry, isClusterMode: false);

    /// <summary>
    /// Prints the plan each arm runs, then times the arms in alternate order. A full collection before
    /// each run makes each run start from the same heap state; it is not in the time.
    /// </summary>
    private static async Task RunCaseAsync(Case probe, Dictionary<string, Engine> engines)
    {
        Console.WriteLine($"=== {probe.Group}: {probe.Name}");
        Console.WriteLine($"    {probe.Sql}");

        foreach (string armName in probe.Arms)
        {
            Console.WriteLine($"  plan [{armName}]");
            foreach (string line in await ExplainAnalyzeAsync(engines[armName], probe.Sql))
                Console.WriteLine("    " + line);
        }

        for (int w = 0; w < probe.Warmups; w++)
        {
            foreach (string armName in probe.Arms)
                await MeasureAsync(engines[armName], probe.Sql);
        }

        Dictionary<string, List<Sample>> samples = probe.Arms.ToDictionary(a => a, _ => new List<Sample>(), StringComparer.Ordinal);

        for (int round = 0; round < probe.Rounds; round++)
        {
            for (int i = 0; i < probe.Arms.Length; i++)
            {
                string armName = probe.Arms[(i + round) % probe.Arms.Length];
                samples[armName].Add(await MeasureAsync(engines[armName], probe.Sql));
            }
        }

        double baseline = Median(samples[probe.Arms[0]].Select(s => s.Milliseconds));

        Console.WriteLine($"  {"arm",-11} {"rows",10} {"median ms",10} {"min ms",9} {"max ms",9} {"ratio",6} {"alloc MiB",10}");

        foreach (string armName in probe.Arms)
        {
            List<Sample> armSamples = samples[armName];
            double median = Median(armSamples.Select(s => s.Milliseconds));
            double allocated = Median(armSamples.Select(s => (double)s.AllocatedBytes)) / (1024 * 1024);

            Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"  {armName,-11} {armSamples[0].Rows,10:N0} {median,10:F1} {armSamples.Min(s => s.Milliseconds),9:F1} " +
                $"{armSamples.Max(s => s.Milliseconds),9:F1} {median / baseline,6:F2} {allocated,10:F1}"));
        }

        Console.WriteLine();
    }

    private static async Task<Sample> MeasureAsync(Engine engine, string sql)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
        Stopwatch watch = Stopwatch.StartNew();
        long rows = await RunAsync(engine, sql);
        watch.Stop();
        long allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;

        return new Sample(watch.Elapsed.TotalMilliseconds, allocated, rows);
    }

    /// <summary>Runs one statement in its own transaction and counts the rows it returns.</summary>
    private static async Task<long> RunAsync(Engine engine, string sql)
    {
        KvTransaction tx = await engine.Database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await engine.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, engine.Database.Name, sql, parameters: null));

            long rows = 0;
            await foreach (QueryResultRow _ in cursor)
                rows++;

            await engine.Database.Transactions.CommitAsync(tx);
            return rows;
        }
        catch
        {
            await engine.Database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    /// <summary>The <c>EXPLAIN (ANALYZE)</c> rows of a statement, one line per plan node.</summary>
    private static async Task<List<string>> ExplainAnalyzeAsync(Engine engine, string sql)
    {
        KvTransaction tx = await engine.Database.Transactions.BeginAsync();
        List<string> lines = [];

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await engine.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, engine.Database.Name, "EXPLAIN (ANALYZE) " + sql, parameters: null));

        await foreach (QueryResultRow row in cursor)
        {
            StringBuilder line = new();

            foreach (string column in new[] { "node", "detail", "actual_rows", "rows_read" })
            {
                if (!row.Row.TryGetValue(column, out ColumnValue? value))
                    continue;

                string? text = value.Type switch
                {
                    ColumnType.String => value.StrValue,
                    ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
                    _ => null,
                };

                if (!string.IsNullOrEmpty(text))
                    line.Append(column).Append('=').Append(text).Append("  ");
            }

            lines.Add(line.ToString().TrimEnd());
        }

        await engine.Database.Transactions.CommitAsync(tx);
        return lines;
    }

    /// <summary>
    /// Loads <paramref name="rowCount"/> rows of <c>(id, k, v[, cat])</c>: <c>k</c> is the row number and
    /// <c>v</c> a permutation of it, so a sort on <c>v</c> has real work and no index helps.
    /// </summary>
    private static async Task LoadNarrowAsync(
        CommandExecutor executor, DatabaseDescriptor db, string dbName, string table, int rowCount, bool withCategory)
    {
        List<ColumnInfo> columns =
        [
            new("id", ColumnType.Id),
            new("k", ColumnType.Integer64),
            new("v", ColumnType.Integer64),
        ];

        if (withCategory)
            columns.Add(new ColumnInfo("cat", ColumnType.Integer64));

        await CreateTableAsync(executor, dbName, table, [.. columns]);

        await InsertBatchesAsync(executor, db, dbName, table, rowCount, i =>
        {
            Dictionary<string, ColumnValue> row = new(4)
            {
                ["id"] = new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()),
                ["k"] = new(ColumnType.Integer64, (long)i),
                ["v"] = new(ColumnType.Integer64, Permute(i, rowCount)),
            };

            if (withCategory)
                row["cat"] = new(ColumnType.Integer64, (long)(i % 10));

            return row;
        });
    }

    /// <summary>Loads <paramref name="rowCount"/> rows of <c>(id, k, pad)</c>, each pad a string of <paramref name="padChars"/> characters.</summary>
    private static async Task LoadWideAsync(CommandExecutor executor, DatabaseDescriptor db, string dbName, int rowCount, int padChars)
    {
        await CreateTableAsync(executor, dbName, "wide",
        [
            new("id", ColumnType.Id),
            new("k", ColumnType.Integer64),
            new("pad", ColumnType.String),
        ]);

        string pad = new('x', padChars);

        await InsertBatchesAsync(executor, db, dbName, "wide", rowCount, i => new Dictionary<string, ColumnValue>(3)
        {
            ["id"] = new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()),
            ["k"] = new(ColumnType.Integer64, (long)i),
            ["pad"] = new(ColumnType.String, pad),
        });
    }

    private static Task CreateTableAsync(CommandExecutor executor, string dbName, string table, ColumnInfo[] columns) =>
        executor.CreateTable(new CreateTableTicket(
            databaseName: dbName,
            tableName: table,
            columns: columns,
            constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

    /// <summary>Inserts rows in transactions of 2,000 rows, with four transactions open at a time.</summary>
    private static async Task InsertBatchesAsync(
        CommandExecutor executor, DatabaseDescriptor db, string dbName, string table, int rowCount,
        Func<int, Dictionary<string, ColumnValue>> makeRow)
    {
        const int BatchSize = 2_000;
        const int Writers = 4;

        int next = 0;

        async Task WriterAsync()
        {
            while (true)
            {
                int start = Interlocked.Add(ref next, BatchSize) - BatchSize;

                if (start >= rowCount)
                    return;

                int end = Math.Min(start + BatchSize, rowCount);
                List<Dictionary<string, ColumnValue>> rows = new(end - start);

                for (int i = start; i < end; i++)
                    rows.Add(makeRow(i));

                KvTransaction tx = await db.Transactions.BeginAsync();
                await executor.Insert(new InsertTicket(txnState: tx, databaseName: dbName, tableName: table, values: rows));
                await db.Transactions.CommitAsync(tx);
            }
        }

        Task[] writers = new Task[Writers];
        for (int w = 0; w < Writers; w++)
            writers[w] = Task.Run(WriterAsync);

        await Task.WhenAll(writers);
    }

    /// <summary>A permutation of <c>0 … count-1</c>: multiplication by a prime that does not divide <paramref name="count"/>.</summary>
    private static long Permute(int i, int count)
    {
        const long Prime = 1_000_003;
        long multiplier = count % Prime == 0 ? 7_919 : Prime;
        return i * multiplier % count;
    }

    private static double Median(IEnumerable<double> values)
    {
        double[] sorted = values.Order().ToArray();
        int middle = sorted.Length / 2;
        return sorted.Length % 2 == 1 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
    }

    private static int IntArg(string[] args, string name, int fallback)
    {
        int index = Array.IndexOf(args, name);
        return index >= 0 && index + 1 < args.Length ? int.Parse(args[index + 1], CultureInfo.InvariantCulture) : fallback;
    }
}
