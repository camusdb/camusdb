/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Routing;
using CamusDB.Core.Transactions;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Measurements about a multi-row INSERT that BenchmarkDotNet cannot take: the largest statement the
/// transaction mutation budget admits, the row count at which the recursive VALUES walk exhausts the
/// stack, and how much the SQL parser cache retains while a restore streams unique statements
/// through it.
///
/// <para>These are one-shot probes, not benchmarks. They print a report and exit. Run them with
/// <c>dotnet run -c Release -- probe &lt;name&gt;</c>.</para>
///
/// <para>The stack probe deliberately runs in a child process: a stack overflow cannot be caught in
/// .NET and ends the process, so the parent reads the child's exit code instead of trying to
/// recover.</para>
/// </summary>
internal static class InsertRestoreProbes
{
    public static async Task<int> RunAsync(string[] args)
    {
        string name = args.Length > 1 ? args[1] : "all";

        switch (name)
        {
            case "smoke":
                await SmokeAsync();
                return 0;

            case "admissible":
                await AdmissibleRowsAsync();
                return 0;

            case "parsercache":
                await ParserCacheRetentionAsync();
                return 0;

            case "parsecount":
                await ParseCountAsync();
                return 0;

            case "stack":
                // Child mode: parse + shape a statement of the given size and exit 0 if it survives.
                return await StackChildAsync(int.Parse(args[2]));

            case "stackscan":
                StackScan();
                return 0;

            case "all":
                await SmokeAsync();
                await ParseCountAsync();
                await AdmissibleRowsAsync();
                await ParserCacheRetentionAsync();
                StackScan();
                return 0;

            default:
                Console.Error.WriteLine($"unknown probe '{name}'");
                return 2;
        }
    }

    /// <summary>
    /// Runs one 500-row INSERT end to end and reads the rows back, so a harness fault shows up here
    /// rather than as a misleading benchmark number.
    /// </summary>
    private static async Task SmokeAsync()
    {
        Console.WriteLine("── smoke ─────────────────────────────────────────────");

        InsertBenchHarness.Context ctx = await InsertBenchHarness.StartAsync(
            "probe-smoke", InsertBenchIndexShape.PrimaryKeyOnly, wideRows: false, CamusDBOptions.Default);

        try
        {
            string sql = InsertBenchHarness.BuildInsertSql(500, wideRows: false);
            Console.WriteLine($"statement text: {sql.Length} chars");

            KvTransaction tx = await ctx.Db.Transactions.BeginAsync();
            await ctx.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, ctx.DbName, sql, null));
            Console.WriteLine($"mutations reserved for 500 rows, primary key only: {tx.MutationCount}");
            await ctx.Db.Transactions.CommitAsync(tx);

            long count = await CountRowsAsync(ctx);
            Console.WriteLine($"rows after insert: {count}");

            await InsertBenchHarness.TruncateAsync(ctx);
            Console.WriteLine($"rows after truncate: {await CountRowsAsync(ctx)}");
        }
        finally
        {
            await ctx.DisposeAsync();
        }

        Console.WriteLine();
    }

    private static async Task<long> CountRowsAsync(InsertBenchHarness.Context ctx)
    {
        KvTransaction tx = await ctx.Db.Transactions.BeginAsync();

        try
        {
            long count = 0;

            (_, IAsyncEnumerable<QueryResultRow> cursor) = await ctx.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, ctx.DbName, "SELECT id FROM bench", null));

            await foreach (QueryResultRow _ in cursor)
                count++;

            return count;
        }
        finally
        {
            await ctx.Db.Transactions.CommitAsync(tx);
        }
    }


    /// <summary>
    /// Counts how many times one INSERT statement is lexed and parsed on its way through the engine,
    /// with the parser cache on and with it off. This decides whether a guard that keeps large
    /// statements out of the cache would make a restore slower: every call after the first is a cache
    /// hit today, and becomes a full parse once the statement stops being cached.
    /// </summary>
    private static async Task ParseCountAsync()
    {
        Console.WriteLine("── parses per statement ──────────────────────────────");

        foreach (bool cacheEnabled in new[] { true, false })
        {
            CamusDBOptions options = cacheEnabled
                ? CamusDBOptions.Default
                : CamusDBOptions.Default with { SqlParserCacheTtlSeconds = 0 };

            InsertBenchHarness.Context ctx = await InsertBenchHarness.StartAsync(
                "probe-parsecount-" + cacheEnabled, InsertBenchIndexShape.PrimaryKeyOnly, wideRows: false, options);

            try
            {
                string sql = InsertBenchHarness.BuildInsertSql(500, wideRows: false);

                // Plain dispatcher path.
                long before = SQLParserProcessor.TotalParses;

                KvTransaction tx = await ctx.Db.Transactions.BeginAsync();
                await ctx.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, ctx.DbName, sql, null));
                await ctx.Db.Transactions.CommitAsync(tx);

                long plain = SQLParserProcessor.TotalParses - before;

                // Same statement with routing negotiated, which is what a learned-routing client sends.
                string sql2 = InsertBenchHarness.BuildInsertSql(500, wideRows: false);
                before = SQLParserProcessor.TotalParses;

                KvTransaction tx2 = await ctx.Db.Transactions.BeginAsync();
                await ctx.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    tx2, ctx.DbName, sql2, null, null, default, null, new StatementRoutingCollector()));
                await ctx.Db.Transactions.CommitAsync(tx2);

                long routed = SQLParserProcessor.TotalParses - before;

                // The HTTP controller parses once more, before the engine, purely to read .nodeType
                // for an inline request (ExecuteSQLController.Resolve).
                Console.WriteLine(
                    $"cache {(cacheEnabled ? "on " : "off")} → engine {plain} parse(s), engine+routing {routed}, " +
                    $"and the HTTP controller adds one more before either");
            }
            finally
            {
                await ctx.DisposeAsync();
            }
        }

        Console.WriteLine();
    }

    /// <summary>
    /// Reports how many mutations one row costs in each table shape, and therefore the largest
    /// statement the default budget of <see cref="CamusDBOptions.MaxMutationsPerTransaction"/>
    /// admits. The documented restore formula depends on this number being right.
    /// </summary>
    private static async Task AdmissibleRowsAsync()
    {
        Console.WriteLine("── admissible rows per statement ─────────────────────");

        foreach (InsertBenchIndexShape shape in Enum.GetValues<InsertBenchIndexShape>())
        {
            InsertBenchHarness.Context ctx = await InsertBenchHarness.StartAsync(
                "probe-admissible-" + shape, shape, wideRows: false, CamusDBOptions.Default);

            try
            {
                const int Probe = 100;

                KvTransaction tx = await ctx.Db.Transactions.BeginAsync();
                await ctx.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    tx, ctx.DbName, InsertBenchHarness.BuildInsertSql(Probe, wideRows: false), null));

                int mutations = tx.MutationCount;
                await ctx.Db.Transactions.RollbackAsync(tx);

                double perRow = (double)mutations / Probe;
                int limit = CamusDBOptions.Default.MaxMutationsPerTransaction;

                Console.WriteLine(
                    $"{shape,-16} {perRow:0.##} mutations/row → at most {(int)(limit / perRow):N0} rows per statement " +
                    $"(budget {limit:N0})");
            }
            finally
            {
                await ctx.DisposeAsync();
            }
        }

        Console.WriteLine();
    }

    /// <summary>
    /// Streams unique INSERT statements through a parser cache the way a restore does, and reports
    /// the entry count and the managed bytes the cache retains. Every dump statement is unique text,
    /// so the cache never returns a hit and keeps every AST until the TTL expires.
    /// </summary>
    private static async Task ParserCacheRetentionAsync()
    {
        Console.WriteLine("── parser cache retention during a restore ───────────");

        foreach (int rowsPerStatement in new[] { 1, 100, 500 })
        {
            await using SqlParserCache cache = new(
                logger: null,
                ttlSeconds: CamusDBOptions.Default.SqlParserCacheTtlSeconds,
                maxEntries: CamusDBOptions.Default.SqlParserCacheMaxEntries,
                sweepSeconds: CamusDBOptions.Default.SqlParserCacheSweepSeconds,
                maxBytes: CamusDBOptions.Default.SqlParserCacheMaxBytes);

            int statements = CamusDBOptions.Default.SqlParserCacheMaxEntries;

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long before = GC.GetTotalMemory(forceFullCollection: true);

            for (int i = 0; i < statements; i++)
                SQLParserProcessor.Parse(InsertBenchHarness.BuildInsertSql(rowsPerStatement, wideRows: false), cache);

            long after = GC.GetTotalMemory(forceFullCollection: true);

            Console.WriteLine(
                $"{rowsPerStatement,5} rows/statement × {statements:N0} unique statements → " +
                $"entries {cache.Count:N0}, hits {cache.Hits:N0}, misses {cache.Misses:N0}, " +
                $"budget {cache.ApproxBytes / 1024.0 / 1024.0:N1} MiB, " +
                $"retained {(after - before) / 1024.0 / 1024.0:N1} MiB");
        }

        Console.WriteLine();
    }

    /// <summary>
    /// Parses and shapes one statement of <paramref name="rows"/> rows in a child process, through
    /// the real ticket creator, so the measured stack depth is the depth production code reaches.
    /// Returns 0 when the recursive VALUES walk survives; the process dies instead when the stack
    /// overflows, which is what <see cref="StackScan"/> detects.
    /// </summary>
    private static async Task<int> StackChildAsync(int rows)
    {
        InsertBenchHarness.Context ctx = await InsertBenchHarness.StartAsync(
            "probe-stack", InsertBenchIndexShape.PrimaryKeyOnly, wideRows: false, CamusDBOptions.Default);

        try
        {
            string sql = InsertBenchHarness.BuildInsertSql(rows, wideRows: false);
            NodeAst ast = SQLParserProcessor.Parse(sql);

            if (ast.nodeType != NodeType.Insert)
            {
                Console.Error.WriteLine($"unexpected root node {ast.nodeType}");
                return 4;
            }

            KvTransaction tx = await ctx.Db.Transactions.BeginAsync(deferStart: true);

            InsertTicket ticket = await new SQLExecutorInsertCreator().CreateInsertTicket(
                ctx.Executor, ctx.Db, new ExecuteSQLTicket(tx, ctx.DbName, sql, null), ast);

            int code = ticket.Values.Count == rows ? 0 : 3;

            // Exit without a clean shutdown on purpose. The only question this child answers is
            // whether the recursive walk survived, and disposing an executor that still holds an
            // uncommitted transaction blocks.
            Console.Out.Flush();
            Environment.Exit(code);
            return code;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex);
            Console.Error.Flush();
            Environment.Exit(5);
            return 5;
        }
    }

    /// <summary>
    /// Finds the row count at which the recursive VALUES walk stops working, by running
    /// <see cref="StackChild"/> in a child process at growing sizes and then bisecting. A stack
    /// overflow ends a .NET process and cannot be caught, so the exit code is the only signal.
    /// </summary>
    private static void StackScan()
    {
        Console.WriteLine("── recursive VALUES walk: stack limit ────────────────");

        string exe = Environment.ProcessPath!;
        string dll = typeof(InsertRestoreProbes).Assembly.Location;

        int lastGood = 0;
        int firstBad = 0;

        foreach (int rows in new[] { 1_000, 2_500, 5_000, 10_000, 20_000, 50_000 })
        {
            StackChildOutcome outcome = RunStackChild(exe, dll, rows);
            Console.WriteLine($"{rows,8:N0} rows → {outcome}");
            Console.Out.Flush();

            if (outcome == StackChildOutcome.TimedOut)
            {
                Console.WriteLine("child ran past its deadline; the scan cannot classify this size");
                Console.WriteLine();
                return;
            }

            if (outcome == StackChildOutcome.Survived)
            {
                lastGood = rows;
                continue;
            }

            firstBad = rows;
            break;
        }

        if (firstBad == 0)
        {
            Console.WriteLine("no failure inside the scanned range");
            Console.WriteLine();
            return;
        }

        while (firstBad - lastGood > 250)
        {
            int mid = lastGood + ((firstBad - lastGood) / 2);

            StackChildOutcome outcome = RunStackChild(exe, dll, mid);
            Console.WriteLine($"{mid,8:N0} rows → {outcome}");
            Console.Out.Flush();

            if (outcome == StackChildOutcome.TimedOut)
            {
                Console.WriteLine("child ran past its deadline; stopping the bisection here");
                break;
            }

            if (outcome == StackChildOutcome.Survived)
                lastGood = mid;
            else
                firstBad = mid;
        }

        Console.WriteLine($"largest surviving statement: about {lastGood:N0} rows; fails by {firstBad:N0}");
        Console.WriteLine();
    }

    /// <summary>
    /// Runs one child and classifies it: it survived, it died (a stack overflow), or it ran past the
    /// deadline. A hang is not evidence either way, so the scan stops rather than bisect on it.
    /// </summary>
    private enum StackChildOutcome { Survived, Died, TimedOut }

    private static StackChildOutcome RunStackChild(string exe, string dll, int rows)
    {
        ProcessStartInfo psi = new()
        {
            FileName = exe,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };

        // Environment.ProcessPath is the apphost when one exists, and `dotnet` otherwise; only the
        // second form needs the assembly path passed through.
        if (Path.GetFileNameWithoutExtension(exe).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
            psi.ArgumentList.Add(dll);

        psi.ArgumentList.Add("probe");
        psi.ArgumentList.Add("stack");
        psi.ArgumentList.Add(rows.ToString());

        using Process child = Process.Start(psi)!;

        Task<string> stdout = child.StandardOutput.ReadToEndAsync();
        Task<string> stderr = child.StandardError.ReadToEndAsync();

        if (!child.WaitForExit(ChildTimeoutMs))
        {
            try { child.Kill(entireProcessTree: true); } catch { /* already gone */ }
            return StackChildOutcome.TimedOut;
        }

        stdout.GetAwaiter().GetResult();
        stderr.GetAwaiter().GetResult();

        return child.ExitCode == 0 ? StackChildOutcome.Survived : StackChildOutcome.Died;
    }

    /// <summary>Wall-clock bound for one child. Generous: a child boots a node before it parses.</summary>
    private const int ChildTimeoutMs = 180_000;
}
