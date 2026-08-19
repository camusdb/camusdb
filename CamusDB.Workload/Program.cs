/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Reflection;
using CamusDB.Client;
using CamusDB.Workload.Cli;
using CamusDB.Workload.Client;
using CamusDB.Workload.Metrics;
using CamusDB.Workload.Operations;
using CamusDB.Workload.Results;
using CamusDB.Workload.Scheduling;
using CamusDB.Workload.Util;
using CamusDB.Workload.Workload;
using CommandLine;

namespace CamusDB.Workload;

/// <summary>
/// Entry point for the mixed-workload driver. Dispatches the <c>init</c> / <c>run</c> / <c>cleanup</c>
/// verbs. All setup (schema, seeding) happens outside any measured interval; only the <c>run</c> verb's
/// measurement window feeds the reported numbers.
/// </summary>
public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        using CancellationTokenSource cts = new();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        try
        {
            return await Parser.Default.ParseArguments<InitOptions, RunOptions, CleanupOptions, ReportOptions>(args)
                .MapResult(
                    (InitOptions o) => RunInitAsync(o, cts.Token),
                    (RunOptions o) => RunWorkloadAsync(o, cts.Token),
                    (CleanupOptions o) => RunCleanupAsync(o, cts.Token),
                    (ReportOptions o) => RunReportAsync(o, cts.Token),
                    errors => Task.FromResult(1))
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            Console.Error.WriteLine("Canceled.");
            return 130;
        }
        catch (Exception ex)
        {
            // A transport transient (e.g. a client request timeout) during setup/reconciliation should
            // fail cleanly with a message and a non-zero code, not an unhandled stack trace.
            Console.Error.WriteLine($"Workload failed: {ex.GetType().Name}: {ex.Message}");
            return 1;
        }
    }

    private static async Task<int> RunInitAsync(InitOptions o, CancellationToken ct)
    {
        Dataset dataset = new(o.Seed, o.Rows, o.PayloadBytes);
        await using CamusConnection conn = await OpenSingleAsync(o, ct).ConfigureAwait(false);

        Console.WriteLine($"Ensuring schema for database '{o.Database}' (fingerprint {dataset.Fingerprint()})...");
        await dataset.EnsureSchemaAsync(conn, ct).ConfigureAwait(false);
        Console.WriteLine($"Seeding {o.Rows} rows (batch {o.Batch})...");
        await dataset.SeedAsync(conn, o.Batch, ct).ConfigureAwait(false);
        Console.WriteLine("Init complete.");
        return 0;
    }

    private static async Task<int> RunWorkloadAsync(RunOptions o, CancellationToken ct)
    {
        if (o.ReadPercent + o.WritePercent != 100)
        {
            Console.Error.WriteLine($"--read-percent + --write-percent must equal 100 (got {o.ReadPercent}+{o.WritePercent}).");
            return 2;
        }
        if (!TryParseLocking(o.Locking, out CamusLocking locking))
        {
            Console.Error.WriteLine($"--locking must be optimistic or pessimistic (got '{o.Locking}').");
            return 2;
        }
        if (!TryParseIsolation(o.Isolation, out CamusIsolationLevel isolation))
        {
            Console.Error.WriteLine($"--isolation must be read_committed or serializable (got '{o.Isolation}').");
            return 2;
        }
        if (o.Workload is not ("accounts" or "bank"))
        {
            Console.Error.WriteLine($"--workload must be accounts or bank (got '{o.Workload}').");
            return 2;
        }
        bool bank = o.Workload == "bank";
        if (Directory.Exists(o.Output))
        {
            Console.Error.WriteLine($"Output directory already exists: {o.Output}. Refusing to overwrite.");
            return 2;
        }

        Dataset dataset = new(o.Seed, o.Rows, o.PayloadBytes);

        // Setup (never measured). Capture the SUM(version) baseline here so reconciliation measures only
        // this run's increments, even when the dataset was already written by a previous run.
        long baselineVersionSum;
        long baselineBalanceSum = 0;
        await using (CamusConnection setup = await OpenSingleAsync(o, ct).ConfigureAwait(false))
        {
            if (o.InitIfMissing)
            {
                await dataset.EnsureSchemaAsync(setup, ct).ConfigureAwait(false);
                await dataset.SeedAsync(setup, 500, ct).ConfigureAwait(false);
            }
            else if (!await dataset.IsSeededAsync(setup, ct).ConfigureAwait(false))
            {
                Console.Error.WriteLine("Dataset is not seeded. Run `init` first or pass --init-if-missing.");
                return 2;
            }

            baselineVersionSum = await Reconciliation.ReadVersionSumAsync(setup, ct).ConfigureAwait(false);
            if (bank)
                baselineBalanceSum = await Reconciliation.ReadBalanceSumAsync(setup, ct).ConfigureAwait(false);
        }

        ConnectionSettings settings = new(locking, isolation, o.NoAutoPrepare, o.RequestTimeout);
        await using ConnectionSet connections =
            await ConnectionSet.OpenAsync(o.Endpoint, o.Database, o.Protocol, o.Connections, settings, ct).ConfigureAwait(false);

        // The bank transfer contends across the whole keyspace and conserves SUM(balance); the accounts
        // write stays shard-disjoint and conflict-free. Both report as writes, so scheduling and metrics
        // are identical either way.
        IWriteOperation writeOperation = bank
            ? new TransferOperation(connections, dataset, o.Rows, locking, isolation)
            : new WriteOperation(connections, dataset, o.WritesPerTransaction, locking, isolation);
        OperationDispatcher dispatcher = new(
            new ReadOperation(connections, dataset),
            writeOperation);

        TimeSpan warmup = DurationParser.Parse(o.Warmup);
        TimeSpan measure = DurationParser.Parse(o.Duration);
        TimeSpan drain = DurationParser.Parse(o.Drain);

        Console.WriteLine($"Running {o.Mode}-loop workload: {o.ReadPercent}/{o.WritePercent} read/write, " +
                          $"{o.Workers} workers over {o.Connections} connections, warmup {warmup}, measure {measure}.");

        RunMetrics metrics;
        IReadOnlyList<IntervalRow> intervals;

        if (o.Mode == "closed")
        {
            ClosedLoopScheduler scheduler = new(dispatcher, o.WritesPerTransaction);
            WorkerState[] workers = WorkerState.Build(o.Seed, o.Workers, o.ReadPercent, o.Rows);
            (metrics, intervals) = await scheduler.RunAsync(workers, warmup, measure, ct).ConfigureAwait(false);
        }
        else
        {
            OpenLoopScheduler scheduler = new(dispatcher, o.WritesPerTransaction, o.Seed);
            WorkerState[] workers = WorkerState.Build(o.Seed, o.Workers, o.ReadPercent, o.Rows);
            (metrics, intervals) = await scheduler
                .RunAsync(workers, o.TargetOps, o.MaxInFlight, warmup, measure, drain, ct).ConfigureAwait(false);
        }

        // Write the measured artifacts FIRST, before reconciliation. The summary/intervals/errors are
        // the run's actual results and are always valid; reconciliation is a separate post-run check
        // that queries a cluster which may still be recovering from a fault. Writing the measured data
        // first means a reconciliation that cannot complete downgrades the verdict to "could not
        // verify" — it never discards a run that produced perfectly good data.
        RunSummary summary = RunSummary.Build(metrics, o.Mode, o.TargetOps, measure.TotalSeconds, o.ExpectFaults);
        RunManifest manifest = BuildManifest(o, dataset, locking, isolation);

        ResultWriter writer = new(o.Output);
        await writer.WriteAsync(manifest, summary, intervals, metrics.Errors, ct).ConfigureAwait(false);

        // Time anchor for aligning intervals.csv (second 0 == MeasureStartUtc) with wall-clock event
        // streams. Kept as a small standalone artifact so it can be added without disturbing the
        // established summary.json / manifest.json shapes their own consumers depend on.
        await File.WriteAllTextAsync(
            Path.Combine(o.Output, "run-meta.json"),
            System.Text.Json.JsonSerializer.Serialize(
                new
                {
                    measureStartUtc = metrics.MeasureStartUtc.ToString("O"),
                    warmupSeconds = warmup.TotalSeconds,
                    measureSeconds = measure.TotalSeconds,
                    drainSeconds = drain.TotalSeconds,
                },
                new System.Text.Json.JsonSerializerOptions { WriteIndented = true }),
            ct).ConfigureAwait(false);

        // Reconciliation (outside the measured window). Never crashes the run: neither a query that
        // cannot complete nor a connection that cannot even open discards the artifacts above — both
        // downgrade the verdict to "could not verify".
        ReconciliationResult reconciliation;
        try
        {
            await using CamusConnection verify = await OpenSingleAsync(o, ct).ConfigureAwait(false);
            reconciliation = await Reconciliation
                .VerifyOrInconclusiveAsync(
                    verify, metrics, baselineVersionSum, writeOperation.CommittedRows,
                    writeOperation.IndeterminateTxns, o.WritesPerTransaction, o.ExpectFaults, o.Rows, ct,
                    bankMode: bank, baselineBalanceSum: baselineBalanceSum)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            reconciliation = Reconciliation.Inconclusive(
                $"{ex.GetType().Name}: {ex.Message}", writeOperation.IndeterminateTxns, metrics.Conflicts, baselineBalanceSum);
        }

        await File.WriteAllTextAsync(
            Path.Combine(o.Output, "reconciliation.json"),
            System.Text.Json.JsonSerializer.Serialize(reconciliation, new System.Text.Json.JsonSerializerOptions { WriteIndented = true }),
            ct).ConfigureAwait(false);

        PrintConsole(summary, reconciliation, o.Output);
        return summary.Valid && reconciliation.Passed ? 0 : 1;
    }

    private static async Task<int> RunCleanupAsync(CleanupOptions o, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(o.Database) ||
            o.Database is "default" or "system" ||
            !string.Equals(o.Confirm, o.Database, StringComparison.Ordinal))
        {
            Console.Error.WriteLine("cleanup refused: --confirm must equal a non-empty, non-default --database.");
            return 2;
        }

        await using CamusConnection conn = await OpenSingleAsync(o, ct).ConfigureAwait(false);
        Console.WriteLine($"Dropping workload database '{o.Database}'...");
        await conn.DropDatabaseAsync(o.Database, ct).ConfigureAwait(false);
        Console.WriteLine("Cleanup complete.");
        return 0;
    }

    private static async Task<int> RunReportAsync(ReportOptions o, CancellationToken ct)
    {
        string manifestPath = Path.Combine(o.Output, "manifest.json");
        string summaryPath = Path.Combine(o.Output, "summary.json");
        if (!File.Exists(manifestPath) || !File.Exists(summaryPath))
        {
            Console.Error.WriteLine($"Run artifacts not found in {o.Output} (need manifest.json and summary.json).");
            return 2;
        }
        if (!File.Exists(o.Metrics))
        {
            Console.Error.WriteLine($"Metrics scrape file not found: {o.Metrics}");
            return 2;
        }

        var json = new System.Text.Json.JsonSerializerOptions(System.Text.Json.JsonSerializerDefaults.Web);
        RunManifest manifest = System.Text.Json.JsonSerializer.Deserialize<RunManifest>(
            await File.ReadAllTextAsync(manifestPath, ct).ConfigureAwait(false), json)!;
        RunSummary summary = System.Text.Json.JsonSerializer.Deserialize<RunSummary>(
            await File.ReadAllTextAsync(summaryPath, ct).ConfigureAwait(false), json)!;
        Reporting.PrometheusScrape scrape = Reporting.PrometheusScrape.Parse(
            await File.ReadAllTextAsync(o.Metrics, ct).ConfigureAwait(false));

        string report = Reporting.BottleneckReport.Build(manifest, summary, scrape);
        string reportPath = Path.Combine(o.Output, "bottleneck-report.md");
        await File.WriteAllTextAsync(reportPath, report, ct).ConfigureAwait(false);
        Console.WriteLine($"Wrote {reportPath}");
        return 0;
    }

    private static Task<CamusConnection> OpenSingleAsync(CommonOptions o, CancellationToken ct)
        => ConnectionSet.OpenSingleAsync(
            o.Endpoint, o.Database, o.Protocol,
            new ConnectionSettings(NoAutoPrepare: o.NoAutoPrepare, RequestTimeoutSeconds: o.RequestTimeout), ct);

    /// <summary>Accepts the CLI spelling (snake_case, case-insensitive) for concurrency-control knobs.</summary>
    private static bool TryParseLocking(string value, out CamusLocking locking)
    {
        switch (value.Trim().ToLowerInvariant())
        {
            case "optimistic": locking = CamusLocking.Optimistic; return true;
            case "pessimistic": locking = CamusLocking.Pessimistic; return true;
            default: locking = CamusLocking.Optimistic; return false;
        }
    }

    private static bool TryParseIsolation(string value, out CamusIsolationLevel isolation)
    {
        switch (value.Trim().ToLowerInvariant())
        {
            case "read_committed": isolation = CamusIsolationLevel.ReadCommitted; return true;
            case "serializable": isolation = CamusIsolationLevel.Serializable; return true;
            default: isolation = CamusIsolationLevel.ReadCommitted; return false;
        }
    }

    private static RunManifest BuildManifest(
        RunOptions o, Dataset dataset, CamusLocking locking, CamusIsolationLevel isolation) => new(
        ToolVersion: Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "0.0.0",
        GitCommit: Environment.GetEnvironmentVariable("CAMUS_GIT_COMMIT"),
        Endpoint: o.Endpoint,
        Database: o.Database,
        Protocol: o.Protocol,
        Mode: o.Mode,
        Seed: o.Seed,
        Rows: o.Rows,
        PayloadBytes: o.PayloadBytes,
        Workers: o.Workers,
        Connections: o.Connections,
        TargetOps: o.TargetOps,
        ReadPercent: o.ReadPercent,
        WritePercent: o.WritePercent,
        WritesPerTransaction: o.WritesPerTransaction,
        Locking: locking.ToString(),
        Isolation: isolation.ToString(),
        NoAutoPrepare: o.NoAutoPrepare,
        RequestTimeoutSeconds: o.RequestTimeout,
        ExpectFaults: o.ExpectFaults,
        SchemaFingerprint: dataset.Fingerprint(),
        StartedAtUtc: DateTime.UtcNow.ToString("O"),
        Runtime: Environment.Version.ToString(),
        Os: Environment.OSVersion.ToString(),
        ProcessorCount: Environment.ProcessorCount,
        ClientPackageVersion: "CamusDB.Client 0.7.3");

    private static void PrintConsole(RunSummary s, ReconciliationResult r, string output)
    {
        Console.WriteLine();
        Console.WriteLine($"  Completed ops/s     : {s.AchievedOpsPerSec:F1}");
        Console.WriteLine($"  Read ops/s          : {s.ReadOpsPerSec:F1}");
        Console.WriteLine($"  Write txns/s        : {s.WriteTxnsPerSec:F1}");
        Console.WriteLine($"  Mix reads/writes    : {s.ReadPercentActual:F1}% / {s.WritePercentActual:F1}%");
        Console.WriteLine($"  Read p50/p99 (ms)   : {s.ReadLatency.P50:F2} / {s.ReadLatency.P99:F2}");
        Console.WriteLine($"  Write p50/p99 (ms)  : {s.WriteLatency.P50:F2} / {s.WriteLatency.P99:F2}");
        Console.WriteLine($"  Commit p50/p99 (ms) : {s.WriteCommit.P50:F2} / {s.WriteCommit.P99:F2}");
        Console.WriteLine($"  Conflicts           : {s.Conflicts}");
        Console.WriteLine($"  Indeterminate       : {s.Indeterminate}");
        Console.WriteLine($"  Reconciliation      : {(r.Passed ? "PASS" : "FAIL")}");
        foreach (string f in r.Failures)
            Console.WriteLine($"    ✗ {f}");
        Console.WriteLine($"  Run validity        : {(s.Valid ? "VALID" : "INVALID")}" +
                          (s.ExpectFaults ? " (expect-faults waivers active)" : ""));
        foreach (string w in s.ValidityWarnings)
            Console.WriteLine($"    ⚠ {w}");
        Console.WriteLine();
        Console.WriteLine($"  Artifacts written to: {output}");
    }
}
