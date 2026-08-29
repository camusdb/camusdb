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
            return await Parser.Default
                .ParseArguments<InitOptions, RunOptions, CleanupOptions, ReportOptions, CompareOptions, BaselineOptions>(args)
                .MapResult(
                    (InitOptions o) => RunInitAsync(o, cts.Token),
                    (RunOptions o) => RunWorkloadAsync(o, cts.Token),
                    (CleanupOptions o) => RunCleanupAsync(o, cts.Token),
                    (ReportOptions o) => RunReportAsync(o, cts.Token),
                    (CompareOptions o) => RunCompareAsync(o, cts.Token),
                    (BaselineOptions o) => RunBaselineAsync(o, cts.Token),
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
        if (DatasetShapeError(o) is string shapeError)
        {
            Console.Error.WriteLine(shapeError);
            return 2;
        }

        Dataset dataset = new(o.Seed, o.Rows, o.PayloadBytes, o.Tables);
        await using CamusConnection conn = await OpenSingleAsync(o, ct).ConfigureAwait(false);

        Console.WriteLine($"Ensuring schema for database '{o.Database}' (fingerprint {dataset.Fingerprint()})...");
        await dataset.EnsureSchemaAsync(conn, ct).ConfigureAwait(false);
        Console.WriteLine($"Seeding {o.Rows} rows over {dataset.Tables} table(s) (batch {o.Batch})...");
        await dataset.SeedAsync(conn, o.Batch, ct).ConfigureAwait(false);
        Console.WriteLine("Init complete.");
        return 0;
    }

    /// <summary>
    /// The message for a dataset shape that cannot be seeded, or null when the shape is sound. A table
    /// per row is the hard ceiling: a table with no rows is created, never read, and never splits, so
    /// the run would quietly test less than it claims. Checked by both verbs because <c>init</c> writes
    /// the schema the later <c>run</c> depends on.
    /// </summary>
    private static string? DatasetShapeError(CommonOptions o)
    {
        if (o.Tables < 1)
            return $"--tables must be >= 1 (got {o.Tables}).";
        if (o.Rows < 1)
            return $"--rows must be >= 1 (got {o.Rows}).";
        if (o.Tables > o.Rows)
            return $"--tables ({o.Tables}) cannot exceed --rows ({o.Rows}); every table must hold at least one row.";
        return null;
    }

    /// <summary>The result of one measured run: its exit code plus what a sweep needs to rank it.</summary>
    private sealed record RunOutcome(int ExitCode, RunSummary? Summary, bool ReconciliationPassed)
    {
        /// <summary>An option the run refused before doing any work.</summary>
        public static RunOutcome Rejected { get; } = new(2, null, false);
    }

    /// <summary>
    /// Dispatches the <c>run</c> verb: one measured run, or a concurrency sweep that performs one
    /// complete run per worker count.
    ///
    /// <para>A sweep point is a full run — its own warm-up, measured window, artifacts and
    /// reconciliation — rather than a second measured window sharing the first one's setup. Sharing
    /// would make each point's correctness accounting depend on the point before it, so a single bad
    /// point would invalidate the rest of the sweep instead of itself.</para>
    /// </summary>
    private static async Task<int> RunWorkloadAsync(RunOptions o, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(o.ConcurrencySweep))
            return (await RunOneAsync(o, ct).ConfigureAwait(false)).ExitCode;

        if (!TryParseSweep(o.ConcurrencySweep, out List<int> points, out string? sweepError))
        {
            Console.Error.WriteLine($"--concurrency-sweep: {sweepError}");
            return 2;
        }
        if (o.Mode != "closed")
        {
            Console.Error.WriteLine(
                "--concurrency-sweep varies the number of in-flight workers, which only defines the load in " +
                "closed-loop mode. Pass --mode closed, or sweep --target-ops with separate open-loop runs.");
            return 2;
        }
        if (Directory.Exists(o.Output))
        {
            Console.Error.WriteLine($"Output directory already exists: {o.Output}. Refusing to overwrite.");
            return 2;
        }

        Console.WriteLine($"Concurrency sweep over {points.Count} point(s): {string.Join(", ", points)}");

        List<SweepPoint> results = new();
        int worstExitCode = 0;

        foreach (int workers in points)
        {
            string pointOutput = Path.Combine(o.Output, $"workers-{workers}");
            Console.WriteLine();
            Console.WriteLine($"===> Sweep point: {workers} worker(s) -> {pointOutput}");

            RunOutcome outcome = await RunOneAsync(o.CloneFor(workers, pointOutput), ct).ConfigureAwait(false);
            worstExitCode = Math.Max(worstExitCode, outcome.ExitCode);

            if (outcome.Summary is null)
            {
                // The point was refused before it ran, so there is nothing to rank. Later points would
                // be refused for the same reason, and continuing would bury the message.
                Console.Error.WriteLine($"Sweep stopped at {workers} worker(s): the run was refused.");
                break;
            }

            results.Add(new SweepPoint(workers, outcome.Summary, outcome.ReconciliationPassed, pointOutput));

            // Write the cross-point artifacts after every point, so an interrupted sweep still leaves a
            // readable table of what it measured before it stopped.
            Directory.CreateDirectory(o.Output);
            await File.WriteAllTextAsync(Path.Combine(o.Output, "sweep.csv"), SweepReport.RenderCsv(results), ct).ConfigureAwait(false);
            await File.WriteAllTextAsync(Path.Combine(o.Output, "sweep.md"), SweepReport.RenderMarkdown(results), ct).ConfigureAwait(false);
        }

        if (results.Count > 0)
        {
            Console.WriteLine();
            Console.Write(SweepReport.RenderMarkdown(results));
            Console.WriteLine($"Sweep artifacts: {Path.Combine(o.Output, "sweep.md")}");
        }

        return worstExitCode;
    }

    /// <summary>Parses the sweep list, rejecting a non-positive or duplicated point.</summary>
    private static bool TryParseSweep(string value, out List<int> points, out string? error)
    {
        points = new List<int>();
        error = null;

        foreach (string part in value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (!int.TryParse(part, out int workers) || workers < 1)
            {
                error = $"'{part}' is not a worker count (expected a positive integer).";
                return false;
            }
            if (points.Contains(workers))
            {
                error = $"worker count {workers} appears more than once.";
                return false;
            }
            points.Add(workers);
        }

        if (points.Count == 0)
        {
            error = "no worker counts were given.";
            return false;
        }
        return true;
    }

    /// <summary>
    /// Runs one measured workload end to end and reports what it produced, not just whether it
    /// worked: a concurrency sweep needs each point's summary and reconciliation verdict to decide
    /// where the saturation point is.
    /// </summary>
    private static async Task<RunOutcome> RunOneAsync(RunOptions o, CancellationToken ct)
    {
        if (o.ReadPercent + o.WritePercent != 100)
        {
            Console.Error.WriteLine($"--read-percent + --write-percent must equal 100 (got {o.ReadPercent}+{o.WritePercent}).");
            return RunOutcome.Rejected;
        }
        if (!TryParseLocking(o.Locking, out CamusLocking locking))
        {
            Console.Error.WriteLine($"--locking must be optimistic or pessimistic (got '{o.Locking}').");
            return RunOutcome.Rejected;
        }
        if (!TryParseIsolation(o.Isolation, out CamusIsolationLevel isolation))
        {
            Console.Error.WriteLine($"--isolation must be read_committed or serializable (got '{o.Isolation}').");
            return RunOutcome.Rejected;
        }
        if (o.Workload is not ("accounts" or "bank" or "fanout"))
        {
            Console.Error.WriteLine($"--workload must be accounts, bank or fanout (got '{o.Workload}').");
            return RunOutcome.Rejected;
        }
        if (DatasetShapeError(o) is string shapeError)
        {
            Console.Error.WriteLine(shapeError);
            return RunOutcome.Rejected;
        }
        if (!Reporting.NodeTarget.TryParseAll(o.MetricsEndpoints, out List<Reporting.NodeTarget> metricsTargets, out string? targetError))
        {
            Console.Error.WriteLine($"--metrics-endpoint: {targetError}");
            return RunOutcome.Rejected;
        }
        TimeSpan metricsInterval;
        try
        {
            metricsInterval = DurationParser.Parse(o.MetricsInterval);
        }
        catch (FormatException)
        {
            Console.Error.WriteLine($"--metrics-interval is not a duration (got '{o.MetricsInterval}').");
            return RunOutcome.Rejected;
        }
        // An API base carries no default path: /v1/version and /v1/cluster/health are appended to it.
        if (!Reporting.NodeTarget.TryParseAll(o.NodeEndpoints, out List<Reporting.NodeTarget> nodeEndpoints, out string? nodeError, defaultPath: null))
        {
            Console.Error.WriteLine($"--node-endpoint: {nodeError}");
            return RunOutcome.Rejected;
        }
        if (o.Workload == "fanout" && o.Tables < 2)
        {
            Console.Error.WriteLine(
                $"--workload fanout moves each transfer between two different tables, so it needs --tables >= 2 (got {o.Tables}).");
            return RunOutcome.Rejected;
        }

        // Both transfer shapes conserve SUM(balance) and both retry a conflicted transfer, so they take
        // the same reconciliation treatment; only the choice of the second row differs.
        bool transfers = o.Workload is "bank" or "fanout";
        bool crossTable = o.Workload == "fanout";
        if (Directory.Exists(o.Output))
        {
            Console.Error.WriteLine($"Output directory already exists: {o.Output}. Refusing to overwrite.");
            return RunOutcome.Rejected;
        }

        Dataset dataset = new(o.Seed, o.Rows, o.PayloadBytes, o.Tables);

        // The per-row check exists because SUM(balance) conservation cannot see an atomicity break whose
        // leaked writes cancel each other out. It needs a starting balance and version for every row, so
        // it is built before setup and given its baseline from a scan there. Only the transfer shapes
        // keep a journal to compare against, so only they can run it.
        Metrics.RowAttribution? attribution = null;
        RowAttributionResult? attributionSkip = null;
        if (transfers && o.NoRowAttribution)
        {
            attributionSkip = RowAttributionResult.Disabled("turned off by --no-row-attribution.");
        }
        else if (transfers)
        {
            attribution = Metrics.RowAttribution.TryCreate(dataset, o.Seed, out string? why);
            if (attribution is null)
                attributionSkip = RowAttributionResult.Unavailable(why!);
        }

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
                return RunOutcome.Rejected;
            }

            // One baseline scan serves both purposes when the per-row check is on: it yields the same two
            // aggregate totals the bands need, so the run pays for a scan instead of a scan plus two
            // aggregate queries. A scan that fails costs the sharper check, never the run — the aggregate
            // baselines are read the old way and the verdict says which check went missing.
            RowScanTotals? baseline = null;
            if (attribution is not null)
            {
                try
                {
                    baseline = await attribution.CaptureBaselineAsync(setup, ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    Console.Error.WriteLine(
                        $"Per-row baseline scan failed ({ex.GetType().Name}: {ex.Message}); " +
                        "continuing with the aggregate invariants only.");
                    attribution = null;
                    attributionSkip = RowAttributionResult.Unavailable(
                        $"the per-row baseline scan did not complete: {ex.GetType().Name}: {ex.Message}");
                }
            }

            if (baseline is not null)
            {
                baselineVersionSum = baseline.VersionSum;
                baselineBalanceSum = baseline.BalanceSum;
            }
            else
            {
                baselineVersionSum = await Reconciliation.ReadVersionSumAsync(setup, dataset, ct).ConfigureAwait(false);
                if (transfers)
                    baselineBalanceSum = await Reconciliation.ReadBalanceSumAsync(setup, dataset, ct).ConfigureAwait(false);
            }
        }

        ConnectionSettings settings = new(locking, isolation, o.NoAutoPrepare, o.RequestTimeout);
        await using ConnectionSet connections =
            await ConnectionSet.OpenAsync(o.Endpoint, o.Database, o.Protocol, o.Connections, settings, ct).ConfigureAwait(false);

        // The transfer shapes contend across the whole keyspace and conserve SUM(balance); the accounts
        // write stays shard-disjoint and conflict-free. Both report as writes, so scheduling and metrics
        // are identical either way. Fanout differs from bank only in where the second leg lands: always
        // in another table, so every transaction spans two key spaces and the load reaches every
        // partition instead of the one that owns a single table.
        // The transfer ledger journals every terminal transfer attempt so a conservation deficit can be
        // attributed to exact rows and moments; see TransferLedger. Only a transfer workload keeps one.
        Metrics.TransferLedger? transferLedger = transfers ? new Metrics.TransferLedger(attribution) : null;

        IWriteOperation writeOperation = transfers
            ? new TransferOperation(
                connections, dataset, o.Rows, locking, isolation, ledger: transferLedger, crossTable: crossTable)
            : new WriteOperation(connections, dataset, o.WritesPerTransaction, locking, isolation);
        OperationDispatcher dispatcher = new(
            new ReadOperation(connections, dataset),
            writeOperation);

        TimeSpan warmup = DurationParser.Parse(o.Warmup);
        TimeSpan measure = DurationParser.Parse(o.Duration);
        TimeSpan drain = DurationParser.Parse(o.Drain);

        Console.WriteLine($"Running {o.Mode}-loop {o.Workload} workload: {o.ReadPercent}/{o.WritePercent} read/write, " +
                          $"{o.Workers} workers over {o.Connections} connections, {o.Rows} rows over " +
                          $"{dataset.Tables} table(s), warmup {warmup}, measure {measure}.");

        RunMetrics metrics;
        IReadOnlyList<IntervalRow> intervals;

        // The collector covers warm-up, measurement and drain. Warm-up is included on purpose: a
        // backlog that was already climbing before the measured window opened is the difference
        // between a steady-state result and a snapshot of a transient.
        // The generator watches itself for the whole run, always. A client that ran out of CPU, paused
        // for GC, or was never allowed enough in-flight work produces a flat curve that is easy to
        // read as a saturated server.
        ClientResourceSampler clientResources = new();
        clientResources.Start();

        Reporting.MetricsSampler? sampler = null;
        Reporting.MetricsSamplerResult? samplerResult = null;
        if (metricsTargets.Count > 0)
        {
            Directory.CreateDirectory(o.Output);
            sampler = new Reporting.MetricsSampler(metricsTargets, metricsInterval, Path.Combine(o.Output, "node-metrics.csv"));
            sampler.Start();
            Console.WriteLine($"Collecting metrics from {metricsTargets.Count} node(s) every " +
                              $"{Math.Max(metricsInterval.TotalSeconds, Reporting.MetricsSampler.MinInterval.TotalSeconds):F0}s: " +
                              string.Join(", ", metricsTargets.Select(t => t.Name)));
        }

        try
        {
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
        }
        finally
        {
            await clientResources.StopAsync().ConfigureAwait(false);
            if (sampler is not null)
                samplerResult = await sampler.StopAsync().ConfigureAwait(false);
        }

        // Write the measured artifacts FIRST, before reconciliation. The summary/intervals/errors are
        // the run's actual results and are always valid; reconciliation is a separate post-run check
        // that queries a cluster which may still be recovering from a fault. Writing the measured data
        // first means a reconciliation that cannot complete downgrades the verdict to "could not
        // verify" — it never discards a run that produced perfectly good data.
        RunSummary summary = RunSummary.Build(
            metrics, o.Mode, o.TargetOps, measure.TotalSeconds, o.ExpectFaults,
            writeOperation.RetryAttempts, writeOperation.RetriedTxns, writeOperation.MaxAttemptsUsed);
        RunManifest manifest = BuildManifest(o, dataset, locking, isolation);

        ResultWriter writer = new(o.Output);
        await writer.WriteAsync(manifest, summary, intervals, metrics.Errors, ct).ConfigureAwait(false);

        if (transferLedger is not null)
            await transferLedger.WriteAsync(o.Output, dataset, o.Rows, ct).ConfigureAwait(false);

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

        // The generator's own resource use over the measured window, and whether it was the limiter.
        ClientResources? clientResourceSummary = clientResources.Summarize(
            metrics.MeasureStartUtc, summary.MeasuredSeconds, o.Mode, o.Workers, o.Connections,
            o.MaxInFlight, summary.AchievedOpsPerSec, MeanOperationLatencyMs(summary));

        if (clientResourceSummary is not null)
        {
            await File.WriteAllTextAsync(
                Path.Combine(o.Output, "client-resources.json"),
                System.Text.Json.JsonSerializer.Serialize(
                    clientResourceSummary, new System.Text.Json.JsonSerializerOptions { WriteIndented = true }),
                ct).ConfigureAwait(false);
        }

        // Server-side evidence, when the run collected it. Written before reconciliation for the same
        // reason the measured artifacts are: a cluster that is still settling must not cost the run the
        // metrics it already gathered.
        if (sampler is not null && samplerResult is not null)
        {
            await File.WriteAllTextAsync(
                Path.Combine(o.Output, "node-metrics-meta.json"),
                System.Text.Json.JsonSerializer.Serialize(
                    samplerResult, new System.Text.Json.JsonSerializerOptions { WriteIndented = true }),
                ct).ConfigureAwait(false);

            await sampler.WriteLastScrapesAsync(o.Output, ct).ConfigureAwait(false);

            foreach (Reporting.NodeSampleStats node in samplerResult.Nodes)
            {
                if (node.Failed > 0)
                    Console.Error.WriteLine(
                        $"  ⚠ metrics scrape for '{node.Node}' failed {node.Failed} of " +
                        $"{node.Failed + node.Succeeded} time(s); last error: {node.LastError}");
            }

            await WriteBottleneckReportAsync(
                o.Output, manifest, summary, sampler, metrics, measure, clientResourceSummary, ct).ConfigureAwait(false);
        }

        // What the cluster says it is. Captured after the measured window so the recorded placement is
        // the one the run left behind, and never allowed to fail the run: this is provenance for a
        // measurement that already happened.
        if (nodeEndpoints.Count > 0)
        {
            try
            {
                Cluster.ClusterProbe probe = new(nodeEndpoints, o.Database);
                await using CamusConnection rangeReader = await OpenSingleAsync(o, ct).ConfigureAwait(false);
                Cluster.ClusterFacts facts = await probe.CaptureAsync(rangeReader, dataset.TableNames, ct).ConfigureAwait(false);

                await File.WriteAllTextAsync(
                    Path.Combine(o.Output, "cluster-facts.json"), Cluster.ClusterProbe.Serialize(facts), ct).ConfigureAwait(false);

                Console.WriteLine($"  Cluster fingerprint : {facts.DurabilityFingerprint}");
                foreach (Cluster.NodeFacts node in facts.Nodes)
                {
                    foreach (string error in node.Errors)
                        Console.Error.WriteLine($"  ⚠ node '{node.Node}' could not answer {error}");
                }
                foreach (string error in facts.Errors)
                    Console.Error.WriteLine($"  ⚠ {error}");
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                Console.Error.WriteLine($"  ⚠ could not capture cluster facts: {ex.GetType().Name}: {ex.Message}");
            }
        }

        // Reconciliation (outside the measured window). Never crashes the run: neither a query that
        // cannot complete nor a connection that cannot even open discards the artifacts above — both
        // downgrade the verdict to "could not verify".
        ReconciliationResult reconciliation;
        try
        {
            await using CamusConnection verify = await OpenSingleAsync(o, ct).ConfigureAwait(false);
            reconciliation = await Reconciliation
                .VerifyOrInconclusiveAsync(
                    verify, dataset, metrics, baselineVersionSum, writeOperation.CommittedRows,
                    writeOperation.IndeterminateTxns, o.WritesPerTransaction, o.ExpectFaults, o.Rows, ct,
                    bankMode: transfers, baselineBalanceSum: baselineBalanceSum,
                    retryBudget: TimeSpan.FromSeconds(Math.Max(1, o.ReconcileTimeout)),
                    rowAttribution: attribution, rowAttributionSkip: attributionSkip)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            reconciliation = Reconciliation.Inconclusive(
                $"{ex.GetType().Name}: {ex.Message}", writeOperation.IndeterminateTxns, metrics.Conflicts,
                baselineBalanceSum, rowAttributionExpected: attribution is not null);
        }

        if (reconciliation.RowAttribution is RowAttributionResult rowResult)
            await Metrics.RowAttribution.WriteViolationsAsync(o.Output, rowResult, ct).ConfigureAwait(false);

        await File.WriteAllTextAsync(
            Path.Combine(o.Output, "reconciliation.json"),
            System.Text.Json.JsonSerializer.Serialize(reconciliation, new System.Text.Json.JsonSerializerOptions { WriteIndented = true }),
            ct).ConfigureAwait(false);

        PrintConsole(summary, reconciliation, o.Output, clientResourceSummary);
        return new RunOutcome(summary.Valid && reconciliation.Passed ? 0 : 1, summary, reconciliation.Passed);
    }

    /// <summary>
    /// Writes <c>bottleneck-report.md</c> from the run's own artifacts plus the metrics it collected,
    /// so a cluster run needs no second command to produce its evidence. The window handed to the
    /// report is the measured one, which is what makes the server figures comparable between runs —
    /// the collector also covered warm-up and drain, and those must not enter the numbers.
    ///
    /// <para>Failure here is reported and swallowed. The report is a convenience over data already on
    /// disk; the <c>report</c> verb can rebuild it later, and losing it must not fail a valid run.</para>
    /// </summary>
    private static async Task WriteBottleneckReportAsync(
        string output, RunManifest manifest, RunSummary summary, Reporting.MetricsSampler sampler,
        RunMetrics metrics, TimeSpan measure, ClientResources? clientResources, CancellationToken ct)
    {
        try
        {
            string csvPath = Path.Combine(output, "node-metrics.csv");
            if (!File.Exists(csvPath))
                return;

            Reporting.NodeMetricsSeries series = Reporting.NodeMetricsSeries.Load(csvPath);

            // Histogram means still come from a raw scrape: the series keeps every _sum and _count but
            // drops the buckets, and one node's stage means are worth more than none.
            Reporting.PrometheusScrape? scrape = sampler.LastScrapes.Count > 0
                ? Reporting.PrometheusScrape.Parse(sampler.LastScrapes.Values.First())
                : null;

            string report = Reporting.BottleneckReport.Build(
                manifest, summary, scrape, series,
                Reporting.MetricsWindow.Measured(metrics.MeasureStartUtc, measure.TotalSeconds),
                clientResources);

            await File.WriteAllTextAsync(Path.Combine(output, "bottleneck-report.md"), report, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            Console.Error.WriteLine($"  ⚠ could not build bottleneck-report.md: {ex.GetType().Name}: {ex.Message}");
        }
    }

    /// <summary>
    /// Compares two run directories. Exit code 3 means the pair is not comparable — deliberately
    /// distinct from 1, a gate failure: "these runs cannot be compared" and "this run missed its
    /// target" call for different actions, and a script that conflates them will re-run the wrong
    /// thing.
    /// </summary>
    private static async Task<int> RunCompareAsync(CompareOptions o, CancellationToken ct)
    {
        RunBundle baseline;
        RunBundle candidate;
        try
        {
            baseline = RunBundle.Load(o.Baseline);
            candidate = RunBundle.Load(o.Candidate);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Could not load a run directory: {ex.Message}");
            return 2;
        }

        ComparisonResult result = RunComparison.Compare(
            baseline, candidate, o.Allow.ToList(), o.RequireRatio, o.RequireOps, o.P99BudgetMs);

        string report = RunComparison.Render(baseline, candidate, result);
        Console.Write(report);

        if (!string.IsNullOrWhiteSpace(o.Output))
        {
            Directory.CreateDirectory(o.Output);
            await File.WriteAllTextAsync(Path.Combine(o.Output, "comparison.md"), report, ct).ConfigureAwait(false);
            Console.WriteLine($"Wrote {Path.Combine(o.Output, "comparison.md")}");
        }

        if (!result.Comparable)
            return 3;
        return result.GatesPassed ? 0 : 1;
    }

    /// <summary>
    /// Aggregates repeated runs into a baseline. Exit code 3 means the runs are not one baseline —
    /// the same distinction the comparison draws, because averaging different experiments is a
    /// different mistake from failing to reach a bar.
    /// </summary>
    private static async Task<int> RunBaselineAsync(BaselineOptions o, CancellationToken ct)
    {
        List<RunBundle> bundles = new();
        foreach (string directory in o.Runs)
        {
            try
            {
                bundles.Add(RunBundle.Load(directory));
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Could not load '{directory}': {ex.Message}");
                return 2;
            }
        }

        if (bundles.Count == 0)
        {
            Console.Error.WriteLine("--runs named no run directories.");
            return 2;
        }

        BaselineResult result = BaselineSummary.Build(bundles);
        string report = BaselineSummary.Render(result);
        Console.Write(report);

        if (!string.IsNullOrWhiteSpace(o.Output))
        {
            Directory.CreateDirectory(o.Output);
            await File.WriteAllTextAsync(Path.Combine(o.Output, "baseline.md"), report, ct).ConfigureAwait(false);
            Console.WriteLine($"Wrote {Path.Combine(o.Output, "baseline.md")}");
        }

        if (!result.Comparable)
            return 3;
        return result.Established ? 0 : 1;
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
        if (string.IsNullOrWhiteSpace(o.Metrics) && string.IsNullOrWhiteSpace(o.NodeMetrics))
        {
            Console.Error.WriteLine("Pass --metrics (a single /metrics scrape), --node-metrics (a collected node-metrics.csv), or both.");
            return 2;
        }
        if (!string.IsNullOrWhiteSpace(o.Metrics) && !File.Exists(o.Metrics))
        {
            Console.Error.WriteLine($"Metrics scrape file not found: {o.Metrics}");
            return 2;
        }
        if (!string.IsNullOrWhiteSpace(o.NodeMetrics) && !File.Exists(o.NodeMetrics))
        {
            Console.Error.WriteLine($"Node metrics file not found: {o.NodeMetrics}");
            return 2;
        }

        var json = new System.Text.Json.JsonSerializerOptions(System.Text.Json.JsonSerializerDefaults.Web);
        RunManifest manifest = System.Text.Json.JsonSerializer.Deserialize<RunManifest>(
            await File.ReadAllTextAsync(manifestPath, ct).ConfigureAwait(false), json)!;
        RunSummary summary = System.Text.Json.JsonSerializer.Deserialize<RunSummary>(
            await File.ReadAllTextAsync(summaryPath, ct).ConfigureAwait(false), json)!;
        Reporting.PrometheusScrape? scrape = string.IsNullOrWhiteSpace(o.Metrics)
            ? null
            : Reporting.PrometheusScrape.Parse(await File.ReadAllTextAsync(o.Metrics, ct).ConfigureAwait(false));

        Reporting.NodeMetricsSeries? series = string.IsNullOrWhiteSpace(o.NodeMetrics)
            ? null
            : Reporting.NodeMetricsSeries.Load(o.NodeMetrics);

        // The measured window comes from the run's own anchor when it wrote one. Without it the report
        // covers every sample collected, which includes warm-up and drain — so it says which it used.
        Reporting.MetricsWindow window = ReadMeasuredWindow(o.Output) ?? Reporting.MetricsWindow.All;

        string report = Reporting.BottleneckReport.Build(manifest, summary, scrape, series, window);
        string reportPath = Path.Combine(o.Output, "bottleneck-report.md");
        await File.WriteAllTextAsync(reportPath, report, ct).ConfigureAwait(false);
        Console.WriteLine($"Wrote {reportPath}");
        return 0;
    }

    /// <summary>
    /// The measured window recorded by the run, or null when <c>run-meta.json</c> is absent or
    /// unreadable. A report built without it still works; it just covers warm-up and drain too, which
    /// is why the caller must not silently substitute one for the other.
    /// </summary>
    private static Reporting.MetricsWindow? ReadMeasuredWindow(string outputDir)
    {
        try
        {
            string path = Path.Combine(outputDir, "run-meta.json");
            if (!File.Exists(path))
                return null;

            using System.Text.Json.JsonDocument doc = System.Text.Json.JsonDocument.Parse(File.ReadAllText(path));
            if (!doc.RootElement.TryGetProperty("measureStartUtc", out System.Text.Json.JsonElement startElement) ||
                !doc.RootElement.TryGetProperty("measureSeconds", out System.Text.Json.JsonElement secondsElement))
                return null;

            if (!DateTime.TryParse(startElement.GetString(), null,
                    System.Globalization.DateTimeStyles.RoundtripKind, out DateTime start))
                return null;

            return Reporting.MetricsWindow.Measured(start.ToUniversalTime(), secondsElement.GetDouble());
        }
        catch (Exception)
        {
            // A malformed anchor is not worth failing a report over; the full-range window is correct,
            // just wider than the measured one.
            return null;
        }
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
        Tables: dataset.Tables,
        WorkloadKind: o.Workload,
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
        ClientPackageVersion: ClientPackageVersion());

    /// <summary>
    /// The client library version actually loaded, read from the assembly rather than written down.
    /// A hand-maintained string silently keeps naming an old version after a package bump, and a
    /// comparison that trusts it then compares two different clients as if they were one.
    /// </summary>
    private static string ClientPackageVersion()
    {
        Assembly assembly = typeof(CamusConnection).Assembly;
        string version = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? assembly.GetName().Version?.ToString()
            ?? "unknown";
        return $"{assembly.GetName().Name} {version}";
    }

    /// <summary>
    /// The mean whole-operation latency the run saw, weighted by how many of each kind completed. It
    /// feeds the Little's Law check, which needs the average time an operation occupied a worker.
    /// </summary>
    private static double MeanOperationLatencyMs(RunSummary s)
    {
        long total = s.CompletedRead + s.CompletedWrite;
        if (total == 0)
            return 0;

        // p50 stands in for the mean here: the summary keeps percentiles, not a sum, and the check
        // only needs the order of magnitude of in-flight work.
        return ((s.ReadLatency.P50 * s.CompletedRead) + (s.WriteLatency.P50 * s.CompletedWrite)) / total;
    }

    private static void PrintConsole(RunSummary s, ReconciliationResult r, string output, ClientResources? client)
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
        Console.WriteLine($"  Conflict retries    : {s.RetryAttempts} over {s.RetriedTxns} txn(s)");
        Console.WriteLine($"  Retries per write   : {s.RetriesPerWriteTxn:F3}");
        Console.WriteLine($"  Max attempts used   : {s.MaxAttemptsUsed}");
        Console.WriteLine($"  Indeterminate       : {s.Indeterminate}");
        Console.WriteLine($"  Reconciliation      : {(r.Passed ? "PASS" : "FAIL")}");
        Console.WriteLine($"  Row attribution     : {DescribeRowAttribution(r.RowAttribution)}");
        foreach (string f in r.Failures)
            Console.WriteLine($"    ✗ {f}");
        Console.WriteLine($"  Run validity        : {(s.Valid ? "VALID" : "INVALID")}" +
                          (s.ExpectFaults ? " (expect-faults waivers active)" : ""));
        foreach (string w in s.ValidityWarnings)
            Console.WriteLine($"    ⚠ {w}");
        if (client is not null)
        {
            Console.WriteLine($"  Client headroom     : {(client.HeadroomAvailable ? "OK" : "SUSPECT")} " +
                              $"(CPU {client.CpuUtilization:P0} of {client.ProcessorCount} core(s), " +
                              $"alloc {client.AllocatedMbPerSecond:F0} MB/s, peak pool queue {client.PeakThreadPoolQueue})");
            foreach (string warning in client.Warnings)
                Console.WriteLine($"    ⚠ {warning}");
        }
        Console.WriteLine();
        Console.WriteLine($"  Artifacts written to: {output}");
    }

    /// <summary>
    /// One line for the per-row verdict. It always prints for a transfer run, including when the check
    /// did not run: a reader who sees only "Reconciliation: PASS" cannot tell whether the strongest
    /// check ran, and that is precisely how a leak that cancels in the totals has passed before.
    /// </summary>
    private static string DescribeRowAttribution(RowAttributionResult? r) => r switch
    {
        null => "n/a (only the transfer workloads keep a per-row journal)",
        { Status: RowAttributionStatus.Disabled } =>
            $"OFF — {r.Reason} SUM(balance) alone cannot see leaked writes that cancel out.",
        { Status: RowAttributionStatus.Unavailable } => $"NOT VERIFIED — {r.Reason}",
        { TotalViolations: 0 } =>
            $"PASS ({r.RowsScanned} row(s) scanned, {r.RowsInAmbiguityBand} inside the indeterminate band)",
        _ =>
            $"FAIL ({r.TotalViolations} violating row(s) of {r.RowsScanned} scanned; " +
            $"{r.RowsInAmbiguityBand} inside the indeterminate band)",
    };
}
