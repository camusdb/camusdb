/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;
using CamusDB.Workload.Metrics;
using CamusDB.Workload.Results;

namespace CamusDB.Workload.Reporting;

/// <summary>
/// Builds the diagnostic <c>bottleneck-report.md</c> by aligning the client-side run summary with
/// server-side metrics. It is <b>evidence, not a verdict</b>: it surfaces the candidate limiting
/// stages each backed by a named measurement, and it deliberately refuses to declare a root cause
/// from the largest inclusive duration alone — awaited durability can dominate latency while using
/// little CPU, and overlapping stage durations are not additive. Missing metrics render as "n/a" so a
/// partial scrape still produces a usable report.
///
/// <para>Two server-side sources are accepted and they answer different questions. A single
/// end-of-run <see cref="PrometheusScrape"/> gives histogram means and totals for one node since that
/// node started. A collected <see cref="NodeMetricsSeries"/> gives every node's increase over the
/// measured window, which is what a cluster measurement needs: it can show that one leader carried
/// the writes, that a gateway pool did not spread the load, and that a backlog grew all run. When the
/// series is present the cumulative single-node totals are dropped rather than printed beside the
/// windowed ones, so a reader cannot quote the wrong number.</para>
/// </summary>
public static class BottleneckReport
{
    public static string Build(
        RunManifest manifest,
        RunSummary summary,
        PrometheusScrape? scrape,
        NodeMetricsSeries? series = null,
        MetricsWindow? window = null,
        ClientResources? clientResources = null)
    {
        MetricsWindow w = window ?? MetricsWindow.All;
        bool hasSeries = series is not null && !series.IsEmpty;

        StringBuilder sb = new();
        sb.AppendLine("# Bottleneck report").AppendLine();
        sb.AppendLine($"Run against `{manifest.Endpoint}` (db `{manifest.Database}`), seed {manifest.Seed}, " +
                      $"{manifest.Workers} workers / {manifest.Connections} connections, mode `{summary.Mode}`.");
        sb.AppendLine($"Validity: **{(summary.Valid ? "VALID" : "INVALID")}**. " +
                      "Server durability config is operator-supplied — do not compare runs with different settings.");
        if (hasSeries)
        {
            sb.AppendLine($"Server figures below are **increases over the measured window** " +
                          $"({w.StartUtc:HH:mm:ss}–{w.EndUtc:HH:mm:ss} UTC), collected from " +
                          $"{series!.Nodes.Count} node(s): {string.Join(", ", series.Nodes)}.");
        }
        else
        {
            sb.AppendLine("No per-node time series was collected (`--metrics-endpoint`), so server totals below " +
                          "are cumulative since node start and include seeding and warm-up.");
        }
        sb.AppendLine();

        // ── Client: offered vs completed ────────────────────────────────────────
        sb.AppendLine("## Offered vs completed (client)").AppendLine();
        sb.AppendLine("| Metric | Value |");
        sb.AppendLine("|---|---|");
        sb.AppendLine($"| Completed ops/s | {F(summary.AchievedOpsPerSec)} |");
        sb.AppendLine($"| Read ops/s / Write txns/s | {F(summary.ReadOpsPerSec)} / {F(summary.WriteTxnsPerSec)} |");
        sb.AppendLine($"| Offered / Started / Completed | {summary.Offered} / {summary.Started} / {summary.Completed} |");
        sb.AppendLine($"| Schedule drops | {summary.ScheduleDrops} |");
        sb.AppendLine($"| Scheduling delay p50/p99 (ms) | {F(summary.ScheduleDelay.P50)} / {F(summary.ScheduleDelay.P99)} |");
        sb.AppendLine($"| Read latency p50/p95/p99 (ms) | {F(summary.ReadLatency.P50)} / {F(summary.ReadLatency.P95)} / {F(summary.ReadLatency.P99)} |");
        sb.AppendLine($"| Write latency p50/p95/p99 (ms) | {F(summary.WriteLatency.P50)} / {F(summary.WriteLatency.P95)} / {F(summary.WriteLatency.P99)} |");
        sb.AppendLine();

        if (clientResources is not null)
            AppendClientHeadroom(sb, clientResources);

        if (hasSeries)
            AppendWorkDistribution(sb, manifest, series!, w);

        // ── Server stage latency ────────────────────────────────────────────────
        double? handler = scrape?.HistogramMean("camus_request_duration_milliseconds");
        double? execute = scrape?.HistogramMean("camus_execute_duration_milliseconds");
        double? commit = scrape?.HistogramMean("camus_transaction_commit_duration_milliseconds");
        double? scan = scrape?.HistogramMean("camus_query_scan_duration_milliseconds");

        if (scrape is not null)
        {
            sb.AppendLine("## Server stage latency (mean, from one node's end-of-run scrape)").AppendLine();
            sb.AppendLine("| Stage | Mean (ms) | Count |");
            sb.AppendLine("|---|---|---|");
            sb.AppendLine($"| Request handler (all ops) | {Fn(handler)} | {Fc(scrape.HistogramCount("camus_request_duration_milliseconds"))} |");
            sb.AppendLine($"| Executor | {Fn(execute)} | {Fc(scrape.HistogramCount("camus_execute_duration_milliseconds"))} |");
            sb.AppendLine($"| Commit (2PC/WAL finalize) | {Fn(commit)} | {Fc(scrape.HistogramCount("camus_transaction_commit_duration_milliseconds"))} |");
            sb.AppendLine($"| Query scan | {Fn(scan)} | {Fc(scrape.HistogramCount("camus_query_scan_duration_milliseconds"))} |");
            double parseHits = scrape.Sum("camus_sql_parse_cache_total", ("result", "hit"));
            double parseMiss = scrape.Sum("camus_sql_parse_cache_total", ("result", "miss"));
            sb.AppendLine($"| SQL parse cache | hits {Fc(parseHits)} / misses {Fc(parseMiss)} | — |");
            sb.AppendLine();

            // ── Resources ───────────────────────────────────────────────────────
            // .NET runtime metric names follow OpenTelemetry.Instrumentation.Runtime 1.10+ (the "dotnet_*"
            // scheme); the generation label is "gc_heap_generation". Earlier "process_runtime_dotnet_*"
            // names were emitted by 1.9 and are no longer produced.
            double? gcAlloc = scrape.Sum("dotnet_gc_heap_total_allocated_bytes_total");
            double? tpQueue = scrape.Gauge("dotnet_thread_pool_queue_length_total");
            double? tpThreads = scrape.Gauge("dotnet_thread_pool_thread_count_total");
            double gcGen0 = scrape.Sum("dotnet_gc_collections_total", ("gc_heap_generation", "gen0"));
            double gcGen2 = scrape.Sum("dotnet_gc_collections_total", ("gc_heap_generation", "gen2"));

            sb.AppendLine("## Runtime / resources (one node, cumulative)").AppendLine();
            sb.AppendLine("| Metric | Value |");
            sb.AppendLine("|---|---|");
            sb.AppendLine($"| GC allocations (bytes, cumulative) | {Fc(gcAlloc ?? 0)} |");
            sb.AppendLine($"| GC collections gen0 / gen2 | {Fc(gcGen0)} / {Fc(gcGen2)} |");
            sb.AppendLine($"| Thread-pool threads / queue length | {Fn(tpThreads)} / {Fn(tpQueue)} |");
            sb.AppendLine();
        }

        // ── Storage / WAL ───────────────────────────────────────────────────────
        if (hasSeries)
        {
            AppendPerNodeStorage(sb, series!, w);
            AppendCommitShape(sb, series!, w);
            AppendBacklogGrowth(sb, series!, w);
        }
        else if (scrape is not null)
        {
            double kvBatches = scrape.Sum("kahuna_kv_write_batches_total");
            double kvEntries = scrape.Sum("kahuna_kv_write_entries_total");
            double walBatches = scrape.Sum("raft_wal_batches_total");
            double walOps = scrape.Sum("raft_wal_operations_total");
            double? durableOutstanding = scrape.Gauge("kahuna_durable_tx_outstanding");

            sb.AppendLine("## Storage / WAL (one node, cumulative since node start)").AppendLine();
            sb.AppendLine("| Metric | Value |");
            sb.AppendLine("|---|---|");
            sb.AppendLine($"| Kahuna KV write batches / entries | {Fc(kvBatches)} / {Fc(kvEntries)} |");
            sb.AppendLine($"| Kahuna KV entries per batch | {Ratio(kvEntries, kvBatches)} |");
            sb.AppendLine($"| Kommander WAL batches / operations | {Fc(walBatches)} / {Fc(walOps)} |");
            sb.AppendLine($"| WAL operations per batch | {Ratio(walOps, walBatches)} |");
            sb.AppendLine($"| Durable-tx outstanding (gauge) | {Fn(durableOutstanding)} |");
            sb.AppendLine("");
            sb.AppendLine("> These totals include seeding and warm-up and cover one node only. Collect a per-node");
            sb.AppendLine("> series with `--metrics-endpoint` to get the measured window's increase per node.");
            sb.AppendLine();
        }

        // ── Errors ──────────────────────────────────────────────────────────────
        sb.AppendLine("## Errors / conflicts / retries").AppendLine();
        sb.AppendLine($"- Failed: {summary.Failed} (conflict {summary.Conflicts}, transient {summary.Transient}, " +
                      $"domain {summary.DomainErrors}, internal {summary.InternalErrors})");
        sb.AppendLine();

        // ── Candidate limiting stages ───────────────────────────────────────────
        if (scrape is not null)
        {
            sb.AppendLine("## Candidate limiting stages").AppendLine();
            sb.AppendLine("Ranked by mean stage duration. **This is evidence, not a verdict** — a stage with the");
            sb.AppendLine("largest mean is not automatically the cause: awaited durability (commit/WAL) can dominate");
            sb.AppendLine("latency while consuming little CPU, and overlapping stage durations are not additive.");
            sb.AppendLine();

            List<(string Name, double? Mean, string Evidence)> stages = new()
            {
                ("Commit (2PC/WAL durability)", commit, "camus_transaction_commit_duration_milliseconds + raft_wal_* batch density"),
                ("Executor (parse/plan/stage)", execute, "camus_execute_duration_milliseconds + camus_sql_parse_cache_total"),
                ("Query scan (storage read)", scan, "camus_query_scan_duration_milliseconds + camus_query_rows_total"),
                ("Request handler (transport+dispatch)", handler, "camus_request_duration_milliseconds"),
            };
            int rank = 1;
            foreach (var stage in stages.Where(s => s.Mean is not null).OrderByDescending(s => s.Mean))
            {
                sb.AppendLine($"{rank}. **{stage.Name}** — mean {Fn(stage.Mean)} ms. Evidence: `{stage.Evidence}`.");
                rank++;
                if (rank > 3)
                    break;
            }
            sb.AppendLine();

            // Durability-bound heuristic, stated as unverified inference.
            if (commit is double c && execute is double e && c > 2 * Math.Max(e, 0.001))
            {
                bool tpIdle = (scrape.Gauge("dotnet_thread_pool_queue_length_total") ?? 0) < 4;
                sb.AppendLine($"> Inference (unverified): commit mean ({F(c)} ms) is ≫ executor mean ({F(e)} ms)" +
                              (tpIdle ? " while the thread-pool queue is short" : "") +
                              ", consistent with a **durability-bound** write path (awaited Raft/WAL fsync), not CPU or " +
                              "query execution. Confirm with WAL fsync latency (Kommander WalPhaseInstrumentation, " +
                              "captured separately in a single-writer window) before acting.");
                sb.AppendLine();
            }
        }

        sb.AppendLine("---");
        sb.AppendLine($"_Generated from `summary.json`" +
                      (scrape is not null ? " + a server `/metrics` scrape" : "") +
                      (hasSeries ? " + `node-metrics.csv`" : "") +
                      $" (run id `{manifest.GitCommit ?? "n/a"}` correlation via CAMUS_DIAGNOSTICS_RUN_ID). " +
                      "Client latency measures the user-visible operation; server metrics explain it._");

        return sb.ToString();
    }

    /// <summary>
    /// Whether the load generator had room to spare. This section comes before every server figure on
    /// purpose: if the client was the limiter, nothing below it is a statement about the database.
    /// </summary>
    private static void AppendClientHeadroom(StringBuilder sb, ClientResources c)
    {
        sb.AppendLine("## Load generator headroom").AppendLine();
        sb.AppendLine("| Metric | Value |");
        sb.AppendLine("|---|---|");
        sb.AppendLine($"| CPU used | {Pct(c.CpuUtilization)} of {c.ProcessorCount} core(s) |");
        sb.AppendLine($"| Allocation rate | {F(c.AllocatedMbPerSecond)} MB/s |");
        sb.AppendLine($"| GC collections gen0/1/2 | {c.Gen0Collections} / {c.Gen1Collections} / {c.Gen2Collections} |");
        sb.AppendLine($"| GC pause share of window | {Pct(c.GcPauseFraction)} |");
        sb.AppendLine($"| Peak thread-pool queue | {Fc(c.PeakThreadPoolQueue)} |");
        sb.AppendLine($"| Peak working set | {Fc(c.PeakWorkingSetBytes / 1024.0 / 1024.0)} MB |");
        sb.AppendLine($"| In-flight needed | {F(c.RequiredInFlight)} " +
                      $"(closed loop caps it at {c.Workers} worker(s); open loop at {c.MaxInFlight}) |");
        sb.AppendLine();

        if (c.HeadroomAvailable)
        {
            sb.AppendLine("> The generator had headroom, so the ceiling below is the server's.");
            if (string.Equals(c.Mode, "closed", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine("> Note that a closed-loop run holds exactly its worker count in flight, so this figure");
                sb.AppendLine($"> measures the latency at {c.Workers} worker(s). It becomes a capacity number only when a");
                sb.AppendLine("> sweep shows throughput no longer rising as workers are added.");
            }
        }
        else
        {
            sb.AppendLine("> **The generator may be the limiter.** Fix or isolate it before reading the server");
            sb.AppendLine("> figures as a capacity result:");
            foreach (string warning in c.Warnings)
                sb.AppendLine($"> - {warning}");
        }
        sb.AppendLine();
    }

    /// <summary>
    /// Per-node share of handled requests, and the verdict on whether an endpoint pool actually
    /// distributed them. A comma-separated endpoint list in a connection string is a configuration,
    /// not a measurement: the client can name three nodes and still send everything to one.
    /// </summary>
    private static void AppendWorkDistribution(StringBuilder sb, RunManifest manifest, NodeMetricsSeries series, MetricsWindow w)
    {
        sb.AppendLine("## Per-node work distribution (measured window)").AppendLine();

        IReadOnlyList<(string Node, double Delta)> requests = series.PerNodeDelta("camus_request_count", w);
        double total = requests.Sum(r => r.Delta);

        sb.AppendLine("| Node | Requests handled | Share | Commits | Scrapes ok/failed |");
        sb.AppendLine("|---|---|---|---|---|");
        foreach (string node in series.Nodes)
        {
            double reqs = requests.FirstOrDefault(r => r.Node == node).Delta;
            // Commits come from the request counter's operation tag rather than from a transaction
            // counter: the request path records every transport, and it is the surface a client
            // actually reached this node through.
            double? commits = series.Delta("camus_request_count", w, node, labelContains: "operation=commit");
            double? okLast = series.Gauge("workload_scrape_ok", w, GaugeAggregate.Last, node);
            sb.AppendLine($"| {node} | {Fc(reqs)} | {Share(reqs, total)} | {Fn2(commits)} | " +
                          $"{(okLast is null ? "n/a" : okLast.Value > 0 ? "answering" : "NOT answering at window end")} |");
        }
        sb.AppendLine();

        int reporting = requests.Count(r => r.Delta > 0);
        bool poolConfigured = manifest.Endpoint.Contains(',');
        double maxShare = total > 0 ? requests.Max(r => r.Delta) / total : 0;

        if (total <= 0)
        {
            sb.AppendLine("> **Cannot verify distribution** — no node reported a handled request in the window. Either");
            sb.AppendLine("> diagnostics are off on the scraped nodes, or the scraped nodes are not the ones under load.");
        }
        else if (poolConfigured && reporting <= 1)
        {
            sb.AppendLine($"> **FAIL** — the connection string names an endpoint pool, but one node handled " +
                          $"{Pct(maxShare)} of the requests. The pool is not distributing; treat any per-node");
            sb.AppendLine("> capacity conclusion from this run as measuring a single gateway.");
        }
        else if (poolConfigured && maxShare > 1.5 / Math.Max(reporting, 1))
        {
            sb.AppendLine($"> **Uneven** — {reporting} node(s) handled requests and the busiest took {Pct(maxShare)}, " +
                          $"above 1.5x an even share. Check the client's endpoint rotation before reading a");
            sb.AppendLine("> per-node capacity number from this run.");
        }
        else if (poolConfigured)
        {
            sb.AppendLine($"> **Distributed** — {reporting} node(s) handled requests, busiest share {Pct(maxShare)}.");
        }
        else if (reporting > 1)
        {
            sb.AppendLine($"> A single endpoint was configured, yet {reporting} nodes handled requests " +
                          $"(busiest {Pct(maxShare)}). Something other than the client is spreading the work.");
        }
        else
        {
            sb.AppendLine($"> Single gateway as configured — one node handled {Pct(maxShare)} of the requests. This");
            sb.AppendLine("> run measures one gateway's capacity, not the cluster's.");
        }
        sb.AppendLine();
    }

    /// <summary>
    /// Per-node batch density over the window. Entries per batch near one with a low queue age means
    /// work is not arriving together; a full batch with a growing queue age means the partition is
    /// already saturated and more concurrency will only lengthen the tail.
    /// </summary>
    private static void AppendPerNodeStorage(StringBuilder sb, NodeMetricsSeries series, MetricsWindow w)
    {
        sb.AppendLine("## Per-node storage and WAL (increase over the measured window)").AppendLine();
        sb.AppendLine("| Node | KV entries | KV batches | Entries/batch | WAL ops | WAL batches | Ops/batch |");
        sb.AppendLine("|---|---|---|---|---|---|---|");

        foreach (string node in series.Nodes)
        {
            double? entries = series.Delta("kahuna_kv_write_entries", w, node);
            double? batches = series.Delta("kahuna_kv_write_batches", w, node);
            double? walOps = series.Delta("raft_wal_operations", w, node);
            double? walBatches = series.Delta("raft_wal_batches", w, node);

            sb.AppendLine($"| {node} | {Fn2(entries)} | {Fn2(batches)} | {RatioN(entries, batches)} | " +
                          $"{Fn2(walOps)} | {Fn2(walBatches)} | {RatioN(walOps, walBatches)} |");
        }
        sb.AppendLine();
    }

    /// <summary>
    /// Which commit path the writes actually took. A cluster read-modify-write transaction is expected
    /// to fall back to the full two-phase path; the counters say whether that is what happened, so a
    /// commit-cost investigation starts from the measured path rather than an assumed one.
    /// </summary>
    private static void AppendCommitShape(StringBuilder sb, NodeMetricsSeries series, MetricsWindow w)
    {
        double? onePhase = series.DeltaAcrossNodes("kahuna_durable_tx_one_phase_commits", w);
        double? fallbacks = series.DeltaAcrossNodes("kahuna_durable_tx_one_phase_fallbacks", w);
        double? prepareRejections = series.DeltaAcrossNodes("kahuna_durable_tx_one_phase_prepare_rejections", w);
        double? lateRejections = series.DeltaAcrossNodes("kahuna_durable_tx_late_commit_rejections", w);
        double? redundantApplies = series.DeltaAcrossNodes("kahuna_durable_tx_redundant_applies_skipped", w);

        if (onePhase is null && fallbacks is null && prepareRejections is null)
            return;

        sb.AppendLine("## Commit path (cluster totals over the measured window)").AppendLine();
        sb.AppendLine("| Counter | Value |");
        sb.AppendLine("|---|---|");
        sb.AppendLine($"| One-phase commits | {Fn2(onePhase)} |");
        sb.AppendLine($"| One-phase fallbacks to 2PC | {Fn2(fallbacks)} |");
        sb.AppendLine($"| One-phase prepare rejections | {Fn2(prepareRejections)} |");
        sb.AppendLine($"| Late commit rejections | {Fn2(lateRejections)} |");
        sb.AppendLine($"| Redundant applies skipped | {Fn2(redundantApplies)} |");
        sb.AppendLine();

        if (onePhase is double one && fallbacks is double fb && one + fb > 0 && one / (one + fb) < 0.05)
        {
            sb.AppendLine("> Nearly every transaction took the full two-phase path. For a multi-node");
            sb.AppendLine("> read-modify-write that is the expected safe fallback, not a misconfiguration — so the");
            sb.AppendLine("> commit cost here is the protocol's cost, and reducing it is dependency work.");
            sb.AppendLine();
        }
    }

    /// <summary>
    /// Backlogs the foreground path defers work into. A throughput gain that only moved work into one
    /// of these queues is not a gain: it is an unbounded recovery obligation that shows up later as a
    /// stall. The verdict flags a series that both grew and was still near its peak at the window's
    /// end.
    /// </summary>
    private static void AppendBacklogGrowth(StringBuilder sb, NodeMetricsSeries series, MetricsWindow w)
    {
        (string Metric, string Label)[] watched =
        {
            ("kahuna_durable_tx_outstanding", "Durable transactions outstanding"),
            ("kahuna_durable_tx_resident_records", "Resident durable records"),
            ("kahuna_durable_tx_resident_prepared_intents", "Resident prepared intents"),
            ("kahuna_durable_tx_resident_receipts", "Resident completion receipts"),
            ("raft_wal_queue_depth", "WAL queue depth"),
            ("raft_executor_client_queue_depth", "Raft executor client queue"),
            ("kahuna_kv_write_queue_age", "KV write queue age (ms)"),
            ("dotnet_thread_pool_queue_length", "Thread-pool queue length"),
        };

        List<string> rows = new();
        foreach ((string metric, string label) in watched)
        {
            if (series.Resolve(metric) is null)
                continue;

            foreach (string node in series.Nodes)
            {
                double? first = series.Gauge(metric, w, GaugeAggregate.First, node);
                double? last = series.Gauge(metric, w, GaugeAggregate.Last, node);
                double? max = series.Gauge(metric, w, GaugeAggregate.Max, node);
                if (first is null || last is null || max is null)
                    continue;

                rows.Add($"| {label} | {node} | {Fn2(first)} | {Fn2(last)} | {Fn2(max)} | {GrowthVerdict(first.Value, last.Value, max.Value)} |");
            }
        }

        if (rows.Count == 0)
            return;

        sb.AppendLine("## Backlog and queue growth (measured window)").AppendLine();
        sb.AppendLine("| Series | Node | First | Last | Max | Verdict |");
        sb.AppendLine("|---|---|---|---|---|---|");
        foreach (string row in rows)
            sb.AppendLine(row);
        sb.AppendLine();
        sb.AppendLine("> \"Still rising\" means the last reading was both well above the first and close to the");
        sb.AppendLine("> window's peak. Such a series has not reached a plateau, so the run does not show the");
        sb.AppendLine("> steady state it claims to measure — extend the window or add backpressure before");
        sb.AppendLine("> accepting the throughput number.");
        sb.AppendLine();
    }

    /// <summary>
    /// Classifies one backlog series. "Still rising" needs both a real increase and a last reading near
    /// the peak — a queue that spiked and drained is bounded behaviour, not a leak.
    /// </summary>
    public static string GrowthVerdict(double first, double last, double max)
    {
        if (max <= 0)
            return "idle";

        bool nearPeak = last >= 0.9 * max;
        bool grew = last > first + Math.Max(1, 0.5 * Math.Abs(first));

        if (grew && nearPeak)
            return "**still rising**";
        if (grew)
            return "grew, then eased";
        if (last <= first)
            return "bounded";
        return "grew slightly";
    }

    private static string F(double v) => v.ToString("F3", CultureInfo.InvariantCulture);
    private static string Fn(double? v) => v is null ? "n/a" : v.Value.ToString("F3", CultureInfo.InvariantCulture);
    private static string Fn2(double? v) => v is null ? "n/a" : v.Value.ToString("N0", CultureInfo.InvariantCulture);
    private static string Fc(double v) => v.ToString("N0", CultureInfo.InvariantCulture);
    private static string Ratio(double num, double den) => den > 0 ? (num / den).ToString("F2", CultureInfo.InvariantCulture) : "n/a";
    private static string RatioN(double? num, double? den)
        => num is null || den is null || den.Value <= 0 ? "n/a" : (num.Value / den.Value).ToString("F2", CultureInfo.InvariantCulture);
    private static string Share(double part, double total) => total > 0 ? Pct(part / total) : "n/a";
    private static string Pct(double fraction) => (fraction * 100).ToString("F1", CultureInfo.InvariantCulture) + "%";
}
