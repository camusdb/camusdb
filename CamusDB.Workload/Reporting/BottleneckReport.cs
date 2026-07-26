/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;
using CamusDB.Workload.Results;

namespace CamusDB.Workload.Reporting;

/// <summary>
/// Builds the diagnostic <c>bottleneck-report.md</c> by aligning the client-side run summary with a
/// server-side Prometheus scrape captured under the same run id. It is <b>evidence, not a verdict</b>:
/// it surfaces the candidate limiting stages each backed by a named measurement, and it deliberately
/// refuses to declare a root cause from the largest inclusive duration alone — awaited durability can
/// dominate latency while using little CPU, and overlapping stage durations are not additive. Missing
/// metrics render as "n/a" so a partial scrape still produces a usable report.
/// </summary>
public static class BottleneckReport
{
    public static string Build(RunManifest manifest, RunSummary summary, PrometheusScrape scrape)
    {
        StringBuilder sb = new();
        sb.AppendLine("# Bottleneck report").AppendLine();
        sb.AppendLine($"Run against `{manifest.Endpoint}` (db `{manifest.Database}`), seed {manifest.Seed}, " +
                      $"{manifest.Workers} workers / {manifest.Connections} connections, mode `{summary.Mode}`.");
        sb.AppendLine($"Validity: **{(summary.Valid ? "VALID" : "INVALID")}**. " +
                      "Server durability config is operator-supplied — do not compare runs with different settings.");
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

        // ── Server stage latency ────────────────────────────────────────────────
        double? handler = scrape.HistogramMean("camus_request_duration_milliseconds");
        double? execute = scrape.HistogramMean("camus_execute_duration_milliseconds");
        double? commit = scrape.HistogramMean("camus_transaction_commit_duration_milliseconds");
        double? scan = scrape.HistogramMean("camus_query_scan_duration_milliseconds");

        sb.AppendLine("## Server stage latency (mean, from server metrics)").AppendLine();
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

        // ── Resources ───────────────────────────────────────────────────────────
        double? gcAlloc = scrape.Sum("process_runtime_dotnet_gc_allocations_size_bytes_total");
        double? tpQueue = scrape.Gauge("process_runtime_dotnet_thread_pool_queue_length");
        double? tpThreads = scrape.Gauge("process_runtime_dotnet_thread_pool_threads_count");
        double gcGen0 = scrape.Sum("process_runtime_dotnet_gc_collections_count_total", ("generation", "gen0"));
        double gcGen2 = scrape.Sum("process_runtime_dotnet_gc_collections_count_total", ("generation", "gen2"));

        sb.AppendLine("## Runtime / resources (server)").AppendLine();
        sb.AppendLine("| Metric | Value |");
        sb.AppendLine("|---|---|");
        sb.AppendLine($"| GC allocations (bytes, cumulative) | {Fc(gcAlloc ?? 0)} |");
        sb.AppendLine($"| GC collections gen0 / gen2 | {Fc(gcGen0)} / {Fc(gcGen2)} |");
        sb.AppendLine($"| Thread-pool threads / queue length | {Fn(tpThreads)} / {Fn(tpQueue)} |");
        sb.AppendLine();

        // ── Storage / WAL ───────────────────────────────────────────────────────
        double kvBatches = scrape.Sum("kahuna_kv_write_batches_total");
        double kvEntries = scrape.Sum("kahuna_kv_write_entries_total");
        double walBatches = scrape.Sum("raft_wal_batches_total");
        double walOps = scrape.Sum("raft_wal_operations_total");
        double? durableOutstanding = scrape.Gauge("kahuna_durable_tx_outstanding");

        sb.AppendLine("## Storage / WAL (dependencies)").AppendLine();
        sb.AppendLine("| Metric | Value |");
        sb.AppendLine("|---|---|");
        sb.AppendLine($"| Kahuna KV write batches / entries | {Fc(kvBatches)} / {Fc(kvEntries)} |");
        sb.AppendLine($"| Kahuna KV entries per batch | {Ratio(kvEntries, kvBatches)} |");
        sb.AppendLine($"| Kommander WAL batches / operations | {Fc(walBatches)} / {Fc(walOps)} |");
        sb.AppendLine($"| WAL operations per batch | {Ratio(walOps, walBatches)} |");
        sb.AppendLine($"| Durable-tx outstanding (gauge) | {Fn(durableOutstanding)} |");
        sb.AppendLine();

        // ── Errors ──────────────────────────────────────────────────────────────
        sb.AppendLine("## Errors / conflicts / retries").AppendLine();
        sb.AppendLine($"- Failed: {summary.Failed} (conflict {summary.Conflicts}, transient {summary.Transient}, " +
                      $"domain {summary.DomainErrors}, internal {summary.InternalErrors})");
        sb.AppendLine();

        // ── Candidate limiting stages ───────────────────────────────────────────
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
            bool tpIdle = (tpQueue ?? 0) < 4;
            sb.AppendLine($"> Inference (unverified): commit mean ({F(c)} ms) is ≫ executor mean ({F(e)} ms)" +
                          (tpIdle ? " while the thread-pool queue is short" : "") +
                          ", consistent with a **durability-bound** write path (awaited Raft/WAL fsync), not CPU or " +
                          "query execution. Confirm with WAL fsync latency (Kommander WalPhaseInstrumentation, " +
                          "captured separately in a single-writer window) before acting.");
            sb.AppendLine();
        }

        sb.AppendLine("---");
        sb.AppendLine($"_Generated from `summary.json` + server `/metrics` scrape (run id `{manifest.GitCommit ?? "n/a"}` " +
                      "correlation via CAMUS_DIAGNOSTICS_RUN_ID). Client latency measures the user-visible operation; " +
                      "server metrics explain it._");

        return sb.ToString();
    }

    private static string F(double v) => v.ToString("F3", CultureInfo.InvariantCulture);
    private static string Fn(double? v) => v is null ? "n/a" : v.Value.ToString("F3", CultureInfo.InvariantCulture);
    private static string Fc(double v) => v.ToString("N0", CultureInfo.InvariantCulture);
    private static string Ratio(double num, double den) => den > 0 ? (num / den).ToString("F2", CultureInfo.InvariantCulture) : "n/a";
}
