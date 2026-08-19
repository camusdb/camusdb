/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;
using System.Text.Json;
using CamusDB.Workload.Metrics;

namespace CamusDB.Workload.Results;

/// <summary>
/// Writes the five run artifacts beneath the explicit output directory: <c>manifest.json</c>,
/// <c>summary.json</c>, <c>summary.md</c>, <c>intervals.csv</c>, and <c>errors.json</c>. The directory
/// is created fresh; the caller is responsible for refusing to overwrite an existing one so a result
/// bundle is never silently clobbered.
/// </summary>
public sealed class ResultWriter
{
    private static readonly JsonSerializerOptions Json = new() { WriteIndented = true };

    private readonly string _outputDir;

    public ResultWriter(string outputDir)
    {
        _outputDir = outputDir;
    }

    public async Task WriteAsync(
        RunManifest manifest, RunSummary summary,
        IReadOnlyList<IntervalRow> intervals, ErrorSampler errors, CancellationToken ct)
    {
        Directory.CreateDirectory(_outputDir);

        await File.WriteAllTextAsync(Path(  "manifest.json"), JsonSerializer.Serialize(manifest, Json), ct).ConfigureAwait(false);
        await File.WriteAllTextAsync(Path(  "summary.json"), JsonSerializer.Serialize(summary, Json), ct).ConfigureAwait(false);
        await File.WriteAllTextAsync(Path(  "summary.md"), RenderMarkdown(manifest, summary), ct).ConfigureAwait(false);
        await File.WriteAllTextAsync(Path(  "intervals.csv"), RenderIntervals(intervals), ct).ConfigureAwait(false);
        await File.WriteAllTextAsync(Path(  "errors.json"), RenderErrors(errors), ct).ConfigureAwait(false);
    }

    private string Path(string name) => System.IO.Path.Combine(_outputDir, name);

    private static string RenderErrors(ErrorSampler errors)
    {
        var snapshot = errors.Snapshot();
        var payload = new
        {
            total = errors.TotalCount(),
            byCode = snapshot.ToDictionary(
                kv => kv.Key,
                kv => new { count = kv.Value.Count, samples = kv.Value.Samples }),
        };
        return JsonSerializer.Serialize(payload, Json);
    }

    private static string RenderIntervals(IReadOnlyList<IntervalRow> rows)
    {
        StringBuilder sb = new();
        sb.AppendLine("second,offered,started,completed,failed,in_flight,schedule_drops," +
                      "read_p50_ms,read_p95_ms,read_p99_ms,write_p50_ms,write_p95_ms,write_p99_ms");
        foreach (IntervalRow r in rows)
        {
            sb.Append(r.Second).Append(',')
              .Append(r.Offered).Append(',')
              .Append(r.Started).Append(',')
              .Append(r.Completed).Append(',')
              .Append(r.Failed).Append(',')
              .Append(r.InFlight).Append(',')
              .Append(r.ScheduleDrops).Append(',')
              .Append(F(r.ReadP50)).Append(',')
              .Append(F(r.ReadP95)).Append(',')
              .Append(F(r.ReadP99)).Append(',')
              .Append(F(r.WriteP50)).Append(',')
              .Append(F(r.WriteP95)).Append(',')
              .Append(F(r.WriteP99)).AppendLine();
        }
        return sb.ToString();
    }

    private static string RenderMarkdown(RunManifest m, RunSummary s)
    {
        StringBuilder sb = new();
        sb.AppendLine("# Workload run summary").AppendLine();
        sb.AppendLine($"- **Validity:** {(s.Valid ? "VALID" : "INVALID (see warnings)")}" +
                      (s.ExpectFaults ? " — run executed with `--expect-faults` (conflict/pacing waivers active)" : ""));
        foreach (string w in s.ValidityWarnings)
            sb.AppendLine($"  - ⚠️ {w}");
        sb.AppendLine();
        sb.AppendLine($"- Mode: `{s.Mode}` | workers: {m.Workers} | connections: {m.Connections} | target-ops: {m.TargetOps}");
        sb.AppendLine($"- Mix (submitted default 60/40): reads {s.ReadPercentActual:F1}% / writes {s.WritePercentActual:F1}%");
        sb.AppendLine($"- Measured window: {s.MeasuredSeconds:F0}s | seed: {m.Seed} | rows: {m.Rows} | payload: {m.PayloadBytes}B");
        sb.AppendLine($"- Schema fingerprint: `{m.SchemaFingerprint}`");
        sb.AppendLine($"- Server durability config is operator-supplied; do not compare runs with different settings.").AppendLine();

        sb.AppendLine("## Throughput").AppendLine();
        sb.AppendLine("| Metric | Value |");
        sb.AppendLine("|---|---|");
        sb.AppendLine($"| Completed ops/s | {s.AchievedOpsPerSec:F1} |");
        sb.AppendLine($"| Read ops/s | {s.ReadOpsPerSec:F1} |");
        sb.AppendLine($"| Committed write txns/s | {s.WriteTxnsPerSec:F1} |");
        sb.AppendLine($"| Rows/s | {s.RowsPerSec:F1} |");
        sb.AppendLine($"| Offered / Started / Completed | {s.Offered} / {s.Started} / {s.Completed} |");
        sb.AppendLine($"| Failed (conflict/transient/indeterminate/domain/internal) | {s.Failed} ({s.Conflicts}/{s.Transient}/{s.Indeterminate}/{s.DomainErrors}/{s.InternalErrors}) |");
        sb.AppendLine($"| Schedule drops | {s.ScheduleDrops} |").AppendLine();

        sb.AppendLine("## Latency (ms)").AppendLine();
        sb.AppendLine("| Distribution | p50 | p90 | p95 | p99 | p99.9 | max |");
        sb.AppendLine("|---|---|---|---|---|---|---|");
        AppendLatency(sb, "Read", s.ReadLatency);
        AppendLatency(sb, "Write txn (whole)", s.WriteLatency);
        AppendLatency(sb, "Write: begin", s.WriteBegin);
        AppendLatency(sb, "Write: read", s.WriteRead);
        AppendLatency(sb, "Write: update", s.WriteUpdate);
        AppendLatency(sb, "Write: commit", s.WriteCommit);
        AppendLatency(sb, "Schedule delay", s.ScheduleDelay);

        return sb.ToString();
    }

    private static void AppendLatency(StringBuilder sb, string name, LatencySummary l)
        => sb.AppendLine($"| {name} | {F(l.P50)} | {F(l.P90)} | {F(l.P95)} | {F(l.P99)} | {F(l.P999)} | {F(l.Max)} |");

    private static string F(double v) => v.ToString("F3", CultureInfo.InvariantCulture);
}
