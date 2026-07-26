/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Metrics;

namespace CamusDB.Workload.Results;

/// <summary>Latency percentiles for one measured distribution, in milliseconds.</summary>
public sealed record LatencySummary(double P50, double P90, double P95, double P99, double P999, double Max)
{
    public static LatencySummary From(LatencyHistogram h) => new(
        h.Percentile(50), h.Percentile(90), h.Percentile(95), h.Percentile(99), h.Percentile(99.9), h.MaxMilliseconds);
}

/// <summary>
/// The computed outcome of a measured run: throughput, correctness accounting, and latency
/// distributions, plus explicit validity flags. A run can be perfectly executed yet invalid for a
/// capacity claim (too many schedule drops, sub-target submission, or — fatally — any conflict in the
/// non-conflicting baseline); the flags say so instead of letting a pretty number stand unqualified.
/// </summary>
public sealed record RunSummary(
    double MeasuredSeconds,
    string Mode,
    long Offered,
    long Started,
    long Completed,
    long CompletedRead,
    long CompletedWrite,
    long Failed,
    long Conflicts,
    long Transient,
    long DomainErrors,
    long InternalErrors,
    long ScheduleDrops,
    double AchievedOpsPerSec,
    double ReadOpsPerSec,
    double WriteTxnsPerSec,
    double RowsPerSec,
    double ReadPercentActual,
    double WritePercentActual,
    LatencySummary ReadLatency,
    LatencySummary WriteLatency,
    LatencySummary WriteBegin,
    LatencySummary WriteRead,
    LatencySummary WriteUpdate,
    LatencySummary WriteCommit,
    LatencySummary ScheduleDelay,
    bool Valid,
    IReadOnlyList<string> ValidityWarnings)
{
    public static RunSummary Build(RunMetrics m, string mode, int targetOps, double measuredSeconds)
    {
        double secs = measuredSeconds <= 0 ? 1 : measuredSeconds;
        long completed = m.Completed;
        double achieved = completed / secs;
        double readOps = m.CompletedRead / secs;
        double writeTps = m.CompletedWrite / secs;
        double rowsPerSec = (m.RowsRead + m.WriteRowsCommitted) / secs;
        double readPct = completed == 0 ? 0 : 100.0 * m.CompletedRead / completed;
        double writePct = completed == 0 ? 0 : 100.0 * m.CompletedWrite / completed;

        List<string> warnings = new();
        bool valid = true;

        if (m.Conflicts > 0)
        {
            valid = false;
            warnings.Add($"{m.Conflicts} transaction conflict(s) occurred; the non-conflicting baseline is invalid.");
        }

        if (mode == "open")
        {
            long offered = m.Offered;
            double dropRatio = offered == 0 ? 0 : (double)m.ScheduleDrops / offered;
            if (dropRatio > 0.001)
            {
                valid = false;
                warnings.Add($"schedule drops {dropRatio:P3} exceed 0.1%: capacity claims invalid (overload evidence only).");
            }
            double achievedSubmission = targetOps <= 0 ? 1 : (double)m.Started / (targetOps * secs);
            if (achievedSubmission < 0.99)
            {
                valid = false;
                warnings.Add($"achieved submission {achievedSubmission:P1} below 99% of target: capacity claims invalid.");
            }
        }

        if (m.DomainErrors > 0 || m.InternalErrors > 0)
            warnings.Add($"{m.DomainErrors} domain and {m.InternalErrors} internal error(s) recorded (see errors.json).");

        return new RunSummary(
            secs, mode,
            m.Offered, m.Started, completed, m.CompletedRead, m.CompletedWrite,
            m.Failed, m.Conflicts, m.Transient, m.DomainErrors, m.InternalErrors, m.ScheduleDrops,
            achieved, readOps, writeTps, rowsPerSec, readPct, writePct,
            LatencySummary.From(m.ReadLatency),
            LatencySummary.From(m.WriteLatency),
            LatencySummary.From(m.BeginLatency),
            LatencySummary.From(m.WriteReadLatency),
            LatencySummary.From(m.UpdateLatency),
            LatencySummary.From(m.CommitLatency),
            LatencySummary.From(m.ScheduleDelay),
            valid, warnings);
    }
}
