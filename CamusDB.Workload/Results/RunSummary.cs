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
/// Under <c>--expect-faults</c> (chaos runs that kill nodes mid-commit) conflicts and pacing
/// shortfalls are expected collateral, so they downgrade to warnings — but the flag itself is recorded
/// (<see cref="ExpectFaults"/>) so a reader knows the waivers were active and never compares such a
/// run against a strict baseline.
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
    long Indeterminate,
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
    bool ExpectFaults,
    bool Valid,
    IReadOnlyList<string> ValidityWarnings,
    long RetryAttempts,
    long RetriedTxns,
    long MaxAttemptsUsed,
    double RetriesPerWriteTxn)
{
    public static RunSummary Build(
        RunMetrics m, string mode, int targetOps, double measuredSeconds, bool expectFaults = false,
        long retryAttempts = 0, long retriedTxns = 0, long maxAttemptsUsed = 0)
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

        // With --expect-faults these conditions still surface as warnings (the numbers must not stand
        // unqualified) but no longer invalidate: a chaos run that kills nodes is expected to conflict
        // and to fall short of its offered schedule while the cluster recovers.
        void Flag(string message)
        {
            if (expectFaults)
            {
                warnings.Add(message + " (waived by --expect-faults)");
                return;
            }
            valid = false;
            warnings.Add(message);
        }

        if (m.Conflicts > 0)
            Flag($"{m.Conflicts} transaction conflict(s) occurred; the non-conflicting baseline is invalid.");

        if (mode == "open")
        {
            long offered = m.Offered;
            double dropRatio = offered == 0 ? 0 : (double)m.ScheduleDrops / offered;
            if (dropRatio > 0.001)
                Flag($"schedule drops {dropRatio:P3} exceed 0.1%: capacity claims invalid (overload evidence only).");
            double achievedSubmission = targetOps <= 0 ? 1 : (double)m.Started / (targetOps * secs);
            if (achievedSubmission < 0.99)
                Flag($"achieved submission {achievedSubmission:P1} below 99% of target: capacity claims invalid.");
        }

        if (m.Indeterminate > 0)
            warnings.Add($"{m.Indeterminate} write(s) with indeterminate commit outcome (no server verdict); " +
                         "reconciliation admits them via the ambiguity band.");

        if (m.DomainErrors > 0 || m.InternalErrors > 0)
            warnings.Add($"{m.DomainErrors} domain and {m.InternalErrors} internal error(s) recorded (see errors.json).");

        // Retries are the contention signal a contended write shape produces. They are informational,
        // never a validity failure: the retry loop absorbing a conflict is the design working.
        if (retryAttempts > 0)
            warnings.Add($"{retryAttempts} conflict retry attempt(s) across {retriedTxns} transaction(s); " +
                         $"deepest transaction used {maxAttemptsUsed} attempt(s).");

        return new RunSummary(
            secs, mode,
            m.Offered, m.Started, completed, m.CompletedRead, m.CompletedWrite,
            m.Failed, m.Conflicts, m.Transient, m.Indeterminate, m.DomainErrors, m.InternalErrors, m.ScheduleDrops,
            achieved, readOps, writeTps, rowsPerSec, readPct, writePct,
            LatencySummary.From(m.ReadLatency),
            LatencySummary.From(m.WriteLatency),
            LatencySummary.From(m.BeginLatency),
            LatencySummary.From(m.WriteReadLatency),
            LatencySummary.From(m.UpdateLatency),
            LatencySummary.From(m.CommitLatency),
            LatencySummary.From(m.ScheduleDelay),
            expectFaults, valid, warnings,
            retryAttempts, retriedTxns, maxAttemptsUsed,
            m.CompletedWrite == 0 ? 0 : (double)retryAttempts / m.CompletedWrite);
    }
}
