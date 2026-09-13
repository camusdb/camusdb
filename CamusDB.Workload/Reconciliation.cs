/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Client;
using CamusDB.Workload.Metrics;
using CamusDB.Workload.Workload;

namespace CamusDB.Workload;

/// <summary>
/// Outcome of the post-run correctness reconciliation. The version check is a band, not a point:
/// <see cref="ExpectedMin"/> is the rows the client knows committed, <see cref="ExpectedMax"/> adds the
/// rows that <em>may</em> have committed through indeterminate commits, and <see cref="Observed"/> is
/// the persisted <c>SUM(version)</c> delta that must fall inside. With zero indeterminate transactions
/// the band collapses to the historical exact equality. <see cref="ConflictsWaived"/> records that
/// conflicts were tolerated because the run declared <c>--expect-faults</c>.
///
/// <para><c>RowAttribution</c> carries the per-row verdict for a transfer run, which is the only part of
/// this result that can see an atomicity break whose stray writes cancel in the totals. It is null for a
/// workload that has no journal to compare against.</para>
/// </summary>
public sealed record ReconciliationResult(
    long ExpectedMin,
    long ExpectedMax,
    long Observed,
    long IndeterminateTxns,
    bool VersionsMatch,
    long RowCount,
    bool RowCountMatches,
    bool AccountingBalances,
    bool NoConflicts,
    bool ConflictsWaived,
    IReadOnlyList<string> Failures,
    bool BalanceConserved = true,
    long BalanceBaseline = 0,
    long BalanceFinal = 0,
    bool VersionCheckWaived = false,
    RowAttributionResult? RowAttribution = null,
    ScanProbeResult? ScanProbe = null,
    IReadOnlyList<AggregateRead>? Aggregates = null)
{
    // BalanceConserved is the bank-transfer atomicity invariant: SUM(balance) must be unchanged after a
    // transfer run. It is never waived — unlike conflicts, an atomicity break is a correctness failure a
    // chaos run must catch, not tolerate. Defaults to true so the accounts workload (whose writes change
    // balances by design) is unaffected.
    //
    // VersionCheckWaived turns off the SUM(version) accounting for the bank workload: its transfers
    // retry on failure and are NOT idempotent, so an indeterminate commit followed by a retry can apply
    // the transfer twice — which inflates the version count while still conserving balance. The version
    // band assumes at-most-once application (true only for the non-retrying accounts writes), so in bank
    // mode SUM(balance) conservation is the consistency guard and the version delta is informational.
    //
    // RowAttribution is the per-row check that the aggregates cannot perform. SUM(balance) proves only
    // that the net of every stray write is zero, so an even number of leaked legs that cancel — one row
    // credited, another debited, neither acknowledged to the client — passes it untouched. SUM(version)
    // has the same blind spot for a row that gained an increment and another that lost one. The per-row
    // comparison has no such cancellation, and it is never waived: like BalanceConserved, it reports an
    // atomicity break, which is the failure a chaos run exists to catch rather than tolerate.
    //
    // ScanProbe is the in-window witness the post-run aggregates cannot be: they count rows on a quiet
    // cluster, and a scan that drops rows only while writes are in flight passes them every time. Null
    // when the run did not probe; when it did, a short count or (on a fault-free run) a failed read
    // fails reconciliation exactly like a broken invariant.
    //
    // Aggregates records how each verification read went — value, client-observed time, attempts — so
    // scan-time growth is visible run to run instead of surfacing only once a read crosses a deadline.
    public bool Passed => (VersionsMatch || VersionCheckWaived)
        && RowCountMatches && AccountingBalances && (NoConflicts || ConflictsWaived) && BalanceConserved
        && (RowAttribution is null || RowAttribution.Passed)
        && (ScanProbe is null || ScanProbe.Passed);
}

/// <summary>
/// How one reconciliation aggregate went. <see cref="ElapsedMs"/> is the client-observed round trip
/// of the attempt that answered (the client library does not expose the server's own
/// <c>serverTimeMs</c>), and <see cref="Attempts"/> how many tries it took — a read that needed
/// several is a cluster that was still settling.
/// </summary>
public sealed record AggregateRead(string Sql, long Value, double ElapsedMs, int Attempts);

/// <summary>
/// A reconciliation read that gave up. The message names the cause — a per-request deadline the
/// aggregate outgrew, an endpoint that could not be reached, a server verdict — because "the cluster
/// stayed unavailable" was the one wording for all three, and on a live cluster whose aggregate had
/// merely crossed the client deadline it sent the reader looking for an outage that never happened.
/// </summary>
public sealed class ReconciliationReadException(string message, Exception inner) : Exception(message, inner);

/// <summary>
/// Verifies at shutdown that the run was internally consistent, so a "fast" number that lost or
/// double-counted work cannot pass silently. Because every row is seeded at <c>version = 0</c> and each
/// committed write increments <c>version</c> on exactly <c>writes-per-transaction</c> rows, the
/// persisted <c>SUM(version)</c> delta must equal committed writes × writes-per-transaction — a single
/// cheap aggregate that proves the durable effect matches the client's committed count without scanning
/// every row by hand. A commit whose response was lost (a node killed mid-commit) leaves the client
/// unable to say whether its rows landed, so each such transaction widens the acceptable delta upward
/// by at most one transaction's worth of rows: the durable state is correct anywhere inside
/// [committed, committed + indeterminate × writes-per-transaction]. It also confirms the seeded row
/// count survived and that the operation accounting (offered = started + drops,
/// started = completed + failed) has no lost operations.
///
/// <para>Every check above is an <em>aggregate</em>, and an aggregate can only see the net of what went
/// wrong. A transfer run that leaked an even number of legs which cancel — one row credited, another
/// debited, neither commit acknowledged — leaves both <c>SUM(balance)</c> and <c>SUM(version)</c>
/// exactly where they should be, and such a run has been reported as PASS. For the transfer workloads
/// reconciliation therefore also runs <see cref="Metrics.RowAttribution"/>, which compares every row
/// against the effect the transfer journal predicts for it. Cancellation is impossible per row, so it
/// catches what the totals cannot; it is the strongest signal this check produces.</para>
/// </summary>
public static class Reconciliation
{
    /// <summary>Reads the current <c>SUM(version)</c> baseline; captured once before the run so
    /// reconciliation measures only this run's increments, not the accumulation of prior runs on the
    /// same seeded dataset.</summary>
    public static Task<long> ReadVersionSumAsync(
        CamusConnection conn, Dataset dataset, CancellationToken ct, TimeSpan? retryBudget = null)
        => AggregateOverTablesAsync(conn, dataset, "SUM(version)", ct, retryBudget);

    /// <summary>Reads the current <c>SUM(balance)</c> baseline; captured once before a transfer run
    /// so the conservation invariant measures against the pre-run total.</summary>
    public static Task<long> ReadBalanceSumAsync(
        CamusConnection conn, Dataset dataset, CancellationToken ct, TimeSpan? retryBudget = null)
        => AggregateOverTablesAsync(conn, dataset, "SUM(balance)", ct, retryBudget);

    /// <summary>
    /// Sums one aggregate over every table in the dataset. A multi-table dataset has no single table to
    /// aggregate, and the invariants are whole-dataset invariants — a transfer between two tables
    /// conserves the total, not either table's own sum — so each table is read and the results are
    /// added. Each read carries the full retry budget, because the reason a post-fault aggregate is
    /// slow (one node still draining) applies to every table equally.
    /// </summary>
    private static async Task<long> AggregateOverTablesAsync(
        CamusConnection conn, Dataset dataset, string aggregate, CancellationToken ct, TimeSpan? retryBudget,
        List<AggregateRead>? reads = null)
    {
        long total = 0;
        foreach (string table in dataset.TableNames)
        {
            string sql = $"SELECT {aggregate} FROM {table}";
            (long value, double elapsedMs, int attempts) = await ScalarTimedAsync(conn, sql, ct, retryBudget).ConfigureAwait(false);
            reads?.Add(new AggregateRead(sql, value, elapsedMs, attempts));
            total += value;
        }
        return total;
    }

    /// <summary>
    /// The inclusive band of acceptable <c>SUM(version)</c> deltas. An indeterminate transaction
    /// committed either none or all of its row writes, so the lower bound stays at the rows known to
    /// have committed and only the upper bound grows — by at most writes-per-transaction rows each.
    /// </summary>
    public static (long Min, long Max) VersionDeltaBand(
        long committedRowWrites, long indeterminateTxns, int writesPerTransaction)
        => (committedRowWrites, committedRowWrites + indeterminateTxns * Math.Max(1, writesPerTransaction));

    /// <summary>True when the observed persisted delta is admissible for the given commit accounting.</summary>
    public static bool VersionDeltaWithinBand(
        long observedDelta, long committedRowWrites, long indeterminateTxns, int writesPerTransaction)
    {
        (long min, long max) = VersionDeltaBand(committedRowWrites, indeterminateTxns, writesPerTransaction);
        return observedDelta >= min && observedDelta <= max;
    }

    /// <param name="baselineVersionSum">
    /// <c>SUM(version)</c> captured before the run started (warm-up included). The run is correct when
    /// the persisted sum grew by a value inside the band anchored at <paramref name="committedRowWrites"/>.
    /// </param>
    /// <param name="committedRowWrites">
    /// Rows the driver durably committed across the whole run (warm-up + measured) — the minimum amount
    /// by which <c>SUM(version)</c> must have increased.
    /// </param>
    /// <param name="indeterminateTxns">
    /// Whole-run count of write transactions whose commit round trip got no server verdict; each may
    /// have added up to <paramref name="writesPerTransaction"/> additional version increments.
    /// </param>
    /// <param name="expectFaults">
    /// When set, observed conflicts are reported but do not fail reconciliation — a chaos run that
    /// kills nodes expects them. Without the flag the strict non-conflicting contract applies.
    /// </param>
    /// <param name="rowAttribution">
    /// The per-row check, or null when the run has none (the accounts workload, or an operator opt-out
    /// that the caller reports through <paramref name="rowAttributionSkip"/>). When present it is the
    /// strongest signal reconciliation produces, and it runs last so that a cluster too slow to answer
    /// downgrades it to "could not verify" without discarding the aggregate verdict.
    /// </param>
    /// <param name="rowAttributionSkip">
    /// A pre-decided outcome for the per-row check when it could not even be built — the dataset was too
    /// large, or the baseline scan failed. Carried through so the verdict says which check was missing
    /// instead of quietly reporting one fewer check.
    /// </param>
    public static async Task<ReconciliationResult> VerifyAsync(
        CamusConnection conn, Dataset dataset, RunMetrics metrics, long baselineVersionSum, long committedRowWrites,
        long indeterminateTxns, int writesPerTransaction, bool expectFaults,
        long expectedRows, CancellationToken ct,
        bool bankMode = false, long baselineBalanceSum = 0, TimeSpan? retryBudget = null,
        Metrics.RowAttribution? rowAttribution = null, RowAttributionResult? rowAttributionSkip = null,
        ScanProbeResult? scanProbe = null)
    {
        List<AggregateRead> reads = [];
        long persistedSum = await AggregateOverTablesAsync(conn, dataset, "SUM(version)", ct, retryBudget, reads).ConfigureAwait(false);
        long rowCount = await AggregateOverTablesAsync(conn, dataset, "COUNT(*)", ct, retryBudget, reads).ConfigureAwait(false);

        long persistedDelta = persistedSum - baselineVersionSum;
        (long expectedMin, long expectedMax) = VersionDeltaBand(committedRowWrites, indeterminateTxns, writesPerTransaction);
        bool versionsMatch = persistedDelta >= expectedMin && persistedDelta <= expectedMax;
        bool rowCountMatches = rowCount == expectedRows;
        bool accounting = metrics.Offered == metrics.Started + metrics.ScheduleDrops
                          && metrics.Started == metrics.Completed + metrics.Failed;
        bool noConflicts = metrics.Conflicts == 0;

        // Transfer atomicity invariant: every transfer conserves total balance and commits
        // atomically, so SUM(balance) over the whole dataset must be unchanged — including a transfer
        // whose two legs land in different tables. This holds across faults and indeterminate
        // commits (an atomic transfer applies both legs or neither), so a changed sum is a genuine
        // atomicity break — never waived. In accounts mode balances change by design, so it is skipped.
        long balanceFinal = bankMode
            ? await AggregateOverTablesAsync(conn, dataset, "SUM(balance)", ct, retryBudget, reads).ConfigureAwait(false)
            : 0;
        bool balanceConserved = !bankMode || balanceFinal == baselineBalanceSum;

        RowAttributionResult? rowResult = await RunRowAttributionAsync(
            conn, metrics, rowAttribution, rowAttributionSkip, ct, retryBudget).ConfigureAwait(false);

        List<string> failures = new();
        if (!versionsMatch && !bankMode)
            failures.Add($"persisted SUM(version) delta={persistedDelta} outside [{expectedMin}, {expectedMax}] " +
                         $"(committed rows + up to {indeterminateTxns} indeterminate txn(s) × {writesPerTransaction}; " +
                         $"baseline={baselineVersionSum}, final={persistedSum}).");
        if (!rowCountMatches)
            failures.Add($"row count {rowCount} != expected {expectedRows}.");
        if (!accounting)
            failures.Add($"operation accounting mismatch: offered={metrics.Offered}, started={metrics.Started}, " +
                         $"completed={metrics.Completed}, failed={metrics.Failed}, drops={metrics.ScheduleDrops}.");
        if (!noConflicts && !expectFaults)
            failures.Add($"{metrics.Conflicts} conflict(s) in the non-conflicting baseline.");
        if (!balanceConserved)
            failures.Add($"SUM(balance) changed from {baselineBalanceSum} to {balanceFinal} " +
                         $"(delta {balanceFinal - baselineBalanceSum}) — a bank transfer broke atomicity.");
        failures.AddRange(DescribeRowAttribution(rowResult));
        if (scanProbe is { Passed: false })
            failures.AddRange(scanProbe.Failures);

        return new ReconciliationResult(
            expectedMin, expectedMax, persistedDelta, indeterminateTxns,
            versionsMatch, rowCount, rowCountMatches, accounting, noConflicts, expectFaults && !noConflicts, failures,
            balanceConserved, baselineBalanceSum, balanceFinal, VersionCheckWaived: bankMode,
            RowAttribution: rowResult, ScanProbe: scanProbe, Aggregates: reads);
    }

    /// <summary>
    /// Produces the per-row verdict, or the reason there is none. Three things can stop it: the caller
    /// never built one, the run ended with work still in flight, or the verification scan could not
    /// complete. None of them throws — each becomes an <see cref="RowAttributionStatus.Unavailable"/>
    /// result, which does not pass but also does not discard the aggregate verdict alongside it.
    /// </summary>
    private static async Task<RowAttributionResult?> RunRowAttributionAsync(
        CamusConnection conn, RunMetrics metrics, Metrics.RowAttribution? attribution,
        RowAttributionResult? skip, CancellationToken ct, TimeSpan? retryBudget)
    {
        if (attribution is null)
            return skip;

        // An operation still running can commit after the scan below has already read its rows, which
        // would show up as a row carrying a write the journal does not have — a violation the harness
        // manufactured itself. Refuse to judge rather than report it.
        if (metrics.UnfinishedAtDrain > 0)
        {
            return RowAttributionResult.Unavailable(
                $"{metrics.UnfinishedAtDrain} operation(s) were still in flight when the drain budget " +
                "expired, so their writes could land after the verification scan.");
        }

        try
        {
            return await attribution.VerifyAsync(conn, ct, retryBudget).ConfigureAwait(false);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            return RowAttributionResult.Unavailable(
                $"the per-row verification scan did not complete: {ex.GetType().Name}: {ex.Message}");
        }
    }

    /// <summary>
    /// Turns a per-row verdict into reconciliation failure lines. Each violation class gets its own
    /// line because they mean different things: a balance that disagrees is value moved without a
    /// committed transfer, while a version above the journalled count is a write the client never
    /// committed — the sharper signal, since it fires even when the stray writes leave the total right.
    /// </summary>
    private static IEnumerable<string> DescribeRowAttribution(RowAttributionResult? r)
    {
        if (r is null)
            yield break;

        // Reported before the pass gate on purpose. A truncated pairing check that found nothing has
        // not established that nothing is there, and that caveat matters most precisely when the rest
        // of the verdict is clean.
        if (r.HalfAppliedCheckTruncated)
            yield return "the leg-pairing check saw only part of this run's indeterminate transfers " +
                         "(retention cap reached), so it cannot report the absence of half-applied ones.";

        if (r.Passed)
            yield break;

        if (r.Status != RowAttributionStatus.Verified)
        {
            yield return $"per-row attribution could not verify atomicity: {r.Reason}";
            yield break;
        }

        string sample = r.Violations.Count == 0
            ? ""
            : " First: " + string.Join("; ", r.Violations.Take(3).Select(v => $"row {v.RowIndex} in {v.Table}: {v.Detail}"));

        if (r.BalanceViolations > 0)
            yield return $"{r.BalanceViolations} row(s) hold a balance the transfer journal does not " +
                         $"account for, outside their indeterminate ambiguity band." + sample;
        if (r.HalfAppliedTransfers > 0)
            yield return $"{r.HalfAppliedTransfers} transfer(s) applied to one of their two rows and not " +
                         "the other — value moved without its counterpart. Neither the per-row bands nor " +
                         "SUM(balance) can see this on their own: each leg stays inside its own band, and " +
                         "half-applied transfers of opposite sign cancel in the total." + sample;
        if (r.UncountedWriteRows > 0)
            yield return $"{r.UncountedWriteRows} row(s) carry more version increments than the client " +
                         "ever committed against them — a write leaked past an aborted transaction.";
        if (r.LostWriteRows > 0)
            yield return $"{r.LostWriteRows} row(s) carry fewer version increments than the client " +
                         "committed against them — a committed write was lost.";
        if (r.RowsMissing > 0)
            yield return $"{r.RowsMissing} seeded row(s) were absent from the final scan.";
        if (r.RowsDuplicated > 0)
            yield return $"{r.RowsDuplicated} row id(s) were returned more than once by the final scan.";
        if (r.RowsForeign > 0)
            yield return $"{r.RowsForeign} row(s) in the workload tables do not belong to this seed's dataset.";
    }

    // Reconciliation runs right after the measured window, when a cluster that just lost and recovered a
    // node may still be slow to answer — a single scalar read can hit a client timeout or a retryable
    // server verdict. These reads are pure aggregates outside the measurement, so retrying them is both
    // safe and necessary: without it a transient blip during the post-fault settle crashes the whole run
    // (unhandled) instead of producing the consistency verdict the run exists to report.
    // A node recovering from a fault — a restart, or a full disk that was just freed and is now
    // compacting — can stay slow for tens of seconds. Reconciliation is post-measurement, so it can
    // afford to be patient. Even when it gives up the caller keeps the measured artifacts (see
    // VerifyOrInconclusive).
    //
    // The bound is WALL-CLOCK, not an attempt count. An attempt cap is unpredictable here because
    // each failed attempt costs whatever the client's request timeout is: with a ~11 s timeout,
    // "30 attempts" is ~5.5 minutes, but with a shorter timeout the same cap gives up in seconds.
    // Measured case that motivated this (bank soak, 2026-08-26): a node kept draining a read backlog
    // after the measured window; reconciliation gave up 5.5 minutes after drain, and the very same
    // aggregate succeeded 78 seconds later in 7 s. The run reported "cluster stayed unavailable"
    // for a cluster that was merely still busy, and the SUM(balance) invariant went unverified.
    public static readonly TimeSpan DefaultRetryBudget = TimeSpan.FromMinutes(10);

    // Safety net only: the wall-clock budget is the real bound. This stops a pathological
    // fail-fast loop (an error that returns in microseconds) from spinning millions of times.
    private const int MaxScalarAttempts = 10_000;

    private static async Task<long> ScalarAsync(
        CamusConnection conn, string sql, CancellationToken ct, TimeSpan? retryBudget = null)
        => (await ScalarTimedAsync(conn, sql, ct, retryBudget).ConfigureAwait(false)).Value;

    /// <summary>
    /// One aggregate with its provenance: the value, the round trip of the attempt that answered, and
    /// how many attempts it took. Gives up with a <see cref="ReconciliationReadException"/> whose
    /// message says <em>why</em> — see <see cref="DescribeGiveUp"/>.
    /// </summary>
    private static async Task<(long Value, double ElapsedMs, int Attempts)> ScalarTimedAsync(
        CamusConnection conn, string sql, CancellationToken ct, TimeSpan? retryBudget = null)
    {
        TimeSpan budget = retryBudget ?? DefaultRetryBudget;
        long startedAt = Stopwatch.GetTimestamp();

        for (int attempt = 1; ; attempt++)
        {
            long attemptStart = Stopwatch.GetTimestamp();
            try
            {
                using CamusCommand cmd = conn.CreateCamusCommand(sql);
                using CamusDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                long value = !await reader.ReadAsync(ct).ConfigureAwait(false) || reader.IsDBNull(0) ? 0 : reader.GetInt64(0);
                return (value, Stopwatch.GetElapsedTime(attemptStart).TotalMilliseconds, attempt);
            }
            catch (Exception ex)
            {
                // Reconciliation reads are idempotent, so the retry bar is lower here than on the
                // write path: a transport failure carrying a server code is still worth another
                // attempt. See ErrorClassifier.IsRetryableForIdempotentRead.
                bool retryable = Operations.ErrorClassifier.IsRetryableForIdempotentRead(ex);
                bool budgetSpent = Stopwatch.GetElapsedTime(startedAt) >= budget;
                if (ct.IsCancellationRequested)
                    throw;
                if (!retryable || budgetSpent || attempt >= MaxScalarAttempts)
                    throw new ReconciliationReadException(
                        DescribeGiveUp(sql, ex, attempt, Stopwatch.GetElapsedTime(startedAt), Stopwatch.GetElapsedTime(attemptStart), retryable, budgetSpent),
                        ex);

                await Task.Delay(Math.Min(200 * attempt, 2000), ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// The give-up message, worded by cause. Three failures used to share one sentence, and they call
    /// for three different responses: a read that keeps crossing the client's per-request deadline is
    /// a slow aggregate on a live cluster (raise <c>--reconcile-request-timeout</c>, or look at scan
    /// cost); a connection that is refused is an endpoint that is down; a server verdict is an
    /// answer, and its code is the lead.
    /// </summary>
    public static string DescribeGiveUp(
        string sql, Exception last, int attempts, TimeSpan waited, TimeSpan lastAttempt, bool retryable, bool budgetSpent)
    {
        string tail = budgetSpent
            ? $"after {attempts} attempt(s) over {waited.TotalSeconds:F0}s (retry budget spent)"
            : retryable
                ? $"after {attempts} attempt(s) over {waited.TotalSeconds:F0}s"
                : $"on attempt {attempts} (not retryable)";

        string cause = Operations.ErrorClassifier.DescribeReadFailure(last, lastAttempt);
        return $"{sql} did not complete {tail}: {cause}";
    }

    /// <summary>
    /// Runs <see cref="VerifyAsync"/> but never throws: if the verification queries cannot complete —
    /// a node too slow after a fault, a transport that stays down — it returns an <c>inconclusive</c>
    /// result (not passed, with the reason) instead of unwinding the run. The measured artifacts are
    /// already written by then, so a run that produced valid data but could not be verified is reported
    /// as "could not verify", not lost as "crashed".
    /// </summary>
    public static async Task<ReconciliationResult> VerifyOrInconclusiveAsync(
        CamusConnection conn, Dataset dataset, RunMetrics metrics, long baselineVersionSum, long committedRowWrites,
        long indeterminateTxns, int writesPerTransaction, bool expectFaults,
        long expectedRows, CancellationToken ct, bool bankMode, long baselineBalanceSum,
        TimeSpan? retryBudget = null,
        Metrics.RowAttribution? rowAttribution = null, RowAttributionResult? rowAttributionSkip = null,
        ScanProbeResult? scanProbe = null)
    {
        long startedAt = Stopwatch.GetTimestamp();
        try
        {
            return await VerifyAsync(conn, dataset, metrics, baselineVersionSum, committedRowWrites, indeterminateTxns,
                writesPerTransaction, expectFaults, expectedRows, ct, bankMode, baselineBalanceSum,
                retryBudget, rowAttribution, rowAttributionSkip, scanProbe).ConfigureAwait(false);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // Say how long it actually waited and why it stopped. A ReconciliationReadException already
            // names its cause (deadline / unreachable / server verdict); anything else is reported as
            // what it is. "Unavailable" is no longer assumed: a run that gave up on an aggregate that
            // kept outgrowing the client deadline is a different diagnosis from a dead cluster.
            TimeSpan waited = Stopwatch.GetElapsedTime(startedAt);
            string reason = ex is ReconciliationReadException ? ex.Message : $"{ex.GetType().Name}: {ex.Message}";
            return Inconclusive(
                $"{reason} (reconciliation ran for {waited.TotalSeconds:F0}s of a " +
                $"{(retryBudget ?? DefaultRetryBudget).TotalSeconds:F0}s budget)",
                indeterminateTxns, metrics.Conflicts, baselineBalanceSum, rowAttribution is not null, scanProbe);
        }
    }

    /// <summary>Builds a not-passed "could not verify" result carrying the reason, for when the
    /// verification cannot run at all (the reconciliation connection could not even be opened).</summary>
    public static ReconciliationResult Inconclusive(
        string reason, long indeterminateTxns, long conflicts, long baselineBalanceSum,
        bool rowAttributionExpected = false, ScanProbeResult? scanProbe = null)
    {
        // The probe's verdict stands on its own evidence: a short count seen during the window is a
        // finding whether or not the post-run aggregates could be read afterwards.
        List<string> failures = [$"reconciliation could not complete: {reason}"];
        if (scanProbe is { Passed: false })
            failures.AddRange(scanProbe.Failures);

        return new(
            0, 0, 0, indeterminateTxns,
            VersionsMatch: false, RowCount: -1, RowCountMatches: false, AccountingBalances: false,
            NoConflicts: conflicts == 0, ConflictsWaived: false,
            Failures: failures,
            BalanceConserved: false, BalanceBaseline: baselineBalanceSum, BalanceFinal: 0, VersionCheckWaived: false,
            RowAttribution: rowAttributionExpected
                ? RowAttributionResult.Unavailable("reconciliation could not complete: " + reason)
                : null,
            ScanProbe: scanProbe);
    }
}
