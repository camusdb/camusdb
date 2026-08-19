/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
    bool VersionCheckWaived = false)
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
    public bool Passed => (VersionsMatch || VersionCheckWaived)
        && RowCountMatches && AccountingBalances && (NoConflicts || ConflictsWaived) && BalanceConserved;
}

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
/// </summary>
public static class Reconciliation
{
    /// <summary>Reads the current <c>SUM(version)</c> baseline; captured once before the run so
    /// reconciliation measures only this run's increments, not the accumulation of prior runs on the
    /// same seeded dataset.</summary>
    public static Task<long> ReadVersionSumAsync(CamusConnection conn, CancellationToken ct)
        => ScalarAsync(conn, $"SELECT SUM(version) FROM {Dataset.TableName}", ct);

    /// <summary>Reads the current <c>SUM(balance)</c> baseline; captured once before a bank-transfer run
    /// so the conservation invariant measures against the pre-run total.</summary>
    public static Task<long> ReadBalanceSumAsync(CamusConnection conn, CancellationToken ct)
        => ScalarAsync(conn, $"SELECT SUM(balance) FROM {Dataset.TableName}", ct);

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
    public static async Task<ReconciliationResult> VerifyAsync(
        CamusConnection conn, RunMetrics metrics, long baselineVersionSum, long committedRowWrites,
        long indeterminateTxns, int writesPerTransaction, bool expectFaults,
        long expectedRows, CancellationToken ct,
        bool bankMode = false, long baselineBalanceSum = 0)
    {
        long persistedSum = await ScalarAsync(conn, $"SELECT SUM(version) FROM {Dataset.TableName}", ct).ConfigureAwait(false);
        long rowCount = await ScalarAsync(conn, $"SELECT COUNT(*) FROM {Dataset.TableName}", ct).ConfigureAwait(false);

        long persistedDelta = persistedSum - baselineVersionSum;
        (long expectedMin, long expectedMax) = VersionDeltaBand(committedRowWrites, indeterminateTxns, writesPerTransaction);
        bool versionsMatch = persistedDelta >= expectedMin && persistedDelta <= expectedMax;
        bool rowCountMatches = rowCount == expectedRows;
        bool accounting = metrics.Offered == metrics.Started + metrics.ScheduleDrops
                          && metrics.Started == metrics.Completed + metrics.Failed;
        bool noConflicts = metrics.Conflicts == 0;

        // Bank-transfer atomicity invariant: every transfer conserves total balance and commits
        // atomically, so SUM(balance) must be unchanged. This holds across faults and indeterminate
        // commits (an atomic transfer applies both legs or neither), so a changed sum is a genuine
        // atomicity break — never waived. In accounts mode balances change by design, so it is skipped.
        long balanceFinal = bankMode
            ? await ScalarAsync(conn, $"SELECT SUM(balance) FROM {Dataset.TableName}", ct).ConfigureAwait(false)
            : 0;
        bool balanceConserved = !bankMode || balanceFinal == baselineBalanceSum;

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

        return new ReconciliationResult(
            expectedMin, expectedMax, persistedDelta, indeterminateTxns,
            versionsMatch, rowCount, rowCountMatches, accounting, noConflicts, expectFaults && !noConflicts, failures,
            balanceConserved, baselineBalanceSum, balanceFinal, VersionCheckWaived: bankMode);
    }

    // Reconciliation runs right after the measured window, when a cluster that just lost and recovered a
    // node may still be slow to answer — a single scalar read can hit a client timeout or a retryable
    // server verdict. These reads are pure aggregates outside the measurement, so retrying them is both
    // safe and necessary: without it a transient blip during the post-fault settle crashes the whole run
    // (unhandled) instead of producing the consistency verdict the run exists to report.
    // A node recovering from a fault — a restart, or a full disk that was just freed and is now
    // compacting — can stay slow for tens of seconds. Reconciliation is post-measurement, so it can
    // afford to be patient: ~30 attempts with up to 2 s of backoff is roughly a minute of tolerance
    // before it gives up. Even then the caller keeps the measured artifacts (see VerifyOrInconclusive).
    private const int MaxScalarAttempts = 30;

    private static async Task<long> ScalarAsync(CamusConnection conn, string sql, CancellationToken ct)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                using CamusCommand cmd = conn.CreateCamusCommand(sql);
                using CamusDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                if (!await reader.ReadAsync(ct).ConfigureAwait(false) || reader.IsDBNull(0))
                    return 0;
                return reader.GetInt64(0);
            }
            catch (Exception ex)
            {
                Operations.OperationStatus status = Operations.ErrorClassifier.Classify(ex).Status;
                bool retryable = status is Operations.OperationStatus.Conflict or Operations.OperationStatus.Transient;
                if (!retryable || attempt >= MaxScalarAttempts || ct.IsCancellationRequested)
                    throw;

                await Task.Delay(Math.Min(200 * attempt, 2000), ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Runs <see cref="VerifyAsync"/> but never throws: if the verification queries cannot complete —
    /// a node too slow after a fault, a transport that stays down — it returns an <c>inconclusive</c>
    /// result (not passed, with the reason) instead of unwinding the run. The measured artifacts are
    /// already written by then, so a run that produced valid data but could not be verified is reported
    /// as "could not verify", not lost as "crashed".
    /// </summary>
    public static async Task<ReconciliationResult> VerifyOrInconclusiveAsync(
        CamusConnection conn, RunMetrics metrics, long baselineVersionSum, long committedRowWrites,
        long indeterminateTxns, int writesPerTransaction, bool expectFaults,
        long expectedRows, CancellationToken ct, bool bankMode, long baselineBalanceSum)
    {
        try
        {
            return await VerifyAsync(conn, metrics, baselineVersionSum, committedRowWrites, indeterminateTxns,
                writesPerTransaction, expectFaults, expectedRows, ct, bankMode, baselineBalanceSum).ConfigureAwait(false);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            return Inconclusive($"{ex.GetType().Name}: {ex.Message}", indeterminateTxns, metrics.Conflicts, baselineBalanceSum);
        }
    }

    /// <summary>Builds a not-passed "could not verify" result carrying the reason, for when the
    /// verification cannot run at all (the reconciliation connection could not even be opened).</summary>
    public static ReconciliationResult Inconclusive(string reason, long indeterminateTxns, long conflicts, long baselineBalanceSum)
        => new(
            0, 0, 0, indeterminateTxns,
            VersionsMatch: false, RowCount: -1, RowCountMatches: false, AccountingBalances: false,
            NoConflicts: conflicts == 0, ConflictsWaived: false,
            Failures: [$"reconciliation could not complete (the cluster stayed unavailable after the fault): {reason}"],
            BalanceConserved: false, BalanceBaseline: baselineBalanceSum, BalanceFinal: 0, VersionCheckWaived: false);
}
