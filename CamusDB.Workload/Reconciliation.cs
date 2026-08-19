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
    IReadOnlyList<string> Failures)
{
    public bool Passed => VersionsMatch && RowCountMatches && AccountingBalances && (NoConflicts || ConflictsWaived);
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
        long expectedRows, CancellationToken ct)
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

        List<string> failures = new();
        if (!versionsMatch)
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

        return new ReconciliationResult(
            expectedMin, expectedMax, persistedDelta, indeterminateTxns,
            versionsMatch, rowCount, rowCountMatches, accounting, noConflicts, expectFaults && !noConflicts, failures);
    }

    private static async Task<long> ScalarAsync(CamusConnection conn, string sql, CancellationToken ct)
    {
        using CamusCommand cmd = conn.CreateCamusCommand(sql);
        using CamusDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false) || reader.IsDBNull(0))
            return 0;
        return reader.GetInt64(0);
    }
}
