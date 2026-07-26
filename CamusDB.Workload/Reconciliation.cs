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

/// <summary>Outcome of the post-run correctness reconciliation. All checks must pass for a valid run.</summary>
public sealed record ReconciliationResult(
    long ExpectedVersionSum,
    long PersistedVersionSum,
    bool VersionsMatch,
    long RowCount,
    bool RowCountMatches,
    bool AccountingBalances,
    bool NoConflicts,
    IReadOnlyList<string> Failures)
{
    public bool Passed => VersionsMatch && RowCountMatches && AccountingBalances && NoConflicts;
}

/// <summary>
/// Verifies at shutdown that the run was internally consistent, so a "fast" number that lost or
/// double-counted work cannot pass silently. Because every row is seeded at <c>version = 0</c> and each
/// committed write increments <c>version</c> on exactly <c>writes-per-transaction</c> rows, the
/// persisted <c>SUM(version)</c> must equal committed writes × writes-per-transaction — a single cheap
/// aggregate that proves the durable effect matches the client's committed count without scanning
/// every row by hand. It also confirms the seeded row count survived and that the operation accounting
/// (offered = started + drops, started = completed + failed) has no lost operations.
/// </summary>
public static class Reconciliation
{
    /// <summary>Reads the current <c>SUM(version)</c> baseline; captured once before the run so
    /// reconciliation measures only this run's increments, not the accumulation of prior runs on the
    /// same seeded dataset.</summary>
    public static Task<long> ReadVersionSumAsync(CamusConnection conn, CancellationToken ct)
        => ScalarAsync(conn, $"SELECT SUM(version) FROM {Dataset.TableName}", ct);

    /// <param name="baselineVersionSum">
    /// <c>SUM(version)</c> captured before the run started (warm-up included). The run is correct when
    /// the persisted sum grew by exactly <paramref name="expectedVersionDelta"/>.
    /// </param>
    /// <param name="expectedVersionDelta">
    /// Rows the driver durably committed across the whole run (warm-up + measured) — the amount by which
    /// <c>SUM(version)</c> must have increased.
    /// </param>
    public static async Task<ReconciliationResult> VerifyAsync(
        CamusConnection conn, RunMetrics metrics, long baselineVersionSum, long expectedVersionDelta,
        long expectedRows, CancellationToken ct)
    {
        long persistedSum = await ScalarAsync(conn, $"SELECT SUM(version) FROM {Dataset.TableName}", ct).ConfigureAwait(false);
        long rowCount = await ScalarAsync(conn, $"SELECT COUNT(*) FROM {Dataset.TableName}", ct).ConfigureAwait(false);

        long persistedDelta = persistedSum - baselineVersionSum;
        long expectedSum = expectedVersionDelta;
        bool versionsMatch = persistedDelta == expectedSum;
        bool rowCountMatches = rowCount == expectedRows;
        bool accounting = metrics.Offered == metrics.Started + metrics.ScheduleDrops
                          && metrics.Started == metrics.Completed + metrics.Failed;
        bool noConflicts = metrics.Conflicts == 0;

        List<string> failures = new();
        if (!versionsMatch)
            failures.Add($"persisted SUM(version) delta={persistedDelta} != expected {expectedSum} " +
                         $"(rows committed across the whole run; baseline={baselineVersionSum}, final={persistedSum}).");
        if (!rowCountMatches)
            failures.Add($"row count {rowCount} != expected {expectedRows}.");
        if (!accounting)
            failures.Add($"operation accounting mismatch: offered={metrics.Offered}, started={metrics.Started}, " +
                         $"completed={metrics.Completed}, failed={metrics.Failed}, drops={metrics.ScheduleDrops}.");
        if (!noConflicts)
            failures.Add($"{metrics.Conflicts} conflict(s) in the non-conflicting baseline.");

        return new ReconciliationResult(
            expectedSum, persistedDelta, versionsMatch, rowCount, rowCountMatches, accounting, noConflicts, failures);
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
