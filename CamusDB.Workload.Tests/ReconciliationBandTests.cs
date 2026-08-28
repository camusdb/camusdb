/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Metrics;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// The SUM(version) ambiguity band: an indeterminate commit (no server verdict) applied either none or
/// all of its row writes, so the acceptable persisted delta is a closed interval — anchored below at
/// the rows known committed and widened above by at most writes-per-transaction rows per indeterminate
/// transaction. With zero indeterminate transactions it must collapse to the historical exact equality.
/// </summary>
[TestFixture]
public sealed class ReconciliationBandTests
{
    [Test]
    public void ZeroIndeterminateCollapsesToExactEquality()
    {
        (long min, long max) = Reconciliation.VersionDeltaBand(
            committedRowWrites: 1_000, indeterminateTxns: 0, writesPerTransaction: 3);
        Assert.That(min, Is.EqualTo(1_000));
        Assert.That(max, Is.EqualTo(1_000));

        Assert.That(Reconciliation.VersionDeltaWithinBand(1_000, 1_000, 0, 3), Is.True);
        Assert.That(Reconciliation.VersionDeltaWithinBand(999, 1_000, 0, 3), Is.False, "a lost committed write must fail");
        Assert.That(Reconciliation.VersionDeltaWithinBand(1_001, 1_000, 0, 3), Is.False, "a phantom write must fail");
    }

    [Test]
    public void BandAcceptsMinInteriorAndMax()
    {
        // 4 indeterminate txns × 3 rows each: [1000, 1012].
        (long min, long max) = Reconciliation.VersionDeltaBand(1_000, 4, 3);
        Assert.That((min, max), Is.EqualTo((1_000L, 1_012L)));

        Assert.That(Reconciliation.VersionDeltaWithinBand(1_000, 1_000, 4, 3), Is.True, "all indeterminate commits lost");
        Assert.That(Reconciliation.VersionDeltaWithinBand(1_006, 1_000, 4, 3), Is.True, "two of four landed");
        Assert.That(Reconciliation.VersionDeltaWithinBand(1_012, 1_000, 4, 3), Is.True, "all indeterminate commits landed");
    }

    [Test]
    public void BandRejectsOutsideEitherEdge()
    {
        Assert.That(Reconciliation.VersionDeltaWithinBand(999, 1_000, 4, 3), Is.False,
            "below the committed floor means a durably committed write was lost");
        Assert.That(Reconciliation.VersionDeltaWithinBand(1_013, 1_000, 4, 3), Is.False,
            "above the ceiling means more writes landed than were ever submitted");
    }

    [Test]
    public void SingleRowTransactionsWidenByOneEach()
    {
        (long min, long max) = Reconciliation.VersionDeltaBand(50, 7, 1);
        Assert.That((min, max), Is.EqualTo((50L, 57L)));
    }

    /// <summary>
    /// The reason the per-row check exists: every aggregate can be green while atomicity is broken. A
    /// result whose aggregates all agree must still fail when the per-row comparison found a violation,
    /// otherwise the sharper check would be computed and then ignored.
    /// </summary>
    [Test]
    public void AFailingPerRowCheckFailsAnOtherwiseGreenReconciliation()
    {
        Assert.That(GreenAggregates(null).Passed, Is.True, "the aggregates alone are all satisfied");

        RowAttributionResult leaked = new(
            RowAttributionStatus.Verified, null, RowsScanned: 100_000, RowsInAmbiguityBand: 800,
            BalanceViolations: 8, UncountedWriteRows: 8, LostWriteRows: 0,
            RowsMissing: 0, RowsDuplicated: 0, RowsForeign: 0, Violations: []);

        Assert.That(GreenAggregates(leaked).Passed, Is.False);
    }

    [Test]
    public void AnUnverifiablePerRowCheckDoesNotPassButAnOptedOutOneDoes()
    {
        Assert.That(GreenAggregates(RowAttributionResult.Unavailable("scan timed out")).Passed, Is.False,
            "'could not verify' must never read as 'verified clean'");
        Assert.That(GreenAggregates(RowAttributionResult.Disabled("--no-row-attribution")).Passed, Is.True,
            "an explicit opt-out is the operator's call, not a hidden failure");
    }

    /// <summary>A reconciliation result whose every aggregate check is satisfied, carrying the supplied
    /// per-row verdict.</summary>
    private static ReconciliationResult GreenAggregates(RowAttributionResult? rows) => new(
        ExpectedMin: 1_000, ExpectedMax: 1_000, Observed: 1_000, IndeterminateTxns: 0,
        VersionsMatch: true, RowCount: 100_000, RowCountMatches: true, AccountingBalances: true,
        NoConflicts: true, ConflictsWaived: false, Failures: [],
        BalanceConserved: true, BalanceBaseline: 42, BalanceFinal: 42, VersionCheckWaived: true,
        RowAttribution: rows);

    [Test]
    public void NonPositiveWritesPerTransactionIsClampedLikeTheWritePath()
    {
        // WriteOperation clamps writes-per-transaction to at least 1; the band must mirror that so an
        // indeterminate transaction always widens the ceiling by at least its one possible row write.
        (long min, long max) = Reconciliation.VersionDeltaBand(10, 2, 0);
        Assert.That((min, max), Is.EqualTo((10L, 12L)));
    }
}
