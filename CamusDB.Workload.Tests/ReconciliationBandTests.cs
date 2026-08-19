/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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

    [Test]
    public void NonPositiveWritesPerTransactionIsClampedLikeTheWritePath()
    {
        // WriteOperation clamps writes-per-transaction to at least 1; the band must mirror that so an
        // indeterminate transaction always widens the ceiling by at least its one possible row write.
        (long min, long max) = Reconciliation.VersionDeltaBand(10, 2, 0);
        Assert.That((min, max), Is.EqualTo((10L, 12L)));
    }
}
