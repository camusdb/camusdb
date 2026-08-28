/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Metrics;
using CamusDB.Workload.Util;
using CamusDB.Workload.Workload;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// The per-row atomicity check. Each test builds a durable state by hand — including states no healthy
/// engine would produce — and asserts what the check says about it.
///
/// <para>The case that motivated the check is <see cref="LeakedLegsThatCancelPassTheAggregateAndFailPerRow"/>:
/// a run in which eight writes leaked in cancelling pairs left both <c>SUM(balance)</c> and
/// <c>SUM(version)</c> exactly where they belonged, and was reported as PASS.</para>
/// </summary>
[TestFixture]
public sealed class RowAttributionTests
{
    private const ulong Seed = 1847;
    private const long Rows = 64;

    /// <summary>An in-memory table the check can scan, keyed by row index.</summary>
    private sealed class FakeStore
    {
        private readonly Dataset _dataset;
        private readonly Dictionary<long, (long Balance, long Version)> _rows = new();
        private readonly List<(string Table, ScannedRow Row)> _extra = new();
        private readonly HashSet<long> _deleted = new();

        public FakeStore(Dataset dataset)
        {
            _dataset = dataset;
            for (long i = 0; i < dataset.Rows; i++)
                _rows[i] = (dataset.RowFor(i).Balance, 0);
        }

        public long BalanceOf(long index) => _rows[index].Balance;

        /// <summary>Applies one transfer leg the way a committed transaction would.</summary>
        public void ApplyLeg(long index, long delta)
        {
            (long balance, long version) = _rows[index];
            _rows[index] = (balance + delta, version + 1);
        }

        /// <summary>Moves a balance without touching the version, to separate the two checks.</summary>
        public void SetBalance(long index, long balance) => _rows[index] = (balance, _rows[index].Version);

        public void SetVersion(long index, long version) => _rows[index] = (_rows[index].Balance, version);

        public void Delete(long index) => _deleted.Add(index);

        public void AddExtraRow(string table, ScannedRow row) => _extra.Add((table, row));

        public long BalanceSum()
        {
            long total = 0;
            foreach (long i in _rows.Keys)
            {
                if (!_deleted.Contains(i))
                    total += _rows[i].Balance;
            }
            return total;
        }

        public long VersionSum()
        {
            long total = 0;
            foreach (long i in _rows.Keys)
            {
                if (!_deleted.Contains(i))
                    total += _rows[i].Version;
            }
            return total;
        }

        public TableRowSource Source => (table, _) => Stream(table);

        private async IAsyncEnumerable<ScannedRow> Stream(string table)
        {
            await Task.Yield();
            foreach ((long index, (long balance, long version)) in _rows)
            {
                if (_deleted.Contains(index) || _dataset.TableOf(index) != table)
                    continue;
                yield return new ScannedRow(RowIdFactory.ForRow(Seed, index), balance, version);
            }
            foreach ((string extraTable, ScannedRow row) in _extra)
            {
                if (extraTable == table)
                    yield return row;
            }
        }
    }

    private static Dataset NewDataset(int tables = 1) => new(Seed, Rows, payloadBytes: 8, tables);

    private static RowAttribution NewAttribution(Dataset dataset)
    {
        RowAttribution? attribution = RowAttribution.TryCreate(dataset, Seed, out string? why);
        Assert.That(attribution, Is.Not.Null, why);
        return attribution!;
    }

    private static async Task<RowAttributionResult> RunAsync(
        RowAttribution attribution, FakeStore store)
    {
        await attribution.CaptureBaselineAsync(store.Source, CancellationToken.None);
        return await VerifyOnlyAsync(attribution, store);
    }

    /// <summary>Verifies against a store whose baseline was already captured (possibly a different one).</summary>
    private static Task<RowAttributionResult> VerifyOnlyAsync(RowAttribution attribution, FakeStore store)
        => attribution.VerifyAsync(store.Source, CancellationToken.None);

    [Test]
    public async Task CleanTransferRunPasses()
    {
        Dataset dataset = NewDataset();
        RowAttribution attribution = NewAttribution(dataset);
        FakeStore store = new(dataset);
        await attribution.CaptureBaselineAsync(store.Source, CancellationToken.None);

        // Ten transfers, journalled and applied.
        for (long i = 0; i < 10; i++)
        {
            long low = i, high = i + 20;
            attribution.Record(low, high, -1, TransferOutcome.Committed);
            store.ApplyLeg(low, -1);
            store.ApplyLeg(high, +1);
        }

        RowAttributionResult result = await VerifyOnlyAsync(attribution, store);

        Assert.That(result.Status, Is.EqualTo(RowAttributionStatus.Verified));
        Assert.That(result.TotalViolations, Is.Zero, string.Join("; ", result.Violations.Select(v => v.Detail)));
        Assert.That(result.RowsScanned, Is.EqualTo(Rows));
        Assert.That(result.RowsInAmbiguityBand, Is.Zero);
        Assert.That(result.Passed, Is.True);
    }

    /// <summary>
    /// The finding this check exists for. Eight legs are applied that the client never counted, in
    /// cancelling pairs: four credits and four debits. Both aggregate totals land exactly where a clean
    /// run would leave them, so the aggregate verdict is PASS. The per-row comparison has no such
    /// cancellation and reports every one of the eight rows.
    /// </summary>
    [Test]
    public async Task LeakedLegsThatCancelPassTheAggregateAndFailPerRow()
    {
        Dataset dataset = NewDataset();
        RowAttribution attribution = NewAttribution(dataset);
        FakeStore store = new(dataset);
        await attribution.CaptureBaselineAsync(store.Source, CancellationToken.None);

        long baselineBalance = store.BalanceSum();
        long baselineVersion = store.VersionSum();

        // Four transfers the server aborted — the client journalled nothing durable — whose legs the
        // engine applied anyway. Each pair's +1 and -1 cancel in the totals.
        for (long i = 0; i < 4; i++)
        {
            long low = 30 + i, high = 40 + i;
            attribution.Record(low, high, -1, TransferOutcome.ConflictFinal);
            store.ApplyLeg(low, -1);
            store.ApplyLeg(high, +1);
        }

        // The aggregate invariants both see a perfectly healthy run.
        Assert.That(store.BalanceSum(), Is.EqualTo(baselineBalance), "SUM(balance) is blind to legs that cancel");
        Assert.That(store.VersionSum() - baselineVersion, Is.EqualTo(8),
            "SUM(version) moved, but by the amount 4 committed transfers would also produce");

        RowAttributionResult result = await VerifyOnlyAsync(attribution, store);

        Assert.That(result.Passed, Is.False, "the per-row check must catch what the totals cannot");
        Assert.That(result.BalanceViolations, Is.EqualTo(8), "every leaked leg moved its row's balance");
        Assert.That(result.UncountedWriteRows, Is.EqualTo(8), "every leaked leg added an uncounted version increment");
        Assert.That(result.RowsInAmbiguityBand, Is.Zero, "no attempt was indeterminate, so nothing is excused");
    }

    /// <summary>
    /// Version accounting is the sharper of the two checks: a leaked pair of legs against the <em>same</em>
    /// row leaves its balance exactly right, and only the version count shows the writes happened.
    /// </summary>
    [Test]
    public async Task VersionAccountingCatchesALeakThatLeavesTheBalanceCorrect()
    {
        Dataset dataset = NewDataset();
        RowAttribution attribution = NewAttribution(dataset);
        FakeStore store = new(dataset);
        await attribution.CaptureBaselineAsync(store.Source, CancellationToken.None);

        long before = store.BalanceOf(7);
        store.ApplyLeg(7, +1);
        store.ApplyLeg(7, -1);
        Assert.That(store.BalanceOf(7), Is.EqualTo(before), "the two stray writes left the balance unchanged");

        RowAttributionResult result = await VerifyOnlyAsync(attribution, store);

        Assert.That(result.BalanceViolations, Is.Zero);
        Assert.That(result.UncountedWriteRows, Is.EqualTo(1));
        Assert.That(result.Passed, Is.False);
        Assert.That(result.Violations.Single().Kind, Is.EqualTo("version-high"));
    }

    [Test]
    public async Task ALostCommittedWriteIsReportedSeparately()
    {
        Dataset dataset = NewDataset();
        RowAttribution attribution = NewAttribution(dataset);
        FakeStore store = new(dataset);
        await attribution.CaptureBaselineAsync(store.Source, CancellationToken.None);

        // The client saw a commit; the durable state never took it.
        attribution.Record(3, 9, -1, TransferOutcome.Committed);

        RowAttributionResult result = await VerifyOnlyAsync(attribution, store);

        Assert.That(result.LostWriteRows, Is.EqualTo(2));
        Assert.That(result.BalanceViolations, Is.EqualTo(2));
        Assert.That(result.Passed, Is.False);
    }

    /// <summary>
    /// An indeterminate attempt may have applied or not, so both outcomes must be accepted — otherwise
    /// the check turns noisy exactly when the cluster is unhealthy, which is when it is needed most.
    /// </summary>
    [TestCase(true)]
    [TestCase(false)]
    public async Task AnIndeterminateTransferIsAcceptedWhetherOrNotItLanded(bool landed)
    {
        Dataset dataset = NewDataset();
        RowAttribution attribution = NewAttribution(dataset);
        FakeStore store = new(dataset);
        await attribution.CaptureBaselineAsync(store.Source, CancellationToken.None);

        attribution.Record(5, 15, -1, TransferOutcome.Indeterminate);
        if (landed)
        {
            store.ApplyLeg(5, -1);
            store.ApplyLeg(15, +1);
        }

        RowAttributionResult result = await VerifyOnlyAsync(attribution, store);

        Assert.That(result.TotalViolations, Is.Zero, string.Join("; ", result.Violations.Select(v => v.Detail)));
        Assert.That(result.RowsInAmbiguityBand, Is.EqualTo(2), "both rows are reported as unknowable, not as clean");
        Assert.That(result.Passed, Is.True);
    }

    /// <summary>The band is exactly one attempt wide, so a second uncounted application still fails.</summary>
    [Test]
    public async Task ARowOutsideItsWidenedIndeterminateBandIsStillAViolation()
    {
        Dataset dataset = NewDataset();
        RowAttribution attribution = NewAttribution(dataset);
        FakeStore store = new(dataset);
        await attribution.CaptureBaselineAsync(store.Source, CancellationToken.None);

        attribution.Record(5, 15, -1, TransferOutcome.Indeterminate);
        store.ApplyLeg(5, -1);
        store.ApplyLeg(5, -1);   // applied twice: one attempt cannot explain two increments
        store.ApplyLeg(15, +1);

        RowAttributionResult result = await VerifyOnlyAsync(attribution, store);

        Assert.That(result.Passed, Is.False);
        Assert.That(result.UncountedWriteRows, Is.EqualTo(1));
        Assert.That(result.BalanceViolations, Is.EqualTo(1));
    }

    /// <summary>
    /// An indeterminate attempt widens the band only in the direction its own delta could move the row,
    /// so a drift the opposite way is still caught.
    /// </summary>
    [Test]
    public async Task TheIndeterminateBandIsSignedNotSymmetric()
    {
        Dataset dataset = NewDataset();
        RowAttribution attribution = NewAttribution(dataset);
        FakeStore store = new(dataset);
        await attribution.CaptureBaselineAsync(store.Source, CancellationToken.None);

        // Row 5 could only have been debited by the indeterminate attempt; it turns up credited.
        attribution.Record(5, 15, -1, TransferOutcome.Indeterminate);
        store.SetBalance(5, store.BalanceOf(5) + 1);

        RowAttributionResult result = await VerifyOnlyAsync(attribution, store);

        Assert.That(result.BalanceViolations, Is.EqualTo(1));
        Assert.That(result.Passed, Is.False);
    }

    [Test]
    public async Task AMissingRowAndAForeignRowAreBothReported()
    {
        Dataset dataset = NewDataset();
        RowAttribution attribution = NewAttribution(dataset);
        FakeStore baseline = new(dataset);
        await attribution.CaptureBaselineAsync(baseline.Source, CancellationToken.None);

        FakeStore after = new(dataset);
        after.Delete(11);
        after.AddExtraRow(Dataset.TableName, new ScannedRow(new string('a', 24), 5, 0));

        RowAttributionResult result = await VerifyOnlyAsync(attribution, after);

        Assert.That(result.RowsMissing, Is.EqualTo(1));
        Assert.That(result.RowsForeign, Is.EqualTo(1));
        Assert.That(result.Passed, Is.False);
    }

    [Test]
    public async Task ARowReturnedTwiceIsReported()
    {
        Dataset dataset = NewDataset();
        RowAttribution attribution = NewAttribution(dataset);
        FakeStore baseline = new(dataset);
        await attribution.CaptureBaselineAsync(baseline.Source, CancellationToken.None);

        FakeStore after = new(dataset);
        after.AddExtraRow(Dataset.TableName, new ScannedRow(RowIdFactory.ForRow(Seed, 4), 1, 0));

        RowAttributionResult result = await VerifyOnlyAsync(attribution, after);

        Assert.That(result.RowsDuplicated, Is.EqualTo(1));
        Assert.That(result.Passed, Is.False);
    }

    [Test]
    public async Task TheBaselineIsTheDatasetsActualStartingStateNotItsSeededValues()
    {
        // A dataset a previous run already moved: non-seeded balances and non-zero versions. The check
        // must measure this run's effect against what it found, not against the seeded values.
        Dataset dataset = NewDataset();
        RowAttribution attribution = NewAttribution(dataset);
        FakeStore store = new(dataset);
        for (long i = 0; i < Rows; i++)
        {
            store.SetBalance(i, store.BalanceOf(i) + 500);
            store.SetVersion(i, 17);
        }

        RowScanTotals totals = await attribution.CaptureBaselineAsync(store.Source, CancellationToken.None);
        Assert.That(totals.Rows, Is.EqualTo(Rows));
        Assert.That(totals.VersionSum, Is.EqualTo(17 * Rows));
        Assert.That(totals.BalanceSum, Is.EqualTo(store.BalanceSum()));

        attribution.Record(1, 2, -1, TransferOutcome.Committed);
        store.ApplyLeg(1, -1);
        store.ApplyLeg(2, +1);

        RowAttributionResult result = await VerifyOnlyAsync(attribution, store);
        Assert.That(result.TotalViolations, Is.Zero, string.Join("; ", result.Violations.Select(v => v.Detail)));
    }

    [Test]
    public async Task TransfersAcrossTablesAreAttributedToTheRightTable()
    {
        Dataset dataset = NewDataset(tables: 4);
        RowAttribution attribution = NewAttribution(dataset);
        FakeStore store = new(dataset);
        await attribution.CaptureBaselineAsync(store.Source, CancellationToken.None);

        // Row 2 and row 50 live in different tables at 64 rows over 4 tables.
        Assert.That(dataset.TableOf(2), Is.Not.EqualTo(dataset.TableOf(50)));
        store.ApplyLeg(50, +1);   // a leaked credit in the far table

        RowAttributionResult result = await VerifyOnlyAsync(attribution, store);

        Assert.That(result.RowsScanned, Is.EqualTo(Rows), "every table must be scanned");
        Assert.That(result.UncountedWriteRows, Is.EqualTo(1));
        Assert.That(result.BalanceViolations, Is.EqualTo(1));
        Assert.That(result.Violations.Select(v => v.Table), Is.All.EqualTo(dataset.TableOf(50)));
        Assert.That(result.Violations.Select(v => v.RowIndex), Is.All.EqualTo(50L));
    }

    [Test]
    public async Task VerifyWithoutABaselineDoesNotPass()
    {
        Dataset dataset = NewDataset();
        RowAttribution attribution = NewAttribution(dataset);
        FakeStore store = new(dataset);

        RowAttributionResult result = await VerifyOnlyAsync(attribution, store);

        Assert.That(result.Status, Is.EqualTo(RowAttributionStatus.Unavailable));
        Assert.That(result.Passed, Is.False, "'we could not look' must never read as 'we looked and it was clean'");
    }

    [Test]
    public void ABaselineScanThatCannotSeeTheWholeDatasetIsRefused()
    {
        Dataset dataset = NewDataset();
        RowAttribution attribution = NewAttribution(dataset);
        FakeStore store = new(dataset);
        store.Delete(0);

        Assert.ThrowsAsync<InvalidOperationException>(
            () => attribution.CaptureBaselineAsync(store.Source, CancellationToken.None));
        Assert.That(attribution.HasBaseline, Is.False);
    }

    [Test]
    public void ADatasetTooLargeToAttributeIsReportedRatherThanCrashing()
    {
        Dataset huge = new(Seed, RowAttribution.MaxAttributedRows + 1, payloadBytes: 8);
        RowAttribution? attribution = RowAttribution.TryCreate(huge, Seed, out string? why);

        Assert.That(attribution, Is.Null);
        Assert.That(why, Does.Contain("per-row attribution"));
    }

    [Test]
    public void ADisabledCheckPassesButAnUnavailableOneDoesNot()
    {
        Assert.That(RowAttributionResult.Disabled("opted out").Passed, Is.True);
        Assert.That(RowAttributionResult.Unavailable("scan failed").Passed, Is.False);
    }
}
