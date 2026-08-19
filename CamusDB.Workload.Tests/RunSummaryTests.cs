/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Metrics;
using CamusDB.Workload.Operations;
using CamusDB.Workload.Results;
using CamusDB.Workload.Workload;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

[TestFixture]
public sealed class RunSummaryTests
{
    private static RunMetrics MetricsWith(int reads, int writes, int conflicts, int indeterminate = 0)
    {
        var m = new RunMetrics(writesPerTransaction: 1);
        for (int i = 0; i < reads; i++)
        {
            m.MarkOffered(); m.MarkStarted();
            m.RecordResult(OperationResult.ReadOk(), 1.0);
        }
        for (int i = 0; i < writes; i++)
        {
            m.MarkOffered(); m.MarkStarted();
            m.RecordResult(new OperationResult(OperationKind.Write, OperationStatus.Ok, null, 0.1, 0.2, 0.3, 5.0), 6.0);
        }
        for (int i = 0; i < conflicts; i++)
        {
            m.MarkOffered(); m.MarkStarted();
            m.RecordResult(OperationResult.Failure(OperationKind.Write, OperationStatus.Conflict, "CADB0502"), 3.0);
        }
        for (int i = 0; i < indeterminate; i++)
        {
            m.MarkOffered(); m.MarkStarted();
            m.RecordResult(OperationResult.Failure(OperationKind.Write, OperationStatus.Indeterminate, "IOException"), 8.0);
        }
        return m;
    }

    [Test]
    public void AnyConflictInvalidatesTheBaseline()
    {
        var m = MetricsWith(reads: 600, writes: 400, conflicts: 1);
        var summary = RunSummary.Build(m, mode: "closed", targetOps: 0, measuredSeconds: 10);
        Assert.That(summary.Valid, Is.False);
        Assert.That(summary.ValidityWarnings, Has.Some.Contains("conflict"));
    }

    [Test]
    public void CleanClosedRunIsValidAndReportsMix()
    {
        var m = MetricsWith(reads: 600, writes: 400, conflicts: 0);
        var summary = RunSummary.Build(m, mode: "closed", targetOps: 0, measuredSeconds: 10);
        Assert.That(summary.Valid, Is.True);
        Assert.That(summary.ReadPercentActual, Is.EqualTo(60).Within(0.01));
        Assert.That(summary.WritePercentActual, Is.EqualTo(40).Within(0.01));
        Assert.That(summary.WriteTxnsPerSec, Is.EqualTo(40).Within(0.01));
    }

    [Test]
    public void OpenLoopWithExcessiveDropsIsInvalid()
    {
        var m = MetricsWith(reads: 600, writes: 400, conflicts: 0);
        for (int i = 0; i < 100; i++) { m.MarkOffered(); m.MarkScheduleDrop(); }
        var summary = RunSummary.Build(m, mode: "open", targetOps: 200, measuredSeconds: 10);
        Assert.That(summary.Valid, Is.False, "drops well over 0.1% must invalidate capacity claims");
    }

    [Test]
    public void ExpectFaultsWaivesConflictsIntoWarnings()
    {
        var m = MetricsWith(reads: 600, writes: 400, conflicts: 3);
        var summary = RunSummary.Build(m, mode: "closed", targetOps: 0, measuredSeconds: 10, expectFaults: true);
        Assert.That(summary.Valid, Is.True, "conflicts are expected collateral under --expect-faults");
        Assert.That(summary.ExpectFaults, Is.True, "the waiver must be recorded for report readers");
        Assert.That(summary.Conflicts, Is.EqualTo(3), "waived conflicts must still be counted");
        Assert.That(summary.ValidityWarnings, Has.Some.Contains("waived by --expect-faults"));
    }

    [Test]
    public void ExpectFaultsWaivesOpenLoopPacingShortfalls()
    {
        var m = MetricsWith(reads: 600, writes: 400, conflicts: 0);
        for (int i = 0; i < 100; i++) { m.MarkOffered(); m.MarkScheduleDrop(); }
        var summary = RunSummary.Build(m, mode: "open", targetOps: 200, measuredSeconds: 10, expectFaults: true);
        Assert.That(summary.Valid, Is.True, "drops/submission shortfalls are expected while nodes are down");
        Assert.That(summary.ValidityWarnings, Has.Some.Contains("waived by --expect-faults"));
    }

    [Test]
    public void WithoutExpectFaultsStrictBehaviorIsUnchanged()
    {
        var m = MetricsWith(reads: 600, writes: 400, conflicts: 1);
        var summary = RunSummary.Build(m, mode: "closed", targetOps: 0, measuredSeconds: 10);
        Assert.That(summary.ExpectFaults, Is.False);
        Assert.That(summary.Valid, Is.False, "default runs keep the strict non-conflicting contract");
        Assert.That(summary.ValidityWarnings, Has.None.Contains("waived"));
    }

    [Test]
    public void IndeterminateWritesAreCountedAndWarnWithoutInvalidating()
    {
        var m = MetricsWith(reads: 600, writes: 400, conflicts: 0, indeterminate: 2);
        var summary = RunSummary.Build(m, mode: "closed", targetOps: 0, measuredSeconds: 10);
        Assert.That(summary.Indeterminate, Is.EqualTo(2));
        Assert.That(summary.Failed, Is.EqualTo(2), "indeterminate writes still count as not-completed");
        Assert.That(summary.Valid, Is.True, "reconciliation, not validity, arbitrates indeterminate outcomes");
        Assert.That(summary.ValidityWarnings, Has.Some.Contains("indeterminate"));
        Assert.That(m.Errors.TotalCount(), Is.EqualTo(2), "indeterminate failures must be sampled into errors.json");
    }
}
