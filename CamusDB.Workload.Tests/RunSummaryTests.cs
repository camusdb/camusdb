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
    private static RunMetrics MetricsWith(int reads, int writes, int conflicts)
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
}
