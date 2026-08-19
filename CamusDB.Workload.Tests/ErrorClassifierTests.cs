/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using CamusDB.Workload.Operations;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// The commit-phase boundary of error classification: a failure before the commit request is a
/// definite abort (nothing was asked to persist), while a verdict-less failure of the commit round
/// trip itself is <see cref="OperationStatus.Indeterminate"/> — the write may have landed. A
/// <see cref="CamusException"/> is a server verdict and therefore definite even during commit.
/// </summary>
[TestFixture]
public sealed class ErrorClassifierTests
{
    [Test]
    public void TransportFailureBeforeCommitIsADefiniteTransientAbort()
    {
        (OperationStatus status, string code) = ErrorClassifier.Classify(new IOException("stream reset"), commitSubmitted: false);
        Assert.That(status, Is.EqualTo(OperationStatus.Transient));
        Assert.That(code, Is.EqualTo("IOException"));
    }

    [Test]
    public void TransportFailureDuringCommitIsIndeterminate()
    {
        (OperationStatus status, string code) = ErrorClassifier.Classify(new IOException("stream reset"), commitSubmitted: true);
        Assert.That(status, Is.EqualTo(OperationStatus.Indeterminate));
        Assert.That(code, Is.EqualTo("IOException"), "the underlying code survives for errors.json aggregation");
    }

    [Test]
    public void CancellationDuringCommitIsIndeterminate()
    {
        (OperationStatus status, string code) = ErrorClassifier.Classify(new OperationCanceledException(), commitSubmitted: true);
        Assert.That(status, Is.EqualTo(OperationStatus.Indeterminate));
        Assert.That(code, Is.EqualTo("CANCELED"));
    }

    [Test]
    public void UnexpectedClientFaultDuringCommitIsIndeterminate()
    {
        (OperationStatus status, _) = ErrorClassifier.Classify(new InvalidOperationException("broken pipe"), commitSubmitted: true);
        Assert.That(status, Is.EqualTo(OperationStatus.Indeterminate),
            "without a server verdict the commit may still have durably applied");
    }

    [Test]
    public void ServerConflictVerdictDuringCommitStaysAConflict()
    {
        var conflict = new CamusException("CADB0502", "lock conflict");
        (OperationStatus status, string code) = ErrorClassifier.Classify(conflict, commitSubmitted: true);
        Assert.That(status, Is.EqualTo(OperationStatus.Conflict), "a server answer is a definite abort, never indeterminate");
        Assert.That(code, Is.EqualTo("CADB0502"));
    }

    [Test]
    public void ServerDomainErrorDuringCommitStaysADomainError()
    {
        var domain = new CamusException("CADB0100", "table not found");
        (OperationStatus status, _) = ErrorClassifier.Classify(domain, commitSubmitted: true);
        Assert.That(status, Is.EqualTo(OperationStatus.DomainError));
    }

    [Test]
    public void CommitFlagDoesNotDisturbOrdinaryClassification()
    {
        (OperationStatus status, _) = ErrorClassifier.Classify(new InvalidOperationException("row missing"), commitSubmitted: false);
        Assert.That(status, Is.EqualTo(OperationStatus.InternalError));
    }
}
