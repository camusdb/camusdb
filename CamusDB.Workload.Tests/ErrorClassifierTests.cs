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
/// definite abort (nothing was asked to persist), while a failure of the commit round trip that
/// carries no verdict is <see cref="OperationStatus.Indeterminate"/> — the write may have landed.
/// Verdict-less shapes are: a <see cref="CamusException"/> with no code and a transport-condition
/// message (nothing answered), and the explicit outcome-unavailable server codes CADB0501 /
/// CADB0509 (the server answered that it does not know the outcome). Any other coded
/// <see cref="CamusException"/> is a server verdict and stays definite even during commit.
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

    /// <summary>
    /// The gRPC client wraps a transport failure in a <see cref="CamusException"/> with an empty code
    /// (the gRPC status never becomes a CamusDB error code). Nothing answered, so the commit may have
    /// landed. Reading it as a definite abort tells the per-row attribution the transfer applied
    /// nothing, and a commit that did land then looks exactly like a leaked write — the harness would
    /// report its own misreading as an engine defect.
    /// </summary>
    [TestCase("Status(StatusCode=\"DeadlineExceeded\", Detail=\"\")")]
    [TestCase("Status(StatusCode=\"Unavailable\", Detail=\"failed to connect\")")]
    [TestCase("Status(StatusCode=\"Cancelled\", Detail=\"\")")]
    public void CodelessTransportFailureDuringCommitIsIndeterminate(string message)
    {
        (OperationStatus status, _) = ErrorClassifier.Classify(new CamusException("", message), commitSubmitted: true);
        Assert.That(status, Is.EqualTo(OperationStatus.Indeterminate));
    }

    [Test]
    public void CodelessTransportFailureBeforeCommitKeepsItsOrdinaryClassification()
    {
        (OperationStatus status, _) = ErrorClassifier.Classify(
            new CamusException("", "Status(StatusCode=\"Unavailable\")"), commitSubmitted: false);
        Assert.That(status, Is.EqualTo(OperationStatus.DomainError),
            "no commit was requested, so nothing can have landed");
    }

    [Test]
    public void CodelessServerErrorDuringCommitIsStillADefiniteAbort()
    {
        // The wire drops the code on genuine server errors too, so an empty code alone is not enough:
        // a message that names no transport condition still describes a verdict the server reached.
        (OperationStatus status, _) = ErrorClassifier.Classify(
            new CamusException("", "Duplicate entry for key 'workload_accounts_pk'"), commitSubmitted: true);
        Assert.That(status, Is.EqualTo(OperationStatus.DomainError));
    }

    [Test]
    public void CommitFlagDoesNotDisturbOrdinaryClassification()
    {
        (OperationStatus status, _) = ErrorClassifier.Classify(new InvalidOperationException("row missing"), commitSubmitted: false);
        Assert.That(status, Is.EqualTo(OperationStatus.InternalError));
    }

    /// <summary>
    /// CADB0501 (the coordinator session is gone and the durable outcome could not be read back) and
    /// CADB0509 (commit outcome not yet known) both report that the outcome is UNAVAILABLE, not that the
    /// transaction failed. A commit reported under either can have committed durably — store forensics of a
    /// soak run found every one of its "leaked" rows was a transfer the client had recorded under CADB0501
    /// that Kahuna had in fact committed. So on a submitted commit these must widen the ambiguity, exactly
    /// like a codeless transport fault, rather than count as a definite non-commit.
    /// </summary>
    [TestCase("CADB0501")]
    [TestCase("CADB0509")]
    public void OutcomeUnavailableCodeDuringCommitIsIndeterminate(string code)
    {
        var ex = new CamusException(code, "the coordinator session is gone and the outcome is unavailable");
        (OperationStatus status, string returnedCode) = ErrorClassifier.Classify(ex, commitSubmitted: true);
        Assert.That(status, Is.EqualTo(OperationStatus.Indeterminate));
        Assert.That(returnedCode, Is.EqualTo(code), "the underlying code survives for errors.json aggregation");
    }

    [Test]
    public void OutcomeUnavailableCodeBeforeCommitKeepsItsOrdinaryClassification()
    {
        // No commit was requested, so the "outcome unavailable" widening does not apply: nothing could have
        // landed, and the ordinary (definite) classification stands.
        (OperationStatus status, _) = ErrorClassifier.Classify(
            new CamusException("CADB0501", "a commit or rollback is in progress"), commitSubmitted: false);
        Assert.That(status, Is.EqualTo(OperationStatus.DomainError));
    }

    /// <summary>
    /// CADB0000 is the server's stamp for an UNEXPECTED exception at the RPC boundary, and the gRPC
    /// client fabricates the same code for a trailer-less transport failure (a node that died
    /// mid-call). Neither is a transaction verdict. In the split-nemesis fault soak, commits that had
    /// durably applied were answered CADB0000 when the coordinator's node was SIGKILLed, and counting
    /// the code as a definite abort reported the engine's own committed writes as leaked.
    /// </summary>
    [Test]
    public void InternalUnmappedCodeDuringCommitIsIndeterminate()
    {
        var ex = new CamusException("CADB0000", "Status(StatusCode=\"Unavailable\", Detail=\"Error reading next message\")");
        (OperationStatus status, string returnedCode) = ErrorClassifier.Classify(ex, commitSubmitted: true);
        Assert.That(status, Is.EqualTo(OperationStatus.Indeterminate));
        Assert.That(returnedCode, Is.EqualTo("CADB0000"), "the underlying code survives for errors.json aggregation");
    }

    [Test]
    public void InternalUnmappedCodeBeforeCommitStaysADefiniteDomainError()
    {
        // Before the commit request nothing could have landed: an internal error on a read or update
        // aborts the transaction definitively and must not inflate the ambiguity band.
        (OperationStatus status, _) = ErrorClassifier.Classify(
            new CamusException("CADB0000", "Internal server error"), commitSubmitted: false);
        Assert.That(status, Is.EqualTo(OperationStatus.DomainError));
    }
}
