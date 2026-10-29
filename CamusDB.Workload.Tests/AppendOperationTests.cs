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
/// How a failed append attempt is judged. The one case that differs from the classifier's own
/// verdict is the duplicate key of the INSERT fallback: the list exists by the time the INSERT runs
/// (the UPDATE's statement snapshot predates the commit that started it, or another starter won), so
/// the attempt is a definite failure that a fresh BEGIN resolves, not a domain error of the workload.
/// The nightly append-read-committed of 2026-10-29 recorded 139 of them in 60 s at 64 appends per
/// list, and 1,248 at 8, every one of them final.
/// </summary>
[TestFixture]
public sealed class AppendOperationTests
{
    [Test]
    public void DuplicateKeyBeforeCommitIsARetryableConflict()
    {
        CamusException duplicate = new("CADB0300", "Duplicate entry for key 'elle_append.~pk'");

        (OperationStatus status, string code, bool retry) =
            AppendOperation.ClassifyFailure(duplicate, readOnly: false, commitSubmitted: false);

        Assert.That(status, Is.EqualTo(OperationStatus.Conflict));
        Assert.That(code, Is.EqualTo("CADB0300"));
        Assert.That(retry, Is.True);
    }

    /// <summary>The client does not always carry the server code; the message still identifies it.</summary>
    [Test]
    public void DuplicateKeyWithoutACodeIsStillARetryableConflict()
    {
        CamusException duplicate = new("", "Duplicate entry for key 'elle_append.~pk'");

        (OperationStatus status, _, bool retry) =
            AppendOperation.ClassifyFailure(duplicate, readOnly: false, commitSubmitted: false);

        Assert.That(status, Is.EqualTo(OperationStatus.Conflict));
        Assert.That(retry, Is.True);
    }

    /// <summary>Under optimistic locking the duplicate can arrive at the commit; a coded answer is a verdict.</summary>
    [Test]
    public void DuplicateKeyDuringCommitIsARetryableConflict()
    {
        CamusException duplicate = new("CADB0300", "Duplicate entry for key 'elle_append.~pk'");

        (OperationStatus status, _, bool retry) =
            AppendOperation.ClassifyFailure(duplicate, readOnly: false, commitSubmitted: true);

        Assert.That(status, Is.EqualTo(OperationStatus.Conflict));
        Assert.That(retry, Is.True);
    }

    [Test]
    public void LockConflictIsRetried()
    {
        CamusException conflict = new("CADB0502", "Key 1:2|r/1 is locked by another transaction");

        (OperationStatus status, string code, bool retry) =
            AppendOperation.ClassifyFailure(conflict, readOnly: false, commitSubmitted: false);

        Assert.That(status, Is.EqualTo(OperationStatus.Conflict));
        Assert.That(code, Is.EqualTo("CADB0502"));
        Assert.That(retry, Is.True);
    }

    [Test]
    public void OtherDomainErrorsAreFinal()
    {
        CamusException domain = new("CADB0100", "table not found");

        (OperationStatus status, _, bool retry) =
            AppendOperation.ClassifyFailure(domain, readOnly: false, commitSubmitted: false);

        Assert.That(status, Is.EqualTo(OperationStatus.DomainError));
        Assert.That(retry, Is.False);
    }

    /// <summary>A commit reply that carries no verdict may have landed; it is never retried.</summary>
    [Test]
    public void VerdictlessCommitFailureIsIndeterminateAndNotRetried()
    {
        (OperationStatus status, _, bool retry) =
            AppendOperation.ClassifyFailure(new IOException("stream reset"), readOnly: false, commitSubmitted: true);

        Assert.That(status, Is.EqualTo(OperationStatus.Indeterminate));
        Assert.That(retry, Is.False);
    }

    /// <summary>A read-only transaction has no effect to repeat, so nothing about it is retried.</summary>
    [Test]
    public void ReadOnlyFailuresAreNeverRetried()
    {
        CamusException conflict = new("CADB0502", "locked by another transaction");

        (OperationStatus status, _, bool retry) =
            AppendOperation.ClassifyFailure(conflict, readOnly: true, commitSubmitted: false);

        Assert.That(status, Is.EqualTo(OperationStatus.Conflict));
        Assert.That(retry, Is.False);
    }

    [Test]
    public void IsDuplicateKeyReadsTheCodeOrTheMessage()
    {
        Assert.That(ErrorClassifier.IsDuplicateKey(new CamusException("CADB0300", "anything")), Is.True);
        Assert.That(ErrorClassifier.IsDuplicateKey(new CamusException("", "Duplicate entry for key 'x'")), Is.True);
        Assert.That(ErrorClassifier.IsDuplicateKey(new CamusException("CADB0502", "locked")), Is.False);
        Assert.That(ErrorClassifier.IsDuplicateKey(new IOException("Duplicate entry")), Is.False);
    }
}
