/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;

namespace CamusDB.Workload.Operations;

/// <summary>
/// Maps a thrown exception to a bounded <see cref="OperationStatus"/> plus a stable error code. The
/// conflict codes (lock conflict, must-retry, lifetime-exceeded) are the ones that, in the
/// non-conflicting baseline, invalidate a run; transport faults surface as <c>Transient</c> and are
/// reported separately because retry time still counts toward user-visible latency but does not mean
/// the workload contended for data.
/// </summary>
public static class ErrorClassifier
{
    // Retryable serializable/optimistic conflict codes surfaced by the server.
    private const string LockConflict = "CADB0502";
    private const string MustRetry = "CADB0504";
    private const string LifetimeExceeded = "CADB0505";

    /// <summary>
    /// Classifies a write-path failure with knowledge of whether the commit request had already been
    /// submitted. A <see cref="CamusException"/> that carries a server error code is a server verdict —
    /// the server answered, so the transaction definitely aborted (conflict/domain error) even when the
    /// answer arrived during the commit round trip. Any other failure after commit submission
    /// (cancellation, transport drop, timeout, unexpected client fault) means no verdict was received:
    /// the commit may have durably applied before the failure, so the outcome is
    /// <see cref="OperationStatus.Indeterminate"/> rather than a definite abort. Failures before commit
    /// submission keep their ordinary classification — no commit was requested, so nothing can have
    /// landed.
    ///
    /// <para>The gRPC client does not always turn a transport failure into a plain transport exception:
    /// a deadline that expires, or a node that goes away, during the commit round trip can surface as a
    /// <see cref="CamusException"/> whose <see cref="CamusException.Code"/> is empty. That is not a
    /// verdict — nothing answered — so it must widen the ambiguity, not close it. Reading it as a
    /// definite abort tells the per-row attribution that the transfer applied nothing, and a commit that
    /// did land then looks exactly like a leaked write; the check would report the harness's own
    /// misreading as an engine defect.</para>
    /// </summary>
    public static (OperationStatus Status, string Code) Classify(Exception ex, bool commitSubmitted)
    {
        (OperationStatus status, string code) = Classify(ex);
        if (!commitSubmitted)
            return (status, code);
        if (ex is not CamusException camus || CarriesNoVerdict(camus))
            return (OperationStatus.Indeterminate, code);
        return (status, code);
    }

    // Server codes that, on a submitted commit, report that the outcome is UNAVAILABLE rather than a
    // decision. CADB0501 (TransactionAlreadyCompleted) is raised when the coordinator session is gone and
    // the durable outcome could not be read back; CADB0509 (TransactionFinalizeUnresolved) is the
    // commit-outcome-not-yet-known signal by construction. A transaction reported under either can have
    // committed durably — treating it as a definite non-commit tells the per-row attribution the transfer
    // applied nothing, and a commit that did land then reads as a leaked write. All must widen the
    // ambiguity, not close it.
    private const string TransactionAlreadyCompleted = "CADB0501";
    private const string FinalizeUnresolved = "CADB0509";

    // CADB0000 is not a verdict either: the server stamps it on any UNEXPECTED exception that escaped
    // to the RPC boundary, and the gRPC client fabricates the same code for a trailer-less transport
    // failure (a node that died mid-call). Neither shape says anything about the transaction's outcome.
    // In the split-nemesis fault soak, transfers whose commit had durably applied were answered CADB0000
    // when the coordinator's node was SIGKILLed, and counting the code as a definite abort reported the
    // engine's committed writes as leaks.
    private const string InternalUnmapped = "CADB0000";

    /// <summary>
    /// True when a <see cref="CamusException"/> reports that the commit outcome is unavailable rather than a
    /// server decision. Three shapes qualify: a transport failure that came back with no error code and a
    /// message naming a gRPC transport condition (deadline / unavailable / cancelled); an explicit
    /// "outcome unavailable / unresolved" server code (CADB0501 / CADB0509), whose own contract is that the
    /// commit may already have committed; and the generic internal code (CADB0000), which marks an
    /// unexpected exception or a fabricated transport verdict, never a transaction decision. A plain empty
    /// code alone is not enough for the transport case — real server errors whose code the wire dropped did
    /// carry a verdict — which is why the codeless branch also requires the transport-condition message.
    /// </summary>
    private static bool CarriesNoVerdict(CamusException camus)
    {
        if (camus.Code is TransactionAlreadyCompleted or FinalizeUnresolved or InternalUnmapped)
            return true;

        if (!string.IsNullOrEmpty(camus.Code))
            return false;

        string message = camus.Message;
        return message.Contains("DeadlineExceeded", StringComparison.OrdinalIgnoreCase)
            || message.Contains("deadline", StringComparison.OrdinalIgnoreCase)
            || message.Contains("Unavailable", StringComparison.OrdinalIgnoreCase)
            || message.Contains("Cancelled", StringComparison.OrdinalIgnoreCase)
            || message.Contains("Canceled", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// A client exception that reports a transport failure rather than a server verdict: the
    /// connection could not be established or was torn down.
    ///
    /// <para>Only consulted when the exception carries <b>no server error code</b>, the same guard
    /// <see cref="CarriesNoVerdict"/> uses. A coded exception is the server's answer and is never
    /// reinterpreted from its message text.</para>
    ///
    /// <para>This matters beyond bookkeeping. A transport failure is retryable and a domain error is
    /// not, so misclassifying one ended a post-run reconciliation after six seconds of a
    /// ten-minute budget — reporting "could not verify" for a cluster that was merely restarting a
    /// killed node. It also inflates the domain-error count under a fault, burying real domain errors
    /// among tens of thousands of connection failures.</para>
    /// </summary>
    private static bool IsTransportFailure(CamusException camus)
    {
        if (!string.IsNullOrEmpty(camus.Code))
            return false;

        string message = camus.Message;
        return message.Contains("subchannel", StringComparison.OrdinalIgnoreCase)
            || message.Contains("Error connecting", StringComparison.OrdinalIgnoreCase)
            || message.Contains("Connection refused", StringComparison.OrdinalIgnoreCase)
            || message.Contains("Unavailable", StringComparison.OrdinalIgnoreCase)
            || message.Contains("DeadlineExceeded", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Whether an <b>idempotent read</b> should be retried after this exception.
    ///
    /// <para>Deliberately more permissive than <see cref="Classify"/>, and used only by post-run
    /// reconciliation. Reconciliation issues <c>SUM</c>, <c>COUNT</c> and a row scan; re-running one
    /// cannot change the database, so the caution that governs the write path does not apply. The
    /// measured run keeps <see cref="Classify"/> untouched.</para>
    ///
    /// <para>The case that forced this: a query against a node still restarting from an injected kill
    /// fails as <c>Error connecting to subchannel</c> carrying code <c>CADB0000</c>. That code is a
    /// domain error for a write and must stay one — the server stamps it on genuine unexpected
    /// exceptions too — but for a read it is worth another attempt. Classifying it by message alone
    /// inside <see cref="Classify"/> was tried and rejected: it would reinterpret a coded server
    /// verdict from its text, and it would disturb the ambiguity band that the loss detection depends
    /// on.</para>
    ///
    /// <para>The retry budget remains the bound. A permanently broken cluster still costs the full
    /// budget and still reports "could not verify" — honest, where abandoning after zero seconds and
    /// reporting the same thing was not.</para>
    /// </summary>
    public static bool IsRetryableForIdempotentRead(Exception ex)
    {
        if (Classify(ex).Status is OperationStatus.Conflict or OperationStatus.Transient)
            return true;

        // Transport shape, whatever code rode along with it.
        string message = ex.Message;
        return message.Contains("subchannel", StringComparison.OrdinalIgnoreCase)
            || message.Contains("Error connecting", StringComparison.OrdinalIgnoreCase)
            || message.Contains("Connection refused", StringComparison.OrdinalIgnoreCase)
            || message.Contains("Unavailable", StringComparison.OrdinalIgnoreCase)
            || message.Contains("DeadlineExceeded", StringComparison.OrdinalIgnoreCase);
    }

    public static (OperationStatus Status, string Code) Classify(Exception ex)
    {
        if (ex is CamusException camus)
        {
            string code = string.IsNullOrEmpty(camus.Code) ? "CADB_UNKNOWN" : camus.Code;
            if (code is LockConflict or MustRetry or LifetimeExceeded || SerializableRetryHelper.IsRetryable(camus))
                return (OperationStatus.Conflict, code);
            if (IsTransportFailure(camus))
                return (OperationStatus.Transient, code);
            return (OperationStatus.DomainError, code);
        }

        if (ex is OperationCanceledException)
            return (OperationStatus.Transient, "CANCELED");

        if (ex is IOException or TimeoutException or System.Net.Sockets.SocketException)
            return (OperationStatus.Transient, ex.GetType().Name);

        return (OperationStatus.InternalError, ex.GetType().Name);
    }
}
