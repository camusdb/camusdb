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
    /// submitted. A <see cref="CamusException"/> is a server verdict — the server answered, so the
    /// transaction definitely aborted (conflict/domain error) even when the answer arrived during the
    /// commit round trip. Any other failure after commit submission (cancellation, transport drop,
    /// timeout, unexpected client fault) means no verdict was received: the commit may have durably
    /// applied before the failure, so the outcome is <see cref="OperationStatus.Indeterminate"/> rather
    /// than a definite abort. Failures before commit submission keep their ordinary classification —
    /// no commit was requested, so nothing can have landed.
    /// </summary>
    public static (OperationStatus Status, string Code) Classify(Exception ex, bool commitSubmitted)
    {
        (OperationStatus status, string code) = Classify(ex);
        if (commitSubmitted && ex is not CamusException)
            return (OperationStatus.Indeterminate, code);
        return (status, code);
    }

    public static (OperationStatus Status, string Code) Classify(Exception ex)
    {
        if (ex is CamusException camus)
        {
            string code = string.IsNullOrEmpty(camus.Code) ? "CADB_UNKNOWN" : camus.Code;
            if (code is LockConflict or MustRetry or LifetimeExceeded || SerializableRetryHelper.IsRetryable(camus))
                return (OperationStatus.Conflict, code);
            return (OperationStatus.DomainError, code);
        }

        if (ex is OperationCanceledException)
            return (OperationStatus.Transient, "CANCELED");

        if (ex is IOException or TimeoutException or System.Net.Sockets.SocketException)
            return (OperationStatus.Transient, ex.GetType().Name);

        return (OperationStatus.InternalError, ex.GetType().Name);
    }
}
