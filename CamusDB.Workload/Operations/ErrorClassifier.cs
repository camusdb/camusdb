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
