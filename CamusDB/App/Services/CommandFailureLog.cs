
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Core;

using CamusDB.Core;
using CamusDB.App.Grpc;

namespace CamusDB.App.Services;

/// <summary>
/// Chooses the log level for a failed request from who caused the failure, so every transport
/// (REST, gRPC) classifies the same error the same way.
///
/// <para>An error the caller can fix — bad input, an unknown object, a denied privilege, a quota the
/// caller is over — is logged as a warning; only a failure that maps to a 5xx (a genuine server-side
/// fault) is logged as an error, with its stack trace. Client mistakes are routine traffic on a
/// shared server, and reporting them at error level makes the operator's error stream a poor signal
/// of server health: one misbehaving client can flood it with failures no operator can act on.</para>
///
/// <para>The verdict reuses the two status mappings the transports already maintain rather than
/// introducing a third table that would drift from them: a code is caller-attributable if it maps to
/// a non-5xx HTTP status <em>or</em> to a gRPC status other than <c>Internal</c>. Both maps are
/// partial and fall back to "server fault", but they are partial in different places — the HTTP map
/// classifies the backup/view/quota codes, the gRPC map classifies the input, not-found, conflict and
/// retryable-abort codes — so the union classifies far more of the surface than either alone, and an
/// unmapped code keeps the conservative error level.</para>
/// </summary>
public static class CommandFailureLog
{
    public static void LogFailure(ILogger logger, CamusDBException e)
    {
        if (IsCallerError(e.Code))
            logger.LogWarning("{Name}: {Code}: {Message}", e.GetType().Name, e.Code, e.Message);
        else
            logger.LogError("{Name}: {Code}: {Message}\n{StackTrace}", e.GetType().Name, e.Code, e.Message, e.StackTrace);
    }

    /// <summary>
    /// Whether <paramref name="code"/> describes something the caller did, as opposed to a fault of
    /// the server. Retryable transaction conflicts count as caller-attributable: they are the normal
    /// outcome of contention under serializable isolation and the client's job is to replay them.
    /// </summary>
    public static bool IsCallerError(string code)
    {
        // Spill storage failing is a disk-space or permission problem on this node: it reaches the
        // caller as a resource-exhaustion status, but only an operator can fix it, so it stays loud.
        if (code == CamusDBErrorCodes.SpillStorageUnavailable)
            return false;

        return CamusDBErrorCodes.GetHttpStatus(code) < 500
            || GrpcErrorMapper.GetGrpcStatus(code) != StatusCode.Internal;
    }
}
