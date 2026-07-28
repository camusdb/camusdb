
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Core;
using CamusDB.Core;

namespace CamusDB.App.Grpc;

/// <summary>
/// Maps <see cref="CamusDBException"/> domain codes to gRPC <see cref="StatusCode"/> values and
/// surfaces the CamusDB error code + message in trailing metadata so callers can distinguish the
/// full retryable family:
///   ABORTED + CADB0502/0504/0505 → replay from a fresh BEGIN.
///   ABORTED + CADB0509           → retry the SAME commit/rollback on the SAME handle.
/// No stack trace is ever sent over the wire.
/// </summary>
public static class GrpcErrorMapper
{
    private const string MetaCode    = "camus-error-code";
    private const string MetaMessage = "camus-error-message";

    /// <summary>
    /// Converts a <see cref="CamusDBException"/> to an <see cref="RpcException"/> whose
    /// <see cref="StatusCode"/> mirrors the HTTP-status mapping and whose trailing metadata
    /// carries the domain <paramref name="code"/> and <paramref name="message"/>.
    /// </summary>
    public static RpcException ToRpcException(CamusDBException ex)
    {
        StatusCode grpcStatus = GetGrpcStatus(ex.Code);
        Metadata trailers = BuildTrailers(ex.Code, ex.Message);
        return new RpcException(new Status(grpcStatus, ex.Message), trailers);
    }

    /// <summary>
    /// Wraps an unexpected (non-<see cref="CamusDBException"/>) exception as an
    /// <see cref="StatusCode.Internal"/> RPC error without leaking a stack trace.
    /// </summary>
    public static RpcException ToRpcException(Exception ex)
    {
        Metadata trailers = BuildTrailers("CADB0000", ex.Message);
        return new RpcException(new Status(StatusCode.Internal, "Internal server error"), trailers);
    }

    // ─── Status mapping ───────────────────────────────────────────────────────

    /// <summary>
    /// Maps a CamusDB error code to the appropriate gRPC <see cref="StatusCode"/>.
    /// The retryable family (ABORTED) is intentionally broad; the <c>camus-error-code</c> trailer
    /// disambiguates replay-from-BEGIN vs retry-same-finalize for callers that need to distinguish.
    /// </summary>
    public static StatusCode GetGrpcStatus(string code) => code switch
    {
        // Client input errors → INVALID_ARGUMENT
        CamusDBErrorCodes.InvalidInput     => StatusCode.InvalidArgument,
        CamusDBErrorCodes.SqlSyntaxError   => StatusCode.InvalidArgument,
        CamusDBErrorCodes.InvalidAstStmt   => StatusCode.InvalidArgument,
        CamusDBErrorCodes.UnknownColumn    => StatusCode.InvalidArgument,
        CamusDBErrorCodes.UnknownType      => StatusCode.InvalidArgument,
        CamusDBErrorCodes.ValueTooLong     => StatusCode.InvalidArgument,
        CamusDBErrorCodes.SchemaLimitExceeded => StatusCode.InvalidArgument,
        CamusDBErrorCodes.NotNullViolation => StatusCode.InvalidArgument,
        CamusDBErrorCodes.CheckConstraintViolation => StatusCode.InvalidArgument,

        // Not found
        CamusDBErrorCodes.DatabaseDoesntExist => StatusCode.NotFound,
        CamusDBErrorCodes.TableDoesntExist    => StatusCode.NotFound,
        CamusDBErrorCodes.IndexDoesntExist    => StatusCode.NotFound,
        CamusDBErrorCodes.UnknownKey          => StatusCode.NotFound,
        CamusDBErrorCodes.OrphanNotFound      => StatusCode.NotFound,

        // Already exists
        CamusDBErrorCodes.DuplicateUniqueKeyValue => StatusCode.AlreadyExists,
        CamusDBErrorCodes.DuplicatePrimaryKey     => StatusCode.AlreadyExists,
        CamusDBErrorCodes.DatabaseAlreadyExists   => StatusCode.AlreadyExists,
        CamusDBErrorCodes.TableAlreadyExists      => StatusCode.AlreadyExists,

        // Failed precondition (non-retryable transaction state)
        CamusDBErrorCodes.TransactionAlreadyCompleted => StatusCode.FailedPrecondition,
        CamusDBErrorCodes.DatabaseHasLiveDescendants  => StatusCode.FailedPrecondition,

        // Retryable transaction conflicts → ABORTED (trailer code disambiguates)
        CamusDBErrorCodes.TransactionConflict        => StatusCode.Aborted,
        CamusDBErrorCodes.TransactionMustRetry       => StatusCode.Aborted,
        CamusDBErrorCodes.TransactionLifetimeExceeded => StatusCode.Aborted,
        CamusDBErrorCodes.TransactionFinalizeUnresolved => StatusCode.Aborted,

        // Resource exhaustion (permanent for this operation)
        CamusDBErrorCodes.TransactionMutationLimitExceeded => StatusCode.ResourceExhausted,
        CamusDBErrorCodes.SpillStorageUnavailable           => StatusCode.ResourceExhausted,
        CamusDBErrorCodes.TooManyAuthAttempts               => StatusCode.ResourceExhausted,

        // Authentication / authorization
        CamusDBErrorCodes.AuthenticationFailed   => StatusCode.Unauthenticated,
        CamusDBErrorCodes.InsufficientPrivilege  => StatusCode.PermissionDenied,

        _ => StatusCode.Internal,
    };

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static Metadata BuildTrailers(string code, string message)
    {
        Metadata trailers = new();
        trailers.Add(MetaCode, code);
        trailers.Add(MetaMessage, message);
        return trailers;
    }
}
