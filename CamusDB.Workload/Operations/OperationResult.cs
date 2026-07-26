/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Operations;

/// <summary>Coarse, bounded outcome classes for one operation — safe to use as a metric dimension.</summary>
public enum OperationStatus
{
    Ok,

    /// <summary>
    /// A retryable serializable/optimistic conflict (e.g. CADB0502/0504/0505). In the non-conflicting
    /// baseline this must never happen — its presence invalidates the run rather than being retried.
    /// </summary>
    Conflict,

    /// <summary>A transport-transient failure (stream drop, unavailable) — reported separately from conflicts.</summary>
    Transient,

    /// <summary>A domain error surfaced by the server (a CADBxxxx that is not a conflict/transient class).</summary>
    DomainError,

    /// <summary>Anything else (unexpected client/runtime failure).</summary>
    InternalError,
}

/// <summary>
/// The result of one submitted operation. Latency fields are filled by the operation itself so a
/// write can report its begin/read/update/commit sub-stage split; the whole-operation latency is
/// timed by the scheduler around <c>ExecuteAsync</c>. <see cref="ErrorCode"/> is a stable CADBxxxx
/// (or a short synthetic class) — never a raw message — so error aggregation stays bounded.
/// </summary>
public readonly record struct OperationResult(
    OperationKind Kind,
    OperationStatus Status,
    string? ErrorCode,
    double BeginMs,
    double ReadMs,
    double UpdateMs,
    double CommitMs)
{
    public bool IsSuccess => Status == OperationStatus.Ok;

    public static OperationResult ReadOk() => new(OperationKind.Read, OperationStatus.Ok, null, 0, 0, 0, 0);

    public static OperationResult Failure(OperationKind kind, OperationStatus status, string? code)
        => new(kind, status, code, 0, 0, 0, 0);
}
