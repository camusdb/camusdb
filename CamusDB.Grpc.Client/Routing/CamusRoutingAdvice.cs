
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Grpc.Client.Routing;

/// <summary>Wire-level routing constants shared by the connection's request and learn paths.</summary>
internal static class RoutingWire
{
    /// <summary>The only metadata version this client accepts and requests.</summary>
    public const int AcceptVersion = 1;
}

/// <summary>Client-side view of a response's routing disposition.</summary>
public enum CamusRoutingDisposition
{
    /// <summary>The server sent a disposition this client version does not know. Ignore the advice.</summary>
    Unknown = 0,

    /// <summary>Remember the advertised node for future executions of this statement context.</summary>
    Prefer = 1,

    /// <summary>Forget any previously learned destination for this statement context.</summary>
    Clear = 2,
}

/// <summary>
/// The routing advice one response carried, decoupled from the wire message so callers of
/// <see cref="Batching.QueryResult"/> / <see cref="Batching.NonQueryResult"/> can inspect it
/// without depending on generated protobuf types. Advisory only: the connection has already
/// applied it to its route cache by the time a caller sees it, so this exists for diagnostics
/// and tests, not as something an application must act on.
/// </summary>
public sealed record CamusRoutingAdvice(
    int Version,
    CamusRoutingDisposition Disposition,
    string? PreferredNodeId,
    bool ParametersIndependentScope,
    string? DependencyToken,
    int MaxAgeMs,
    string? Reason)
{
    /// <summary>
    /// Maps the wire message, absent-in → null-out. An unrecognized disposition or reuse scope is
    /// preserved as <see cref="CamusRoutingDisposition.Unknown"/> / a false scope flag rather than
    /// guessed at, so the learner ignores what it does not understand.
    /// </summary>
    internal static CamusRoutingAdvice? From(RoutingAdvice? wire)
    {
        if (wire is null)
            return null;

        CamusRoutingDisposition disposition = wire.Disposition switch
        {
            RoutingDisposition.Prefer => CamusRoutingDisposition.Prefer,
            RoutingDisposition.Clear => CamusRoutingDisposition.Clear,
            _ => CamusRoutingDisposition.Unknown,
        };

        return new CamusRoutingAdvice(
            Version: wire.Version,
            Disposition: disposition,
            PreferredNodeId: wire.PreferredNodeId.Length > 0 ? wire.PreferredNodeId : null,
            ParametersIndependentScope: wire.ReuseScope == RoutingReuseScope.StatementParametersIndependent,
            DependencyToken: wire.DependencyToken.Length > 0 ? wire.DependencyToken : null,
            MaxAgeMs: wire.MaxAgeMs,
            Reason: wire.Reason.Length > 0 ? wire.Reason : null);
    }
}
