
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Routing;

/// <summary>
/// Whether routing advice asks the client to remember or to forget a destination for a
/// statement context. See <see cref="StatementRoutingAdvice"/>.
/// </summary>
public enum StatementRoutingDisposition
{
    /// <summary>Remember the advertised node for future executions of this statement context.</summary>
    Prefer = 0,

    /// <summary>Forget any previously learned destination for this statement context.</summary>
    Clear = 1,
}

/// <summary>
/// Advisory routing metadata for one successful statement execution. The transport attaches it to
/// the response when the client negotiated routing metadata.
///
/// <para><b>This is advice, never an instruction.</b> The operation that produced it already
/// executed normally on this node. A client that ignores it loses only a network hop. It carries
/// no SQL literals, no row values, no encoded keys, no credentials, and no cluster map, and it is
/// built only after the statement passed its normal authorization checks.</para>
///
/// <para><b>Provenance is a placement belief.</b> The preferred node comes from this node's local
/// placement view (see <see cref="StatementRoutingResolver"/>), not from proof of which node
/// executed the underlying storage operations. That is why <see cref="MaxAgeMs"/> bounds reuse
/// and why the wire contract names the provenance "placementHint".</para>
/// </summary>
public sealed record StatementRoutingAdvice(
    StatementRoutingDisposition Disposition,
    string? PreferredNodeId,
    string? DependencyToken,
    int MaxAgeMs,
    string Reason)
{
    /// <summary>The only metadata contract version this server emits.</summary>
    public const int WireVersion = 1;

    /// <summary>The provenance label for advice built from a local placement snapshot.</summary>
    public const string ProvenancePlacementHint = "placementHint";

    /// <summary>Reason: a single ordinary hash-routed table backs the statement.</summary>
    public const string ReasonSingleTableHash = "singleTableHash";

    /// <summary>Reason: the statement's shape or footprint is outside the supported set.</summary>
    public const string ReasonIneligible = "ineligible";

    /// <summary>Reason: this node cannot currently name a leader for the statement's data.</summary>
    public const string ReasonPlacementUnknown = "placementUnknown";

    /// <summary>Reason: a query-result-cache hint took the statement onto the cache path.</summary>
    public const string ReasonCacheAffinity = "cacheAffinity";

    /// <summary>
    /// True when the advice tells the client to reuse <see cref="PreferredNodeId"/> across
    /// parameter values. Only a <see cref="StatementRoutingDisposition.Prefer"/> disposition
    /// carries the parameter-independent reuse scope; the clear disposition carries none.
    /// </summary>
    public bool ParametersIndependent => Disposition == StatementRoutingDisposition.Prefer;
}
