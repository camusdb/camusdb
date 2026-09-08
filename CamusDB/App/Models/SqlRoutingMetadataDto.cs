
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Routing;

namespace CamusDB.App.Models;

/// <summary>
/// Advisory routing metadata on a successful SQL response, mirroring the gRPC
/// <c>RoutingAdvice</c> message field-for-field. Present only when the request negotiated it
/// (<c>routingAcceptVersion = 1</c>) and the statement produced advice; the property is omitted
/// from the JSON entirely otherwise, so pre-routing clients see their exact response shape.
/// </summary>
public sealed class SqlRoutingMetadataDto
{
    /// <summary>Metadata contract version; today always 1. Ignore unknown versions.</summary>
    public int Version { get; init; }

    /// <summary>"prefer" to remember the destination, "clear" to forget it.</summary>
    public string Disposition { get; init; } = "";

    /// <summary>
    /// Opaque node identity to prefer. The client maps it to a configured endpoint address; it is
    /// never an address to dial directly. Null on a clear disposition.
    /// </summary>
    public string? PreferredNodeId { get; init; }

    /// <summary>
    /// Reuse scope of a prefer disposition. "statementParametersIndependent" is the only value
    /// this server emits; a client must not treat an unknown scope as parameter-independent.
    /// Null on a clear disposition.
    /// </summary>
    public string? ReuseScope { get; init; }

    /// <summary>Opaque change detector over the statement's resolved dependencies. Not sortable.</summary>
    public string? DependencyToken { get; init; }

    /// <summary>Maximum reuse period, measured by the client with a monotonic clock from receipt.</summary>
    public int MaxAgeMs { get; init; }

    /// <summary>How the server knows — "placementHint": this node's local placement belief.</summary>
    public string Provenance { get; init; } = "";

    /// <summary>"singleTableHash", "ineligible", "placementUnknown", or "cacheAffinity".</summary>
    public string Reason { get; init; } = "";

    /// <summary>Maps engine advice to the wire DTO. Null in, null out.</summary>
    public static SqlRoutingMetadataDto? From(StatementRoutingAdvice? advice)
    {
        if (advice is null)
            return null;

        return new SqlRoutingMetadataDto
        {
            Version = StatementRoutingAdvice.WireVersion,
            Disposition = advice.Disposition == StatementRoutingDisposition.Prefer ? "prefer" : "clear",
            PreferredNodeId = advice.PreferredNodeId,
            ReuseScope = advice.ParametersIndependent ? "statementParametersIndependent" : null,
            DependencyToken = advice.DependencyToken,
            MaxAgeMs = advice.MaxAgeMs,
            Provenance = StatementRoutingAdvice.ProvenancePlacementHint,
            Reason = advice.Reason,
        };
    }
}
