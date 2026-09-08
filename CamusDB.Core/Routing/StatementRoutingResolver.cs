
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Core.Routing;

/// <summary>
/// Turns a statement's recorded footprint (<see cref="StatementRoutingCollector"/>) into the
/// advisory routing metadata a transport attaches to a successful response.
///
/// <para><b>Local and side-effect-free.</b> Resolution reads this node's own placement snapshot —
/// no network discovery, no KV read, no waiting for an election — because a routing lookup runs on
/// the response path of an operation that already committed, where any new failure mode would be
/// worse than stale advice. Callers must still guard the call: a failure here must never turn a
/// committed success into an error.</para>
///
/// <para><b>Hash-only, conservatively.</b> Advice is emitted only when the whole deployment runs
/// hash placement (<c>KeyRangeShardingEnabled</c> off). Under hash routing every key space of a
/// table shares one placement group — the grouped layout <see cref="KvKeyBuilder"/> documents — so
/// one lookup of the row space covers the index spaces too, and the answer is independent of
/// parameter values. Under key-range routing a statement's destination depends on the parameter,
/// so parameter-independent advice would be wrong by construction; the deployment-wide gate keeps
/// that entire regime out of the first version.</para>
///
/// <para><b>The initialized flag is respected.</b> <see cref="EmbeddedKahuna.GetPlacement"/>
/// discards the range-map initialized bit, so this resolver goes through
/// <see cref="EmbeddedKahuna.ReadPlacementUncached"/> and keeps its own short-TTL bounded cache
/// instead. Startup uncertainty is answered with a clear disposition, never presented as a known
/// hash placement.</para>
///
/// <para>Emission enablement and the advertised TTL are latched at construction
/// (Restart/Node — see <see cref="CamusDBOptions.SqlRoutingAdviceEnabled"/>).</para>
/// </summary>
public sealed class StatementRoutingResolver
{
    /// <summary>
    /// Mirrors the planner placement cache TTL: placement is advisory on both paths, and a longer
    /// TTL here would only make routing advice staler than the planner's own view.
    /// </summary>
    private const long PlacementCacheTtlMs = 2_000;

    /// <summary>
    /// Hard bound on cached key spaces. The cache holds one entry per distinct advised table, so
    /// this is far above any realistic count; on overflow the whole cache is dropped — it refills
    /// within one TTL and correctness never depended on it.
    /// </summary>
    private const int PlacementCacheMaxEntries = 4_096;

    private readonly record struct CachedPlacement(TablePlacement Placement, bool Initialized, long CapturedAtTicks);

    private readonly ConcurrentDictionary<string, CachedPlacement> placementCache = new();

    private readonly bool enabled;

    private readonly int maxAgeMs;

    public StatementRoutingResolver(CamusDBOptions options)
    {
        enabled = options.SqlRoutingAdviceEnabled;
        maxAgeMs = options.SqlRoutingAdviceTtlMs;
    }

    /// <summary>
    /// True when this node emits routing metadata at all. Transports check it before allocating a
    /// collector so a disabled node pays nothing per statement.
    /// </summary>
    public bool EmissionEnabled => enabled;

    /// <summary>
    /// Resolves the advice for one finished statement, or null when no metadata should be emitted:
    /// emission disabled, standalone node, no database context, or a statement kind no record site
    /// classified. Never throws for a placement it cannot answer — that is the clear disposition.
    /// </summary>
    public StatementRoutingAdvice? Resolve(DatabaseDescriptor? database, StatementRoutingCollector collector)
    {
        if (!enabled || database is null || collector.IsUntouched)
            return null;

        // A standalone node has exactly one destination; advice would tell the client nothing.
        if (!database.Kahuna.IsClusterMode)
            return null;

        string? ineligible = collector.IneligibleReason;
        if (ineligible is not null)
            return Clear(ineligible);

        StatementRoutingCandidate? candidate = collector.Candidate;
        if (candidate is null)
            return null;

        // Deployment-wide gate: any key-range routing in the process makes parameter-independent
        // reuse unsound for a ranged access path, so the whole regime is excluded.
        if (database.Options.KeyRangeShardingEnabled)
            return Clear(StatementRoutingAdvice.ReasonIneligible);

        (TablePlacement placement, bool initialized) = LookupPlacement(database.Kahuna, candidate.RowKeySpace);

        if (!initialized)
            return Clear(StatementRoutingAdvice.ReasonPlacementUnknown);

        if (placement.IsKeyRange)
            return Clear(StatementRoutingAdvice.ReasonIneligible);

        if (placement.Spans.Count != 1 || placement.Spans[0].LeaderEndpoint is null)
            return Clear(StatementRoutingAdvice.ReasonPlacementUnknown);

        return new StatementRoutingAdvice(
            StatementRoutingDisposition.Prefer,
            PreferredNodeId: placement.Spans[0].LeaderEndpoint,
            DependencyToken: BuildDependencyToken(database.Id, candidate),
            MaxAgeMs: maxAgeMs,
            Reason: StatementRoutingAdvice.ReasonSingleTableHash);
    }

    private static StatementRoutingAdvice Clear(string reason) => new(
        StatementRoutingDisposition.Clear,
        PreferredNodeId: null,
        DependencyToken: null,
        MaxAgeMs: 0,
        Reason: reason);

    /// <summary>
    /// A short-TTL cached read of the uncached placement API, keeping the initialized bit the
    /// planner cache drops. Serving a snapshot up to two seconds old is fine — stale advice costs
    /// one ordinary forwarded request and then expires — but a snapshot that hides "unknown"
    /// behind a hash fallback would keep advertising a leader this node cannot actually name.
    /// </summary>
    private (TablePlacement Placement, bool Initialized) LookupPlacement(EmbeddedKahuna kahuna, string keySpace)
    {
        long now = Environment.TickCount64;

        if (placementCache.TryGetValue(keySpace, out CachedPlacement cached)
            && now - cached.CapturedAtTicks < PlacementCacheTtlMs)
            return (cached.Placement, cached.Initialized);

        TablePlacement placement = kahuna.ReadPlacementUncached(keySpace, out bool initialized);

        if (placementCache.Count >= PlacementCacheMaxEntries)
            placementCache.Clear();
        placementCache[keySpace] = new CachedPlacement(placement, initialized, now);

        return (placement, initialized);
    }

    /// <summary>
    /// The opaque change detector over the statement's resolved dependencies. Composed of the
    /// database identity (changes on drop/recreate), the relation identity, the effective storage
    /// identity plus contents generation (change on TRUNCATE and materialized-view refresh), and
    /// the schema version (changes on ALTER) — then hashed, so the client cannot be tempted to
    /// parse it and the raw identifiers never travel. Deterministic across nodes on purpose: the
    /// same dependencies must yield the same token whichever node answers, or a client alternating
    /// gateways would see a phantom dependency change on every hop.
    /// </summary>
    private static string BuildDependencyToken(string databaseId, StatementRoutingCandidate candidate)
    {
        string composed = string.Concat(
            databaseId, "|", candidate.TableId, "|", candidate.StorageId, "|",
            candidate.ContentsGeneration.ToString(), "|", candidate.SchemaVersion.ToString());

        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(Encoding.UTF8.GetBytes(composed), hash);
        return Convert.ToHexString(hash[..12]);
    }
}
