
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO.Hashing;
using System.Text;
using CamusDB.Core.Catalogs.Models;
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
/// <para><b>One partition for every parameter value.</b> Advice names one leader that every
/// parameter value of the statement reaches. Under hash routing every key space of a table shares
/// one placement group — the grouped layout <see cref="KvKeyBuilder"/> documents — so one lookup of
/// the row space covers the index spaces too. Under key-range routing each key space of the table has
/// its own ranges, and a statement's destination depends on its parameter once any space is divided.
/// So a key-range table gets advice only while its row space, every index space and its large-value
/// space are each one range, all on the row range's partition. That holds from creation, because
/// Kahuna seeds every space of one placement group on the same partition, until the first split.
/// Stale advice after a split costs one forwarded request, never a wrong answer, and the placement
/// cache below lets it expire within its TTL.</para>
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

    /// <summary>
    /// Stack buffer for the dependency-token input. Three short identifiers plus two fixed-width
    /// numbers stay far below it; an unusually long identifier falls back to a pooled array.
    /// </summary>
    private const int TokenStackBufferSize = 512;

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

        (TablePlacement placement, bool initialized) = LookupPlacement(database.Kahuna, candidate.RowKeySpace);

        if (!initialized)
            return Clear(StatementRoutingAdvice.ReasonPlacementUnknown);

        if (placement.IsKeyRange)
        {
            if (placement.Spans.Count != 1)
                return Clear(StatementRoutingAdvice.ReasonIneligible);

            string? companionVerdict = CheckCompanionSpaces(database.Kahuna, candidate, placement.Spans[0].PartitionId);
            if (companionVerdict is not null)
                return Clear(companionVerdict);
        }

        if (placement.Spans.Count != 1 || placement.Spans[0].LeaderEndpoint is null)
            return Clear(StatementRoutingAdvice.ReasonPlacementUnknown);

        return new StatementRoutingAdvice(
            StatementRoutingDisposition.Prefer,
            PreferredNodeId: placement.Spans[0].LeaderEndpoint,
            DependencyToken: BuildDependencyToken(database.Id, candidate),
            MaxAgeMs: maxAgeMs,
            Reason: placement.IsKeyRange ? StatementRoutingAdvice.ReasonSingleRangeTable : StatementRoutingAdvice.ReasonSingleTableHash);
    }

    /// <summary>
    /// For a key-range table, checks that every index space and the large-value space is one span on
    /// <paramref name="rowPartition"/>, and returns null when they all are. A space that is divided, or
    /// that sits on another partition — a split moved it, or an index that is not range-routed stays on
    /// the group's hash partition — makes the destination depend on the parameter, so the statement is
    /// ineligible. A space this node cannot place yet is answered as unknown, like the row space.
    /// </summary>
    private string? CheckCompanionSpaces(EmbeddedKahuna kahuna, StatementRoutingCandidate candidate, int rowPartition)
    {
        string? verdict = CheckSpace(kahuna, candidate.Store.LargeValueKeySpace, rowPartition);
        if (verdict is not null)
            return verdict;

        foreach (TableIndexSchema index in candidate.Indexes.Values)
        {
            verdict = CheckSpace(kahuna, candidate.Store.IndexKeySpace(index.KvId), rowPartition);
            if (verdict is not null)
                return verdict;
        }

        return null;
    }

    private string? CheckSpace(EmbeddedKahuna kahuna, string keySpace, int rowPartition)
    {
        (TablePlacement placement, bool initialized) = LookupPlacement(kahuna, keySpace);

        if (!initialized)
            return StatementRoutingAdvice.ReasonPlacementUnknown;

        return placement.Spans.Count == 1 && placement.Spans[0].PartitionId == rowPartition
            ? null
            : StatementRoutingAdvice.ReasonIneligible;
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
    /// parse it. Deterministic across nodes on purpose: the same dependencies must yield the same
    /// token whichever node answers, or a client alternating gateways would see a phantom
    /// dependency change on every hop — hence the fixed little-endian layout and unseeded hash.
    ///
    /// <para><b>Non-cryptographic on purpose.</b> The token only has to change when a dependency
    /// changes; it is not authentication and not a secret (the inputs are low-entropy identifiers
    /// that no hash could hide from a guesser). XxHash128 truncated to 96 bits keeps the
    /// accidental-collision odds negligible at a fraction of SHA-256's cost on the response
    /// path. The input is length-prefixed, so two distinct dependency sets never compose the same
    /// bytes whatever characters the identifiers hold.</para>
    /// </summary>
    private static string BuildDependencyToken(string databaseId, StatementRoutingCandidate candidate)
    {
        string tableId = candidate.TableId;
        string storageId = candidate.StorageId;

        // Upper bound, not exact: avoids a second UTF-8 scan of each identifier. Identifiers are a
        // few dozen characters, so the bound fits the stack buffer in practice.
        int maxLength = 3 * sizeof(int)
            + Encoding.UTF8.GetMaxByteCount(databaseId.Length)
            + Encoding.UTF8.GetMaxByteCount(tableId.Length)
            + Encoding.UTF8.GetMaxByteCount(storageId.Length)
            + sizeof(long) + sizeof(int);

        byte[]? rented = maxLength > TokenStackBufferSize ? ArrayPool<byte>.Shared.Rent(maxLength) : null;
        try
        {
            Span<byte> buffer = rented is not null ? rented : stackalloc byte[TokenStackBufferSize];

            int written = WriteLengthPrefixed(buffer, databaseId);
            written += WriteLengthPrefixed(buffer[written..], tableId);
            written += WriteLengthPrefixed(buffer[written..], storageId);

            BinaryPrimitives.WriteInt64LittleEndian(buffer[written..], candidate.ContentsGeneration);
            written += sizeof(long);
            BinaryPrimitives.WriteInt32LittleEndian(buffer[written..], candidate.SchemaVersion);
            written += sizeof(int);

            Span<byte> hash = stackalloc byte[16];
            XxHash128.Hash(buffer[..written], hash);
            return Convert.ToHexString(hash[..12]);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Writes <paramref name="value"/> as a little-endian <c>int</c> UTF-8 byte length followed by
    /// its UTF-8 bytes, and returns the number of bytes written. The caller sizes
    /// <paramref name="destination"/> from <see cref="Encoding.GetMaxByteCount"/>.
    /// </summary>
    private static int WriteLengthPrefixed(Span<byte> destination, string value)
    {
        int byteCount = Encoding.UTF8.GetBytes(value, destination[sizeof(int)..]);
        BinaryPrimitives.WriteInt32LittleEndian(destination, byteCount);
        return sizeof(int) + byteCount;
    }
}
