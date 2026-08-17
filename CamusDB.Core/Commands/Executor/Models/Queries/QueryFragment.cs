
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Everything a peer node needs to execute one span-scan fragment: which table to scan, the
/// span's row-id bounds, the snapshot timestamp to read at, and the residual filter to apply
/// remotely. Deliberately narrow — not SQL re-parse, not physical-plan serialization. The
/// executing node opens the database and table through its own descriptors and verifies the
/// ids and schema version it was handed; any mismatch fails closed with a retryable error and
/// the coordinator falls back to scanning the span locally.
///
/// <para>The fragment returns the <b>raw row bytes</b> of rows that pass the filter (the
/// coordinator re-decodes survivors exactly as it decodes local spans). Shipping bytes rather
/// than decoded values keeps schema-history decode entirely on the coordinator and defers a
/// value wire-codec until partial aggregation needs one; the win shipped here is that
/// filtered-out rows never cross the wire and decode/filter CPU runs at the data.</para>
/// </summary>
public sealed class QueryFragmentRequest
{
    /// <summary>Bumped when the fragment contract changes; receivers reject unknown versions.</summary>
    public const int CurrentFormatVersion = 1;

    public int FormatVersion { get; set; } = CurrentFormatVersion;

    /// <summary>Coordinator-minted id for logging/cancellation correlation.</summary>
    public string FragmentId { get; set; } = "";

    /// <summary>Resolution key (databases open by name) — verified against <see cref="DatabaseId"/>.</summary>
    public string DatabaseName { get; set; } = "";

    /// <summary>The immutable id the coordinator resolved; a mismatch (rename race) fails closed.</summary>
    public string DatabaseId { get; set; } = "";

    public string TableName { get; set; } = "";

    /// <summary>The immutable table id the coordinator resolved; a mismatch fails closed.</summary>
    public string TableId { get; set; } = "";

    /// <summary>
    /// The coordinator's table schema version. The executing node refuses when its own version
    /// differs — filter columns and decode narrowing were chosen against this version, and a
    /// lagging or leading peer must not guess.
    /// </summary>
    public int SchemaVersion { get; set; }

    /// <summary>Span bounds: 24-hex row ids; null = unbounded. Start inclusive, until exclusive.</summary>
    public string? FromRowIdHex { get; set; }

    public string? UntilRowIdHex { get; set; }

    /// <summary>Snapshot HLC timestamp (node id / physical / counter) every read runs at.</summary>
    public int ReadTsNode { get; set; }

    public long ReadTsPhysical { get; set; }

    public uint ReadTsCounter { get; set; }

    /// <summary>Per-span cap on rows SCANNED (the coordinator's pushed-down scan limit), null = unlimited.</summary>
    public long? MaxRows { get; set; }

    /// <summary>
    /// Per-span cap on rows that SURVIVE the filter (the coordinator's <c>limit + offset</c>);
    /// the fragment stops scanning once this many survivors shipped. Null = unlimited.
    /// </summary>
    public long? MaxSurvivors { get; set; }

    /// <summary>
    /// Residual WHERE filter as a serialized <c>NodeAst</c> (see <c>NodeAstWireCodec</c>).
    /// Required for row fragments: an unfiltered span is served just as well by locator-routed
    /// pages, so the coordinator scans it locally. Optional for aggregate fragments
    /// (<see cref="AggregateJson"/> set): a partial COUNT over an unfiltered span still ships
    /// one row instead of the span.
    /// </summary>
    public string FilterJson { get; set; } = "";

    /// <summary>
    /// When set: the fragment computes partial aggregates instead of shipping rows. Each
    /// element is one serialized projection (aliased aggregate calls and, for grouped
    /// partials, bare group-key identifiers); the remote runs the engine's own aggregator
    /// over its span's filter-survivors and streams one value row per group
    /// (<see cref="QueryFragmentRow.CellsJson"/>) — exactly one row for global aggregates.
    /// </summary>
    public string[]? AggregatesJson { get; set; }

    /// <summary>Serialized fragment-side GROUP BY expressions; null for global aggregates.</summary>
    public string[]? GroupByJson { get; set; }

    /// <summary>Columns the remote must decode (filter columns included); null = all columns.</summary>
    public string[]? RequiredColumns { get; set; }
}

/// <summary>
/// One fragment result frame. Row fragments carry <see cref="RowIdHex"/> + <see cref="Data"/>
/// (raw KV row bytes; the coordinator re-decodes through the same path as local spans).
/// Aggregate fragments carry <see cref="CellsJson"/> — a JSON object of output-name →
/// wire-encoded <c>ColumnValue</c> (see <c>ColumnValueWireCodec</c>) holding the span's
/// partial aggregate states.
/// </summary>
public sealed record QueryFragmentRow(string? RowIdHex, byte[]? Data, string? CellsJson = null);
