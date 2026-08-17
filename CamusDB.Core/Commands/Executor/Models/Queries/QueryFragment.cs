
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

    /// <summary>
    /// When true the executing node appends one terminal <see cref="QueryFragmentRow.Stats"/>
    /// frame after the last row, carrying the span's scan actuals for <c>EXPLAIN (ANALYZE)</c>.
    /// Opt-in on purpose: normal queries pay nothing, and a peer running an older build simply
    /// ignores the unknown JSON field and sends no frame — the coordinator then reports the
    /// span's remote actuals as unknown rather than failing. Not part of the format-version
    /// contract for exactly that reason.
    /// </summary>
    public bool WantStats { get; set; }

    /// <summary>
    /// When set: the fragment is a broadcast-join probe over this span (see
    /// <see cref="QueryFragmentJoinSpec"/>). Mutually exclusive with
    /// <see cref="AggregatesJson"/>; the top-level <see cref="FilterJson"/> stays empty in
    /// join mode (the probe filter travels inside the spec) so a peer on an older build —
    /// which would ignore this unknown field and misread the request as a plain row fragment
    /// — rejects it outright and the coordinator falls back to a local probe.
    /// </summary>
    public QueryFragmentJoinSpec? Join { get; set; }
}

/// <summary>
/// Everything a peer needs to run one span of a broadcast hash-join probe: the coordinator's
/// build rows (small by the broadcast threshold), the key columns for both sides, and the
/// predicates to evaluate at the data. The fragment scans its probe span, applies the probe
/// filter, hashes each survivor's key against a table rebuilt from <see cref="BuildRows"/>,
/// evaluates the full ON predicate on each candidate pair, and ships one frame per probe row
/// that matched: the probe row's raw bytes plus the <b>indices into <see cref="BuildRows"/></b>
/// of its matches (<see cref="QueryFragmentRow.MatchIndices"/>).
///
/// <para>Shipping indices rather than joined rows is the design's core: the coordinator merges
/// the probe row with its <i>own</i> in-memory build rows through the standard
/// <c>QueryRowMerger</c> path, so output is byte-identical to a fully local join; a probe row
/// ships once no matter how many matches it has; and each frame is group-atomic (a mid-stream
/// failure resumes locally after the last delivered probe row with no duplicated or lost
/// pairs). Equal-key build rows appear in <see cref="BuildRows"/> in the coordinator's bucket
/// order, so the remote's rebuilt buckets preserve match order exactly.</para>
/// </summary>
public sealed class QueryFragmentJoinSpec
{
    /// <summary>
    /// Which side of the hash join was built (and therefore shipped). False — the standard
    /// shape — means the build is the join's <b>right</b> source: build rows ship with bare
    /// column names, the probe rows are the left side and are qualified with
    /// <see cref="ProbeAlias"/> before key extraction and merging. True means the build is
    /// the <b>left</b> subtree: build rows ship already qualified, probe rows are the right
    /// source read bare (keys via <see cref="BuildKeyColumns"/>), and the merge qualifies the
    /// probe side. Both shapes must reproduce the coordinator's own probe exactly — same key
    /// columns, same merge argument order.
    /// </summary>
    public bool BuildIsLeft { get; set; }

    /// <summary>The probe table's effective alias (left alias normally; right alias when <see cref="BuildIsLeft"/>).</summary>
    public string ProbeAlias { get; set; } = "";

    /// <summary>Left-side key columns, qualified (<c>alias.column</c>) — read from qualified left rows.</summary>
    public string[] ProbeKeyColumns { get; set; } = [];

    /// <summary>The right source's effective alias, used to qualify bare right rows when merging.</summary>
    public string BuildAlias { get; set; } = "";

    /// <summary>Right-side key columns, unqualified — read from bare right rows.</summary>
    public string[] BuildKeyColumns { get; set; } = [];

    /// <summary>
    /// The build side, one wire-encoded cells object per row (name → <c>ColumnValueWireCodec</c>
    /// value), in the coordinator's bucket enumeration order. NULL-keyed rows are already
    /// excluded (inner-join semantics), so every entry is probe-matchable.
    /// </summary>
    public string[] BuildRows { get; set; } = [];

    /// <summary>
    /// The full ON predicate, evaluated remotely on each merged candidate pair — exactly the
    /// re-check the coordinator performs locally, carrying any non-equi residual conjuncts.
    /// Must be shippable (no subqueries, placeholders, or volatile functions) or the
    /// coordinator does not broadcast at all.
    /// </summary>
    public string OnPredicateJson { get; set; } = "";

    /// <summary>
    /// The probe side's pushed-down scan filter, evaluated remotely on the qualified probe row
    /// before hashing; null when the probe scan is unfiltered. Lives here rather than in the
    /// request's top-level <c>FilterJson</c> — see <see cref="QueryFragmentRequest.Join"/>.
    /// </summary>
    public string? ProbeFilterJson { get; set; }
}

/// <summary>
/// Scan actuals for one executed fragment, shipped as a terminal frame when the coordinator
/// asked for them (<see cref="QueryFragmentRequest.WantStats"/>): how many rows the remote
/// scan examined and how many survived the residual filter and were shipped. Purely
/// observational — the coordinator uses them for <c>EXPLAIN (ANALYZE)</c> attribution and
/// never for correctness decisions.
/// </summary>
public sealed record QueryFragmentScanStats(long RowsScanned, long RowsShipped);

/// <summary>
/// One fragment result frame. Row fragments carry <see cref="RowIdHex"/> + <see cref="Data"/>
/// (raw KV row bytes; the coordinator re-decodes through the same path as local spans).
/// Aggregate fragments carry <see cref="CellsJson"/> — a JSON object of output-name →
/// wire-encoded <c>ColumnValue</c> (see <c>ColumnValueWireCodec</c>) holding the span's
/// partial aggregate states. Broadcast-join probe fragments additionally carry
/// <see cref="MatchIndices"/> — the matched build rows' indices into
/// <see cref="QueryFragmentJoinSpec.BuildRows"/>, in match order. A frame with
/// <see cref="Stats"/> set is the optional terminal stats frame (see
/// <see cref="QueryFragmentScanStats"/>) and carries no row data.
/// </summary>
public sealed record QueryFragmentRow(string? RowIdHex, byte[]? Data, string? CellsJson = null, QueryFragmentScanStats? Stats = null, int[]? MatchIndices = null);
