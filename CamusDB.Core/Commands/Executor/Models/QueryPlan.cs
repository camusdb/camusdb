
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Cache;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Physical query plan for a single-table SELECT.
/// <see cref="Root"/> is the authoritative plan tree; <see cref="Steps"/> is a flattened
/// execution view for the legacy executor.
/// </summary>
public sealed class QueryPlan
{
    /// <summary>Physical plan tree root (outermost operator).</summary>
    public PhysicalPlanNode Root { get; internal set; } = null!;

    /// <summary>
    /// Legacy linear steps derived from <see cref="Root"/>. Execution walks the tree directly;
    /// this list remains only for consumers that pattern-match the flattened shape
    /// (scan-limit and index-only detection in the planner, borrowed-decode eligibility in the
    /// scanner). Do not add new consumers — read <see cref="Root"/> instead.
    /// </summary>
	public List<QueryPlanStep> Steps { get; } = new();

    /// <summary>
    /// Physical plan nodes corresponding 1-to-1 with <see cref="Steps"/> (same DFS order).
    /// Populated by <see cref="Controllers.Queries.QueryPlanStepAdapter"/> alongside <see cref="Steps"/>.
    /// The scan leaf is element 0 — scan operators use that slot to publish per-scan runtime
    /// stats during EXPLAIN ANALYZE.
    /// </summary>
    public List<PhysicalPlanNode> StepNodes { get; } = new();

    /// <summary>Centralized predicate analysis for the query WHERE clause.</summary>
    public PredicateAnalysis PredicateAnalysis { get; internal set; } = PredicateAnalysis.Empty;

    /// <summary>Row filter applied during scan execution after index selection.</summary>
    public NodeAst? ExecutionFilter { get; internal set; }

    /// <summary>Bound multi-source query when executing joins.</summary>
    public BoundSelectQuery? BoundQuery { get; internal set; }

	public DatabaseDescriptor Database { get; }

	public TableDescriptor Table { get; }

	public QueryTicket Ticket { get; }

    public int TableSchemaVersion { get; }

	public IAsyncEnumerable<QueryResultRow>? DataCursor { get; set; }

    /// <summary>
    /// Cached materializations for derived table scans within a join query.
    /// Each <see cref="SpillableRowList"/> may hold rows in memory or overflow to a spill
    /// file when the threshold is exceeded. All lists must be disposed by calling
    /// <see cref="DisposeMaterializationsAsync"/> after the join cursor is fully consumed.
    /// </summary>
    internal Dictionary<BoundDerivedTableSource, SpillableRowList> DerivedMaterializations { get; } =
        new();

    /// <summary>
    /// Disposes all <see cref="SpillableRowList"/> entries in <see cref="DerivedMaterializations"/>,
    /// deleting any spill files they hold. Called from the join executor after the cursor is consumed
    /// (normal completion, cancellation, or exception).
    /// </summary>
    internal async Task DisposeMaterializationsAsync()
    {
        foreach (SpillableRowList list in DerivedMaterializations.Values)
            await list.DisposeAsync().ConfigureAwait(false);
        DerivedMaterializations.Clear();
    }

    /// <summary>Single-table scan column subset. Null means decode all columns.</summary>
    public IReadOnlySet<string>? ScanRequiredColumns { get; internal set; }

    /// <summary>Per-alias scan column subsets for join plans. A null set means decode all columns.</summary>
    internal Dictionary<string, IReadOnlySet<string>?>? RequiredColumnsByAlias { get; set; }

    internal Dictionary<string, int> TableSchemaVersionByAlias { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Stable query-shape identifier. Derived from the logical query structure with all
    /// literal and parameter values abstracted away. Two queries differing only in constant
    /// values share the same shape; structurally different queries differ.
    /// Null until set by <see cref="Controllers.Queries.QueryPlanner"/> /
    /// <see cref="Controllers.Queries.JoinQueryPlanner"/>.
    /// </summary>
    public string? QueryShapeId { get; internal set; }

    /// <summary>
    /// Ordered schema-version dependencies for this plan. Each entry names a table and
    /// the schema version the plan was built against. Used to detect stale cached plans.
    /// Null until set by the planner.
    /// </summary>
    public IReadOnlyList<(string TableId, int SchemaVersion)>? SchemaDeps { get; internal set; }

    /// <summary>
    /// Optional scan-level row cap for LIMIT pushdown.
    /// When set, scan operators may stop after emitting this many rows.
    /// </summary>
    public long? ScanRowLimit { get; internal set; }

    /// <summary>
    /// True when the chosen index scan is covering: every column the query needs is already
    /// present in the index key or is a primary-key (row-id) column, so the executor can
    /// synthesize the result row directly from the decoded index entry with no primary-row
    /// fetch and no <c>RowEncoder.DecodeAsync</c> call.
    ///
    /// Set by <see cref="Controllers.Queries.QueryPlanner"/> after projection pushdown, which
    /// is when <see cref="ScanRequiredColumns"/> is finalized. Always false when
    /// <see cref="ScanRequiredColumns"/> is null (SELECT * or decode-all), or when the plan
    /// uses a full table scan rather than an index scan.
    /// </summary>
    public bool IndexOnly { get; internal set; }

    /// <summary>
    /// The names of the columns the query needs that are served from the index key (including
    /// pk columns reachable without a row fetch). Non-empty only when <see cref="IndexOnly"/>
    /// is true; empty list otherwise.
    /// </summary>
    public IReadOnlyList<string> IndexOnlyColumns { get; internal set; } = [];

    /// <summary>
    /// When true, the executor populates <see cref="Plans.PlanNodeStats"/> on each node
    /// reachable via <see cref="StepNodes"/> as rows flow through the pipeline.
    /// Always false during normal execution; set by EXPLAIN ANALYZE.
    /// </summary>
    public bool CollectRuntimeStats { get; internal set; }

    /// <summary>
    /// Optional dependency collector attached by the cached-read path. When non-null,
    /// scan methods record range and point deps into this collector as rows are read. After
    /// execution completes, <see cref="QueryDependencyCollector.Build"/> returns the
    /// <see cref="QueryDependencySet"/> to attach to the published entry.
    /// Null on uncached queries — all collector calls are guarded by a null check so there is
    /// no overhead on the normal execution path.
    /// </summary>
    internal QueryDependencyCollector? DepCollector { get; set; }

	public QueryPlan(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
	{
		Database = database;
		Table = table;
		Ticket = ticket;
        TableSchemaVersion = table.Schema.Version;
	}    
}
