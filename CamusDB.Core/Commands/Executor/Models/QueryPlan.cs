
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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

    /// <summary>Legacy linear steps derived from <see cref="Root"/>.</summary>
	public List<QueryPlanStep> Steps { get; } = new();

    /// <summary>
    /// Physical plan nodes corresponding 1-to-1 with <see cref="Steps"/> (same DFS order).
    /// Populated by <see cref="Controllers.Queries.QueryPlanStepAdapter"/> alongside <see cref="Steps"/>.
    /// Used by the executor to update per-node runtime stats during EXPLAIN ANALYZE.
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

    /// <summary>Cached materializations for derived table scans within a join query.</summary>
    internal Dictionary<BoundDerivedTableSource, List<Dictionary<string, ColumnValue>>> DerivedMaterializations { get; } =
        new();

    /// <summary>Single-table scan column subset. Null means decode all columns.</summary>
    public IReadOnlySet<string>? ScanRequiredColumns { get; internal set; }

    /// <summary>Per-alias scan column subsets for join plans.</summary>
    internal Dictionary<string, IReadOnlySet<string>>? RequiredColumnsByAlias { get; set; }

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
    public IReadOnlyList<(string TableName, int SchemaVersion)>? SchemaDeps { get; internal set; }

    /// <summary>
    /// Optional scan-level row cap for LIMIT pushdown.
    /// When set, scan operators may stop after emitting this many rows.
    /// </summary>
    public long? ScanRowLimit { get; internal set; }

    /// <summary>
    /// When true, the executor populates <see cref="Plans.PlanNodeStats"/> on each node
    /// reachable via <see cref="StepNodes"/> as rows flow through the pipeline.
    /// Always false during normal execution; set by EXPLAIN ANALYZE.
    /// </summary>
    public bool CollectRuntimeStats { get; internal set; }

	public QueryPlan(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
	{
		Database = database;
		Table = table;
		Ticket = ticket;
        TableSchemaVersion = table.Schema.Version;
	}

	public void AddStep(QueryPlanStep step)
	{
		Steps.Add(step);
	}    
}
