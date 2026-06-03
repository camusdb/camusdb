
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

    /// <summary>Centralized predicate analysis for the query WHERE clause.</summary>
    public PredicateAnalysis PredicateAnalysis { get; internal set; } = PredicateAnalysis.Empty;

    /// <summary>Row filter applied during scan execution after index selection.</summary>
    public NodeAst? ExecutionFilter { get; internal set; }

    /// <summary>Bound multi-source query when executing joins (QP4.3).</summary>
    public BoundSelectQuery? BoundQuery { get; internal set; }

	public DatabaseDescriptor Database { get; }

	public TableDescriptor Table { get; }

	public QueryTicket Ticket { get; }

    public int TableSchemaVersion { get; }

	public IAsyncEnumerable<QueryResultRow>? DataCursor { get; set; }

    /// <summary>Cached materializations for derived table scans within a join query (QP5.5).</summary>
    internal Dictionary<BoundDerivedTableSource, List<Dictionary<string, ColumnValue>>> DerivedMaterializations { get; } =
        new();

    /// <summary>Single-table scan column subset (QP6.1). Null means decode all columns.</summary>
    public IReadOnlySet<string>? ScanRequiredColumns { get; internal set; }

    /// <summary>Per-alias scan column subsets for join plans (QP6.1).</summary>
    internal Dictionary<string, IReadOnlySet<string>>? RequiredColumnsByAlias { get; set; }

    internal Dictionary<string, int> TableSchemaVersionByAlias { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Optional scan-level row cap for LIMIT pushdown (QP6.3).
    /// When set, scan operators may stop after emitting this many rows.
    /// </summary>
    public long? ScanRowLimit { get; internal set; }

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
