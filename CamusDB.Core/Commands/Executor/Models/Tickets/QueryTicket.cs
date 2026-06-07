
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public sealed class QueryTicket
{
    public KvTransaction TxnState { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    public string? IndexName { get; }

    public List<NodeAst>? Projection { get; }

    public List<QueryFilter>? Filters { get; }

    public NodeAst? Where { get; }

    public List<QueryOrderBy>? OrderBy { get; }

    public IReadOnlyList<NodeAst>? GroupBy { get; }

    public NodeAst? Having { get; }

    public NodeAst? Limit { get; }

    public NodeAst? Offset { get; }

    /// <summary>When true, duplicate projected rows are eliminated (QP3.6).</summary>
    public bool IsDistinct { get; }

    public Dictionary<string, ColumnValue>? Parameters { get; }

    public QueryRowNameResolver? RowNameResolver { get; }

    /// <summary>Predicate analysis for <see cref="Where"/>; populated at ticket creation.</summary>
    public PredicateAnalysis? AnalyzedWhere { get; }

    /// <summary>Prepared correlated EXISTS subqueries keyed by rewritten AST nodes (QP5.4).</summary>
    internal ExistsSubqueryRegistry? ExistsSubqueries { get; }

    /// <summary>
    /// The logical SELECT query this ticket was derived from (R10). Retained so that
    /// <see cref="Controllers.Queries.QueryPlanner"/> can compute the query-shape ID without
    /// re-parsing. Null for tickets created via the legacy (non-SQL) execution path.
    /// </summary>
    internal SelectQuery? SelectQuery { get; }

    /// <summary>Semi/anti-join specs extracted by R11 SemiJoinAnalyzer. Null when there are none.</summary>
    internal IReadOnlyList<SemiJoinSpec>? SemiJoinSpecs { get; }

    /// <summary>
    /// When set by the UPDATE/DELETE locate phase (R16), overrides the projection-derived
    /// column set for scan-time partial decoding. Contains exactly the columns needed to
    /// evaluate the WHERE and SET expressions — candidates that fail the filter are decoded
    /// no further. The write phase still does a full <c>DecodeWritableAsync</c>.
    /// Null means use the normal <see cref="RequiredColumnAnalyzer"/> derivation.
    /// </summary>
    internal IReadOnlySet<string>? LocateColumns { get; }

    public QueryTicket(
        KvTransaction txnState,
        string databaseName,
        string tableName,
        string? index,
        List<NodeAst>? projection,
        List<QueryFilter>? filters,
        NodeAst? where,
        List<QueryOrderBy>? orderBy,
        NodeAst? limit,
        NodeAst? offset,
        Dictionary<string, ColumnValue>? parameters,
        IReadOnlyList<NodeAst>? groupBy = null,
        NodeAst? having = null,
        QueryRowNameResolver? rowNameResolver = null,
        PredicateAnalysis? analyzedWhere = null,
        ExistsSubqueryRegistry? existsSubqueries = null,
        bool isDistinct = false,
        SelectQuery? selectQuery = null,
        IReadOnlyList<SemiJoinSpec>? semiJoinSpecs = null,
        IReadOnlySet<string>? locateColumns = null)
    {
        TxnState = txnState;
        DatabaseName = databaseName;
        TableName = tableName;
        IndexName = index;
        Projection = projection;
        Filters = filters;
        Where = where;
        OrderBy = orderBy;
        GroupBy = groupBy;
        Having = having;
        Limit = limit;
        Offset = offset;
        IsDistinct = isDistinct;
        Parameters = parameters;
        RowNameResolver = rowNameResolver;
        AnalyzedWhere = analyzedWhere;
        ExistsSubqueries = existsSubqueries;
        SelectQuery = selectQuery;
        SemiJoinSpecs = semiJoinSpecs;
        LocateColumns = locateColumns;
    }
}
