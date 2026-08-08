
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Cache;
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

    /// <summary>When true, duplicate projected rows are eliminated.</summary>
    public bool IsDistinct { get; }

    public Dictionary<string, ColumnValue>? Parameters { get; }

    public QueryRowNameResolver? RowNameResolver { get; }

    /// <summary>Predicate analysis for <see cref="Where"/>; populated at ticket creation.</summary>
    public PredicateAnalysis? AnalyzedWhere { get; }

    /// <summary>Prepared correlated EXISTS subqueries keyed by rewritten AST nodes.</summary>
    internal ExistsSubqueryRegistry? ExistsSubqueries { get; }

    /// <summary>
    /// Pre-materialized IN-list sets keyed by the ExprInMembership AST node (by reference).
    /// Populated at ticket creation for literal/parameter IN lists extracted by PredicateAnalyzer.
    /// Null when no qualifying IN predicates exist.
    /// </summary>
    internal IReadOnlyDictionary<NodeAst, PreparedInSet>? PreparedInSets { get; }

    /// <summary>
    /// The logical SELECT query this ticket was derived from. Retained so that
    /// <see cref="Controllers.Queries.QueryPlanner"/> can compute the query-shape ID without
    /// re-parsing. Null for tickets created via the legacy (non-SQL) execution path.
    /// </summary>
    internal SelectQuery? SelectQuery { get; }

    /// <summary>Semi/anti-join specs extracted by R11 SemiJoinAnalyzer. Null when there are none.</summary>
    internal IReadOnlyList<SemiJoinSpec>? SemiJoinSpecs { get; }

    /// <summary>
    /// When set by the UPDATE/DELETE locate phase, overrides the projection-derived
    /// column set for scan-time partial decoding. Contains exactly the columns needed to
    /// evaluate the WHERE and SET expressions — candidates that fail the filter are decoded
    /// no further. The write phase still does a full <c>DecodeWritableAsync</c>.
    /// Null means use the normal <see cref="RequiredColumnAnalyzer"/> derivation.
    /// </summary>
    internal IReadOnlySet<string>? LocateColumns { get; }

    /// <summary>
    /// When true, predicate range locks acquired during the locate scan are Exclusive rather
    /// than Shared. Set by UPDATE/DELETE so the scanned row/index range is held against
    /// concurrent Serializable+RW readers until the modifying transaction commits, preventing
    /// them from observing a partial mutation.
    ///
    /// <para>It also marks the scan as write-driving: the rows it observed decide what is
    /// written, so a concurrent change to one of them must invalidate the transaction even if
    /// that row was never written. Whether reads fold into the commit-time read set is decided
    /// by the transaction (<see cref="Transactions.KvTransaction.FoldReads"/>), not by this
    /// flag — an optimistic transaction folds every read, plan shape included.</para>
    /// </summary>
    internal bool ExclusivePredicateLocks { get; }

    /// <summary>
    /// Query-result cache hint from a <c>{cache=name}</c> table-reference hint. Null when the
    /// query carries no cache hint and must follow the uncached execution path.
    /// Derived from <see cref="SelectQuery.CacheHint"/>; stored here for convenient access
    /// without an extra null-check on the SelectQuery reference.
    /// </summary>
    public CacheHintOptions? CacheHint { get; }

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
        IReadOnlySet<string>? locateColumns = null,
        bool exclusivePredicateLocks = false,
        IReadOnlyDictionary<NodeAst, PreparedInSet>? preparedInSets = null,
        CacheHintOptions? cacheHint = null)
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
        ExclusivePredicateLocks = exclusivePredicateLocks;
        PreparedInSets = preparedInSets;
        CacheHint = cacheHint;
    }
}
