
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

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

    public NodeAst? Limit { get; }

    public NodeAst? Offset { get; }

    public Dictionary<string, ColumnValue>? Parameters { get; }

    public QueryRowNameResolver? RowNameResolver { get; }

    /// <summary>Predicate analysis for <see cref="Where"/>; populated at ticket creation.</summary>
    public PredicateAnalysis? AnalyzedWhere { get; }

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
        QueryRowNameResolver? rowNameResolver = null,
        PredicateAnalysis? analyzedWhere = null)
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
        Limit = limit;
        Offset = offset;
        Parameters = parameters;
        RowNameResolver = rowNameResolver;
        AnalyzedWhere = analyzedWhere;
    }
}
