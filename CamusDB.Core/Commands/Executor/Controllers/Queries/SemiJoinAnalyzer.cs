
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// R11 semi/anti-join rewrite: detects eligible IN / NOT IN subquery predicates in a
/// single-table SELECT WHERE clause and converts them to <see cref="SemiJoinSpec"/> descriptors
/// before <see cref="SubqueryRewriter"/> materialises them.
///
/// Eligibility:
/// <list type="bullet">
///   <item>Outer query has a single bare <see cref="TableSource"/> (not a join or derived table).</item>
///   <item>LHS of IN / NOT IN is a bare <see cref="NodeType.Identifier"/>.</item>
///   <item>RHS subquery has exactly one non-star <see cref="NodeType.Identifier"/> projection.</item>
///   <item>RHS subquery has a single <see cref="TableSource"/> FROM clause.</item>
///   <item>RHS subquery has no GROUP BY or HAVING. Inner <c>DISTINCT</c> is allowed: a semi/anti
///     join is an existence test, so deduplicating the inner values cannot change membership
///     (<c>x IN (SELECT DISTINCT c)</c> ≡ <c>x IN (SELECT c)</c>), and the join probes the inner
///     index directly without re-running the inner query.</item>
///   <item>RHS subquery is uncorrelated (<see cref="ExistsSubqueryAnalyzer.IsCorrelated"/> returns false).</item>
///   <item>RHS subquery WHERE (if any) contains no nested subqueries.</item>
/// </list>
/// </summary>
internal sealed class SemiJoinAnalyzer
{
    private readonly TableOpener tableOpener;
    private readonly SelectQueryCreator selectQueryCreator = new();

    public SemiJoinAnalyzer(TableOpener tableOpener)
    {
        this.tableOpener = tableOpener;
    }

    /// <summary>
    /// Inspects <paramref name="query"/>'s WHERE clause for eligible IN / NOT IN predicates
    /// and returns a modified query (with those predicates removed) plus the extracted specs.
    /// </summary>
    public async Task<(SelectQuery Query, List<SemiJoinSpec> Specs)> AnalyzeAsync(
        DatabaseDescriptor database,
        SelectQuery query,
        ExecuteSQLTicket ticket)
    {
        if (query.Source is not TableSource)
            return (query, []);

        if (query.Where is null)
            return (query, []);

        List<NodeAst> predicates = FlattenAnd(query.Where.Expression);
        List<SemiJoinSpec> specs = [];
        List<NodeAst> remaining = [];

        foreach (NodeAst pred in predicates)
        {
            if (pred.nodeType is NodeType.ExprInSubquery or NodeType.ExprNotInSubquery)
            {
                SemiJoinSpec? spec = await TryExtractSpecAsync(database, pred, ticket).ConfigureAwait(false);
                if (spec is not null)
                {
                    specs.Add(spec);
                    continue;
                }
            }
            remaining.Add(pred);
        }

        if (specs.Count == 0)
            return (query, []);

        NodeAst? newWhereExpr = RebuildAnd(remaining);
        BoundPredicate? newWhere = newWhereExpr is not null ? new BoundPredicate(newWhereExpr) : null;
        return (query with { Where = newWhere }, specs);
    }

    private async Task<SemiJoinSpec?> TryExtractSpecAsync(
        DatabaseDescriptor database,
        NodeAst pred,
        ExecuteSQLTicket ticket)
    {
        bool negated = pred.nodeType == NodeType.ExprNotInSubquery;

        if (pred.leftAst is null || pred.rightAst is null)
            return null;

        if (pred.leftAst.nodeType != NodeType.Identifier)
            return null;

        if (pred.rightAst.nodeType != NodeType.Select)
            return null;

        SelectQuery innerQuery;
        try
        {
            innerQuery = selectQueryCreator.CreateSelectQuery(pred.rightAst);
        }
        catch
        {
            return null;
        }

        if (innerQuery.Source is not TableSource innerTableSource)
            return null;
        if (innerQuery.GroupBy is { Count: > 0 })
            return null;
        // Inner DISTINCT is intentionally NOT a disqualifier: a semi/anti join tests existence,
        // so deduplicating the inner values is a no-op for membership. The spec probes the inner
        // index directly and never re-runs the inner query, so DISTINCT is simply dropped.
        if (innerQuery.Having is not null)
            return null;
        if (innerQuery.Projections.Count != 1)
            return null;

        NodeAst projExpr = innerQuery.Projections[0].Expression;
        if (projExpr.nodeType != NodeType.Identifier)
            return null;

        if (ExistsSubqueryAnalyzer.IsCorrelated(innerQuery))
            return null;

        if (innerQuery.Where is not null && ContainsSubquery(innerQuery.Where.Expression))
            return null;

        string outerColumn = pred.leftAst.yytext ?? "";
        string innerColumn = projExpr.yytext ?? "";

        int dot = innerColumn.IndexOf('.');
        if (dot >= 0)
            innerColumn = innerColumn[(dot + 1)..];

        if (string.IsNullOrEmpty(outerColumn) || string.IsNullOrEmpty(innerColumn))
            return null;

        TableDescriptor innerTable;
        try
        {
            innerTable = await tableOpener.Open(database, innerTableSource.TableName).ConfigureAwait(false);
        }
        catch
        {
            return null;
        }

        TableIndexSchema? innerIndex = FindSingleColumnIndex(innerTable, innerColumn);

        // Without an index the semi-join probe would full-scan the inner table per outer row
        // (O(n·m)), which is worse than materialising once (O(n+m)). Fall back to SubqueryRewriter.
        if (innerIndex is null)
            return null;

        SemiJoinMode mode;
        if (negated)
        {
            bool isNotNull = IsColumnNotNull(innerTable, innerColumn);
            mode = isNotNull ? SemiJoinMode.Anti : SemiJoinMode.NullAwareAnti;
        }
        else
        {
            mode = SemiJoinMode.Semi;
        }

        NodeAst? innerFilter = innerQuery.Where?.Expression;

        return new SemiJoinSpec(outerColumn, innerTable, innerColumn, innerIndex, innerFilter, mode);
    }

    private static bool ContainsSubquery(NodeAst expr)
    {
        if (expr.nodeType is NodeType.ExprInSubquery or NodeType.ExprNotInSubquery
            or NodeType.ExprScalarSubquery or NodeType.ExprExistsSubquery
            or NodeType.ExprExistsCorrelated)
            return true;

        if (expr.leftAst is not null && ContainsSubquery(expr.leftAst)) return true;
        if (expr.rightAst is not null && ContainsSubquery(expr.rightAst)) return true;
        return false;
    }

    private static TableIndexSchema? FindSingleColumnIndex(TableDescriptor table, string columnName)
    {
        foreach ((string _, TableIndexSchema index) in table.Indexes)
        {
            if (index.Columns.Length == 1
                && index.Columns[0].Equals(columnName, StringComparison.Ordinal)
                && SchemaElementStateRules.IsReadableIndex(table.Schema, index))
                return index;
        }
        return null;
    }

    private static bool IsColumnNotNull(TableDescriptor table, string columnName)
    {
        if (table.Schema.Columns is null)
            return false;

        foreach (TableColumnSchema col in table.Schema.Columns)
        {
            if (col.Name.Equals(columnName, StringComparison.Ordinal))
                return col.NotNull;
        }
        return false;
    }

    private static List<NodeAst> FlattenAnd(NodeAst expr)
    {
        List<NodeAst> result = [];
        FlattenAndInto(expr, result);
        return result;
    }

    private static void FlattenAndInto(NodeAst expr, List<NodeAst> result)
    {
        if (expr.nodeType == NodeType.ExprAnd)
        {
            if (expr.leftAst is not null) FlattenAndInto(expr.leftAst, result);
            if (expr.rightAst is not null) FlattenAndInto(expr.rightAst, result);
        }
        else
        {
            result.Add(expr);
        }
    }

    private static NodeAst? RebuildAnd(List<NodeAst> predicates)
    {
        if (predicates.Count == 0) return null;
        NodeAst result = predicates[0];
        for (int i = 1; i < predicates.Count; i++)
        {
            result = new NodeAst(
                NodeType.ExprAnd,
                result,
                predicates[i],
                null, null, null, null, null, null);
        }
        return result;
    }
}
