
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
/// Semi/anti-join rewrite: detects eligible IN / NOT IN subquery predicates in a
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
///   <item>RHS subquery is uncorrelated: each name of its output and its WHERE is a column of its one table.</item>
///   <item>RHS subquery WHERE (if any) contains no nested subqueries.</item>
///   <item>RHS subquery WHERE (if any) reads only names its own scope resolves: a bare column, or
///     one qualified with the inner alias (the table name when there is no alias). The spec carries
///     that scope as a <see cref="QueryRowNameResolver"/>, because the executor evaluates the filter
///     against the decoded inner row, which is keyed by bare names. A name the scope cannot resolve
///     leaves the predicate to the general path, which binds the subquery and reports the error with
///     its own message, so both paths refuse the same statement the same way.</item>
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
    public ValueTask<(SelectQuery Query, List<SemiJoinSpec> Specs)> AnalyzeAsync(
        DatabaseDescriptor database,
        SelectQuery query,
        ExecuteSQLTicket ticket)
    {
        // Only a top-level (AND-chain) IN / NOT IN subquery predicate can become a spec — the same
        // positions FlattenAnd exposes. Without one, the flatten/partition lists and the async walk
        // are pure per-statement garbage; return the query untouched, synchronously.
        if (query.Source is not TableSource || query.Where is null || !HasTopLevelInSubquery(query.Where.Expression))
            return new ValueTask<(SelectQuery, List<SemiJoinSpec>)>((query, []));

        return AnalyzeCoreAsync(database, query, ticket);
    }

    /// <summary>
    /// True when the AND-chain of <paramref name="expr"/> contains an IN / NOT IN subquery predicate
    /// at a position <c>FlattenAnd</c> would expose. Mirrors that traversal exactly: it descends only
    /// through <see cref="NodeType.ExprAnd"/>, so a subquery nested under OR/NOT (which the analyzer
    /// leaves in place) does not trigger the slow path.
    /// </summary>
    private static bool HasTopLevelInSubquery(NodeAst expr)
    {
        if (expr.nodeType is NodeType.ExprInSubquery or NodeType.ExprNotInSubquery)
            return true;

        if (expr.nodeType != NodeType.ExprAnd)
            return false;

        if (expr.leftAst is not null && HasTopLevelInSubquery(expr.leftAst))
            return true;

        return expr.rightAst is not null && HasTopLevelInSubquery(expr.rightAst);
    }

    private async ValueTask<(SelectQuery Query, List<SemiJoinSpec> Specs)> AnalyzeCoreAsync(
        DatabaseDescriptor database,
        SelectQuery query,
        ExecuteSQLTicket ticket)
    {
        List<NodeAst> predicates = FlattenAnd(query.Where!.Expression);
        List<SemiJoinSpec> specs = [];
        List<NodeAst> remaining = [];

        foreach (NodeAst pred in predicates)
        {
            if (pred.nodeType is NodeType.ExprInSubquery or NodeType.ExprNotInSubquery)
            {
                SemiJoinSpec? spec = await TryExtractSpecAsync(database, (TableSource)query.Source, pred, ticket).ConfigureAwait(false);
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
        TableSource outerSource,
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
        // LIMIT/OFFSET restricts which inner values participate in the membership set;
        // the rewrite probes the whole inner index, so it must not absorb such queries
        // (IN would match extra rows, NOT IN would silently drop rows).
        if (innerQuery.Limit is not null || innerQuery.Offset is not null)
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

        // The probe looks the outer value up in the inner index, so it finds only a value of the
        // index's own type. IN compares an INT64 with a NUMERIC or a FLOAT64 by value, so a mixed
        // pair must not take the probe: the materialized IN list evaluates it with that rule.
        if (!await ColumnTypesAgreeAsync(database, outerSource, outerColumn, innerTable, innerColumn).ConfigureAwait(false))
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

        // The inner SELECT is its own name scope. The executor evaluates its filter per probe against
        // the decoded inner row, which is keyed by bare column names, so a reference the subquery
        // qualifies with its own alias (s.c, or t.c when there is no alias) must map to the bare
        // name. The resolver of that one-table scope does the mapping, and it validates every name
        // once here instead of once per row. The same pass yields the columns a probe must decode.
        string innerAlias = innerTableSource.Alias ?? innerTableSource.TableName;
        QueryRowNameResolver innerResolver = new([new BoundTableSource(innerTableSource, innerTable, innerAlias)]);
        HashSet<string> innerRequired = new(StringComparer.OrdinalIgnoreCase) { innerColumn };

        // The output must be a column of the inner table as well. A name that is not can resolve to the
        // outer query, as o.x does in IN (SELECT o.x FROM s), and a probe of the inner index cannot read
        // the outer row. The general path decides such a subquery.
        try
        {
            innerResolver.ValidateColumnReference(projExpr.yytext ?? "");
        }
        catch (CamusDBException)
        {
            return null;
        }

        if (innerFilter is not null)
        {
            HashSet<string> references = new(StringComparer.Ordinal);
            QueryExpressionWalker.CollectColumnReferences(innerFilter, references);

            foreach (string reference in references)
            {
                string lookupKey;
                try
                {
                    lookupKey = innerResolver.ResolveRowLookupKey(reference);
                }
                catch (CamusDBException)
                {
                    // An unknown column or alias, or a name of the outer query: the general path binds
                    // the subquery and decides it, so both paths treat the statement the same way.
                    return null;
                }

                innerRequired.Add(lookupKey);
            }

            // An aggregate has no place in a WHERE, and the general path reports it with its own message.
            if (QueryExpressionClassifier.ContainsAggregate(innerFilter))
                return null;

            // The filter runs per probe, so a probe that finds no row never evaluates it. This spec
            // skips the binder, so the statement checks of a bound SELECT run here.
            ConditionalResultTypes.CheckCondition(
                innerFilter, ConditionalResultTypes.LookupFor(innerTable.Schema), innerResolver, "WHERE", allowAggregates: false);
            ConstantDivisionCheck.Check(innerFilter);
        }

        innerResolver.FreezeForSharedReuse();

        return new SemiJoinSpec(outerColumn, innerTable, innerColumn, innerIndex, innerFilter, innerResolver, innerRequired, mode);
    }

    /// <summary>
    /// False when the outer and inner columns both have a known declared type and the two differ.
    /// An outer column that cannot be resolved here keeps the probe, as before.
    /// </summary>
    private async ValueTask<bool> ColumnTypesAgreeAsync(
        DatabaseDescriptor database,
        TableSource outerSource,
        string outerColumn,
        TableDescriptor innerTable,
        string innerColumn)
    {
        int dot = outerColumn.IndexOf('.');
        string outerName = dot >= 0 ? outerColumn[(dot + 1)..] : outerColumn;

        TableDescriptor outerTable;
        try
        {
            outerTable = await tableOpener.Open(database, outerSource.TableName).ConfigureAwait(false);
        }
        catch
        {
            return true;
        }

        ColumnType? outerType = ColumnTypeOf(outerTable, outerName);
        ColumnType? innerType = ColumnTypeOf(innerTable, innerColumn);

        return outerType is null || innerType is null || outerType == innerType;
    }

    private static ColumnType? ColumnTypeOf(TableDescriptor table, string columnName)
    {
        foreach (TableColumnSchema col in table.Schema.Columns ?? [])
        {
            if (col.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                return col.Type;
        }

        return null;
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
                && index.Columns[0].Equals(columnName, StringComparison.OrdinalIgnoreCase)
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
            if (col.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase))
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
        => ExpressionChains.Flatten(expr, NodeType.ExprAnd, result);

    private static NodeAst? RebuildAnd(List<NodeAst> predicates)
        => ExpressionChains.Combine(NodeType.ExprAnd, predicates);
}
