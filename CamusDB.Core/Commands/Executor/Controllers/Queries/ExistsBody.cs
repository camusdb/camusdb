
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>What an EXISTS needs from its body. See <see cref="ExistsBody"/>.</summary>
internal enum ExistsBodyForm
{
    /// <summary>Only FROM and WHERE decide the answer, so nothing else runs.</summary>
    OutputDiscarded,

    /// <summary>A LIMIT of zero: the answer is false, and no row is read.</summary>
    NoRows,

    /// <summary>The whole body must run, because another clause can change the number of rows.</summary>
    MustRun,
}

/// <summary>
/// Decides which clauses of an EXISTS body can change its answer. EXISTS asks only whether a row
/// exists, so the select list, GROUP BY, DISTINCT and ORDER BY cannot change the answer when the body
/// has no aggregate, and a value error in them must not fail the statement. This is the rule of
/// PostgreSQL, and PostgreSQL also does not evaluate those clauses.
///
/// <para>An aggregate in the select list, HAVING and OFFSET keep the whole body. An aggregate with no GROUP
/// BY gives one row from no rows, and HAVING and OFFSET remove rows. ORDER BY needs no rule: the binder
/// accepts an aggregate there only when the select list holds it. A LIMIT above zero cannot change the
/// answer. A set operation and a window function need no rule here: each is already a derived table in
/// FROM, which runs with the output of its operands.</para>
///
/// <para>A discarded clause still holds statement mistakes: an unknown column fails, as PostgreSQL
/// does. <see cref="QueryBinder.CheckDiscardedOutputAsync"/> finds them, with no read and no
/// evaluation.</para>
/// </summary>
internal static class ExistsBody
{
    /// <summary>
    /// The select list that replaces a discarded one. It reads no column, so a scan of the body decodes
    /// only what its WHERE needs.
    /// </summary>
    private static readonly IReadOnlyList<ProjectionItem> NoOutput =
    [
        new ProjectionItem(ColumnValueAstBuilder.FromColumnValue(new ColumnValue(ColumnType.Integer64, 1)), null),
    ];

    /// <summary>Classifies a body as the statement wrote it, with the values of its parameters.</summary>
    public static ExistsBodyForm Classify(SelectQuery query, Dictionary<string, ColumnValue>? parameters)
    {
        if (query.Offset is not null
            && PredicateAnalyzer.TryGetConstant(query.Offset, parameters, out ColumnValue? offset)
            && offset is { Type: ColumnType.Integer64, LongValue: < 0 })
            throw QueryLimiter.Negative("OFFSET");

        if (query.Having is not null || query.Offset is not null)
            return ExistsBodyForm.MustRun;

        foreach (ProjectionItem projection in query.Projections)
            if (QueryExpressionClassifier.ContainsAggregate(projection.Expression))
                return ExistsBodyForm.MustRun;

        if (query.Limit is null)
            return ExistsBodyForm.OutputDiscarded;

        if (!PredicateAnalyzer.TryGetConstant(query.Limit, parameters, out ColumnValue? limit) || limit is not { Type: ColumnType.Integer64 })
            return ExistsBodyForm.MustRun;

        // A negative count fails the statement here, before the body is classified as one that needs
        // no run, so a negative parameter fails even when no outer row reaches the body.
        return limit.LongValue switch
        {
            > 0 => ExistsBodyForm.OutputDiscarded,
            0 => ExistsBodyForm.NoRows,
            _ => throw QueryLimiter.Negative("LIMIT"),
        };
    }

    /// <summary>The body with only the clauses that can change the answer.</summary>
    public static SelectQuery DiscardOutput(SelectQuery query) =>
        query with { Projections = NoOutput, GroupBy = null, OrderBy = null, Limit = null, IsDistinct = false };

    /// <summary>
    /// True when a discarded clause can hold a statement mistake. The usual <c>EXISTS (SELECT 1 ...)</c>
    /// and <c>EXISTS (SELECT * ...)</c> cannot, so they cost no second bind.
    /// </summary>
    public static bool OutputNeedsCheck(SelectQuery query)
    {
        if (query.GroupBy is { Count: > 0 } || query.OrderBy is { Count: > 0 } || query.IsDistinct)
            return true;

        foreach (ProjectionItem projection in query.Projections)
            if (projection.Expression.nodeType is not (NodeType.Integer or NodeType.String or NodeType.Bool or NodeType.Null or NodeType.ExprAllFields))
                return true;

        return false;
    }
}
