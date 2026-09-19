
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Resolves SELECT projection output names and post-aggregate ORDER BY targets.
/// </summary>
internal static class QueryProjectionResolver
{
    public static string GetOutputName(ProjectionItem projection, int index)
    {
        if (!string.IsNullOrEmpty(projection.OutputName))
            return projection.OutputName;

        return GetOutputNameFromProjectionExpression(projection.Expression, index);
    }

    public static string GetOutputNameFromProjectionExpression(NodeAst expression, int index)
    {
        return expression.nodeType switch
        {
            NodeType.ExprAlias => expression.rightAst!.yytext!,
            NodeType.Identifier => GetBareColumnName(expression.yytext!),
            _ => index.ToString(),
        };
    }

    /// <summary>
    /// Returns the key each select-list item occupies in a projected row. Output names are not
    /// unique — <c>SELECT a.id, b.id</c> names both items <c>id</c> — but a row is addressed by key,
    /// so two items that shared a key collapsed into one cell and every reader of the second got the
    /// value of the other.
    ///
    /// <para>The <b>first</b> item that carries a name keeps that name as its key, so a query with no
    /// collision is unchanged and a by-name reference (ORDER BY, HAVING, an outer query over a derived
    /// table) resolves to the first item, as it always did. A <b>later</b> item with the same name
    /// (compared case-insensitively, like the row itself) gets a generated key that no item names.
    /// The display name is unaffected: <see cref="GetOutputNameFromProjectionExpression"/> still
    /// reports <c>id</c> for both, and <see cref="DerivedColumnSchema.RowKey"/> carries the key to
    /// whoever reads the cell.</para>
    ///
    /// <para><paramref name="starColumns"/> are the row keys a <c>*</c> item expands to. They count
    /// as taken <b>regardless of position</b>, because the projector writes the expansion and the
    /// explicit items into one row: <c>SELECT name AS id, *</c> must not let either overwrite the
    /// other. Pass <see langword="null"/> when the list has no <c>*</c>. The schema builder and the
    /// projector must pass the same set, or the declared keys and the row keys disagree.</para>
    ///
    /// <para>A <c>*</c> item occupies no single cell; its slot in the result is its ordinal text and
    /// is never read.</para>
    /// </summary>
    public static string[] GetRowKeys(IReadOnlyList<NodeAst> projections, IEnumerable<string>? starColumns = null)
    {
        string[] keys = new string[projections.Count];
        HashSet<string> firstSeen = new(projections.Count, StringComparer.OrdinalIgnoreCase);

        if (starColumns is not null)
        {
            foreach (string starColumn in starColumns)
                firstSeen.Add(starColumn);
        }

        bool collided = false;

        for (int i = 0; i < projections.Count; i++)
        {
            string name = GetOutputNameFromProjectionExpression(projections[i], i);

            if (projections[i].nodeType == NodeType.ExprAllFields || firstSeen.Add(name))
                keys[i] = name;
            else
                collided = true;
        }

        if (!collided)
            return keys;

        // A generated key must also miss every name that appears later in the list, so the full name
        // set is collected before any key is generated.
        HashSet<string> taken = new(firstSeen, StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < projections.Count; i++)
        {
            if (keys[i] is not null)
                continue;

            string name = GetOutputNameFromProjectionExpression(projections[i], i);
            string candidate = $"{name}$dup{i}";

            while (!taken.Add(candidate))
                candidate += "_";

            keys[i] = candidate;
        }

        return keys;
    }

    private static string GetBareColumnName(string identifier)
    {
        int dotIndex = identifier.LastIndexOf('.');
        return dotIndex >= 0 ? identifier[(dotIndex + 1)..] : identifier;
    }

    public static string GetGroupByOutputName(NodeAst groupExpression, int index)
    {
        return groupExpression.nodeType switch
        {
            NodeType.Identifier => groupExpression.yytext!,
            _ => $"$groupby{index}",
        };
    }

    public static bool TryResolvePostAggregateOrderColumn(
        NodeAst orderExpression,
        IReadOnlyList<ProjectionItem> projections,
        IReadOnlyList<NodeAst>? groupBy,
        out string columnName)
    {
        if (TryResolveProjectionOrderColumn(orderExpression, projections, out columnName))
            return true;

        if (groupBy is not { Count: > 0 })
        {
            columnName = "";
            return false;
        }

        NodeAst orderExpr = QueryExpressionClassifier.UnwrapAlias(orderExpression);

        for (int i = 0; i < groupBy.Count; i++)
        {
            if (!QueryAstComparer.AreEquivalent(groupBy[i], orderExpr))
                continue;

            columnName = GetGroupByOutputName(groupBy[i], i);
            return true;
        }

        columnName = "";
        return false;
    }

    /// <summary>
    /// Resolves <c>ORDER BY name</c> against the select list's <b>explicit</b> aliases, returning the
    /// expression the alias stands for.
    ///
    /// <para>This is the pre-projection half of one precedence rule. A plain SELECT sorts before it
    /// projects, so an alias cannot be looked up as an output column the way the post-aggregate path
    /// does — it has to be resolved back to the expression it names. Both halves agree on the rule
    /// itself: <b>an explicit select-list alias outranks a base column of the same name</b>, which is
    /// what standard SQL and PostgreSQL do, and what
    /// <see cref="TryResolvePostAggregateOrderColumn"/> already did for grouped queries.</para>
    ///
    /// <para>Only an explicit alias participates. A bare <c>SELECT x</c> carries no
    /// <see cref="ProjectionItem.OutputName"/>, so <c>ORDER BY x</c> stays a plain column reference
    /// and the ordinary sort path is untouched.</para>
    /// </summary>
    public static bool TryResolveProjectionAliasTarget(
        NodeAst orderExpression,
        IReadOnlyList<ProjectionItem> projections,
        out NodeAst target)
    {
        target = orderExpression;

        if (orderExpression.nodeType != NodeType.Identifier)
            return false;

        string name = orderExpression.yytext ?? "";

        foreach (ProjectionItem projection in projections)
        {
            if (string.IsNullOrEmpty(projection.OutputName))
                continue;

            if (!string.Equals(projection.OutputName, name, StringComparison.OrdinalIgnoreCase))
                continue;

            target = QueryExpressionClassifier.UnwrapAlias(projection.Expression);
            return true;
        }

        return false;
    }

    private static bool TryResolveProjectionOrderColumn(
        NodeAst orderExpression,
        IReadOnlyList<ProjectionItem> projections,
        out string columnName)
    {
        if (orderExpression.nodeType == NodeType.Identifier)
        {
            string name = orderExpression.yytext!;

            for (int i = 0; i < projections.Count; i++)
            {
                ProjectionItem projection = projections[i];

                if (string.Equals(projection.OutputName, name, StringComparison.OrdinalIgnoreCase))
                {
                    columnName = GetOutputName(projection, i);
                    return true;
                }
            }
        }

        NodeAst orderExpr = QueryExpressionClassifier.UnwrapAlias(orderExpression);

        for (int i = 0; i < projections.Count; i++)
        {
            ProjectionItem projection = projections[i];

            if (QueryAstComparer.AreEquivalent(
                QueryExpressionClassifier.UnwrapAlias(projection.Expression),
                orderExpr))
            {
                columnName = GetOutputName(projection, i);
                return true;
            }
        }

        columnName = "";
        return false;
    }
}
