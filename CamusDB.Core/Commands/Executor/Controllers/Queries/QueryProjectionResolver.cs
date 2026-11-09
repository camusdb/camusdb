
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

    /// <summary>
    /// The name of a select-list item: its alias, the bare name of a column, the name of the single column
    /// of a scalar subquery (see <see cref="ScalarSubqueryColumnName"/>), or else its position. A position
    /// is a row key that no SQL name can reach, because a bare number in ORDER BY is a position.
    /// </summary>
    public static string GetOutputNameFromProjectionExpression(NodeAst expression, int index)
    {
        return expression.nodeType switch
        {
            NodeType.ExprAlias => expression.rightAst!.yytext!,
            NodeType.Identifier => GetBareColumnName(expression.yytext!),
            NodeType.ExprScalarSubquery => ScalarSubqueryColumnName(expression) ?? index.ToString(),
            _ => index.ToString(),
        };
    }

    /// <summary>
    /// The name that a scalar subquery gives its column, as in PostgreSQL: the name of the single column of
    /// its select list. That is an alias, the bare name of a column, or the name of a nested scalar subquery;
    /// a set operation takes the name from its first operand. Null when that column is another expression,
    /// a <c>*</c>, or more than one column: then the subquery column has a position as its name, as any
    /// other expression has (PostgreSQL names an expression <c>?column?</c>, or after its function).
    ///
    /// <para>The select list builder (<c>SelectQueryCreator</c>) gives a top-level scalar subquery this
    /// name as an alias, because the subquery rewrite replaces the subquery node with its value before the
    /// output is named. A FROM-less SELECT, EXPLAIN and the view builder read the parse tree, so they reach
    /// the same name here.</para>
    /// </summary>
    internal static string? ScalarSubqueryColumnName(NodeAst subquery)
    {
        NodeAst? select = subquery.leftAst;

        // A loop, not a recursion: each nested subquery or set operation is one more turn.
        while (select is { nodeType: NodeType.Select })
        {
            if (SetOperationAst.IsSetOperationSelect(select))
            {
                select = select.rightAst!.leftAst;
                continue;
            }

            NodeAst? column = select.leftAst;

            switch (column?.nodeType)
            {
                case NodeType.ExprAlias:
                    return column.rightAst?.yytext;

                case NodeType.Identifier when column.yytext is { } name:
                    return GetBareColumnName(name);

                case NodeType.ExprScalarSubquery:
                    select = column.leftAst;
                    continue;

                default:
                    return null;
            }
        }

        return null;
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
    /// Resolves <c>ORDER BY name</c> against the output names of the select list, returning the
    /// expression of the first item with that name.
    ///
    /// <para>This is the pre-projection half of one precedence rule. A plain SELECT sorts before it
    /// projects, so an output name cannot be looked up as an output column the way the post-aggregate
    /// path does — it has to be resolved back to the expression it names. Both halves agree on the rule
    /// itself, which is the PostgreSQL rule: <b>a bare name in ORDER BY matches an output name first,
    /// and an input column only when no output has the name</b>. The output names are those of
    /// <see cref="OrderMatchName"/>: an alias, the bare name of a column (so <c>SELECT a.id … ORDER BY
    /// id</c> sorts by <c>a.id</c>, even when <c>id</c> is ambiguous among the inputs), and the column
    /// name of a scalar subquery.</para>
    ///
    /// <para>A name that two outputs with different expressions have is ambiguous; the binder refuses it
    /// first (<see cref="RejectAmbiguousOrderName"/>), so the first match here is the only one.</para>
    /// </summary>
    public static bool TryResolveProjectionAliasTarget(
        NodeAst orderExpression,
        IReadOnlyList<ProjectionItem> projections,
        out NodeAst target)
    {
        target = orderExpression;

        if (!IsBareName(orderExpression, projections, out string name))
            return false;

        foreach (ProjectionItem projection in projections)
        {
            if (!string.Equals(OrderMatchName(projection), name, StringComparison.OrdinalIgnoreCase))
                continue;

            target = QueryExpressionClassifier.UnwrapAlias(projection.Expression);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Refuses a bare <c>ORDER BY</c> name that two select-list items with different expressions have, as
    /// PostgreSQL does (<c>ORDER BY "id" is ambiguous</c>). <c>SELECT id, v AS id … ORDER BY id</c> has
    /// no one meaning: by the output-name rule it could sort by either item. Two items with the same
    /// expression (<c>SELECT id, id</c>, or <c>SELECT r.id, id</c> over one table) are one meaning, so
    /// they are not refused. The check applies to every SELECT shape, grouped or not, because the rule
    /// is about names, not about when the sort runs.
    ///
    /// <para><paramref name="sameExpression"/> says whether two matched expressions are the same; the
    /// binder compares column references by the input column they resolve to.
    /// <paramref name="starColumn"/> gives the input column that a <c>*</c> item puts out under the name,
    /// or null when there is none: <c>SELECT *, v AS id … ORDER BY id</c> is ambiguous, because the
    /// <c>*</c> puts out <c>id</c> too.</para>
    /// </summary>
    public static void RejectAmbiguousOrderName(
        NodeAst orderExpression,
        IReadOnlyList<ProjectionItem> projections,
        Func<NodeAst, NodeAst, bool> sameExpression,
        Func<string, NodeAst?> starColumn)
    {
        if (!IsBareName(orderExpression, projections, out string name))
            return;

        NodeAst? first = null;

        foreach (ProjectionItem projection in projections)
        {
            NodeAst target = QueryExpressionClassifier.UnwrapAlias(projection.Expression);
            NodeAst? match;

            if (target.nodeType == NodeType.ExprAllFields)
                match = starColumn(name);
            else
                match = string.Equals(OrderMatchName(projection), name, StringComparison.OrdinalIgnoreCase) ? target : null;

            if (match is null)
                continue;

            if (first is null)
            {
                first = match;
                continue;
            }

            if (!sameExpression(first, match))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"ORDER BY \"{name}\" is ambiguous");
        }
    }

    /// <summary>
    /// The output name that a bare <c>ORDER BY</c> name matches for <paramref name="projection"/>, or null
    /// when the item has none (an expression with no alias, or a <c>*</c>). The select list builder
    /// records the full text of a column reference as its <see cref="ProjectionItem.OutputName"/>
    /// (<c>r.id</c>); the name that ORDER BY matches is the column name without its qualifier, as the
    /// output column is named.
    /// </summary>
    internal static string? OrderMatchName(ProjectionItem projection)
    {
        NodeAst expression = projection.Expression;
        string? name = projection.OutputName;

        if (string.IsNullOrEmpty(name))
        {
            return expression.nodeType switch
            {
                NodeType.ExprAlias => expression.rightAst?.yytext,
                NodeType.Identifier when expression.yytext is { } text => GetBareColumnName(text),
                NodeType.ExprScalarSubquery => ScalarSubqueryColumnName(expression),
                _ => null,
            };
        }

        return expression.nodeType == NodeType.Identifier && string.Equals(expression.yytext, name, StringComparison.Ordinal)
            ? GetBareColumnName(name)
            : name;
    }

    /// <summary>
    /// True when <paramref name="expression"/> is a column name with no qualifier that the statement
    /// wrote in ORDER BY. A position (<c>ORDER BY 2</c>) is not a name: the select list builder resolves
    /// it to the expression node of that item, the same node object, so it is found here and skipped.
    /// Without that, <c>SELECT v AS id, id … ORDER BY 2</c> would match the name <c>id</c> of item 2 to
    /// the alias of item 1 and sort by <c>v</c>.
    /// </summary>
    private static bool IsBareName(NodeAst expression, IReadOnlyList<ProjectionItem> projections, out string name)
    {
        name = expression.yytext ?? "";

        if (expression.nodeType != NodeType.Identifier || name.Length == 0 || name.Contains('.'))
            return false;

        foreach (ProjectionItem projection in projections)
        {
            if (ReferenceEquals(QueryExpressionClassifier.UnwrapAlias(projection.Expression), expression))
                return false;
        }

        return true;
    }

    private static bool TryResolveProjectionOrderColumn(
        NodeAst orderExpression,
        IReadOnlyList<ProjectionItem> projections,
        out string columnName)
    {
        if (IsBareName(orderExpression, projections, out string name))
        {
            for (int i = 0; i < projections.Count; i++)
            {
                ProjectionItem projection = projections[i];

                if (string.Equals(OrderMatchName(projection), name, StringComparison.OrdinalIgnoreCase))
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
