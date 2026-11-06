/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Keeps the static type of a subquery's column after the subquery is replaced by its result, so that
/// the statement checks can compare it. <see cref="SubqueryRewriter"/> runs before the outer statement
/// is bound, and a result can hold no value at all: a scalar subquery with no row becomes a NULL, and
/// an <c>IN</c> over no row becomes an empty list. Without this record <c>id = (SELECT s FROM empty)</c>
/// would pass over no rows, and fail when the subquery returns a value.
///
/// <para><b>Why a side table and not the tree.</b> The record exists for the statement checks only, and
/// they run on the coordinator before execution. A field on the tree would reach each walker, each
/// renderer and the wire format of a query fragment, and a typed CAST in place of the NULL would change
/// the column metadata and the plan of the statement. The table is keyed on the identity of a node of
/// the rewritten tree, which one execution owns, and it holds the node weakly, so the record goes away
/// with the tree.</para>
///
/// <para>A rebuilt node loses its record, and the check then sees no type. That only moves the error to
/// the moment a value reaches the operator, which is where it was before.</para>
/// </summary>
internal static class SubqueryResultTypes
{
    private static readonly ConditionalWeakTable<NodeAst, StrongBox<ExpressionType>> Types = new();

    /// <summary>
    /// The static type of the column of a bound subquery, or null when the statement does not show it. A
    /// set operation declares its output columns, and a String there can be the fallback of the schema
    /// builder, so it reads as unknown.
    /// </summary>
    internal static ExpressionType? OutputTypeOf(BoundSelectQuery bound)
    {
        if (bound.Query.Source is DerivedTableSource { SetOperation: not null } && bound.DerivedSources.Count == 1)
        {
            DerivedColumnSchema column = bound.DerivedSources[0].Columns[0];

            return column.Type == ColumnType.String
                ? null
                : new ExpressionType(column.Type, column.ElementType ?? ColumnType.Null);
        }

        return bound.Query.Projections.Count == 1
            ? ConditionalResultTypes.TypeOf(bound.Query.Projections[0].Expression, ConditionalResultTypes.LookupFor(bound, excludeOutputAliases: false))
            : null;
    }

    /// <summary>
    /// The NULL that replaces a scalar subquery with no value. It is a node of its own when the type is
    /// known, because <see cref="NodeAst.Null"/> is shared by every statement.
    /// </summary>
    internal static NodeAst NullOf(ExpressionType? type)
    {
        if (type is null || type.Value.Type == ColumnType.Null)
            return ColumnValueAstBuilder.FromColumnValue(ColumnValue.Null);

        return Remember(new NodeAst(NodeType.Null, null, null, null, null, null, null, null, null), type);
    }

    /// <summary>Records <paramref name="type"/> for <paramref name="node"/> when it is known, and returns the node.</summary>
    internal static NodeAst Remember(NodeAst node, ExpressionType? type)
    {
        if (type is { } known)
            Types.AddOrUpdate(node, new StrongBox<ExpressionType>(known));

        return node;
    }

    /// <summary>The recorded type of <paramref name="node"/>, or null when it has none.</summary>
    internal static ExpressionType? TypeOf(NodeAst node) =>
        Types.TryGetValue(node, out StrongBox<ExpressionType>? box) ? box.Value : null;
}

/// <summary>
/// Carries the static type of a subquery's column out of its cursor, which an async iterator cannot
/// return. It is set when the first read binds the subquery, also when no row follows.
/// </summary>
internal sealed class SubqueryOutputType
{
    public ExpressionType? Type { get; set; }
}
