/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A <see cref="SetOperationSource"/> after both operands are bound and the result types are known.
///
/// <para><b>Rows are matched by position, not by name.</b> Column i of a left row and column i of a
/// right row are the same output column. The operands produce rows keyed by their own column names,
/// which differ in general, so the executor reads cell i of a left row at
/// <see cref="LeftColumns"/>[i].<see cref="DerivedColumnSchema.RowKey"/>, cell i of a right row at
/// <see cref="RightColumns"/>[i].<see cref="DerivedColumnSchema.RowKey"/>, and writes it to the output
/// row at <see cref="Columns"/>[i].<see cref="DerivedColumnSchema.RowKey"/>.</para>
///
/// <para><b>Each cell is widened to its output type before it is compared.</b> The comparison and
/// the hash of a value include its type, so an Int64 1 and a Float64 1.0 do not match until both are
/// Float64. <see cref="Columns"/> carries the type that each output column has after that step.</para>
/// </summary>
/// <param name="Source">The logical set operation.</param>
/// <param name="Left">The bound left operand.</param>
/// <param name="Right">The bound right operand.</param>
/// <param name="LeftColumns">The output schema of the left operand, in position order.</param>
/// <param name="RightColumns">The output schema of the right operand, in position order.</param>
/// <param name="Columns">The output schema of the set operation: the left names with the result types.</param>
/// <param name="Untyped">
/// Per output column, true when both operands are untyped there (a NULL literal or a placeholder on
/// each side). Such a column declares String, but a set operation over this one still treats it as
/// untyped, so <c>SELECT 1 UNION (SELECT NULL UNION SELECT NULL)</c> types as Int64 just as the
/// left-deep chain does. The parser rebalances long chains (see <c>SetOperationAst</c>), and this is
/// what keeps the result types of a rebalanced chain the same as those of the chain as written.
/// </param>
public sealed record BoundSetOperation(
    SetOperationSource Source,
    BoundSelectQuery Left,
    BoundSelectQuery Right,
    IReadOnlyList<DerivedColumnSchema> LeftColumns,
    IReadOnlyList<DerivedColumnSchema> RightColumns,
    IReadOnlyList<DerivedColumnSchema> Columns,
    IReadOnlyList<bool> Untyped)
{
    /// <summary>
    /// The set operation that <paramref name="operand"/> passes through unchanged, or null. An
    /// operand passes one through when it is the Select wrapper of a nested set operation: <c>*</c>
    /// over that one derived table, with no filter, grouping, ordering, limit or DISTINCT. The
    /// executor runs such an operand's set operation directly, and the type rules read its
    /// <see cref="Untyped"/> flags.
    /// </summary>
    public static BoundSetOperation? PassedThrough(BoundSelectQuery operand)
    {
        SelectQuery query = operand.Query;

        if (query.Source is not DerivedTableSource { SetOperation: not null }
            || operand.Sources.Count != 0
            || operand.DerivedSources.Count != 1
            || query.Projections.Count != 1
            || query.Projections[0].Expression.nodeType != SQLParser.NodeType.ExprAllFields
            || query.Where is not null
            || query.GroupBy is { Count: > 0 }
            || query.Having is not null
            || query.OrderBy is { Count: > 0 }
            || query.Limit is not null
            || query.Offset is not null
            || query.IsDistinct)
            return null;

        return operand.DerivedSources[0].SetOperation;
    }
}
