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

/// <summary>
/// The output columns of a set operation: the names of the left operand, and one result type per
/// position from the declared types of both operands.
///
/// <para><b>Type rules, in order.</b></para>
/// <list type="number">
/// <item>The same type on both sides gives that type.</item>
/// <item>An untyped side gives the type of the other side. A NULL literal and a placeholder are
/// untyped: their declared type is a fallback, not a type, and a placeholder's value is only known
/// when the statement runs. A column that is untyped on both sides declares String and stays untyped
/// for a set operation over this one (<see cref="BoundSetOperation.Untyped"/>).</item>
/// <item>Two numeric types give <see cref="MixedNumericComparison.ArithmeticResultType"/>, the rule
/// the arithmetic operators use.</item>
/// <item>A date with a datetime gives datetime (<see cref="TemporalComparison.WidenDate"/>).</item>
/// <item>Any other pair fails with <see cref="CamusDBErrorCodes.InvalidInput"/>.</item>
/// </list>
///
/// <para>A different number of columns also fails, before any row is read. The result types are a
/// promise the executor keeps: it widens every cell to its output type before rows are compared
/// (see <see cref="BoundSetOperation"/>).</para>
/// </summary>
internal static class SetOperationSchema
{
    public static (IReadOnlyList<DerivedColumnSchema> Columns, IReadOnlyList<bool> Untyped) Resolve(
        SetOperationSource setOperation,
        BoundSelectQuery leftBound,
        IReadOnlyList<DerivedColumnSchema> leftColumns,
        BoundSelectQuery rightBound,
        IReadOnlyList<DerivedColumnSchema> rightColumns)
    {
        if (leftColumns.Count != rightColumns.Count)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Each operand of {setOperation.OperatorText} must have the same number of columns; " +
                $"the left operand has {leftColumns.Count} and the right operand has {rightColumns.Count}");

        IReadOnlyList<bool> leftUntyped = UntypedColumns(setOperation.Left, leftBound, leftColumns.Count);
        IReadOnlyList<bool> rightUntyped = UntypedColumns(setOperation.Right, rightBound, rightColumns.Count);

        DerivedColumnSchema[] columns = new DerivedColumnSchema[leftColumns.Count];
        bool[] untyped = new bool[leftColumns.Count];

        for (int i = 0; i < columns.Length; i++)
        {
            DerivedColumnSchema left = leftColumns[i];
            DerivedColumnSchema right = rightColumns[i];

            ColumnType type = ResultType(setOperation, i, left.Type, leftUntyped[i], right.Type, rightUntyped[i]);
            untyped[i] = leftUntyped[i] && rightUntyped[i];

            columns[i] = new DerivedColumnSchema(left.Name, type)
            {
                RowKey = left.RowKey,
                ElementType = type == ColumnType.Array ? left.ElementType ?? right.ElementType : null,
            };
        }

        return (columns, untyped);
    }

    /// <summary>
    /// The type the cells of one output column are widened to before they are compared. Only a
    /// numeric or temporal widening changes a value; any other cell keeps its type, and a cell whose
    /// type differs from its column's (an untyped placeholder bound to another type) simply never
    /// equals a cell of the column's type.
    /// </summary>
    public static ColumnValue Widen(ColumnValue value, ColumnType resultType)
    {
        if (value.Type == resultType || value.Type == ColumnType.Null)
            return value;

        if (resultType == ColumnType.DateTime)
            return TemporalComparison.WidenDate(value);

        return NumericWidening.Widen(value, resultType);
    }

    private static ColumnType ResultType(
        SetOperationSource setOperation,
        int position,
        ColumnType left,
        bool leftUntyped,
        ColumnType right,
        bool rightUntyped)
    {
        leftUntyped |= left == ColumnType.Null;
        rightUntyped |= right == ColumnType.Null;

        if (leftUntyped && rightUntyped)
            return left == ColumnType.Null ? ColumnType.String : left;

        if (leftUntyped)
            return right;

        if (rightUntyped || left == right)
            return left;

        if (MixedNumericComparison.ArithmeticResultType(left, right) is { } numeric)
            return numeric;

        if (left is ColumnType.Date or ColumnType.DateTime && right is ColumnType.Date or ColumnType.DateTime)
            return ColumnType.DateTime;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Column {position + 1} of {setOperation.OperatorText} has type {left} in the left operand " +
            $"and {right} in the right operand, and the two cannot be combined");
    }

    /// <summary>
    /// Which output columns of an operand are untyped. An operand that passes a nested set operation
    /// through has that one's flags. Otherwise a <c>*</c> expands to table or derived columns, which
    /// always have a type, so only a projection of its own can be untyped; the count of columns each
    /// <c>*</c> adds follows from the total, since every <c>*</c> in one list expands to the same
    /// columns.
    /// </summary>
    private static IReadOnlyList<bool> UntypedColumns(SelectQuery operand, BoundSelectQuery bound, int columnCount)
    {
        if (BoundSetOperation.PassedThrough(bound) is { } nested && nested.Untyped.Count == columnCount)
            return nested.Untyped;

        bool[] untyped = new bool[columnCount];

        int stars = 0;
        foreach (ProjectionItem projection in operand.Projections)
        {
            if (QueryExpressionClassifier.UnwrapAlias(projection.Expression).nodeType == NodeType.ExprAllFields)
                stars++;
        }

        int perStar = stars == 0 ? 0 : (columnCount - (operand.Projections.Count - stars)) / stars;
        int position = 0;

        foreach (ProjectionItem projection in operand.Projections)
        {
            NodeAst target = QueryExpressionClassifier.UnwrapAlias(projection.Expression);

            if (target.nodeType == NodeType.ExprAllFields)
            {
                position += perStar;
                continue;
            }

            if (position < columnCount)
                untyped[position] = target.nodeType is NodeType.Null or NodeType.Placeholder;

            position++;
        }

        return untyped;
    }
}
