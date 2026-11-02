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
/// <para>Rules 1, 3 and 4 are <see cref="ResultTypeRule.Combine"/>, which CASE, COALESCE, IFNULL and
/// IF use as well, so a set operation and a CASE over the same two types agree. As there, a quoted
/// string literal in an operand's select list takes the date or datetime type of the other operand,
/// and is parsed when the statement is bound: <c>SELECT d FROM t UNION SELECT '2026-01-01'</c> is a
/// DATE column, and <see cref="Widen"/> converts the literal's cell, so DISTINCT, INTERSECT and EXCEPT
/// compare dates.</para>
///
/// <para>Two array columns also need element types that agree
/// (<see cref="ComparisonTypeRule.ArrayElementTypesAgree"/>). Both declare
/// <see cref="ColumnType.Array"/>, so rule 1 alone accepts <c>ARRAY&lt;Integer64&gt;</c> with
/// <c>ARRAY&lt;String&gt;</c>; the result column then holds arrays that no comparison can order, so a
/// DISTINCT or INTERSECT calls every pair unequal and an ORDER BY or MIN over the column fails inside
/// the sort. The check applies when both element types are known before the statement runs
/// (<see cref="DerivedColumnSchema.ElementType"/>); an array expression whose element type only its
/// value carries is not checked here.</para>
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
        NodeAst?[] leftLiterals = StringLiteralColumns(setOperation.Left, leftBound, leftColumns.Count);
        NodeAst?[] rightLiterals = StringLiteralColumns(setOperation.Right, rightBound, rightColumns.Count);

        DerivedColumnSchema[] columns = new DerivedColumnSchema[leftColumns.Count];
        bool[] untyped = new bool[leftColumns.Count];

        for (int i = 0; i < columns.Length; i++)
        {
            DerivedColumnSchema left = leftColumns[i];
            DerivedColumnSchema right = rightColumns[i];

            ColumnType type = ResultType(setOperation, i, left.Type, leftUntyped[i], leftLiterals[i], right.Type, rightUntyped[i], rightLiterals[i]);
            untyped[i] = leftUntyped[i] && rightUntyped[i];

            columns[i] = new DerivedColumnSchema(left.Name, type)
            {
                RowKey = left.RowKey,
                ElementType = type == ColumnType.Array ? ArrayElementType(setOperation, i, left, right) : null,
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
    public static ColumnValue Widen(ColumnValue value, ColumnType resultType) =>
        ResultTypeRule.Widen(value, resultType);

    /// <summary>
    /// The element type of an array output column: the known element type of either side, preferring
    /// one that is not <see cref="ColumnType.Null"/> (the type of <c>ARRAY[]</c>). Two known element
    /// types that do not agree fail, as two scalar types with no rule do (see the class summary).
    /// </summary>
    private static ColumnType? ArrayElementType(
        SetOperationSource setOperation,
        int position,
        DerivedColumnSchema left,
        DerivedColumnSchema right)
    {
        if (left.Type != ColumnType.Array || right.Type != ColumnType.Array)
            return left.ElementType ?? right.ElementType;

        if (left.ElementType is not { } leftElement || leftElement == ColumnType.Null)
            return right.ElementType ?? left.ElementType;

        if (right.ElementType is { } rightElement && !ComparisonTypeRule.ArrayElementTypesAgree(leftElement, rightElement))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Column {position + 1} of {setOperation.OperatorText} has type ARRAY<{leftElement}> in the left operand " +
                $"and ARRAY<{rightElement}> in the right operand, and the two cannot be combined");

        return leftElement;
    }

    private static ColumnType ResultType(
        SetOperationSource setOperation,
        int position,
        ColumnType left,
        bool leftUntyped,
        NodeAst? leftLiteral,
        ColumnType right,
        bool rightUntyped,
        NodeAst? rightLiteral)
    {
        leftUntyped |= left == ColumnType.Null;
        rightUntyped |= right == ColumnType.Null;

        if (leftUntyped && rightUntyped)
            return left == ColumnType.Null ? ColumnType.String : left;

        if (leftUntyped)
            return right;

        if (rightUntyped)
            return left;

        // A quoted string literal takes the date or datetime type of the other operand, and is parsed
        // now, so a malformed literal fails before any row is read (GoogleSQL literal coercion). The
        // executor converts the literal's cell when it widens the row (see Widen).
        if (leftLiteral is not null && TemporalComparison.IsTemporal(right))
            return ParsedLiteralType(leftLiteral, right);

        if (rightLiteral is not null && TemporalComparison.IsTemporal(left))
            return ParsedLiteralType(rightLiteral, left);

        // The rule CASE, COALESCE, IFNULL and IF use too: one type, two numbers, or a date with a datetime.
        if (ResultTypeRule.Combine(left, right) is { } combined)
            return combined;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Column {position + 1} of {setOperation.OperatorText} has type {left} in the left operand " +
            $"and {right} in the right operand, and the two cannot be combined");
    }

    private static ColumnType ParsedLiteralType(NodeAst literal, ColumnType target)
    {
        _ = ResultTypeRule.ParseTemporal(DML.SQLExecutorBaseCreator.UnquoteStringLiteral(literal.yytext ?? "''"), target);
        return target;
    }

    /// <summary>
    /// The quoted string literal at each output position of an operand, or null where the column is
    /// anything else. Only a literal written in the operand's own select list counts: a column that a
    /// nested set operation passes through has a type of its own, STRING for two literals, as in
    /// GoogleSQL. The positions follow <see cref="UntypedColumns"/>.
    /// </summary>
    private static NodeAst?[] StringLiteralColumns(SelectQuery operand, BoundSelectQuery bound, int columnCount)
    {
        NodeAst?[] literals = new NodeAst?[columnCount];

        if (BoundSetOperation.PassedThrough(bound) is not null)
            return literals;

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

            if (position < columnCount && target.nodeType == NodeType.String)
                literals[position] = target;

            position++;
        }

        return literals;
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
