/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The errors a statement can show before any row is read, so that they do not depend on the rows of
/// the table, a <c>LIMIT 0</c>, a <c>WHERE FALSE</c>, or a dead branch of an <c>AND</c> or a CASE.
/// <see cref="ConditionalResultTypes.Check"/> applies these rules to each node of each expression.
///
/// <para><b>Known types only.</b> A rule runs only when the statement shows the operand types: a
/// literal, a cast, a column, an operator over those, or the result of a subquery whose column is one of
/// those (<see cref="SubqueryResultTypes"/>). A parameter and a function result have a type that only the
/// value shows, so the evaluator checks them per row, and a NULL literal fits every rule.
/// Function arguments have no signature table, so a built-in function checks its own argument types
/// when it runs. No rule guesses, so a correct statement is never refused here.</para>
///
/// <para><b>One rule, two moments.</b> Each rule is the rule of the evaluator
/// (<see cref="SQLExecutorBaseCreator.EvalExpr"/>) with the same error code and text, through
/// <see cref="ComparisonTypeRule"/>, <see cref="MixedNumericComparison.ArithmeticResultType"/> and
/// <see cref="ConcatAndBitwiseOperators.ResultType"/>. A statement this class accepts can still fail per
/// row, and a rule here that is stricter than the evaluator is a defect. The evaluator keeps its own
/// checks for the types that only a value shows.</para>
/// </summary>
internal static class StaticOperandRules
{
    /// <summary>
    /// Applies the rule of <paramref name="node"/>, with the static types of its operands. Throws for a
    /// node that no row can make correct.
    /// </summary>
    internal static void Check(
        NodeAst node,
        ExpressionType? left,
        ExpressionType? right,
        ExpressionType? one,
        ExpressionType? two,
        ConditionalResultTypes.ColumnTypeLookup? lookup,
        bool allowAggregates)
    {
        switch (node.nodeType)
        {
            case NodeType.ExprFuncCall:
                CheckCall(node, lookup, allowAggregates);
                return;

            case NodeType.ExprEquals or NodeType.ExprNotEquals or NodeType.ExprLessThan or NodeType.ExprGreaterThan
                or NodeType.ExprLessEqualsThan or NodeType.ExprGreaterEqualsThan:
                RequireComparable(SQLExecutorBaseCreator.ComparisonSymbol(node.nodeType), node.leftAst, left, node.rightAst, right);
                return;

            case NodeType.ExprBetween:
                RequireComparable("BETWEEN", node.leftAst, left, node.extendedOne, one);
                RequireComparable("BETWEEN", node.leftAst, left, node.extendedTwo, two);
                return;

            case NodeType.ExprInMembership or NodeType.ExprNotInMembership:
                CheckMembership(node, left, lookup);
                return;

            case NodeType.ExprQuantifiedComparison:
                CheckQuantified(node, left, right);
                return;

            case NodeType.ExprCase:
                CheckCaseClauses(node, left, lookup);
                return;

            case NodeType.ExprAnd or NodeType.ExprOr:
                RequireBooleanOperands(node.nodeType == NodeType.ExprAnd ? "AND" : "OR", left, right);
                return;

            case NodeType.ExprNot:
                RequireBooleanOperand("NOT", left);
                return;

            case NodeType.ExprIsTrue or NodeType.ExprIsNotTrue or NodeType.ExprIsFalse or NodeType.ExprIsNotFalse:
                RequireBooleanOperand(BooleanTruthTest.Describe(node.nodeType), left);
                return;

            case NodeType.ExprAdd or NodeType.ExprSub or NodeType.ExprMult or NodeType.ExprDiv:
                CheckArithmetic(node.nodeType, left, right);
                return;

            case NodeType.ExprNegate:
                if (left is { Type: not ColumnType.Null } negated && !MixedNumericComparison.IsNumeric(negated.Type))
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"No matching signature for unary operator - for argument type: {negated.Type}");
                return;

            case NodeType.ExprConcat or NodeType.ExprBitAnd or NodeType.ExprBitOr or NodeType.ExprBitXor
                or NodeType.ExprShiftLeft or NodeType.ExprShiftRight:
                CheckConcatOrBitwise(node.nodeType, left, right);
                return;

            case NodeType.ExprBitNot:
                if (left is { Type: not (ColumnType.Null or ColumnType.Integer64 or ColumnType.Bytes) } complement)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"No matching signature for unary operator ~ for argument type: {complement.Type}");
                return;

            case NodeType.ExprLike or NodeType.ExprILike or NodeType.ExprRegexMatch or NodeType.ExprRegexMatchCi
                or NodeType.ExprRegexNotMatch or NodeType.ExprRegexNotMatchCi:
                CheckPatternMatch(node.nodeType, left, right);
                return;

            case NodeType.ExprSubscript:
                CheckSubscript(left, right);
                return;
        }
    }

    /// <summary>
    /// The error for a WHERE, HAVING or ON condition that is not a boolean. The text follows the CASE
    /// WHEN error, and a condition with a numeric type is refused as PostgreSQL refuses it.
    /// </summary>
    internal static CamusDBException ConditionNotBoolean(string clause, string type) =>
        new(CamusDBErrorCodes.InvalidInput, $"No matching signature for {clause} condition; expected Bool, got: {type}");

    /// <summary>
    /// Checks the name and the argument count of a call
    /// (<see cref="ScalarFunctionEvaluator.RequireCallable"/>), and the two built-in functions that
    /// compare their arguments with <see cref="ComparisonTypeRule"/>.
    /// </summary>
    private static void CheckCall(NodeAst call, ConditionalResultTypes.ColumnTypeLookup? lookup, bool allowAggregates)
    {
        ScalarFunctionEvaluator.RequireCallable(call, allowAggregates);

        string? name = call.leftAst?.yytext;

        if (string.Equals(name, "nullif", StringComparison.OrdinalIgnoreCase))
        {
            List<NodeAst> arguments = ConditionalResultTypes.Arguments(call.rightAst);
            RequireComparable(
                "=",
                arguments[0],
                ConditionalResultTypes.TypeOf(arguments[0], lookup),
                arguments[1],
                ConditionalResultTypes.TypeOf(arguments[1], lookup));
            return;
        }

        // `x LIKE p ESCAPE e` parses to this call, so a known wrong type is a mistake in a clause, not in a call.
        if (string.Equals(name, "like_escape", StringComparison.OrdinalIgnoreCase))
        {
            List<NodeAst> arguments = ConditionalResultTypes.Arguments(call.rightAst);

            for (int i = 0; i < arguments.Count; i++)
            {
                if (ConditionalResultTypes.TypeOf(arguments[i], lookup) is { Type: not (ColumnType.String or ColumnType.Null) } known)
                    throw ScalarFunctionArguments.WrongType(name!, i, known.Type, ColumnType.String);
            }

            return;
        }

        if (string.Equals(name, "array_contains", StringComparison.OrdinalIgnoreCase))
        {
            List<NodeAst> arguments = ConditionalResultTypes.Arguments(call.rightAst);

            if (ConditionalResultTypes.TypeOf(arguments[0], lookup) is { Type: ColumnType.Array, ElementType: not ColumnType.Null } array
                && ConditionalResultTypes.TypeOf(arguments[1], lookup) is { Type: not ColumnType.Array } value)
                ComparisonTypeRule.Require(name!, array.ElementType, value.Type);
        }
    }

    /// <summary>
    /// Refuses two known types with no comparison rule. A string literal beside a date or a datetime is
    /// parsed here, because the evaluator refuses the text for every row that reaches it.
    /// </summary>
    private static void RequireComparable(string op, NodeAst? leftNode, ExpressionType? left, NodeAst? rightNode, ExpressionType? right)
    {
        if (left is not { } leftType || right is not { } rightType)
            return;

        if (!ComparisonTypeRule.HasRule(leftType, rightType))
            throw ComparisonTypeRule.NoRule(op, leftType, rightType);

        if (rightType.IsStringLiteral && TemporalComparison.IsTemporal(leftType.Type))
            ParseTemporalLiteral(rightNode, leftType.Type);
        else if (leftType.IsStringLiteral && TemporalComparison.IsTemporal(rightType.Type))
            ParseTemporalLiteral(leftNode, rightType.Type);
    }

    private static void ParseTemporalLiteral(NodeAst? literal, ColumnType target)
    {
        if (literal is not null && QueryExpressionClassifier.UnwrapAlias(literal) is { nodeType: NodeType.String, yytext: { } raw })
            _ = TemporalComparison.ParseOperand(new ColumnValue(ColumnType.String, SQLExecutorBaseCreator.UnquoteStringLiteral(raw)), target);
    }

    /// <summary>
    /// <c>x IN (a, b)</c> is <c>x = a OR x = b</c>, and an item with no comparison rule fails whatever
    /// the other items hold, so each item is checked. The list has no recursion here, because a list
    /// that a subquery materialized can be long.
    /// </summary>
    private static void CheckMembership(NodeAst node, ExpressionType? left, ConditionalResultTypes.ColumnTypeLookup? lookup)
    {
        if (left is null)
            return;

        // The column of a subquery has a type even when it returned no value to compare.
        RequireComparable("IN", node.leftAst, left, null, SubqueryResultTypes.TypeOf(node));

        foreach (NodeAst item in SubqueryValueListAst.Items(node.rightAst))
            RequireComparable("IN", node.leftAst, left, item, ConditionalResultTypes.TypeOf(item, lookup));
    }

    /// <summary>
    /// The right operand of <c>x &lt; ANY (a)</c> must be an array, and each element must compare with
    /// <c>x</c>. A subquery operand is a value set, and the type of the subquery's column is what compares.
    /// </summary>
    private static void CheckQuantified(NodeAst node, ExpressionType? left, ExpressionType? right)
    {
        NodeType op = QuantifiedComparison.OperatorOf(node);

        if (QuantifiedComparisonEvaluator.HasValueSet(node))
        {
            RequireComparable(SQLExecutorBaseCreator.ComparisonSymbol(op), node.leftAst, left, null, SubqueryResultTypes.TypeOf(node.rightAst!));
            return;
        }

        if (right is not { } rightType || rightType.Type == ColumnType.Null)
            return;

        if (rightType.Type != ColumnType.Array)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"The right operand of {QuantifiedComparison.OperatorText(op)} " +
                $"{QuantifiedComparison.QuantifierOf(node)} must be an array or a subquery but is {rightType.Type}");

        if (left is { Type: not ColumnType.Array } leftType && rightType.ElementType != ColumnType.Null)
            ComparisonTypeRule.Require(SQLExecutorBaseCreator.ComparisonSymbol(op), leftType.Type, rightType.ElementType);
    }

    /// <summary>
    /// Each WHEN of a simple CASE compares with the operand, and each WHEN of a searched CASE must be a
    /// boolean. A later WHEN never runs when an earlier one matches, and that does not excuse it: the
    /// result types of the branches follow the same rule.
    /// </summary>
    private static void CheckCaseClauses(NodeAst node, ExpressionType? operand, ConditionalResultTypes.ColumnTypeLookup? lookup)
    {
        if (node.rightAst is null)
            return;

        bool searched = node.leftAst is null;

        foreach (NodeAst clause in SQLExecutorBaseCreator.EnumerateWhenClauses(node.rightAst))
        {
            ExpressionType? value = clause.leftAst is null ? null : ConditionalResultTypes.TypeOf(clause.leftAst, lookup);

            if (!searched)
            {
                RequireComparable("CASE", node.leftAst, operand, clause.leftAst, value);
                continue;
            }

            if (value is { Type: not (ColumnType.Bool or ColumnType.Null) } condition)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    ConditionalResultTypes.IsIf(node)
                        ? $"No matching signature for IF condition; expected Bool, got: {condition.Type}"
                        : $"No matching signature for CASE WHEN condition; expected Bool, got: {condition.Type}");
        }
    }

    /// <summary>
    /// Refuses a value whose static type the column does not accept. It is the rule of the write path: a
    /// value converts by <see cref="CastScalarFunctions.HasImplicitConversion"/>, and any other type fails
    /// in <see cref="Storage.Kv.RowSlotAdapter"/>, with the same code and text. A NULL value fits every
    /// column here, because NOT NULL has its own check. A string literal beside a type it converts to is
    /// converted now, because text that does not parse fails for every row that reaches it.
    /// </summary>
    internal static void RequireAssignable(NodeAst? value, ExpressionType type, TableColumnSchema column)
    {
        if (type.Type == ColumnType.Null)
            return;

        if (type.Type == column.Type)
        {
            if (type.Type == ColumnType.Array && column.ArrayElementType is { } target
                && type.ElementType != ColumnType.Null && type.ElementType != target
                && !CastScalarFunctions.HasImplicitConversion(type.ElementType, target))
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownType,
                    $"Type {type} cannot be assigned to {column.Name} (ARRAY<{target}>)");

            return;
        }

        if (!CastScalarFunctions.HasImplicitConversion(type.Type, column.Type))
            throw new CamusDBException(
                CamusDBErrorCodes.UnknownType,
                $"Type {type.Type} cannot be assigned to {column.Name} ({column.Type})");

        if (value is not null && QueryExpressionClassifier.UnwrapAlias(value) is { nodeType: NodeType.String, yytext: { } raw })
            _ = CastScalarFunctions.CoerceToColumnType(new ColumnValue(ColumnType.String, SQLExecutorBaseCreator.UnquoteStringLiteral(raw)), column.Type);
    }

    private static void RequireBooleanOperands(string op, ExpressionType? left, ExpressionType? right)
    {
        if (left is { Type: not (ColumnType.Bool or ColumnType.Null) } || right is { Type: not (ColumnType.Bool or ColumnType.Null) })
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"No matching signature for operator {op} for argument types: {TypeName(left)}, {TypeName(right)}");
    }

    private static void RequireBooleanOperand(string op, ExpressionType? operand)
    {
        if (operand is { Type: not (ColumnType.Bool or ColumnType.Null) } type)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"No matching signature for operator {op} for argument type: {type}");
    }

    /// <summary>
    /// The arithmetic rule of the evaluator. A NULL literal gives NULL with any operand, and
    /// <c>jsonb - x</c> removes keys or an element and is not arithmetic.
    /// </summary>
    private static void CheckArithmetic(NodeType op, ExpressionType? left, ExpressionType? right)
    {
        if (left is not { } leftType || right is not { } rightType)
            return;

        if (leftType.Type == ColumnType.Null || rightType.Type == ColumnType.Null)
            return;

        if (op == NodeType.ExprSub && leftType.Type == ColumnType.Jsonb)
            return;

        if (MixedNumericComparison.ArithmeticResultType(leftType.Type, rightType.Type) is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"No matching signature for operator {SQLExecutorBaseCreator.ArithmeticSymbol(op)} for argument types: {leftType.Type}, {rightType.Type}");
    }

    private static void CheckConcatOrBitwise(NodeType op, ExpressionType? left, ExpressionType? right)
    {
        if (left is not { } leftType || right is not { } rightType)
            return;

        if (leftType.Type == ColumnType.Null || rightType.Type == ColumnType.Null)
            return;

        // The evaluator tests in this order: a document on either side, then an array, then the type table.
        if (op == NodeType.ExprConcat
            && leftType.Type != ColumnType.Jsonb && rightType.Type != ColumnType.Jsonb
            && (leftType.Type == ColumnType.Array || rightType.Type == ColumnType.Array))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Operator || does not support array operands");

        if (ConcatAndBitwiseOperators.ResultType(op, leftType.Type, rightType.Type) is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"No matching signature for operator {ConcatAndBitwiseOperators.Symbol(op)} for argument types: {leftType.Type}, {rightType.Type}");
    }

    /// <summary>
    /// A pattern match takes two strings. The evaluator gives this error the code
    /// <see cref="CamusDBErrorCodes.InvalidAstStmt"/>, so the same statement keeps one code.
    /// </summary>
    private static void CheckPatternMatch(NodeType op, ExpressionType? left, ExpressionType? right)
    {
        if (left is { Type: not (ColumnType.String or ColumnType.Null) } || right is { Type: not (ColumnType.String or ColumnType.Null) })
        {
            string symbol = op switch
            {
                NodeType.ExprLike => "LIKE",
                NodeType.ExprILike => "ILIKE",
                _ => "~",
            };

            throw new CamusDBException(
                CamusDBErrorCodes.InvalidAstStmt,
                $"No matching signature for operator {symbol} for argument types: {TypeName(left)}, {TypeName(right)}");
        }
    }

    private static void CheckSubscript(ExpressionType? array, ExpressionType? index)
    {
        if (array is { Type: not (ColumnType.Array or ColumnType.Null) } arrayType)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Cannot subscript a value of type {arrayType.Type}; only an array can be subscripted");

        if (index is { Type: not (ColumnType.Integer64 or ColumnType.Null) } indexType)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"An array subscript must be an integer, got {indexType.Type}");
    }

    /// <summary>The name of an operand type in an error. An operand with no static type shows as a parameter or a call does: unknown.</summary>
    private static string TypeName(ExpressionType? type) => type?.ToString() ?? "unknown";
}
