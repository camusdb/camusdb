/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Refuses a division by zero that the statement alone shows, before any row is read, so
/// <c>SELECT 1 / 0 FROM t</c> fails the same way on an empty table, under <c>WHERE FALSE</c> and with
/// rows. The rule is PostgreSQL's, where constant folding raises this error at plan time.
///
/// <para><b>The rule.</b> A division fails when both operands are constant and the divisor is zero. A
/// column, a parameter, a subquery or a function call is not constant, so <c>v / 0</c> stays a row-time
/// error. A branch that a constant condition removes is not evaluated: <c>FALSE AND …</c>,
/// <c>TRUE OR …</c>, a CASE or IF with a constant condition, and a COALESCE after a constant that is not
/// NULL. A condition that is not constant removes nothing, so <c>CASE WHEN v &gt; 0 THEN 1 / 0 ELSE 1
/// END</c> fails even when no row selects that branch. A statement that uses this form must guard over a
/// value it reads: <c>CASE WHEN v &lt;&gt; 0 THEN 1 / v END</c> runs.</para>
///
/// <para><b>Why this is not part of <see cref="ConditionalResultTypes.Check"/>.</b> That walk also
/// runs on a stored definition at DDL: a CHECK condition, and a view body. PostgreSQL accepts those,
/// and the statement that uses them fails. So this class is called only from the positions that run a
/// statement. A view body binds through the same pipeline as a query, so
/// <see cref="SuspendForStoredDefinition"/> turns the check off while CREATE VIEW binds it.</para>
///
/// <para><b>Why callers pass the expression from before the subquery rewrite.</b> The rewrite replaces
/// a subquery with a literal, so <c>1 / (SELECT 0 FROM t)</c> would look constant. A subquery is not
/// constant here, and it reaches its own check when it binds.</para>
///
/// <para><b>Values come from the evaluator.</b> A constant goes through
/// <see cref="SQLExecutorBaseCreator.EvalExpr"/>, so it has the value that it has for a row, for each
/// numeric type and for a NULL operand. The evaluator also takes the same dead branches, in the same
/// order: <c>1 / 0 = 1 AND FALSE</c> fails and <c>FALSE AND 1 / 0 = 1</c> does not. Any other value
/// error in a constant, such as an integer overflow, stays a row-time error.</para>
///
/// <para><b>Cost.</b> An expression with no division returns after one pass with no allocation. With a
/// division, <see cref="Scan"/> visits each node once, and it evaluates only the largest constant
/// expressions, so a long chain stays linear. Only a shape that nests a constant AND or OR inside a
/// comparison, many levels deep, evaluates a part twice. Node types outside the list in
/// <see cref="Scan"/> count as not constant: that can refuse a division whose condition a richer
/// folding would remove, and it can never evaluate something with an effect.</para>
/// </summary>
internal static class ConstantDivisionCheck
{
    /// <summary>What the walk knows about the value of an expression.</summary>
    private enum Shape : byte
    {
        /// <summary>A row, a parameter or a function decides the value.</summary>
        Varying,

        /// <summary>The value is fixed and not computed yet. It cannot raise a division by zero.</summary>
        Constant,

        /// <summary>The value is fixed and not computed yet, and the computation holds a division.</summary>
        ConstantWithDivision,

        /// <summary>The value is computed: TRUE.</summary>
        True,

        /// <summary>The value is computed: FALSE.</summary>
        False,

        /// <summary>The value is computed: a NULL boolean.</summary>
        Unknown,
    }

    private static readonly AsyncLocal<bool> Suspended = new();

    private static readonly Dictionary<string, ColumnValue> EmptyRow = new();

    /// <summary>
    /// Turns the check off in this asynchronous flow until the scope ends. Only CREATE VIEW uses it.
    /// The flow includes each subquery that the body runs while it binds, and a view that the body
    /// expands. Reads of the flag happen only for an expression that holds a division.
    /// </summary>
    internal static SuspendScope SuspendForStoredDefinition() => new(Suspended.Value);

    /// <summary>Restores the earlier state of <see cref="SuspendForStoredDefinition"/>.</summary>
    internal readonly struct SuspendScope : IDisposable
    {
        private readonly bool previous;

        internal SuspendScope(bool previous)
        {
            this.previous = previous;
            Suspended.Value = true;
        }

        public void Dispose() => Suspended.Value = previous;
    }

    /// <summary>
    /// Checks every expression position of one SELECT level. A nested SELECT has its own bind, so its
    /// expressions are not visited here.
    /// </summary>
    /// <param name="query">The query as the statement wrote it, before the subquery rewrite. For an EXISTS
    /// body whose output <see cref="ExistsBody"/> discards, this is the body without that output, so a
    /// constant division there does not fail. PostgreSQL fails on one when the whole body must run.</param>
    internal static void CheckSelect(SelectQuery query)
    {
        foreach (ProjectionItem projection in query.Projections)
            Check(projection.Expression);

        if (query.OrderBy is not null)
            foreach (OrderByItem item in query.OrderBy)
                Check(item.Expression);

        Check(query.Where?.Expression);
        CheckSource(query.Source);

        if (query.GroupBy is not null)
            foreach (NodeAst expression in query.GroupBy)
                Check(expression);

        Check(query.Having?.Expression);
        Check(query.Limit);
        Check(query.Offset);
    }

    private static void CheckSource(QuerySource source)
    {
        switch (source)
        {
            case JoinSource join:
                CheckSource(join.Left);
                CheckSource(join.Right);
                Check(join.OnPredicate);
                break;

            case DerivedTableSource { Function: { } function }:
                foreach (NodeAst argument in function.Arguments)
                    Check(argument);
                break;
        }
    }

    /// <summary>
    /// Throws the error of the evaluator when <paramref name="expression"/> holds a division by zero
    /// that must fail with no row. A SELECT runs it after its type checks, so a statement with both
    /// mistakes reports the type error, as PostgreSQL does. An UPDATE or a DELETE runs it first, because
    /// its type checks see the expression after the subquery rewrite. The statement fails either way.
    /// </summary>
    internal static void Check(NodeAst? expression)
    {
        if (expression is null || !ContainsDivision(expression) || Suspended.Value)
            return;

        NodeAst target = QueryExpressionClassifier.UnwrapAlias(expression);
        Flush(target, Scan(target));
    }

    private static bool ContainsDivision(NodeAst? node)
    {
        if (node is null)
            return false;

        switch (node.nodeType)
        {
            case NodeType.ExprDiv:
                return true;

            case NodeType.Select or NodeType.ExprScalarSubquery or NodeType.ExprExistsSubquery or NodeType.ExprExistsCorrelated:
                return false;

            case NodeType.ExprInSubquery or NodeType.ExprNotInSubquery or NodeType.ExprCast or NodeType.ExprAlias:
                return ContainsDivision(node.leftAst);
        }

        return (node.nodeType != NodeType.ExprFuncCall && ContainsDivision(node.leftAst))
            || ContainsDivision(node.rightAst)
            || ContainsDivision(node.extendedOne)
            || ContainsDivision(node.extendedTwo)
            || ContainsDivision(node.extendedThree)
            || ContainsDivision(node.extendedFour)
            || ContainsDivision(node.extendedFive)
            || ContainsDivision(node.extendedSix)
            || ContainsDivision(node.extendedSeven);
    }

    /// <summary>
    /// Returns the shape of <paramref name="node"/>. The walk raises the error for each constant
    /// division under a node that varies, and it does not visit a branch that a constant removes. A
    /// constant division under a constant parent waits, because the parent may be a CASE or an AND that
    /// never reaches it. The depth is capped after parsing (<see cref="StatementDepthGuard"/>), and the
    /// larger node shapes have their own methods, so that this frame stays small.
    /// </summary>
    private static Shape Scan(NodeAst node)
    {
        switch (node.nodeType)
        {
            case NodeType.Integer or NodeType.Float or NodeType.String or NodeType.Bool or NodeType.Null
                or NodeType.BytesLiteral or NodeType.ObjectIdLiteral:
                return Shape.Constant;

            case NodeType.Select or NodeType.ExprScalarSubquery or NodeType.ExprExistsSubquery or NodeType.ExprExistsCorrelated:
                return Shape.Varying;

            case NodeType.ExprInSubquery or NodeType.ExprNotInSubquery:
                Live(node.leftAst);
                return Shape.Varying;

            case NodeType.ExprAlias:
                return Scan(node.leftAst!);

            case NodeType.ExprAnd:
                return ScanLogic(node, decisive: Shape.False);

            case NodeType.ExprOr:
                return ScanLogic(node, decisive: Shape.True);

            case NodeType.ExprCase:
                return ScanCase(node);

            case NodeType.ExprCast:
                return ScanCast(node);

            case NodeType.ExprDiv or NodeType.ExprAdd or NodeType.ExprSub or NodeType.ExprMult or NodeType.ExprNegate
                or NodeType.ExprEquals or NodeType.ExprNotEquals or NodeType.ExprLessThan or NodeType.ExprGreaterThan
                or NodeType.ExprGreaterEqualsThan or NodeType.ExprLessEqualsThan or NodeType.ExprNot
                or NodeType.ExprIsNull or NodeType.ExprIsNotNull or NodeType.ExprIsTrue or NodeType.ExprIsNotTrue
                or NodeType.ExprIsFalse or NodeType.ExprIsNotFalse or NodeType.ExprConcat or NodeType.ExprBitAnd
                or NodeType.ExprBitOr or NodeType.ExprBitXor or NodeType.ExprBitNot or NodeType.ExprShiftLeft
                or NodeType.ExprShiftRight:
                return ScanOperator(node);

            case NodeType.ExprFuncCall when ConditionalResultTypes.IsCoalesceCall(node):
                return ScanCoalesce(node);

            default:
                return ScanOpaque(node);
        }
    }

    /// <summary>
    /// A node that this class does not fold: a column, a parameter, a function call. Each child is an
    /// operand that always counts. The left child of a call is the function name.
    /// </summary>
    private static Shape ScanOpaque(NodeAst node)
    {
        if (node.nodeType != NodeType.ExprFuncCall)
            Live(node.leftAst);

        Live(node.rightAst);
        Live(node.extendedOne);
        Live(node.extendedTwo);
        Live(node.extendedThree);
        Live(node.extendedFour);
        Live(node.extendedFive);
        Live(node.extendedSix);
        Live(node.extendedSeven);

        return Shape.Varying;
    }

    private static Shape ScanCast(NodeAst node)
    {
        Shape operand = Scan(node.leftAst!);

        return operand switch
        {
            Shape.Varying => Shape.Varying,
            Shape.ConstantWithDivision => Shape.ConstantWithDivision,
            _ => Shape.Constant,
        };
    }

    private static Shape ScanOperator(NodeAst node)
    {
        Shape left = node.leftAst is null ? Shape.Constant : Scan(node.leftAst);
        Shape right = node.rightAst is null ? Shape.Constant : Scan(node.rightAst);

        if (left != Shape.Varying && right != Shape.Varying)
        {
            return node.nodeType == NodeType.ExprDiv || left == Shape.ConstantWithDivision || right == Shape.ConstantWithDivision
                ? Shape.ConstantWithDivision
                : Shape.Constant;
        }

        if (node.leftAst is not null)
            Flush(node.leftAst, left);

        if (node.rightAst is not null)
            Flush(node.rightAst, right);

        return Shape.Varying;
    }

    /// <summary>
    /// AND with <paramref name="decisive"/> FALSE, or OR with <paramref name="decisive"/> TRUE. Left to
    /// right, as the evaluator and PostgreSQL do: a decisive constant ends the walk, so the operands
    /// after it do not count, and the operands before it do.
    /// </summary>
    private static Shape ScanLogic(NodeAst node, Shape decisive)
    {
        Shape left = Known(node.leftAst!, Scan(node.leftAst!));
        if (left == decisive)
            return decisive;

        Shape right = Known(node.rightAst!, Scan(node.rightAst!));
        if (right == decisive)
            return decisive;

        if (left == Shape.Varying || right == Shape.Varying)
            return Shape.Varying;

        Shape neutral = decisive == Shape.False ? Shape.True : Shape.False;
        return left == neutral && right == neutral ? neutral : Shape.Unknown;
    }

    /// <summary>
    /// A CASE, or an IF, which the parser builds as a searched CASE. A clause whose condition is
    /// constant and not TRUE cannot run. A clause whose condition is constant TRUE ends the walk. A
    /// condition that varies makes every later branch count, and the whole expression varies. A CASE
    /// with only constant conditions has the shape of the one branch that runs.
    /// </summary>
    private static Shape ScanCase(NodeAst node)
    {
        NodeAst? operand = node.leftAst;
        Shape operandShape = operand is null ? Shape.Constant : Scan(operand);

        if (operand is not null)
            Flush(operand, operandShape);

        bool varying = operandShape == Shape.Varying;

        foreach (NodeAst clause in SQLExecutorBaseCreator.EnumerateWhenClauses(node.rightAst!))
        {
            Shape condition = WhenCondition(operand, operandShape, clause.leftAst!);

            if (condition is Shape.False or Shape.Unknown)
                continue;

            if (condition == Shape.True)
                return Close(clause.rightAst!, varying);

            varying = true;
            Live(clause.rightAst);
        }

        if (node.extendedOne is { } otherwise)
            return Close(otherwise, varying);

        return varying ? Shape.Varying : Shape.Constant;
    }

    private static Shape Close(NodeAst branch, bool varying)
    {
        Shape shape = Scan(branch);

        if (!varying)
            return shape;

        Flush(branch, shape);
        return Shape.Varying;
    }

    /// <summary>The known truth of one WHEN, or <see cref="Shape.Varying"/> when no constant decides it.</summary>
    private static Shape WhenCondition(NodeAst? operand, Shape operandShape, NodeAst when)
    {
        Shape shape = Scan(when);

        if (operand is null)
            return Known(when, shape);

        if (operandShape == Shape.Varying || shape == Shape.Varying)
        {
            Flush(when, shape);
            return Shape.Varying;
        }

        // A simple CASE compares its operand with each WHEN value by the rule of `=`.
        NodeAst comparison = new(NodeType.ExprEquals, operand, when, null, null, null, null, null, null);
        return Known(comparison, Shape.ConstantWithDivision);
    }

    /// <summary>
    /// COALESCE, IFNULL and NVL. A constant that is not NULL ends the walk, as it ends the evaluator, so
    /// an argument after it does not count.
    /// </summary>
    private static Shape ScanCoalesce(NodeAst node)
    {
        List<NodeAst> arguments = ConditionalResultTypes.Arguments(node.rightAst);
        bool varying = false;

        for (int i = 0; i < arguments.Count; i++)
        {
            NodeAst argument = arguments[i];
            Shape shape = Scan(argument);

            if (shape == Shape.Varying || Evaluate(argument) is not { } value)
            {
                varying = true;
                continue;
            }

            if (value.Type == Catalogs.Models.ColumnType.Null)
                continue;

            break;
        }

        return varying ? Shape.Varying : Shape.Constant;
    }

    /// <summary>Visits an operand that always counts, and raises its constant division.</summary>
    private static void Live(NodeAst? node)
    {
        if (node is not null)
            Flush(node, Scan(node));
    }

    private static void Flush(NodeAst node, Shape shape)
    {
        if (shape == Shape.ConstantWithDivision)
            _ = Evaluate(node);
    }

    /// <summary>
    /// The truth of a boolean expression. A constant is computed here, so a division in it raises.
    /// Returns <see cref="Shape.Varying"/> when the value is not a boolean, or another value error
    /// stops the computation.
    /// </summary>
    private static Shape Known(NodeAst node, Shape shape)
    {
        if (shape is Shape.Varying or Shape.True or Shape.False or Shape.Unknown)
            return shape;

        return Evaluate(node) switch
        {
            { Type: Catalogs.Models.ColumnType.Bool, BoolValue: true } => Shape.True,
            { Type: Catalogs.Models.ColumnType.Bool, BoolValue: false } => Shape.False,
            { Type: Catalogs.Models.ColumnType.Null } => Shape.Unknown,
            _ => Shape.Varying,
        };
    }

    /// <summary>
    /// Computes a constant with the evaluator. A division by zero propagates. Any other value error
    /// returns null: it is not in the scope of this check, so it stays an error of the row that
    /// reaches it.
    /// </summary>
    private static ColumnValue? Evaluate(NodeAst node)
    {
        try
        {
            return SQLExecutorBaseCreator.EvalExpr(node, EmptyRow, null);
        }
        catch (CamusDBException ex) when (ex.Message != NumericMath.DivisionByZeroMessage)
        {
            return null;
        }
    }
}
