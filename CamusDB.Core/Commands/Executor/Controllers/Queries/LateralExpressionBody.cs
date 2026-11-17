/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// A <c>LATERAL</c> body that only names expressions of the left row, such as
/// <c>CROSS JOIN LATERAL (SELECT p.y - 100.0 AS denom) AS d</c>. Such a body gives exactly one row for each
/// left row, so the nested-loop join evaluates its expressions on the values of that row
/// (<see cref="Evaluate"/>) and runs no subquery: a subquery run costs a query ticket, schema pins and a
/// one-row query pipeline for each left row.
///
/// <para><b>Which bodies qualify.</b> <see cref="TryCreate"/> accepts a body that binds one time
/// (<see cref="CorrelatedSubqueryBody.Bound"/>) and is one SELECT with no <c>FROM</c>, <c>WHERE</c>,
/// <c>GROUP BY</c>, <c>HAVING</c>, <c>ORDER BY</c>, <c>LIMIT</c>, <c>OFFSET</c> or <c>DISTINCT</c>, and no
/// volatile call. Each select-list item must be made of literals, placeholders, operators, <c>CAST</c>,
/// <c>CASE</c> and scalar calls only (<see cref="IsDirectlyEvaluable"/>). A subquery, a window call, a
/// column of the body itself or any other node keeps the subquery run.</para>
///
/// <para><b>The result is the same.</b> The bind of the body ran the statement checks (types, a constant
/// division by zero) one time, as before. The select-list items are the bound items, and they are
/// evaluated by the evaluator the projection uses (<see cref="SqlExecutor.EvalExpr(NodeAst, IReadOnlyDictionary{string, ColumnValue}, Dictionary{string, ColumnValue}?, QueryRowNameResolver?)"/>),
/// in select-list order, with the parameters a subquery run gets: the statement parameters and the left
/// values under their hidden placeholder names. So an evaluation error comes from the same row and the same
/// item. The cells are keyed by the row keys of the derived table columns, so the
/// <see cref="DerivedTableExecutor"/> reads them as it reads the cells of a run. A volatile body keeps the
/// subquery run, which evaluates it one time for each left row too.</para>
/// </summary>
internal sealed class LateralExpressionBody
{
    private readonly CorrelatedSubqueryBody body;

    /// <summary>The select-list items of the body, alias removed, in select-list order.</summary>
    private readonly NodeAst[] expressions;

    /// <summary>The row key of each item's cell: the row key of the derived table column at that position.</summary>
    private readonly string[] rowKeys;

    /// <summary>The output column names of the body, for EXPLAIN.</summary>
    public IReadOnlyList<string> ColumnNames { get; }

    /// <summary>
    /// The numeric columns of the derived table, whose cells are widened to the declared type after each
    /// evaluation (see <see cref="NumericWidening"/>), computed one time.
    /// </summary>
    public (string RowKey, ColumnType Type)[] NumericColumns { get; }

    private LateralExpressionBody(
        CorrelatedSubqueryBody body,
        NodeAst[] expressions,
        string[] rowKeys,
        string[] columnNames,
        (string RowKey, ColumnType Type)[] numericColumns)
    {
        this.body = body;
        this.expressions = expressions;
        this.rowKeys = rowKeys;
        ColumnNames = columnNames;
        NumericColumns = numericColumns;
    }

    /// <summary>
    /// The direct evaluation of <paramref name="body"/>, or null when the body does not qualify (see the
    /// class summary). <paramref name="columns"/> are the columns of the derived table, after the column
    /// aliases of the <c>AS v (a, b)</c> form.
    /// </summary>
    public static LateralExpressionBody? TryCreate(CorrelatedSubqueryBody body, IReadOnlyList<DerivedColumnSchema> columns)
    {
        if (body.Bound is null)
            return null;

        NodeAst select = body.Select;

        if (select.nodeType != NodeType.Select
            || select.leftAst is null
            || select.rightAst is not null      // FROM
            || select.extendedOne is not null   // WHERE
            || select.extendedTwo is not null   // ORDER BY
            || select.extendedThree is not null // LIMIT
            || select.extendedFour is not null  // OFFSET
            || select.extendedFive is not null  // GROUP BY
            || select.extendedSix is not null   // HAVING
            || select.extendedSeven is not null
            || select.yytext is not null)       // DISTINCT
        {
            return null;
        }

        if (ScalarFunctionEvaluator.ContainsVolatileFunction(select))
            return null;

        IReadOnlyList<ProjectionItem> projections = body.Checked.Query.Projections;

        if (projections.Count == 0 || projections.Count != columns.Count)
            return null;

        NodeAst[] expressions = new NodeAst[projections.Count];
        string[] rowKeys = new string[projections.Count];
        string[] columnNames = new string[projections.Count];

        for (int i = 0; i < projections.Count; i++)
        {
            if (columns[i].Hidden)
                return null;

            NodeAst expression = QueryExpressionClassifier.UnwrapAlias(projections[i].Expression);

            if (!IsDirectlyEvaluable(expression))
                return null;

            expressions[i] = expression;
            rowKeys[i] = columns[i].RowKey;
            columnNames[i] = columns[i].Name;
        }

        return new LateralExpressionBody(body, expressions, rowKeys, columnNames, NumericWidening.NumericColumns(columns));
    }

    /// <summary>The row that the items are evaluated against: they read no column, so it is empty and never written.</summary>
    private static readonly IReadOnlyDictionary<string, ColumnValue> NoColumns = new Dictionary<string, ColumnValue>(0);

    /// <summary>
    /// The one row of the body for the left values <paramref name="outerValues"/>, which
    /// <see cref="CorrelatedSubqueryBody.Outer"/> read from one left row.
    /// </summary>
    /// <param name="parameterBuffer">The parameter map of the caller for this body, or null for a new map.</param>
    public QueryResultRow Evaluate(
        ColumnValue[] outerValues,
        Dictionary<string, ColumnValue>? statementParameters,
        ParameterBuffer? parameterBuffer = null)
    {
        Dictionary<string, ColumnValue> parameters = parameterBuffer is null
            ? body.ParametersFor(outerValues, statementParameters)
            : parameterBuffer.For(body, outerValues, statementParameters);

        Dictionary<string, ColumnValue> cells = new(expressions.Length, StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < expressions.Length; i++)
            cells[rowKeys[i]] = SqlExecutor.EvalExpr(expressions[i], NoColumns, parameters);

        return new QueryResultRow(new ObjectIdValue(), cells);
    }

    /// <summary>
    /// The parameter map of an expression-only body for one nested-loop join: the statement parameters and the
    /// hidden placeholders of the left values. The join builds it for its first left row, and for each later
    /// row only the placeholder values change, so a row does not copy the statement parameters into a new map.
    ///
    /// <para>The reuse is safe because <see cref="Evaluate"/> reads the map synchronously and no cell keeps a
    /// reference to it, and because one buffer belongs to one join operator of one execution, which evaluates
    /// one left row at a time. A subquery run keeps a map of its own for each row: its rows stream lazily, so a
    /// later row must not change the values that an earlier run still reads.</para>
    /// </summary>
    internal sealed class ParameterBuffer
    {
        private Dictionary<string, ColumnValue>? parameters;

        private CorrelatedSubqueryBody? owner;

        private Dictionary<string, ColumnValue>? statementParameters;

        internal Dictionary<string, ColumnValue> For(
            CorrelatedSubqueryBody body,
            ColumnValue[] outerValues,
            Dictionary<string, ColumnValue>? statementParameters)
        {
            if (parameters is not null && ReferenceEquals(owner, body) && ReferenceEquals(this.statementParameters, statementParameters))
            {
                body.SetOuterValues(parameters, outerValues);
                return parameters;
            }

            parameters = body.ParametersFor(outerValues, statementParameters);
            owner = body;
            this.statementParameters = statementParameters;
            return parameters;
        }
    }

    /// <summary>
    /// True when <paramref name="expression"/> holds only nodes that the evaluator computes from literals and
    /// parameters: no column name, subquery, window call or <c>*</c>. The name of a called function and the
    /// target type of a <c>CAST</c> are not read as values.
    /// </summary>
    internal static bool IsDirectlyEvaluable(NodeAst expression)
    {
        Stack<NodeAst> pending = new();
        pending.Push(expression);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            switch (node.nodeType)
            {
                case NodeType.Integer:
                case NodeType.Float:
                case NodeType.String:
                case NodeType.Bool:
                case NodeType.Null:
                case NodeType.BytesLiteral:
                case NodeType.Placeholder:
                    continue;

                case NodeType.ExprFuncCall:
                    // leftAst is the function name.
                    if (node.rightAst is not null)
                        pending.Push(node.rightAst);
                    continue;

                case NodeType.ExprCast:
                    // rightAst is the target type.
                    if (node.leftAst is not null)
                        pending.Push(node.leftAst);
                    continue;

                case NodeType.ArrayLiteral:
                case NodeType.ExprList:
                case NodeType.ExprArgumentList:
                case NodeType.ExprAdd:
                case NodeType.ExprSub:
                case NodeType.ExprMult:
                case NodeType.ExprDiv:
                case NodeType.ExprNegate:
                case NodeType.ExprConcat:
                case NodeType.ExprBitAnd:
                case NodeType.ExprBitOr:
                case NodeType.ExprBitXor:
                case NodeType.ExprBitNot:
                case NodeType.ExprShiftLeft:
                case NodeType.ExprShiftRight:
                case NodeType.ExprCase:
                case NodeType.ExprCaseWhen:
                case NodeType.ExprCaseWhenList:
                case NodeType.ExprEquals:
                case NodeType.ExprNotEquals:
                case NodeType.ExprLessThan:
                case NodeType.ExprLessEqualsThan:
                case NodeType.ExprGreaterThan:
                case NodeType.ExprGreaterEqualsThan:
                case NodeType.ExprBetween:
                case NodeType.ExprAnd:
                case NodeType.ExprOr:
                case NodeType.ExprNot:
                case NodeType.ExprIsNull:
                case NodeType.ExprIsNotNull:
                case NodeType.ExprIsTrue:
                case NodeType.ExprIsNotTrue:
                case NodeType.ExprIsFalse:
                case NodeType.ExprIsNotFalse:
                case NodeType.ExprLike:
                case NodeType.ExprILike:
                case NodeType.ExprInMembership:
                case NodeType.ExprNotInMembership:
                case NodeType.ExprSubscript:
                    PushChildren(node, pending);
                    continue;

                default:
                    return false;
            }
        }

        return true;
    }

    private static void PushChildren(NodeAst node, Stack<NodeAst> pending)
    {
        if (node.leftAst is not null) pending.Push(node.leftAst);
        if (node.rightAst is not null) pending.Push(node.rightAst);
        if (node.extendedOne is not null) pending.Push(node.extendedOne);
        if (node.extendedTwo is not null) pending.Push(node.extendedTwo);
        if (node.extendedThree is not null) pending.Push(node.extendedThree);
        if (node.extendedFour is not null) pending.Push(node.extendedFour);
        if (node.extendedFive is not null) pending.Push(node.extendedFive);
        if (node.extendedSix is not null) pending.Push(node.extendedSix);
        if (node.extendedSeven is not null) pending.Push(node.extendedSeven);
    }
}
