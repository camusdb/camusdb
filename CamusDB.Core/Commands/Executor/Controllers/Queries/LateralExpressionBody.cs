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
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// A <c>LATERAL</c> body that only names expressions of the left row, such as
/// <c>CROSS JOIN LATERAL (SELECT p.y - 100.0 AS denom) AS d</c>, or a <c>VALUES</c> list of such rows. Such a body
/// gives a fixed number of rows for each left row, so the nested-loop join evaluates its expressions on the values
/// of that row (<see cref="EvaluateAsync"/>) and runs no subquery for the body: a subquery run costs a query
/// ticket, schema pins and a query pipeline for each left row, and a body that binds again for each row costs a
/// bind too.
///
/// <para><b>Which bodies qualify.</b> Two forms.</para>
/// <list type="number">
/// <item>One SELECT that binds one time (<see cref="CorrelatedSubqueryBody.Bound"/>), with no <c>FROM</c>,
/// <c>WHERE</c>, <c>GROUP BY</c>, <c>HAVING</c>, <c>ORDER BY</c>, <c>LIMIT</c>, <c>OFFSET</c> or
/// <c>DISTINCT</c> (<see cref="TryCreate"/>). Its items are the bound items, so an uncorrelated subquery ran one
/// time at the bind and is a value here.</item>
/// <item>A <c>UNION ALL</c> of such SELECTs, which is how the parser writes a <c>VALUES</c> list
/// (<see cref="ValuesAst"/>), or one such SELECT that holds a scalar subquery that reads the left row
/// (<see cref="TryCreateAsync"/>). Each SELECT gives one row, in branch order. The items are the items of the
/// body as the parser wrote them, with the left names as hidden placeholders.</item>
/// </list>
/// <para>Each item must be made of literals, placeholders, operators, <c>CAST</c>, <c>CASE</c> and scalar calls
/// (<see cref="IsDirectlyEvaluable"/>). In the second form an item can also hold a scalar subquery that reads the
/// left row only through the hidden placeholders of this body, and that has no nested subquery that reads one.
/// A volatile call, any other subquery, a window call or any other node keeps the subquery run.</para>
///
/// <para><b>Scalar subqueries.</b> Each one binds one time (<see cref="ScalarSubquery"/>) and runs for each left
/// row with the placeholder values of that row. A row whose placeholder values an earlier row had gets the
/// earlier value from a <see cref="CorrelatedResultCache{T}"/>. All subqueries of the row run before any item is
/// evaluated, and an error is thrown at once, also for a subquery in a <c>CASE</c> branch that the row does not
/// take: the bind of the run path runs each subquery of the body before it evaluates an item, so the errors are
/// the same. The value goes in the parameters under a hidden name, which the evaluator reads
/// (<see cref="CorrelatedSubqueryValues.TryRead"/>), and its node carries the type of the subquery column, so a
/// <c>COALESCE</c> or <c>CASE</c> around it has the result type that the run path gives.</para>
///
/// <para><b>The result is the same.</b> The preparation of the body ran the statement checks (types, a constant
/// division by zero) one time, as before. The items are evaluated by the evaluator that the projection uses
/// (<see cref="SqlExecutor.EvalExpr(NodeAst, IReadOnlyDictionary{string, ColumnValue}, Dictionary{string, ColumnValue}?, QueryRowNameResolver?)"/>),
/// in select-list order, with the parameters a subquery run gets: the statement parameters and the left values
/// under their hidden placeholder names. So an evaluation error comes from the same row and the same item. A
/// <c>UNION ALL</c> body widens each cell to the type of its output column, as the set operation does
/// (<see cref="SetOperationSchema.Widen"/>). The cells are keyed by the row keys of the derived table columns,
/// so the <see cref="DerivedTableExecutor"/> reads them as it reads the cells of a run.</para>
/// </summary>
internal sealed class LateralExpressionBody
{
    private readonly CorrelatedSubqueryBody body;

    /// <summary>The items of each output row, alias removed, in select-list order; one array for each branch.</summary>
    private readonly NodeAst[][] rows;

    /// <summary>The row key of each item's cell: the row key of the derived table column at that position.</summary>
    private readonly string[] rowKeys;

    /// <summary>The output column types of a <c>UNION ALL</c> body, which each cell is widened to; null for one SELECT.</summary>
    private readonly ColumnType[]? cellTypes;

    /// <summary>The scalar subqueries of the items, in the order they run.</summary>
    private readonly ScalarSubquery[] subqueries;

    /// <summary>
    /// The result type of each <c>CASE</c>, <c>COALESCE</c>, <c>IFNULL</c> and <c>IF</c> node of the items, which
    /// the evaluator widens the value of the node to, as the projection of a run does. A typed NULL before the
    /// value that <c>COALESCE</c> returns shows its type only here.
    /// </summary>
    private readonly QueryRowNameResolver resultTypes;

    /// <summary>The output column names of the body, for EXPLAIN.</summary>
    public IReadOnlyList<string> ColumnNames { get; }

    /// <summary>
    /// The numeric columns of the derived table, whose cells are widened to the declared type after each
    /// evaluation (see <see cref="NumericWidening"/>), computed one time.
    /// </summary>
    public (string RowKey, ColumnType Type)[] NumericColumns { get; }

    private LateralExpressionBody(
        CorrelatedSubqueryBody body,
        NodeAst[][] rows,
        string[] rowKeys,
        ColumnType[]? cellTypes,
        ScalarSubquery[] subqueries,
        QueryRowNameResolver resultTypes,
        IReadOnlyList<DerivedColumnSchema> columns)
    {
        this.body = body;
        this.rows = rows;
        this.rowKeys = rowKeys;
        this.cellTypes = cellTypes;
        this.subqueries = subqueries;
        this.resultTypes = resultTypes;

        string[] columnNames = new string[columns.Count];
        for (int i = 0; i < columns.Count; i++)
            columnNames[i] = columns[i].Name;

        ColumnNames = columnNames;
        NumericColumns = NumericWidening.NumericColumns(columns);
    }

    /// <summary>
    /// The direct evaluation of <paramref name="body"/> in either form of the class summary, or null when the body
    /// does not qualify. <paramref name="columns"/> are the columns of the derived table, after the column aliases
    /// of the <c>AS v (a, b)</c> form. Through <paramref name="binder"/> each scalar subquery of the second form
    /// binds one time.
    /// </summary>
    public static async Task<LateralExpressionBody?> TryCreateAsync(
        CorrelatedSubqueryBody body,
        IReadOnlyList<DerivedColumnSchema> columns,
        NestedSelectBinder binder,
        DatabaseDescriptor database)
    {
        if (TryCreate(body, columns) is { } plain)
            return plain;

        if (columns.Count == 0 || ScalarFunctionEvaluator.ContainsVolatileFunction(body.Select))
            return null;

        for (int i = 0; i < columns.Count; i++)
        {
            if (columns[i].Hidden)
                return null;
        }

        if (UnionAllBranches(body.Select) is not { } branches)
            return null;

        // One SELECT that binds one time and did not qualify above holds something this form does not take either.
        if (branches.Count == 1 && body.Bound is not null)
            return null;

        List<NodeAst> subqueryNodes = [];
        NodeAst[][] rows = new NodeAst[branches.Count][];

        for (int b = 0; b < branches.Count; b++)
        {
            List<NodeAst> items = [];
            ExpressionChains.Flatten(branches[b].leftAst!, NodeType.IdentifierList, items);

            if (items.Count != columns.Count)
                return null;

            NodeAst[] row = new NodeAst[items.Count];

            for (int i = 0; i < items.Count; i++)
            {
                NodeAst item = QueryExpressionClassifier.UnwrapAlias(items[i]);

                if (!IsDirectlyEvaluable(item, subqueryNodes))
                    return null;

                row[i] = item;
            }

            rows[b] = row;
        }

        ScalarSubquery[] subqueries = new ScalarSubquery[subqueryNodes.Count];
        Dictionary<NodeAst, NodeAst> named = new(ReferenceEqualityComparer.Instance);

        for (int i = 0; i < subqueryNodes.Count; i++)
        {
            NodeAst node = subqueryNodes[i];
            string hiddenName = CorrelatedSubqueryValues.ParameterPrefix + "v" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);

            if (await ScalarSubquery.TryPrepareAsync(node, hiddenName, body, binder, database).ConfigureAwait(false) is not { } subquery)
                return null;

            subqueries[i] = subquery;
            named[node] = subquery.Node;
        }

        if (named.Count > 0)
        {
            foreach (NodeAst[] row in rows)
            {
                for (int i = 0; i < row.Length; i++)
                    row[i] = NodeAstWalk.Replace(row[i], named);
            }
        }

        // The statement checks of a run bind, on these items: they record the result type of each conditional
        // node. A check that fails here keeps the run path, which reports the error as before: at the bind of the
        // body, or when a left row reaches it.
        QueryRowNameResolver resultTypes = new([], []);

        try
        {
            foreach (NodeAst[] row in rows)
            {
                for (int i = 0; i < row.Length; i++)
                    ConditionalResultTypes.Check(row[i], lookup: null, resultTypes, allowAggregates: false);
            }
        }
        catch (CamusDBException)
        {
            return null;
        }

        resultTypes.FreezeForSharedReuse();

        string[] rowKeys = new string[columns.Count];
        ColumnType[]? cellTypes = branches.Count > 1 ? new ColumnType[columns.Count] : null;

        for (int i = 0; i < columns.Count; i++)
        {
            rowKeys[i] = columns[i].RowKey;

            if (cellTypes is not null)
                cellTypes[i] = columns[i].Type;
        }

        return new LateralExpressionBody(body, rows, rowKeys, cellTypes, subqueries, resultTypes, columns);
    }

    /// <summary>
    /// The direct evaluation of a body in the first form of the class summary, or null when the body does not
    /// qualify.
    /// </summary>
    private static LateralExpressionBody? TryCreate(CorrelatedSubqueryBody body, IReadOnlyList<DerivedColumnSchema> columns)
    {
        if (body.Bound is null)
            return null;

        NodeAst select = body.Select;

        if (!IsPlainFromlessSelect(select) || ScalarFunctionEvaluator.ContainsVolatileFunction(select))
            return null;

        IReadOnlyList<ProjectionItem> projections = body.Checked.Query.Projections;

        if (projections.Count == 0 || projections.Count != columns.Count)
            return null;

        NodeAst[] expressions = new NodeAst[projections.Count];
        string[] rowKeys = new string[projections.Count];

        for (int i = 0; i < projections.Count; i++)
        {
            if (columns[i].Hidden)
                return null;

            NodeAst expression = QueryExpressionClassifier.UnwrapAlias(projections[i].Expression);

            if (!IsDirectlyEvaluable(expression, subqueries: null))
                return null;

            expressions[i] = expression;
            rowKeys[i] = columns[i].RowKey;
        }

        // The bind of the body recorded the result types of the bound items.
        return new LateralExpressionBody(body, [expressions], rowKeys, cellTypes: null, subqueries: [], body.Checked.RowNames, columns);
    }

    /// <summary>
    /// True when <paramref name="select"/> is one SELECT with a select list and no <c>FROM</c>, <c>WHERE</c>,
    /// <c>GROUP BY</c>, <c>HAVING</c>, <c>ORDER BY</c>, <c>LIMIT</c>, <c>OFFSET</c> or <c>DISTINCT</c>.
    /// </summary>
    private static bool IsPlainFromlessSelect(NodeAst select) =>
        select.nodeType == NodeType.Select
        && select.leftAst is not null
        && select.leftAst.nodeType != NodeType.ExprAllFields
        && select.rightAst is null      // FROM
        && select.extendedOne is null   // WHERE
        && select.extendedTwo is null   // ORDER BY
        && select.extendedThree is null // LIMIT
        && select.extendedFour is null  // OFFSET
        && select.extendedFive is null  // GROUP BY
        && select.extendedSix is null   // HAVING
        && select.extendedSeven is null
        && select.yytext is null;       // DISTINCT

    /// <summary>
    /// The branches of <paramref name="select"/> in source order when it is a plain FROM-less SELECT or a
    /// <c>UNION ALL</c> of them with no clause of its own at any level, else null. The walk does not recurse.
    /// </summary>
    private static List<NodeAst>? UnionAllBranches(NodeAst select)
    {
        List<NodeAst> branches = [];
        Stack<NodeAst> pending = new();
        pending.Push(select);

        while (pending.TryPop(out NodeAst? node))
        {
            if (SetOperationAst.IsSetOperationSelect(node))
            {
                NodeAst setOperation = node.rightAst!;

                if (!SetOperationAst.IsPlainWrapper(node)
                    || !string.Equals(SetOperationAst.KindOf(setOperation), SetOperationAst.Union, StringComparison.Ordinal)
                    || !SetOperationAst.IsAll(setOperation)
                    || setOperation.leftAst is null
                    || setOperation.rightAst is null)
                {
                    return null;
                }

                // Right first, so the left branch pops first and the rows keep their source order.
                pending.Push(setOperation.rightAst);
                pending.Push(setOperation.leftAst);
                continue;
            }

            if (!IsPlainFromlessSelect(node))
                return null;

            branches.Add(node);
        }

        return branches;
    }

    /// <summary>The row that the items are evaluated against: they read no column, so it is empty and never written.</summary>
    private static readonly IReadOnlyDictionary<string, ColumnValue> NoColumns = new Dictionary<string, ColumnValue>(0);

    /// <summary>
    /// Adds the rows of the body for the left values <paramref name="outerValues"/>, which
    /// <see cref="CorrelatedSubqueryBody.Outer"/> read from one left row, to <paramref name="output"/>. It runs
    /// the scalar subqueries of the body first, when it has any; a body with none completes synchronously.
    /// </summary>
    /// <param name="parameterBuffer">The parameter map of the caller for this body, or null for a new map.</param>
    public ValueTask EvaluateAsync(
        DatabaseDescriptor database,
        ColumnValue[] outerValues,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? statementParameters,
        ParameterBuffer? parameterBuffer,
        List<QueryResultRow> output,
        CancellationToken cancellationToken)
    {
        Dictionary<string, ColumnValue> parameters = parameterBuffer is null
            ? body.ParametersFor(outerValues, statementParameters)
            : parameterBuffer.For(body, outerValues, statementParameters);

        if (subqueries.Length == 0)
        {
            AddRows(parameters, output);
            return ValueTask.CompletedTask;
        }

        return EvaluateWithSubqueriesAsync(database, outerValues, txnState, parameters, output, cancellationToken);
    }

    private async ValueTask EvaluateWithSubqueriesAsync(
        DatabaseDescriptor database,
        ColumnValue[] outerValues,
        KvTransaction txnState,
        Dictionary<string, ColumnValue> parameters,
        List<QueryResultRow> output,
        CancellationToken cancellationToken)
    {
        for (int i = 0; i < subqueries.Length; i++)
        {
            ScalarSubquery subquery = subqueries[i];
            parameters[subquery.HiddenName] = await subquery
                .ValueAsync(database, outerValues, txnState, parameters, cancellationToken)
                .ConfigureAwait(false);
        }

        AddRows(parameters, output);
    }

    private void AddRows(Dictionary<string, ColumnValue> parameters, List<QueryResultRow> output)
    {
        for (int r = 0; r < rows.Length; r++)
        {
            NodeAst[] items = rows[r];
            Dictionary<string, ColumnValue> cells = new(items.Length, StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < items.Length; i++)
            {
                ColumnValue value = SqlExecutor.EvalExpr(items[i], NoColumns, parameters, resultTypes);

                if (cellTypes is not null)
                    value = SetOperationSchema.Widen(value, cellTypes[i]);

                cells[rowKeys[i]] = value;
            }

            output.Add(new QueryResultRow(new ObjectIdValue(), cells));
        }
    }

    /// <summary>
    /// A scalar subquery inside a body of the second form. It reads the left row only through the hidden
    /// placeholders of the body, so it binds one time (<see cref="Prepared"/>) and each run gets the placeholder
    /// values of its row through the parameters.
    ///
    /// <para>Its results are kept for the execution (<see cref="results"/>), keyed by the placeholder values it
    /// reads, as a correlated subquery keeps them. There is no cache when the subquery reads a hidden value of an
    /// enclosing query level, which the key does not hold. The class summary of <see cref="CorrelatedResultCache{T}"/>
    /// gives why a kept result is the same as a new run.</para>
    /// </summary>
    private sealed class ScalarSubquery
    {
        /// <summary>The node that stands for the subquery in the items: its hidden name, and the type of its column.</summary>
        public NodeAst Node { get; }

        /// <summary>The name of the subquery value in the parameters, which the evaluator reads at <see cref="Node"/>.</summary>
        public string HiddenName { get; }

        /// <summary>The subquery, bound one time.</summary>
        private CorrelatedSubqueryBody Prepared { get; }

        /// <summary>The position in the left values of each placeholder that the subquery reads: the key of <see cref="results"/>.</summary>
        private readonly int[] keyOrdinals;

        private readonly CorrelatedResultCache<ColumnValue>? results;

        private ScalarSubquery(NodeAst node, string hiddenName, CorrelatedSubqueryBody prepared, int[] keyOrdinals, CorrelatedResultCache<ColumnValue>? results)
        {
            Node = node;
            HiddenName = hiddenName;
            Prepared = prepared;
            this.keyOrdinals = keyOrdinals;
            this.results = results;
        }

        /// <summary>
        /// Prepares <paramref name="subquery"/>, a scalar subquery node of <paramref name="body"/>, or returns null
        /// when it does not qualify: it reads no placeholder of the body, a nested subquery of it reads one, or it
        /// does not bind for runs. A subquery that does not bind keeps the run path, which reports the same error
        /// when a left row reaches it, and no error when none does.
        /// </summary>
        public static async Task<ScalarSubquery?> TryPrepareAsync(
            NodeAst subquery,
            string hiddenName,
            CorrelatedSubqueryBody body,
            NestedSelectBinder binder,
            DatabaseDescriptor database)
        {
            if (subquery.leftAst is not { nodeType: NodeType.Select } select
                || CorrelatedSubqueryBody.NestedSubqueryReadsOuterValue(select))
            {
                return null;
            }

            IReadOnlyList<string> bodyNames = body.ParameterNames;
            SortedSet<int> ordinals = [];
            bool keyHoldsEveryValue = true;

            Stack<NodeAst> pending = new();
            pending.Push(select);

            while (pending.TryPop(out NodeAst? node))
            {
                if (CorrelatedSubqueryBody.IsHiddenPlaceholder(node))
                {
                    int ordinal = IndexOf(bodyNames, node.yytext!);

                    if (ordinal >= 0)
                        ordinals.Add(ordinal);
                    else
                        keyHoldsEveryValue = false;
                }

                PushChildren(node, pending);
            }

            // A subquery that reads no left value is uncorrelated; the run path runs it at the bind.
            if (ordinals.Count == 0)
                return null;

            CorrelatedSubqueryBody prepared;

            try
            {
                prepared = await binder.PrepareParameterSubqueryAsync(database, select).ConfigureAwait(false);
            }
            catch (CamusDBException)
            {
                return null;
            }

            if (prepared.Bound is null)
                return null;

            NodeAst named = SubqueryResultTypes.Remember(
                new NodeAst(NodeType.ExprScalarSubquery, select, null, null, null, null, null, null, hiddenName),
                prepared.OutputType);

            CorrelatedResultCache<ColumnValue>? results = keyHoldsEveryValue
                ? new CorrelatedResultCache<ColumnValue>(body.Outer)
                : null;

            return new ScalarSubquery(named, hiddenName, prepared, [.. ordinals], results);
        }

        /// <summary>
        /// The value of the subquery for the left values <paramref name="outerValues"/>: NULL for no row, and an
        /// error for a second row. <paramref name="parameters"/> are the statement parameters and the placeholder
        /// values of this row; the run copies them.
        /// </summary>
        public async ValueTask<ColumnValue> ValueAsync(
            DatabaseDescriptor database,
            ColumnValue[] outerValues,
            KvTransaction txnState,
            Dictionary<string, ColumnValue> parameters,
            CancellationToken cancellationToken)
        {
            ColumnValue[]? key = null;

            if (results is not null)
            {
                key = new ColumnValue[keyOrdinals.Length];
                for (int i = 0; i < keyOrdinals.Length; i++)
                    key[i] = outerValues[keyOrdinals[i]];

                if (!CorrelatedResultCache<ColumnValue>.TryKey(key))
                {
                    key = null;
                }
                else if (results.TryGet(key, out ColumnValue kept))
                {
                    CorrelatedRunProbe.CountHit();
                    return kept;
                }
            }

            IAsyncEnumerable<QueryResultRow> runRows = Prepared.RunAsync(database, [], txnState, parameters, cancellationToken);
            ColumnValue value = await CorrelatedSubqueryValues.SingleValueAsync(runRows, cancellationToken).ConfigureAwait(false);

            if (key is not null)
                results!.Add(key, value);

            return value;
        }

        private static int IndexOf(IReadOnlyList<string> names, string name)
        {
            for (int i = 0; i < names.Count; i++)
            {
                if (string.Equals(names[i], name, StringComparison.Ordinal))
                    return i;
            }

            return -1;
        }
    }

    /// <summary>
    /// The parameter map of an expression-only body for one nested-loop join: the statement parameters and the
    /// hidden placeholders of the left values. The join builds it for its first left row, and for each later
    /// row only the placeholder values change, so a row does not copy the statement parameters into a new map.
    ///
    /// <para>The reuse is safe because <see cref="EvaluateAsync"/> reads the map before it returns and no cell
    /// keeps a reference to it, and because one buffer belongs to one join operator of one execution, which
    /// evaluates one left row at a time. A scalar subquery of the body copies the map when it runs. The hidden
    /// value of each scalar subquery is written again for each row before an item reads it. A subquery run of a
    /// whole body keeps a map of its own for each row: its rows stream lazily, so a later row must not change the
    /// values that an earlier run still reads.</para>
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
    ///
    /// <para>When <paramref name="subqueries"/> is not null, a scalar subquery is accepted too: it is added to the
    /// list, in source order (left to right, an outer node before the nodes inside it), and the walk does not
    /// enter it. The caller decides if each one qualifies.</para>
    /// </summary>
    internal static bool IsDirectlyEvaluable(NodeAst expression, List<NodeAst>? subqueries)
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

                case NodeType.ExprScalarSubquery when subqueries is not null:
                    subqueries.Add(node);
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

    /// <summary>Pushes the children of <paramref name="node"/> so that the left child pops first.</summary>
    private static void PushChildren(NodeAst node, Stack<NodeAst> pending)
    {
        if (node.extendedSeven is not null) pending.Push(node.extendedSeven);
        if (node.extendedSix is not null) pending.Push(node.extendedSix);
        if (node.extendedFive is not null) pending.Push(node.extendedFive);
        if (node.extendedFour is not null) pending.Push(node.extendedFour);
        if (node.extendedThree is not null) pending.Push(node.extendedThree);
        if (node.extendedTwo is not null) pending.Push(node.extendedTwo);
        if (node.extendedOne is not null) pending.Push(node.extendedOne);
        if (node.rightAst is not null) pending.Push(node.rightAst);
        if (node.leftAst is not null) pending.Push(node.leftAst);
    }
}
