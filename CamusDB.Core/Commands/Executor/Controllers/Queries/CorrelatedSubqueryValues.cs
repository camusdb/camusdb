/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Gives the value of each correlated subquery of an expression for one outer row, so the synchronous
/// evaluator can read it. The evaluator cannot run a query, so the subqueries run first, here, and the
/// expression is then evaluated with their values.
///
/// <para><b>Why the values go in the parameters.</b> The preparer gives each correlated node a hidden name
/// (<see cref="ParameterPrefix"/> and a number, see <see cref="ExistsSubqueryRegistry.NextHiddenName"/>).
/// For one row, <see cref="AddForRowAsync"/> puts each value in a copy of the statement parameters under
/// that name, and <c>SqlExecutor.EvalExpr</c> reads it when it reaches the node
/// (<see cref="TryRead"/>). The tree is not copied for each row. That matters: the binding records the result
/// type of a <c>COALESCE</c> or a <c>CASE</c> by node identity, and a copy would lose it, so
/// <c>COALESCE((SELECT …), 1.5)</c> could give an INT64. The node keeps its subquery type, so the planner and
/// every other stage still treat it as a subquery. The lexer accepts only letters, digits and underscores
/// after <c>@</c>, so no statement can name a hidden value.</para>
///
/// <para><b>Each form.</b> A correlated <c>EXISTS</c> gives a boolean. A scalar subquery gives its one value,
/// NULL for no row, and an error for more than one row. An <c>IN</c> or <c>NOT IN</c> gives the boolean of the
/// whole test: its tested value is evaluated for this row, against the values the body returned, by the same
/// rules as an uncorrelated <c>IN</c>. A quantified comparison gives the boolean of the whole comparison in
/// the same way. A node inside the tested value of another node gets its value first.</para>
///
/// <para><b>An error waits for the evaluator.</b> PostgreSQL does not evaluate a <c>CASE</c> branch that it does
/// not take, or a <c>COALESCE</c> argument after the first value, so an error of a subquery there does not
/// appear. Here each subquery runs before the evaluator, so an error of a subquery is kept with its value and
/// thrown only when the evaluator reads that value. The cost of a branch that does not run stays: its subquery
/// still runs.</para>
///
/// <para>The body of each subquery runs with the statement parameters, not with this copy, so a hidden name of
/// this level never reaches a body, which numbers its own nodes from zero.</para>
///
/// <para><b>Kept results.</b> A row with the same outer values as an earlier row gets the earlier result of the
/// body, when the prepared subquery has a <see cref="CorrelatedResultCache{T}"/>: the value of a scalar subquery,
/// or the test node of an <c>IN</c>, <c>NOT IN</c> or quantified subquery, which the row then evaluates with its
/// own tested value. An error is not kept.</para>
/// </summary>
internal static class CorrelatedSubqueryValues
{
    /// <summary>The start of each hidden name.</summary>
    internal const string ParameterPrefix = "@#sq";

    /// <summary>
    /// The error of a subquery, keyed by the marker value that stands in its place. A marker is a new NULL
    /// instance for each error, so the key is never a value of the data.
    /// </summary>
    private static readonly ConditionalWeakTable<ColumnValue, ExceptionDispatchInfo> DeferredErrors = new();

    /// <summary>True when <paramref name="expression"/> holds a correlated subquery node.</summary>
    internal static bool Contains(NodeAst? expression) => SubqueryNodeScan.Contains(expression, SubqueryNodeKinds.Correlated);

    /// <summary>
    /// The value of <paramref name="node"/> in <paramref name="parameters"/>, when the node is a correlated
    /// subquery and <see cref="AddForRowAsync"/> gave it a value for this row. Throws the error of its subquery
    /// when that failed. Returns false for every other node, and the evaluator then goes on as before.
    /// </summary>
    internal static bool TryRead(NodeAst node, Dictionary<string, ColumnValue>? parameters, out ColumnValue value)
    {
        value = ColumnValue.Null;

        if (parameters is null || HiddenNameOf(node) is not { } name || !parameters.TryGetValue(name, out ColumnValue? found))
            return false;

        if (DeferredErrors.TryGetValue(found, out ExceptionDispatchInfo? error))
            error.Throw();

        value = found;
        return true;
    }

    /// <summary>
    /// A copy of the statement parameters of <paramref name="ticket"/> for one row, which
    /// <see cref="AddForRowAsync"/> fills.
    /// </summary>
    internal static Dictionary<string, ColumnValue> NewRowParameters(QueryTicket ticket) =>
        ticket.Parameters is { } statement
            ? new Dictionary<string, ColumnValue>(statement, statement.Comparer)
            : new Dictionary<string, ColumnValue>();

    /// <summary>
    /// Runs each correlated subquery of <paramref name="expression"/> for <paramref name="row"/> and adds its
    /// value to <paramref name="rowParameters"/> under its hidden name. A subquery inside the tested value of
    /// another one runs first.
    /// </summary>
    internal static async ValueTask AddForRowAsync(
        NodeAst expression,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket,
        DatabaseDescriptor database,
        Dictionary<string, ColumnValue> rowParameters)
    {
        ExistsSubqueryRegistry registry = ticket.ExistsSubqueries ?? throw NotPrepared();

        foreach (NodeAst node in CollectInEvaluationOrder(expression, registry))
        {
            ColumnValue value;

            try
            {
                value = await EvaluateAsync(node, registry, row, ticket, database, rowParameters).ConfigureAwait(false);
            }
            catch (CamusDBException error)
            {
                value = new ColumnValue(ColumnType.Null, false);
                DeferredErrors.AddOrUpdate(value, ExceptionDispatchInfo.Capture(error));
            }

            rowParameters[HiddenNameOf(node)!] = value;
        }
    }

    /// <summary>
    /// The hidden values of <paramref name="rowParameters"/> as plain cells, for a buffer that can spill them to
    /// disk: the locate scan of an UPDATE computes the subqueries of its SET values, and the write phase reads
    /// them. A marker of an error does not keep its identity on disk, so an error becomes two string cells,
    /// its code and its message, which <see cref="AddStoredCells"/> turns back into the error.
    /// </summary>
    internal static Dictionary<string, ColumnValue> ToStoredCells(Dictionary<string, ColumnValue> rowParameters)
    {
        Dictionary<string, ColumnValue> cells = new(StringComparer.Ordinal);

        foreach ((string name, ColumnValue value) in rowParameters)
        {
            if (!name.StartsWith(ParameterPrefix, StringComparison.Ordinal))
                continue;

            if (DeferredErrors.TryGetValue(value, out ExceptionDispatchInfo? error) && error.SourceException is CamusDBException failed)
            {
                cells[name] = ColumnValue.Null;
                cells[name + ErrorCodeSuffix] = new ColumnValue(ColumnType.String, failed.Code);
                cells[name + ErrorMessageSuffix] = new ColumnValue(ColumnType.String, failed.Message);
                continue;
            }

            cells[name] = value;
        }

        return cells;
    }

    /// <summary>Adds the values that <see cref="ToStoredCells"/> stored to <paramref name="parameters"/>.</summary>
    internal static void AddStoredCells(IReadOnlyDictionary<string, ColumnValue> cells, Dictionary<string, ColumnValue> parameters)
    {
        foreach ((string name, ColumnValue value) in cells)
        {
            if (!name.StartsWith(ParameterPrefix, StringComparison.Ordinal)
                || name.EndsWith(ErrorCodeSuffix, StringComparison.Ordinal)
                || name.EndsWith(ErrorMessageSuffix, StringComparison.Ordinal))
            {
                continue;
            }

            if (cells.TryGetValue(name + ErrorCodeSuffix, out ColumnValue? code) && cells.TryGetValue(name + ErrorMessageSuffix, out ColumnValue? message))
            {
                ColumnValue marker = new(ColumnType.Null, false);
                DeferredErrors.AddOrUpdate(marker, ExceptionDispatchInfo.Capture(new CamusDBException(code.StrValue ?? "", message.StrValue)));
                parameters[name] = marker;
                continue;
            }

            parameters[name] = value;
        }
    }

    private const string ErrorCodeSuffix = "#code";

    private const string ErrorMessageSuffix = "#message";

    /// <summary>
    /// The registered nodes of <paramref name="expression"/>, each after the nodes inside its tested value. The
    /// walk does not recurse, as no walk of a statement tree does, and it does not enter a nested SELECT,
    /// which is its own scope.
    /// </summary>
    private static List<NodeAst> CollectInEvaluationOrder(NodeAst expression, ExistsSubqueryRegistry registry)
    {
        List<NodeAst> ordered = [];
        Stack<(NodeAst Node, bool Expanded)> pending = new();
        pending.Push((expression, false));

        while (pending.TryPop(out (NodeAst Node, bool Expanded) entry))
        {
            NodeAst node = entry.Node;

            if (node.nodeType == NodeType.Select)
                continue;

            bool registered = registry.TryGet(node, out _) || registry.TryGetValue(node, out _);

            if (entry.Expanded)
            {
                if (registered)
                    ordered.Add(node);

                continue;
            }

            pending.Push((node, true));

            if (registered)
            {
                // The tested value is evaluated for this row; the subquery itself is the body.
                if (node.nodeType is NodeType.ExprInSubquery or NodeType.ExprNotInSubquery or NodeType.ExprQuantifiedComparison && node.leftAst is not null)
                    pending.Push((node.leftAst, false));

                continue;
            }

            if (node.nodeType is NodeType.ExprScalarSubquery or NodeType.ExprInSubquery or NodeType.ExprNotInSubquery
                or NodeType.ExprExistsSubquery or NodeType.ExprExistsCorrelated)
            {
                throw NotPrepared();
            }

            foreach (NodeAst? child in (ReadOnlySpan<NodeAst?>)[
                node.leftAst, node.rightAst,
                node.extendedOne, node.extendedTwo, node.extendedThree, node.extendedFour,
                node.extendedFive, node.extendedSix, node.extendedSeven])
            {
                if (child is not null)
                    pending.Push((child, false));
            }
        }

        return ordered;
    }

    private static async ValueTask<ColumnValue> EvaluateAsync(
        NodeAst node,
        ExistsSubqueryRegistry registry,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryTicket ticket,
        DatabaseDescriptor database,
        Dictionary<string, ColumnValue> rowParameters)
    {
        if (registry.TryGet(node, out PreparedExistsSubquery exists))
        {
            bool found = await ExistsSubqueryExecutor.ExecuteCorrelatedAsync(
                database, exists, row, ticket.TxnState, ticket.Parameters, ticket.CancellationToken).ConfigureAwait(false);

            return ColumnValue.FromBool(found);
        }

        registry.TryGetValue(node, out PreparedValueSubquery prepared);

        // The outer values key the result cache and are the input of the run, so the row is read once.
        ColumnValue[] outerValues = prepared.Body.Outer.For(row, ticket.Parameters);
        CorrelatedResultCache<KeptSubqueryResult>? results =
            prepared.Results is { } cache && CorrelatedResultCache<KeptSubqueryResult>.TryKey(outerValues) ? cache : null;

        if (results is not null && results.TryGet(outerValues, out KeptSubqueryResult kept))
        {
            CorrelatedRunProbe.CountHit();

            return kept.Test is { } keptTest
                ? SqlExecutor.EvalExpr(keptTest, row, rowParameters, ticket.RowNameResolver)
                : kept.Value!;
        }

        IAsyncEnumerable<QueryResultRow> rows = prepared.Body.RunAsync(
            database, outerValues, ticket.TxnState, ticket.Parameters, ticket.CancellationToken);

        if (node.nodeType == NodeType.ExprScalarSubquery)
        {
            ColumnValue value = await SingleValueAsync(rows, ticket.CancellationToken).ConfigureAwait(false);
            results?.Add(outerValues, new KeptSubqueryResult(value, null));
            return value;
        }

        await using InSubqueryMaterialization materialization = await InSubqueryExecutor.MaterializeRowsAsync(
            rows, QueryExecutionContext.For(database, ticket.CancellationToken)).ConfigureAwait(false);

        NodeAst test = node.nodeType switch
        {
            NodeType.ExprInSubquery => await SubqueryValueListAst.BuildInMembershipAsync(node.leftAst!, materialization, ticket.CancellationToken).ConfigureAwait(false),
            NodeType.ExprNotInSubquery => await SubqueryValueListAst.BuildNotInMembershipAsync(node.leftAst!, materialization, ticket.CancellationToken).ConfigureAwait(false),
            _ => new NodeAst(
                node.nodeType,
                node.leftAst,
                await SubqueryValueListAst.BuildValueSetAsync(materialization, ticket.CancellationToken).ConfigureAwait(false),
                node.extendedOne,
                node.extendedTwo,
                node.extendedThree,
                node.extendedFour,
                node.extendedFive,
                node.yytext,
                node.extendedSix,
                node.extendedSeven),
        };

        // The test node holds its own copy of the values, so it outlives the materialization. A list that
        // spilled to disk is too large to keep in memory for later rows.
        if (results is not null && !materialization.Values.IsSpilled)
            results.Add(outerValues, new KeptSubqueryResult(null, test), materialization.Values.Count);

        return SqlExecutor.EvalExpr(test, row, rowParameters, ticket.RowNameResolver);
    }

    /// <summary>The one value of a scalar subquery: NULL for no row, and an error for a second row.</summary>
    private static async ValueTask<ColumnValue> SingleValueAsync(IAsyncEnumerable<QueryResultRow> rows, CancellationToken cancellationToken)
    {
        QueryResultRow? first = null;

        await foreach (QueryResultRow row in rows.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (first is not null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Scalar subquery returned more than one row");

            first = row;
        }

        return first is null ? ColumnValue.Null : SubqueryQueryExecutor.ExtractSingleColumnValue(first.Value);
    }

    /// <summary>
    /// The hidden name of a correlated node, or null for any other node. A quantified comparison keeps its
    /// quantifier word in its own text, so its name is on the subquery node of its right operand.
    /// </summary>
    private static string? HiddenNameOf(NodeAst node)
    {
        string? name = node.nodeType switch
        {
            NodeType.ExprScalarSubquery or NodeType.ExprInSubquery or NodeType.ExprNotInSubquery or NodeType.ExprExistsCorrelated => node.yytext,
            NodeType.ExprQuantifiedComparison when node.rightAst is { nodeType: NodeType.ExprScalarSubquery } subquery => subquery.yytext,
            _ => null,
        };

        return name is not null && name.StartsWith(ParameterPrefix, StringComparison.Ordinal) ? name : null;
    }

    private static CamusDBException NotPrepared() =>
        new(CamusDBErrorCodes.InvalidInternalOperation, "Correlated subquery was not prepared");
}
