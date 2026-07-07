
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QueryProjector
{
    internal async IAsyncEnumerable<QueryResultRow> ProjectResultset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        if (ticket.Projection is null || ticket.Projection.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "This resultset shouldn't be projected");

        if (ticket.GroupBy is { Count: > 0 })
        {
            List<string> visibleColumns = GetVisibleProjectionColumns(ticket);

            await foreach (QueryResultRow resultRow in dataCursor.ConfigureAwait(false))
            {
                Dictionary<string, ColumnValue> projected = new(visibleColumns.Count);

                foreach (string columnName in visibleColumns)
                    projected[columnName] = resultRow.Row[columnName];

                yield return new QueryResultRow(resultRow.RowId, projected);
            }

            yield break;
        }

        // Build a fixed projection layout once before the row loop so rows can be emitted as
        // QueryRow (ColumnValue[] + shared RowLayout) instead of a per-row Dictionary.
        // Returns null when any projection item is ExprAllFields (SELECT *) because the output
        // set depends on the input row's columns and cannot be fixed at plan time.
        RowLayout? projLayout = TryBuildProjectionLayout(ticket);

        if (projLayout is not null)
        {
            await foreach (QueryResultRow resultRow in dataCursor)
            {
                QueryRow? inputQr = resultRow.Row as QueryRow;
                ColumnValue[] values = new ColumnValue[projLayout.Count];

                for (int i = 0; i < ticket.Projection.Count; i++)
                {
                    NodeAst ast = ticket.Projection[i];
                    values[i] = EvalOrProjectExprFast(ticket, ast, resultRow.Row, inputQr, i);
                }

                yield return new(resultRow.RowId, new QueryRow(resultRow.RowId, projLayout, values));
            }

            yield break;
        }

        // Fallback path: SELECT * or any other case where the projection layout is not fixed.
        await foreach (QueryResultRow resultRow in dataCursor)
        {
            Dictionary<string, ColumnValue> projected = new(ticket.Projection.Count);

            for (int i = 0; i < ticket.Projection.Count; i++)
            {
                NodeAst ast = ticket.Projection[i];

                switch (ast.nodeType)
                {
                    case NodeType.ExprAllFields:
                    {
                        foreach (KeyValuePair<string, ColumnValue> keyValue in resultRow.Row)
                            projected[keyValue.Key] = keyValue.Value;

                        continue;
                    }

                    case NodeType.Identifier:
                        projected[QueryProjectionResolver.GetOutputNameFromProjectionExpression(ast, i)] =
                            EvalOrProjectExpr(ticket, ast, resultRow.Row, i);
                        continue;

                    case NodeType.ExprAlias:
                        projected[ast.rightAst!.yytext ?? ""] =
                            EvalOrProjectExpr(ticket, ast, resultRow.Row, i);
                        break;

                    default:
                        projected[QueryProjectionResolver.GetOutputNameFromProjectionExpression(ast, i)] =
                            EvalOrProjectExpr(ticket, ast, resultRow.Row, i);
                        break;
                }
            }

            yield return new(resultRow.RowId, projected);
        }
    }

    /// <summary>
    /// Attempts to build a fixed <see cref="RowLayout"/> for the projection output columns.
    /// Returns null and falls back to the dictionary path when:
    /// <list type="bullet">
    ///   <item>Any item is <see cref="NodeType.ExprAllFields"/> (SELECT *) — output column set
    ///     is determined per-row and cannot be fixed ahead of the loop.</item>
    ///   <item>Two items share an output name (e.g. <c>SELECT a AS x, b AS x</c>) — the dictionary
    ///     path uses last-wins semantics; the fast path would produce two slots with the same name,
    ///     which disagrees on both shape and which value <see cref="RowLayout.IndexOf"/> returns.</item>
    /// </list>
    /// </summary>
    private static RowLayout? TryBuildProjectionLayout(QueryTicket ticket)
    {
        List<string> names = new(ticket.Projection!.Count);
        HashSet<string> seen = new(ticket.Projection.Count, StringComparer.Ordinal);

        for (int i = 0; i < ticket.Projection.Count; i++)
        {
            NodeAst ast = ticket.Projection[i];
            if (ast.nodeType == NodeType.ExprAllFields)
                return null;
            string name = QueryProjectionResolver.GetOutputNameFromProjectionExpression(ast, i);
            if (!seen.Add(name))
                return null;
            names.Add(name);
        }

        return RowLayout.ForColumns(names);
    }

    private static List<string> GetVisibleProjectionColumns(QueryTicket ticket)
    {
        List<string> columns = new(ticket.Projection!.Count);

        for (int i = 0; i < ticket.Projection.Count; i++)
        {
            columns.Add(QueryProjectionResolver.GetOutputNameFromProjectionExpression(
                ticket.Projection[i],
                i));
        }

        return columns;
    }

    /// <summary>
    /// Evaluates a single projection expression for the QueryRow fast path.
    /// When <paramref name="inputQr"/> is non-null (scanner-emitted row), passes it to
    /// <see cref="SqlExecutor.EvalExpr(NodeAst,QueryRow,Dictionary{string,ColumnValue}?,QueryRowNameResolver?)"/>
    /// so column identifier lookups use ordinal access rather than the dictionary adapter.
    /// </summary>
    private static ColumnValue EvalOrProjectExprFast(
        QueryTicket ticket,
        NodeAst ast,
        IReadOnlyDictionary<string, ColumnValue> row,
        QueryRow? inputQr,
        int projectionIndex)
    {
        if (QueryExpressionClassifier.IsAggregateProjection(ast))
        {
            string key = QueryProjectionResolver.GetOutputNameFromProjectionExpression(ast, projectionIndex);
            return row[key];
        }

        NodeAst unwrapped = QueryExpressionClassifier.UnwrapAlias(ast);

        if (inputQr is not null)
            return SqlExecutor.EvalExpr(unwrapped, inputQr, ticket.Parameters, ticket.RowNameResolver);

        return SqlExecutor.EvalExpr(unwrapped, row, ticket.Parameters, ticket.RowNameResolver);
    }

    private static ColumnValue EvalOrProjectExpr(
        QueryTicket ticket,
        NodeAst ast,
        IReadOnlyDictionary<string, ColumnValue> row,
        int projectionIndex)
    {
        if (QueryExpressionClassifier.IsAggregateProjection(ast))
        {
            string key = QueryProjectionResolver.GetOutputNameFromProjectionExpression(ast, projectionIndex);
            return row[key];
        }

        return SqlExecutor.EvalExpr(
            QueryExpressionClassifier.UnwrapAlias(ast),
            row,
            ticket.Parameters,
            ticket.RowNameResolver);
    }
}
