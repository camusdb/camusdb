
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
            // The aggregator keyed each projected cell by its row key, so the same keys read it back.
            string[] visibleColumns = ticket.ProjectionRowKeys;

            await foreach (QueryResultRow resultRow in dataCursor.ConfigureAwait(false))
            {
                Dictionary<string, ColumnValue> projected = new(visibleColumns.Length, StringComparer.OrdinalIgnoreCase);

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

        // Fallback path: a select list that mixes * with other items, so the output set depends on
        // the input row's columns. The explicit items' row keys depend on those columns too (an item
        // named like an expanded column must not share its cell), so they are resolved against the
        // first row and again only if a later row arrives under a different layout.
        string[]? starRowKeys = null;
        RowLayout? starKeysLayout = null;

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            RowLayout? inputLayout = (resultRow.Row as QueryRow)?.Layout;

            if (starRowKeys is null || !ReferenceEquals(inputLayout, starKeysLayout))
            {
                starRowKeys = QueryProjectionResolver.GetRowKeys(ticket.Projection, resultRow.Row.Keys);
                starKeysLayout = inputLayout;
            }

            Dictionary<string, ColumnValue> projected = new(ticket.Projection.Count, StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < ticket.Projection.Count; i++)
            {
                NodeAst ast = ticket.Projection[i];

                if (ast.nodeType == NodeType.ExprAllFields)
                {
                    foreach (KeyValuePair<string, ColumnValue> keyValue in resultRow.Row)
                        projected[keyValue.Key] = keyValue.Value;

                    continue;
                }

                projected[starRowKeys[i]] = EvalOrProjectExpr(ticket, ast, resultRow.Row, starRowKeys[i]);
            }

            yield return new(resultRow.RowId, projected);
        }
    }

    /// <summary>
    /// Builds the fixed <see cref="RowLayout"/> of the projection output, one slot per select-list
    /// item, named by the item's row key (<see cref="QueryTicket.ProjectionRowKeys"/>). Row keys are
    /// unique, so two items that share an output name (<c>SELECT a.id, b.id</c>) still get one slot
    /// each. Returns null when any item is <see cref="NodeType.ExprAllFields"/> (SELECT *): the
    /// output column set then depends on the input row and cannot be fixed ahead of the loop.
    /// </summary>
    private static RowLayout? TryBuildProjectionLayout(QueryTicket ticket)
    {
        for (int i = 0; i < ticket.Projection!.Count; i++)
        {
            if (ticket.Projection[i].nodeType == NodeType.ExprAllFields)
                return null;
        }

        return RowLayout.ForColumns(ticket.ProjectionRowKeys);
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
        // The aggregator already computed this cell and stored it under the item's row key.
        if (QueryExpressionClassifier.IsAggregateProjection(ast)
            || QueryExpressionClassifier.IsCompoundAggregateProjection(ast))
        {
            return row[ticket.ProjectionRowKeys[projectionIndex]];
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
        string rowKey)
    {
        if (QueryExpressionClassifier.IsAggregateProjection(ast)
            || QueryExpressionClassifier.IsCompoundAggregateProjection(ast))
        {
            return row[rowKey];
        }

        return SqlExecutor.EvalExpr(
            QueryExpressionClassifier.UnwrapAlias(ast),
            row,
            ticket.Parameters,
            ticket.RowNameResolver);
    }
}
