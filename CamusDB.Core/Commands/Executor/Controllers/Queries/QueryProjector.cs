
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
    /// <summary>
    /// Evaluates the select list for each row of <paramref name="dataCursor"/>. An item with a correlated
    /// subquery runs that subquery for each row first, and the item reads its value, see
    /// <see cref="CorrelatedSubqueryValues"/>. Every other item evaluates as before, with no extra cost.
    /// </summary>
    /// <param name="database">The database a correlated subquery reads. Only a select list that holds one
    /// needs it; a RETURNING list has none.</param>
    internal async IAsyncEnumerable<QueryResultRow> ProjectResultset(
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor,
        DatabaseDescriptor? database = null)
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
        bool[]? correlated = FindCorrelatedItems(ticket, database);

        if (projLayout is not null)
        {
            await foreach (QueryResultRow resultRow in dataCursor)
            {
                QueryRow? inputQr = resultRow.Row as QueryRow;
                ColumnValue[] values = new ColumnValue[projLayout.Count];
                Dictionary<string, ColumnValue>? parameters = correlated is null
                    ? ticket.Parameters
                    : await RowParametersAsync(ticket, correlated, resultRow.Row, database!).ConfigureAwait(false);

                for (int i = 0; i < ticket.Projection.Count; i++)
                {
                    NodeAst ast = ticket.Projection[i];
                    values[i] = EvalOrProjectExprFast(ticket, ast, resultRow.Row, inputQr, i, parameters);
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

        // Over a windowed SELECT's derived table, a * must not expand to the hidden window columns.
        bool skipHidden = QueryPostScanPipeline.IsWindowedSource(ticket);

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            RowLayout? inputLayout = (resultRow.Row as QueryRow)?.Layout;

            if (starRowKeys is null || !ReferenceEquals(inputLayout, starKeysLayout))
            {
                starRowKeys = QueryProjectionResolver.GetRowKeys(ticket.Projection, resultRow.Row.Keys);
                starKeysLayout = inputLayout;
            }

            Dictionary<string, ColumnValue> projected = new(ticket.Projection.Count, StringComparer.OrdinalIgnoreCase);
            Dictionary<string, ColumnValue>? parameters = correlated is null
                ? ticket.Parameters
                : await RowParametersAsync(ticket, correlated, resultRow.Row, database!).ConfigureAwait(false);

            for (int i = 0; i < ticket.Projection.Count; i++)
            {
                NodeAst ast = ticket.Projection[i];

                if (ast.nodeType == NodeType.ExprAllFields)
                {
                    foreach (KeyValuePair<string, ColumnValue> keyValue in resultRow.Row)
                    {
                        if (skipHidden && WindowQueryRewriter.IsHiddenColumn(keyValue.Key))
                            continue;

                        projected[keyValue.Key] = keyValue.Value;
                    }

                    continue;
                }

                projected[starRowKeys[i]] = EvalOrProjectExpr(ticket, ast, resultRow.Row, starRowKeys[i], parameters);
            }

            yield return new(resultRow.RowId, projected);
        }
    }

    /// <summary>
    /// Marks each select-list item that holds a correlated subquery, or returns null when none does, which
    /// keeps the loop free of any extra work. Computed once for the whole result.
    /// </summary>
    private static bool[]? FindCorrelatedItems(QueryTicket ticket, DatabaseDescriptor? database)
    {
        if (ticket.ExistsSubqueries is null)
            return null;

        bool[]? correlated = null;

        for (int i = 0; i < ticket.Projection!.Count; i++)
        {
            if (!CorrelatedSubqueryValues.Contains(ticket.Projection[i]))
                continue;

            if (database is null)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A select list with a correlated subquery needs the database it reads");
            }

            correlated ??= new bool[ticket.Projection.Count];
            correlated[i] = true;
        }

        return correlated;
    }

    /// <summary>The statement parameters and the value of each correlated subquery of the select list for one row.</summary>
    private static async ValueTask<Dictionary<string, ColumnValue>> RowParametersAsync(
        QueryTicket ticket,
        bool[] correlated,
        IReadOnlyDictionary<string, ColumnValue> row,
        DatabaseDescriptor database)
    {
        Dictionary<string, ColumnValue> parameters = CorrelatedSubqueryValues.NewRowParameters(ticket);

        for (int i = 0; i < correlated.Length; i++)
        {
            if (correlated[i])
                await CorrelatedSubqueryValues.AddForRowAsync(ticket.Projection![i], row, ticket, database, parameters).ConfigureAwait(false);
        }

        return parameters;
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
        int projectionIndex,
        Dictionary<string, ColumnValue>? parameters)
    {
        // The aggregator already computed this cell and stored it under the item's row key.
        if (QueryExpressionClassifier.IsAggregateProjection(ast)
            || QueryExpressionClassifier.IsCompoundAggregateProjection(ast))
        {
            return row[ticket.ProjectionRowKeys[projectionIndex]];
        }

        NodeAst unwrapped = QueryExpressionClassifier.UnwrapAlias(ast);

        if (inputQr is not null)
            return SqlExecutor.EvalExpr(unwrapped, inputQr, parameters, ticket.RowNameResolver);

        return SqlExecutor.EvalExpr(unwrapped, row, parameters, ticket.RowNameResolver);
    }

    private static ColumnValue EvalOrProjectExpr(
        QueryTicket ticket,
        NodeAst ast,
        IReadOnlyDictionary<string, ColumnValue> row,
        string rowKey,
        Dictionary<string, ColumnValue>? parameters)
    {
        if (QueryExpressionClassifier.IsAggregateProjection(ast)
            || QueryExpressionClassifier.IsCompoundAggregateProjection(ast))
        {
            return row[rowKey];
        }

        return SqlExecutor.EvalExpr(
            QueryExpressionClassifier.UnwrapAlias(ast),
            row,
            parameters,
            ticket.RowNameResolver);
    }
}
