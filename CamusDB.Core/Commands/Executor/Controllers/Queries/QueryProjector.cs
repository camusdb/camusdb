
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models;
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
