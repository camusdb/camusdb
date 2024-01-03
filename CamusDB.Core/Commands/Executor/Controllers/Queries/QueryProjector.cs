
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QueryProjector
{
    internal async IAsyncEnumerable<QueryResultRow> ProjectResultset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        if (ticket.Projection is null || ticket.Projection.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "This resultset shouldn't be projected");

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            Dictionary<string, ColumnValue> projected = new(ticket.Projection.Count);

            int i = 0;

            foreach (NodeAst ast in ticket.Projection)
            {
                if (ast.nodeType == NodeType.ExprAllFields)
                {
                    foreach (KeyValuePair<string, ColumnValue> xx in resultRow.Row)
                        projected[xx.Key] = xx.Value;
                }
                else
                {
                    if (ast.nodeType == NodeType.Identifier)
                        projected[ast.yytext!] = SqlExecutor.EvalExpr(ast, resultRow.Row, ticket.Parameters);
                    else
                        projected[(i++).ToString()] = SqlExecutor.EvalExpr(ast, resultRow.Row, ticket.Parameters);
                }
            }

            yield return new(resultRow.Tuple, projected);
        }
    }
}