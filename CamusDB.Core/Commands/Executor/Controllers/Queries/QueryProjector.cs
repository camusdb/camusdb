﻿
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

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            Dictionary<string, ColumnValue> projected = new(ticket.Projection.Count);

            int i = 0;

            foreach (NodeAst ast in ticket.Projection)
            {
                switch (ast.nodeType)
                {
                    case NodeType.ExprAllFields:
                    {
                        foreach (KeyValuePair<string, ColumnValue> keyValue in resultRow.Row)
                            projected[keyValue.Key] = keyValue.Value;

                        continue;
                    }
                    
                    case NodeType.Identifier:
                        projected[ast.yytext!] = EvalOrProjectExpr(ast, resultRow.Row, ticket.Parameters);
                        continue;
                    
                    case NodeType.ExprAlias:
                        projected[ast.rightAst!.yytext ?? ""] = EvalOrProjectExpr(ast.leftAst!, resultRow.Row, ticket.Parameters);
                        break;
                    
                    default:
                        projected[(i++).ToString()] = EvalOrProjectExpr(ast, resultRow.Row, ticket.Parameters);
                        break;
                }
            }

            yield return new(resultRow.Tuple, projected);
        }
    }

    private static ColumnValue EvalOrProjectExpr(NodeAst ast, Dictionary<string, ColumnValue> row, Dictionary<string, ColumnValue>? parameters)
    {
        if (ast.nodeType == NodeType.ExprFuncCall && IsAggregation(ast))
            return row["0"];

        return SqlExecutor.EvalExpr(ast, row, parameters);
    }    

    private static bool IsAggregation(NodeAst nodeAst)
    {
        return nodeAst.leftAst!.yytext!.ToLowerInvariant() switch
        {
            "count" or "max" or "min" or "sum" or "avg" or "distinct" => true,
            _ => false,
        };
    }
}