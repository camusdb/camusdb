
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class SqlExecutor
{
    public SqlExecutor()
    {

    }

    public QueryTicket CreateQueryTicket(ExecuteSQLTicket ticket)
    {
        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql);        

        switch (ast.nodeType)
        {
            case NodeType.Select:

                string tableName = ast.rightAst!.yytext!;                

                return new(ticket.DatabaseName, tableName, null, null, ast.extendedOne, GetQueryClause(ast));

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown ast stmt");
        }
    }

    private static List<QueryOrderBy>? GetQueryClause(NodeAst ast)
    {
        if (ast.extendedTwo is null)
            return null;

        List<QueryOrderBy> orderClauses = new();

        foreach (string orderByColumn in GetIdentifierList(ast.extendedTwo))
            orderClauses.Add(new QueryOrderBy(orderByColumn, QueryOrderByType.Ascending));

        return orderClauses;        
    }

    private static List<string> GetIdentifierList(NodeAst orderByAst)
    {
        if (orderByAst.nodeType == NodeType.Identifier)
            return new() { orderByAst.yytext ?? "" };

        if (orderByAst.nodeType == NodeType.IdentifierList)
        {
            List<string> allIdentifiers = new();

            if (orderByAst.leftAst is not null)            
                allIdentifiers.AddRange(GetIdentifierList(orderByAst.leftAst));

            if (orderByAst.rightAst is not null)
                allIdentifiers.AddRange(GetIdentifierList(orderByAst.rightAst));

            return allIdentifiers;
        }        

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid order by clause");
    }
}
