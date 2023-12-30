
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */


using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

internal sealed class SQLExecutorQueryCreator : SQLExecutorBaseCreator
{
    public QueryTicket CreateQueryTicket(ExecuteSQLTicket ticket)
    {
        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql);

        switch (ast.nodeType)
        {
            case NodeType.Select:

                string tableName = ast.rightAst!.yytext!;

                return new(ticket.DatabaseName, tableName, null, null, ast.extendedOne, GetQueryClause(ast));

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown query AST stmt: " + ast.nodeType);
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
}
