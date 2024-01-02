
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
    public async Task<QueryTicket> CreateQueryTicket(CommandExecutor executor, ExecuteSQLTicket ticket)
    {
        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql);

        switch (ast.nodeType)
        {
            case NodeType.Select:

                string tableName = ast.rightAst!.yytext!;

                return new(
                    txnId: await executor.NextTxnId(),
                    databaseName: ticket.DatabaseName,
                    tableName: tableName,
                    index: null,
                    filters: null,
                    where: ast.extendedOne,
                    orderBy: GetQueryClause(ast),
                    parameters: ticket.Parameters
                );

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown query AST stmt: " + ast.nodeType);
        }
    }

    private static List<QueryOrderBy>? GetQueryClause(NodeAst ast)
    {
        if (ast.extendedTwo is null)
            return null;
        
        List<QueryOrderBy> orderClauses = new();
        LinkedList<string> identifierList = new();

        GetIdentifierList(ast.extendedTwo, identifierList);

        foreach (string orderByColumn in identifierList)
            orderClauses.Add(new QueryOrderBy(orderByColumn, QueryOrderByType.Ascending));

        return orderClauses;
    }
}
