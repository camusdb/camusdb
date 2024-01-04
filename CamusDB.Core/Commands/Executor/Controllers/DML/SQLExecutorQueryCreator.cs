
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
                    txnType: TransactionType.ReadOnly,
                    databaseName: ticket.DatabaseName,
                    tableName: tableName,
                    index: null,
                    projection: GetProjection(ast),
                    filters: null,
                    where: ast.extendedOne,
                    orderBy: GetQueryClause(ast),
                    parameters: ticket.Parameters
                );

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown query AST stmt: " + ast.nodeType);
        }
    }

    private static List<NodeAst>? GetProjection(NodeAst? ast)
    {
        if (ast is null)
            return null;

        LinkedList<NodeAst> projectionList = new();

        GetProjectionFields(ast.leftAst!, projectionList);

        return projectionList.ToList();
    }

    private static void GetProjectionFields(NodeAst ast, LinkedList<NodeAst> projectionList)
    {        
        if (ast.nodeType == NodeType.IdentifierList)
        {
            if (ast.leftAst is not null)
                GetProjectionFields(ast.leftAst, projectionList);

            if (ast.rightAst is not null)
                GetProjectionFields(ast.rightAst, projectionList);

            return;
        }

        projectionList.AddLast(ast);
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
