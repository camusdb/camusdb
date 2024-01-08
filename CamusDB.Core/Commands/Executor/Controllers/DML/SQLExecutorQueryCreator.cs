
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
    public async Task<QueryTicket> CreateQueryTicket(CommandExecutor executor, ExecuteSQLTicket ticket, NodeAst ast)
    {
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
            limit: ast.extendedThree,
            offset: ast.extendedFour,
            parameters: ticket.Parameters
        );                   
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
        LinkedList<(string, QueryOrderByType)> sortList = new();

        GetSortList(ast.extendedTwo, sortList);

        foreach ((string projectionName, QueryOrderByType type) in sortList)
            orderClauses.Add(new QueryOrderBy(projectionName, type));

        return orderClauses;
    }

    private static void GetSortList(NodeAst orderByAst, LinkedList<(string, QueryOrderByType)> sortList)
    {
        if (orderByAst.nodeType == NodeType.Identifier)
        {
            sortList.AddLast((orderByAst.yytext ?? "", QueryOrderByType.Ascending));
            return;
        }

        if (orderByAst.nodeType == NodeType.SortAsc)
        {
            sortList.AddLast((orderByAst.leftAst!.yytext ?? "", QueryOrderByType.Ascending));
            return;
        }

        if (orderByAst.nodeType == NodeType.SortDesc)
        {
            sortList.AddLast((orderByAst.leftAst!.yytext ?? "", QueryOrderByType.Descending));
            return;
        }

        if (orderByAst.nodeType == NodeType.IdentifierList)
        {
            if (orderByAst.leftAst is not null)
                GetSortList(orderByAst.leftAst, sortList);

            if (orderByAst.rightAst is not null)
                GetSortList(orderByAst.rightAst, sortList);

            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid order by clause");
    }
}
