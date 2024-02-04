
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
        string tableName;

        if (ast.rightAst!.nodeType == NodeType.Identifier)
            tableName = ast.rightAst.yytext!;
        else if (ast.rightAst!.nodeType == NodeType.IdentifierWithOpts)
            tableName = ast.rightAst.leftAst!.yytext!;
        else
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid table name");

        return new(
            txnId: await executor.NextTxnId().ConfigureAwait(false),
            txnType: TransactionType.ReadOnly,
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            index: GetForcedIndex(ast.rightAst),
            projection: GetProjection(ast),
            filters: null,
            where: ast.extendedOne,
            orderBy: GetQueryClause(ast),
            limit: ast.extendedThree,
            offset: ast.extendedFour,
            parameters: ticket.Parameters
        );                   
    }

    private static string? GetForcedIndex(NodeAst rightAst)
    {
        if (rightAst.nodeType == NodeType.IdentifierWithOpts)
        {
            if (rightAst.rightAst!.yytext!.Equals("FORCE_INDEX", StringComparison.InvariantCultureIgnoreCase))
                return rightAst.extendedOne!.yytext!;
        }

        return null;
    }

    private static List<NodeAst>? GetProjection(NodeAst? ast)
    {
        if (ast is null)
            return null;

        List<NodeAst> projectionList = new();

        GetProjectionFields(ast.leftAst!, projectionList);

        return projectionList.ToList();
    }

    private static void GetProjectionFields(NodeAst ast, List<NodeAst> projectionList)
    {        
        if (ast.nodeType == NodeType.IdentifierList)
        {
            if (ast.leftAst is not null)
                GetProjectionFields(ast.leftAst, projectionList);

            if (ast.rightAst is not null)
                GetProjectionFields(ast.rightAst, projectionList);

            return;
        }

        projectionList.Add(ast);
    }

    private static List<QueryOrderBy>? GetQueryClause(NodeAst ast)
    {
        if (ast.extendedTwo is null)
            return null;
        
        List<QueryOrderBy> orderClauses = new();
        List<(string, OrderType)> sortList = new();

        GetSortList(ast.extendedTwo, sortList);

        foreach ((string projectionName, OrderType type) in sortList)
            orderClauses.Add(new QueryOrderBy(projectionName, type));

        return orderClauses;
    }

    private static void GetSortList(NodeAst orderByAst, List<(string, OrderType)> sortList)
    {
        if (orderByAst.nodeType == NodeType.Identifier)
        {
            sortList.Add((orderByAst.yytext ?? "", OrderType.Ascending));
            return;
        }

        if (orderByAst.nodeType == NodeType.SortAsc)
        {
            sortList.Add((orderByAst.leftAst!.yytext ?? "", OrderType.Ascending));
            return;
        }

        if (orderByAst.nodeType == NodeType.SortDesc)
        {
            sortList.Add((orderByAst.leftAst!.yytext ?? "", OrderType.Descending));
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
