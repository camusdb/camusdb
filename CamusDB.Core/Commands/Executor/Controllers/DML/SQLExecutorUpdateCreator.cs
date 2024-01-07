
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

internal sealed class SQLExecutorUpdateCreator : SQLExecutorBaseCreator
{
    internal async Task<UpdateTicket> CreateUpdateTicket(CommandExecutor executor, ExecuteSQLTicket ticket, NodeAst ast)
    {
        string tableName = ast.leftAst!.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing columns list to update");

        LinkedList<(string, NodeAst)> updateItemList = new();

        GetUpdateItemList(ast.rightAst, updateItemList);

        Dictionary<string, NodeAst> values = new(updateItemList.Count);

        foreach ((string columnName, NodeAst value) updateItem in updateItemList)
            values[updateItem.columnName] = updateItem.value;

        return new(
            txnId: await executor.NextTxnId(),
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            plainValues: null,
            exprValues: values,
            where: ast.extendedOne,
            filters: null,
            parameters: ticket.Parameters
        );
    }

    private static void GetUpdateItemList(NodeAst updateAstItemList, LinkedList<(string, NodeAst)> updateItemList)
    {
        if (updateAstItemList.nodeType == NodeType.UpdateItem)
        {
            updateItemList.AddLast((updateAstItemList.leftAst!.yytext ?? "", updateAstItemList.rightAst!));
            return;
        }

        if (updateAstItemList.nodeType == NodeType.UpdateList)
        {
            if (updateAstItemList.leftAst is not null)
                GetUpdateItemList(updateAstItemList.leftAst, updateItemList);

            if (updateAstItemList.rightAst is not null)
                GetUpdateItemList(updateAstItemList.rightAst, updateItemList);

            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid update values list");
    }
}