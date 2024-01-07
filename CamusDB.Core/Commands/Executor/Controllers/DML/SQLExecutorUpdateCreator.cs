
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

internal sealed class SQLExecutorUpdateCreator : SQLExecutorBaseCreator
{
    internal async Task<UpdateTicket> CreateUpdateTicket(CommandExecutor executor, ExecuteSQLTicket ticket, NodeAst ast)
    {
        string tableName = ast.leftAst!.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing columns list to update");

        LinkedList<(string, ColumnValue)> updateItemList = new();

        GetUpdateItemList(ast.rightAst, new(), ticket.Parameters, updateItemList);

        Dictionary<string, ColumnValue> values = new(updateItemList.Count);

        foreach ((string columnName, ColumnValue value) updateItem in updateItemList)
            values[updateItem.columnName] = updateItem.value;

        return new(
            txnId: await executor.NextTxnId(),
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            plainValues: values,
            where: ast.extendedOne,
            filters: null,
            parameters: ticket.Parameters
        );
    }

    // @todo expressions here must be evaluated at a later stage
    // to take into account the row's values so that we can do: amount = amount + 1
    private static void GetUpdateItemList(NodeAst updateAstItemList, Dictionary<string, ColumnValue> row, Dictionary<string, ColumnValue>? parameters, LinkedList<(string, ColumnValue)> updateItemList)
    {
        if (updateAstItemList.nodeType == NodeType.UpdateItem)
        {
            updateItemList.AddLast((updateAstItemList.leftAst!.yytext ?? "", EvalExpr(updateAstItemList.rightAst!, row, parameters)));
            return;
        }

        if (updateAstItemList.nodeType == NodeType.UpdateList)
        {
            if (updateAstItemList.leftAst is not null)
                GetUpdateItemList(updateAstItemList.leftAst, row, parameters, updateItemList);

            if (updateAstItemList.rightAst is not null)
                GetUpdateItemList(updateAstItemList.rightAst, row, parameters, updateItemList);

            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid update values list");
    }
}