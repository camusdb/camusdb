
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

        List<(string, ColumnValue)> updateItemList = GetUpdateItemList(ast.rightAst, new(), ticket.Parameters);

        Dictionary<string, ColumnValue> values = new(updateItemList.Count);

        foreach ((string columnName, ColumnValue value) updateItem in updateItemList)
            values[updateItem.columnName] = updateItem.value;

        return new(
            txnId: await executor.NextTxnId(),
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            values: values,
            where: ast.extendedOne,
            filters: null,
            parameters: ticket.Parameters
        );
    }

    // @todo expressions here must be evaluated at a later stage
    // to take into account the row's values so that we can do: amount = amount + 1
    private static List<(string, ColumnValue)> GetUpdateItemList(NodeAst updateItemList, Dictionary<string, ColumnValue> row, Dictionary<string, ColumnValue>? parameters)
    {
        if (updateItemList.nodeType == NodeType.UpdateItem)
            return new() { (updateItemList.leftAst!.yytext ?? "", EvalExpr(updateItemList.rightAst!, row, parameters)) };

        if (updateItemList.nodeType == NodeType.UpdateList)
        {
            List<(string, ColumnValue)> allUpdateItems = new();

            if (updateItemList.leftAst is not null)
                allUpdateItems.AddRange(GetUpdateItemList(updateItemList.leftAst, row, parameters));

            if (updateItemList.rightAst is not null)
                allUpdateItems.AddRange(GetUpdateItemList(updateItemList.rightAst, row, parameters));

            return allUpdateItems;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid update values list");
    }
}