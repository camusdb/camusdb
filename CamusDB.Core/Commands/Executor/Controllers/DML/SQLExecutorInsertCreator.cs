
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

internal sealed class SQLExecutorInsertCreator : SQLExecutorBaseCreator
{
    internal async Task<InsertTicket> CreateInsertTicket(CommandExecutor commandExecutor, ExecuteSQLTicket ticket, NodeAst ast)
    {
        string tableName = ast.leftAst!.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing or empty field list");

        if (ast.extendedOne is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing or empty values list");

        List<string> fieldList = GetIdentifierList(ast.rightAst);
        List<ColumnValue> valuesList = GetInsertItemList(ast.extendedOne);

        if (fieldList.Count != valuesList.Count)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"The number of fields is not equal to the number of values.");

        Dictionary<string, ColumnValue> values = new(fieldList.Count);

        for (int i = 0; i < fieldList.Count; i++)
            values.Add(fieldList[i], valuesList[i]);

        return new InsertTicket(
            txnId: await commandExecutor.NextTxnId(),
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            values: values
        );
    }

    static List<ColumnValue> GetInsertItemList(NodeAst valuesList)
    {
        if (valuesList.nodeType == NodeType.ExprList)
        {
            List<ColumnValue> allInsertValues = new();

            if (valuesList.leftAst is not null)
                allInsertValues.AddRange(GetInsertItemList(valuesList.leftAst));

            if (valuesList.rightAst is not null)
                allInsertValues.AddRange(GetInsertItemList(valuesList.rightAst));

            return allInsertValues;
        }

        return new() { EvalExpr(valuesList, new()) };
    }
}