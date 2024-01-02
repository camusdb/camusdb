
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

        LinkedList<string> fieldList = new();
        LinkedList<ColumnValue> valuesList = new();

        GetIdentifierList(ast.rightAst, fieldList);
        GetInsertItemList(ast.extendedOne, new(), ticket.Parameters ?? new(), valuesList);

        if (fieldList.Count != valuesList.Count)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"The number of fields is not equal to the number of values.");

        Dictionary<string, ColumnValue> values = new(fieldList.Count);

        for (int i = 0; i < fieldList.Count; i++)
            values.Add(fieldList.ElementAt(i), valuesList.ElementAt(i)); // @todo optimize this

        return new InsertTicket(
            txnId: await commandExecutor.NextTxnId(),
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            values: values
        );
    }

    private static void GetInsertItemList(NodeAst valuesListAst, Dictionary<string, ColumnValue> row, Dictionary<string, ColumnValue> parameters, LinkedList<ColumnValue> valuesList)
    {
        if (valuesListAst.nodeType == NodeType.ExprList)
        {            
            if (valuesListAst.leftAst is not null)
                GetInsertItemList(valuesListAst.leftAst, row, parameters, valuesList);

            if (valuesListAst.rightAst is not null)
                GetInsertItemList(valuesListAst.rightAst, row, parameters, valuesList);

            return;
        }
        
        valuesList.AddLast(EvalExpr(valuesListAst, row, parameters));
    }
}