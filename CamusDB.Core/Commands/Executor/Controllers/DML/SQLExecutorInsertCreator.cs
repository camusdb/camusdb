
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

internal sealed class SQLExecutorInsertCreator : SQLExecutorBaseCreator
{
    internal async Task<InsertTicket> CreateInsertTicket(CommandExecutor commandExecutor, DatabaseDescriptor database, ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing table name");

        string tableName = ast.leftAst.yytext!;

        LinkedList<string> fieldList = new();

        if (ast.rightAst is null)
        {
            //  If the fields are not provided, we consult them from the latest version of the schema.
            TableDescriptor table = await commandExecutor.OpenTable(new(database.Name, tableName));

            foreach (TableColumnSchema column in table.Schema.Columns!)
                fieldList.AddLast(column.Name);
        }
        else
        {
            GetIdentifierList(ast.rightAst, fieldList);
        }

        if (ast.extendedOne is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing or empty values list");

        LinkedList<ColumnValue> valuesList = new();

        GetInsertItemList(ast.extendedOne, new(), ticket.Parameters, valuesList);

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

    private static void GetInsertItemList(NodeAst valuesListAst, Dictionary<string, ColumnValue> row, Dictionary<string, ColumnValue>? parameters, LinkedList<ColumnValue> valuesList)
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