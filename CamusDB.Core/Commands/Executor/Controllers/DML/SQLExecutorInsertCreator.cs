
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

        // If the fields are not provided, we consult them from the latest version of the schema.
        TableDescriptor table = await commandExecutor.OpenTable(new(database.Name, tableName));

        List<string> fields;

        if (ast.rightAst is null)
        {
            fields = new();

            foreach (TableColumnSchema column in table.Schema.Columns!)
                fields.Add(column.Name);
        }
        else
        {
            LinkedList<string> fieldListLinked = new();

            GetIdentifierList(ast.rightAst, fieldListLinked);

            fields = fieldListLinked.ToList();
        }

        if (ast.extendedOne is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing or empty values list");

        LinkedList<ColumnValue> valuesList = new();

        GetValuesList(table, ast.extendedOne, new(), ticket.Parameters, valuesList);

        if (fields.Count != valuesList.Count)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"The number of fields is not equal to the number of values.");

        Dictionary<string, ColumnValue> values = new(fields.Count);

        for (int i = 0; i < fields.Count; i++)
            values.Add(fields[i], valuesList.ElementAt(i)); // @todo optimize this

        // Try to include any missing field with its default value if available
        foreach (TableColumnSchema column in table.Schema.Columns!)
        {
            if (!values.ContainsKey(column.Name) && column.DefaultValue is not null)
                values.Add(column.Name, column.DefaultValue);
        }

        return new InsertTicket(
            txnId: await commandExecutor.NextTxnId(),
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            values: values
        );
    }

    private static void GetValuesList(
        TableDescriptor table,
        NodeAst valuesListAst,
        Dictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        LinkedList<ColumnValue> valuesList
    )
    {
        if (valuesListAst.nodeType == NodeType.ExprList)
        {
            if (valuesListAst.leftAst is not null)
                GetValuesList(table, valuesListAst.leftAst, row, parameters, valuesList);

            if (valuesListAst.rightAst is not null)
                GetValuesList(table, valuesListAst.rightAst, row, parameters, valuesList);

            return;
        }

        if (valuesListAst.nodeType == NodeType.ExprDefault)
        {
            //table.
            return;
        }

        valuesList.AddLast(EvalExpr(valuesListAst, row, parameters));
    }
}