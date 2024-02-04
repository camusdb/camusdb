
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
    internal async Task<InsertTicket> CreateInsertTicket(
        CommandExecutor commandExecutor,
        DatabaseDescriptor database,
        ExecuteSQLTicket ticket,
        NodeAst ast
    )
    {
        if (ast.leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing table name");

        string tableName = ast.leftAst.yytext!;

        // If the fields are not provided, we consult them from the latest version of the schema.
        TableDescriptor table = await commandExecutor.OpenTable(new(database.Name, tableName)).ConfigureAwait(false);

        List<string> fields = new();

        if (ast.rightAst is null)
        {
            foreach (TableColumnSchema column in table.Schema.Columns!)
                fields.Add(column.Name);
        }
        else
        {
            GetIdentifierList(ast.rightAst, fields);
        }

        if (ast.extendedOne is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing or empty values list");

        List<List<ColumnValue?>> valuesList = new();
        List<Dictionary<string, ColumnValue>> batchValues = new(fields.Count);

        GetBatchValuesList(table, ast.extendedOne, new(), ticket.Parameters, valuesList);

        foreach (var x in valuesList)
        {
            if (fields.Count != valuesList.Count)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"The number of fields is not equal to the number of values.");

            Dictionary<string, ColumnValue> values = new(fields.Count);

            for (int i = 0; i < fields.Count; i++)
            {
                ColumnValue? columnValue = x.ElementAt(i); // @todo optimize this

                if (columnValue is not null)
                    values.Add(fields[i], columnValue);
                else
                    values.Add(fields[i], GetDefaultValue(table.Schema.Columns, fields[i]));
            }

            // Try to include any missing field with its default value if available
            if (values.Count != table.Schema.Columns!.Count)
            {
                foreach (TableColumnSchema column in table.Schema.Columns!)
                {
                    if (!values.ContainsKey(column.Name) && column.DefaultValue is not null)
                        values.Add(column.Name, column.DefaultValue);
                }
            }

            batchValues.Add(values);
        }

        return new InsertTicket(
            txnId: await commandExecutor.NextTxnId().ConfigureAwait(false),
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            values: batchValues
        );
    }

    private static ColumnValue GetDefaultValue(List<TableColumnSchema>? columns, string columnName)
    {
        foreach (TableColumnSchema column in columns!)
        {
            if (column.Name == columnName)
            {
                if (column.DefaultValue is not null)
                    return column.DefaultValue;
            }
        }

        return new ColumnValue(ColumnType.Null, "");
    }

    private static void GetBatchValuesList(
        TableDescriptor table,
        NodeAst batchListAst,
        Dictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        List<List<ColumnValue?>> batchValuesList
    )
    {
        if (batchListAst.nodeType == NodeType.InsertBatchList)
        {
            if (batchListAst.leftAst is not null)
                GetBatchValuesList(table, batchListAst.leftAst, row, parameters, batchValuesList);

            if (batchListAst.rightAst is not null)
                GetBatchValuesList(table, batchListAst.rightAst, row, parameters, batchValuesList);

            return;
        }

        List<ColumnValue?> valuesList = new();
        GetValuesList(table, batchListAst, row, parameters, valuesList);
        batchValuesList.Add(valuesList);
    }

    private static void GetValuesList(
        TableDescriptor table,
        NodeAst valuesListAst,
        Dictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        List<ColumnValue?> valuesList
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

        // DEFAULT expression must be evaluated at a later stage when we know the position of the value in the field list
        if (valuesListAst.nodeType == NodeType.ExprDefault)
        {
            ColumnValue? defaultValue = null;
            valuesList.Add(defaultValue);
            return;
        }

        valuesList.Add(EvalExpr(valuesListAst, row, parameters));
    }
}