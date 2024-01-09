
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using System.Text;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// This controller allows querying the information_schema. The tables of the information_schema
/// are simulated from the internal structures.
/// </summary>
internal sealed class SchemaQuerier
{
    private readonly CatalogsManager catalogs;

    public SchemaQuerier(CatalogsManager catalogsManager)
    {
        this.catalogs = catalogsManager;
    }

    internal async IAsyncEnumerable<QueryResultRow> ShowTables(DatabaseDescriptor database)
    {
        await Task.CompletedTask;

        BTreeTuple tuple = new(new(), new());

        foreach (KeyValuePair<string, TableSchema> table in database.Schema.Tables)
        {
            yield return new QueryResultRow(tuple, new()
            {
                { "tables", new ColumnValue(ColumnType.String, table.Key) }
            });
        }
    }

    internal async IAsyncEnumerable<QueryResultRow> ShowColumns(TableDescriptor table)
    {
        await Task.CompletedTask;

        BTreeTuple tuple = new(new(), new());

        foreach (TableColumnSchema column in table.Schema.Columns!)
        {
            yield return new QueryResultRow(tuple, new()
            {
                { "Field", new ColumnValue(ColumnType.String, column.Name) },
                { "Type", new ColumnValue(ColumnType.String, column.Type.ToString()) },
                { "Null", new ColumnValue(ColumnType.String, column.NotNull ? "NO" : "YES") },
                { "Key", new ColumnValue(ColumnType.String, column.Primary ? "KEY" : "") },
                { "Default", new ColumnValue(ColumnType.String, "NULL") },
                { "Extra", new ColumnValue(ColumnType.String, "") },
            });
        }
    }

    internal async IAsyncEnumerable<QueryResultRow> ShowCreateTable(TableDescriptor table)
    {
        await Task.CompletedTask;

        BTreeTuple tuple = new(new(), new());

        StringBuilder createTableSql = new();

        createTableSql.Append("CREATE TABLE `" + table.Name + "` (");

        int i = 0;
        var columns = table.Schema.Columns!;

        foreach (TableColumnSchema column in columns)
        {
            createTableSql.Append(' ');
            createTableSql.Append('`');
            createTableSql.Append(column.Name);
            createTableSql.Append('`');
            createTableSql.Append(' ');
            createTableSql.Append(GetSQLType(column.Type));
            createTableSql.Append(' ');
            createTableSql.Append(GetSQLConstraint(column));

            if ((++i) != columns.Count)
                createTableSql.Append(',');

            //createTableSql.Append('\n');
        }

        createTableSql.Append(");");

        yield return new QueryResultRow(tuple, new()
        {
            { "Table", new ColumnValue(ColumnType.String, table.Name) },
            { "Create Table", new ColumnValue(ColumnType.String, createTableSql.ToString()) }
        });
    }

    private static string GetSQLType(ColumnType type)
    {
        return type switch
        {
            ColumnType.String => "STRING",
            ColumnType.Id => "OID",
            ColumnType.Integer64 => "INT64",
            ColumnType.Float64 => "FLOAT64",
            ColumnType.Bool => "BOOL",
            _ => throw new NotImplementedException(),
        };
    }

    private static string GetSQLConstraint(TableColumnSchema column)
    {
        if (column.Primary)
            return "PRIMARY KEY";

        if (column.NotNull)
            return "NOT NULL";

        return "NULL";
    }
}