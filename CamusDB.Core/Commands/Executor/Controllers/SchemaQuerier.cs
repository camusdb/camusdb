
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

    internal async IAsyncEnumerable<QueryResultRow> ShowColumns(TableDescriptor tableName)
    {        
        await Task.CompletedTask;

        BTreeTuple tuple = new(new(), new());

        foreach (TableColumnSchema column in tableName.Schema.Columns!)
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
}