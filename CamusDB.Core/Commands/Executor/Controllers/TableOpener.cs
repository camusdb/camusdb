
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Opens a table, returning a <see cref="TableDescriptor"/> that contains the schema
/// and a <see cref="KvTableStore"/> backed by the database's <see cref="EmbeddedKahuna"/> node.
/// Index metadata comes from the <see cref="SystemSchema"/>; no B+Tree pages are loaded.
/// </summary>
internal sealed class TableOpener
{
    private readonly CatalogsManager catalogs;

    private readonly ILogger<ICamusDB> logger;

    public TableOpener(CatalogsManager catalogs, ILogger<ICamusDB> logger)
    {
        this.catalogs = catalogs;
        this.logger = logger;
    }

    public async ValueTask<TableDescriptor> Open(DatabaseDescriptor database, string tableName)
    {
        if (string.IsNullOrEmpty(tableName))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, "Invalid or empty table name");

        TableSchema tableSchema = catalogs.GetTableSchema(database, tableName);

        AsyncLazy<TableDescriptor> openTableLazy = database.TableDescriptors.GetOrAdd(
                                                        tableSchema.Name ?? "",
                                                        (_) => new(() => LoadTable(database, tableSchema))
                                                   );
        return await openTableLazy;
    }

    public ValueTask<TableDescriptor> Open(DatabaseDescriptor database, TableSource tableSource) =>
        Open(database, tableSource.TableName);

    private Task<TableDescriptor> LoadTable(DatabaseDescriptor database, TableSchema tableSchema)
    {
        KvTableStore store = new(database.Kahuna.Kahuna, tableSchema.Id!);

        TableDescriptor tableDescriptor = new(
            tableSchema.Id ?? "",
            tableSchema.Name ?? "",
            tableSchema,
            store
        );

        foreach (DatabaseIndexObject index in GetSystemObjectIndexes(database, tableSchema.Id ?? ""))
        {
            switch (index.Type)
            {
                case IndexType.Unique:
                case IndexType.Multi:
                    tableDescriptor.Indexes.Add(
                        index.Name,
                        new(index.Name, MapColumnsIdsToNames(tableSchema.Columns, index.ColumnIds), index.Type, index.State)
                    );
                    break;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Cannot load invalid type of index");
            }
        }

        logger.LogInformation("Table {TableName} opened", tableSchema.Name);

        return Task.FromResult(tableDescriptor);
    }

    private static string[] MapColumnsIdsToNames(List<TableColumnSchema>? columns, string[] columnIds)
    {
        string[] columnNames = new string[columnIds.Length];

        for (int i = 0; i < columnIds.Length; i++)
        {
            foreach (TableColumnSchema column in columns!)
            {
                if (column.Id == columnIds[i])
                {
                    if (string.IsNullOrEmpty(column.Name))
                        throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, "Table system data is corrupt");

                    columnNames[i] = column.Name;
                }
            }
        }

        return columnNames;
    }

    private static List<DatabaseIndexObject> GetSystemObjectIndexes(DatabaseDescriptor database, string tableId)
    {
        List<DatabaseIndexObject> indexes = new();

        foreach (KeyValuePair<string, DatabaseIndexObject> index in database.SystemSchema.Indexes)
        {
            if (index.Value.TableId == tableId)
                indexes.Add(index.Value);
        }

        return indexes;
    }
}
