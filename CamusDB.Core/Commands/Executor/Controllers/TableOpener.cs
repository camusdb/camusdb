
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
using System.Linq;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Opens a table, returning a <see cref="TableDescriptor"/> that contains the schema
/// and a <see cref="KvTableStore"/> backed by the database's <see cref="EmbeddedKahuna"/> node.
/// Index metadata is read from <see cref="TableSchema.Indexes"/> (the B1 replicated source of
/// truth), falling back to <see cref="SystemSchema"/> for tables not yet migrated.
/// No B+Tree pages are loaded.
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

        // §3.4 fence: if HeadSchemaVersion − SchemaVersion > 1, at least two committed schema
        // deltas are in the apply pipeline but not yet materialised on this node. DML using a
        // schema that is more than one version behind the committed head risks mis-decoding rows
        // written under a newer schema. Reject with a retryable error so the caller can retry
        // once this node has caught up. A gap of exactly 1 (the entry currently being applied
        // under the lock) is tolerated because the two-version invariant bounds the per-row
        // schema distance to ≤1 version.
        long head = database.HeadSchemaVersion;
        long applied = database.Schema.SchemaVersion;
        if (head - applied > 1)
        {
            Diagnostics.SchemaMetrics.RecordFenceRejection(database.Name);
            throw new CamusDBException(
                CamusDBErrorCodes.SchemaCatchingUp,
                $"Database '{database.Name}' schema is catching up (head={head}, applied={applied}); retry this operation once the node has applied all committed schema changes"
            );
        }

        TableSchema tableSchema = catalogs.GetTableSchema(database, tableName);

        AsyncLazy<TableDescriptor> openTableLazy = database.TableDescriptors.GetOrAdd(
                                                        tableSchema.Name ?? "",
                                                        (_) => new(() => LoadTable(database, tableSchema))
                                                   );
        return await openTableLazy;
    }

    public ValueTask<TableDescriptor> Open(DatabaseDescriptor database, TableSource tableSource) =>
        Open(database, tableSource.TableName);

    private async Task<TableDescriptor> LoadTable(DatabaseDescriptor database, TableSchema tableSchema)
    {
        KvTableStore store = new(database.Kahuna.Kahuna, tableSchema.Id!, tableSchema.Name ?? "");

        // Key-range sharding (opt-in): mark this table's row key space as key-range routed on the
        // local node and auto-seed its initial whole-space descriptor. The Kahuna registry is
        // node-local in-memory state (not replicated), so each node registers independently when it
        // first opens the table; this AsyncLazy runs once per node per process, which is exactly the
        // "every node, every startup" contract RegisterKeyRangeAsync requires. The seed itself is a
        // single replicated meta write that only the meta-partition leader commits (a no-op on other
        // nodes — the descriptor arrives by replication). Idempotent. First slice registers the row
        // space only ({tableId}:r); indexes stay hash-routed. Never register {db}/meta (Kahuna rejects
        // it — the schema log must stay hash-routed for total ordering).
        if (CamusDBConfig.KeyRangeShardingEnabled)
            await database.Kahuna.Kahuna.RegisterKeyRangeAsync(store.RowKeySpace);

        TableDescriptor tableDescriptor = new(
            tableSchema.Id ?? "",
            tableSchema.Name ?? "",
            tableSchema,
            store
        );

        // B1: prefer TableSchema.Indexes (replicated source of truth), fall back to the
        // legacy SystemSchema path for tables not yet migrated. LoadMetaAsync populates
        // TableSchema.Indexes in-memory via MigrateIndexesFromSystemSchema, so the
        // SystemSchema fallback is only reached for code paths that open a table descriptor
        // before LoadMetaAsync has run (e.g. unit tests that bypass LoadMetaAsync).
        IReadOnlyList<TableIndexSchema> indexSource =
            tableSchema.Indexes is { Count: > 0 }
                ? tableSchema.Indexes
                : GetSystemObjectIndexes(database, tableSchema.Id ?? "").Select(
                    ix => new TableIndexSchema(ix.Id, ix.Name, ix.ColumnIds, ix.Type, ix.State, ix.StartOffset)
                ).ToList();

        foreach (TableIndexSchema entry in indexSource)
        {
            string[] columnNames = entry.ColumnIds is not null
                ? MapColumnsIdsToNames(tableSchema.Columns, entry.ColumnIds)
                : (entry.Columns ?? []);

            switch (entry.Type)
            {
                case IndexType.Unique:
                case IndexType.Multi:
                    // In-memory projection: carries resolved column names, State, and Type.
                    // Id, ColumnIds, and StartOffset are intentionally absent here — DDL and
                    // backfill read those from table.Schema.Indexes (or SystemSchema fallback),
                    // not from this descriptor. Any future query-time code that needs an index
                    // Id must read table.Schema.Indexes, not TableDescriptor.Indexes.
                    tableDescriptor.Indexes[entry.Name] =
                        new TableIndexSchema(entry.Name, columnNames, entry.Type, entry.State);
                    break;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Cannot load invalid type of index");
            }
        }

        logger.LogInformation("Table {TableName} opened", tableSchema.Name);

        return tableDescriptor;
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
