
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
/// Index metadata is read from <see cref="TableSchema.Indexes"/> (the replicated source of
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

        // Fence: if HeadSchemaVersion − SchemaVersion > 1, at least two committed schema
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

        // Key-range sharding (opt-in): mark this table's row and eligible index key spaces as
        // key-range routed on the local node and auto-seed their initial whole-space descriptors.
        // The Kahuna registry is node-local in-memory state (not replicated), so each node registers
        // independently when it first opens the table; this AsyncLazy runs once per node per process,
        // which is exactly the "every node, every startup" contract RegisterKeyRangeAsync requires.
        // The seed itself is a single replicated meta write that only the meta-partition leader
        // commits (a no-op on other nodes — the descriptor arrives by replication). Idempotent.
        // Never register {db}/meta (Kahuna rejects it — the schema log must stay hash-routed for
        // total ordering). Index spaces are registered below, after column types are resolved.
        //
        // DROP TABLE safety: dropping a table evicts its AsyncLazy<TableDescriptor> entry from
        // database.TableDescriptors by name — locally via TableDropper, and on the replicated apply
        // path via SchemaReplicator.InvalidateAppliedTableDescriptor (which also evicts on
        // AddIndex/DropIndex/SetElementState(Index), so index DDL rebuilds rangedIndexIds too). A
        // recreated table with the same name gets a NEW ObjectId, so RegisterKeyRangeAsync is called with a
        // brand-new key space ({newId}:r) — completely disjoint from the old one ({oldId}:r). Kahuna
        // has no deregistration API, so the old key-range descriptor stays in node memory until
        // process restart, but it is never written to and poses no correctness risk (bounded leak).
        // Lazy-on-open registration is the correct contract: K1 forwarding means the very first
        // writer pays at most one extra round-trip to establish the range descriptor.
        if (CamusDBConfig.KeyRangeShardingEnabled)
            await database.Kahuna.Kahuna.RegisterKeyRangeAsync(store.RowKeySpace);

        // Build a column-ID→type lookup used by IsIndexRangeable to gate index registration.
        // Only non-String column types (Integer64/Float64/Bool/Id/Null) encode to pure ASCII, making
        // their key spaces safe to range under ordinal string comparison end-to-end. String columns
        // stay hash-routed until the persistence-comparator alignment work (C3b) lands.
        Dictionary<string, ColumnType>? columnTypeById =
            CamusDBConfig.KeyRangeShardingEnabled && tableSchema.Columns is { Count: > 0 }
                ? tableSchema.Columns.ToDictionary(c => c.Id, c => c.Type)
                : null;

        TableDescriptor tableDescriptor = new(
            tableSchema.Id ?? "",
            tableSchema.Name ?? "",
            tableSchema,
            store
        );

        // Prefer TableSchema.Indexes (replicated source of truth), fall back to the
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

                    // Register this index's key space for key-range routing if every key
                    // column is a non-String ASCII-encoding type. String stays hash-routed (C3b).
                    if (columnTypeById is not null && IsIndexRangeable(entry, columnTypeById))
                    {
                        await database.Kahuna.Kahuna.RegisterKeyRangeAsync(store.IndexKeySpace(entry.Name));
                        store.MarkIndexAsRanged(entry.Name);
                    }
                    break;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Cannot load invalid type of index");
            }
        }

        Log.LogTableOpened(logger, tableSchema.Name!);

        return tableDescriptor;
    }

    /// <summary>
    /// Returns true when this index's key columns are safe to place in a Kahuna key-range space.
    /// All column types now encode to pure ASCII end-to-end — numeric/bool/id are inherently ASCII,
    /// and <see cref="KeyEncoder"/> encodes String as fixed-width hex (C3b) so its UTF-8 byte order
    /// matches the UTF-16-ordinal order the in-memory path uses. The only disqualifier left is
    /// missing/unresolvable column IDs (conservative fallback: keep hash-routed).
    /// </summary>
    internal static bool IsIndexRangeable(TableIndexSchema entry, Dictionary<string, ColumnType> typeById)
    {
        if (entry.ColumnIds is not { Length: > 0 })
            return false;

        foreach (string colId in entry.ColumnIds)
        {
            if (!typeById.TryGetValue(colId, out _))
                return false; // unknown column — conservative
        }
        return true;
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
