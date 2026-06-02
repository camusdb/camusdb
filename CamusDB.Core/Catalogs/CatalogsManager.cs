
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Serializer;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace CamusDB.Core.Catalogs;

/// <summary>
/// Maintains references to all objects in the database.
/// Allows knowing the description and characteristics of tables, views, indexes, etc.
/// </summary>
public sealed class CatalogsManager
{
    private readonly ILogger<ICamusDB> logger;

    public CatalogsManager(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    /// <summary>
    /// Adds a new table object to the database schema as well as its indexes.    
    /// </summary>
    /// <param name="database"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    public async Task<TableSchema> CreateTable(DatabaseDescriptor database, CreateTableTicket ticket, KvTransaction tx)
    {
        try
        {
            await database.Schema.Semaphore.WaitAsync().ConfigureAwait(false);

            SchemaChangeLogEntry entry = new()
            {
                Ts = tx.TransactionId,
                Database = database.Name,
                FromVersion = database.Schema.SchemaVersion,
                ToVersion = database.Schema.SchemaVersion + 1,
                Op = SchemaOp.CreateTable,
                Payload = Serializator.Serialize(new SchemaCreateTablePayload
                {
                    TableName = ticket.TableName,
                    Columns = ticket.Columns.Select(SchemaColumnPayload.FromColumnInfo).ToArray()
                })
            };

            TableSchema tableSchema = ApplySchemaDelta(database.Schema, entry) ?? throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Schema change '{entry.Op}' did not create table '{ticket.TableName}'"
            );

            await PersistSchemaTableAsync(database, tableSchema, tx).ConfigureAwait(false);

            logger.LogInformation("Added table {TableName} to schema", ticket.TableName);

            return tableSchema;
        }
        finally
        {
            database.Schema.Semaphore.Release();
        }
    }

    /// <summary>
    /// Modifies an existing table object allowing to add or remove columns.
    /// </summary>
    /// <param name="database"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    public async Task<TableSchema> AlterTable(DatabaseDescriptor database, AlterColumnTicket ticket, KvTransaction tx)
    {
        try
        {
            await database.Schema.Semaphore.WaitAsync().ConfigureAwait(false);

            SchemaOp op = ticket.Operation switch
            {
                AlterTableOperation.AddColumn => SchemaOp.AddColumn,
                AlterTableOperation.DropColumn => SchemaOp.DropColumn,
                _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown alter table operation '{ticket.Operation}'")
            };

            SchemaChangeLogEntry entry = new()
            {
                Ts = tx.TransactionId,
                Database = database.Name,
                FromVersion = database.Schema.SchemaVersion,
                ToVersion = database.Schema.SchemaVersion + 1,
                Op = op,
                Payload = Serializator.Serialize(new SchemaAlterColumnPayload
                {
                    TableName = ticket.TableName,
                    Column = SchemaColumnPayload.FromColumnInfo(ticket.Column)
                })
            };

            TableSchema tableSchema = ApplySchemaDelta(database.Schema, entry) ?? throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Schema change '{entry.Op}' did not alter table '{ticket.TableName}'"
            );

            await PersistSchemaTableAsync(database, tableSchema, tx).ConfigureAwait(false);

            logger.LogInformation("Modifed table {TableName} schema", ticket.TableName);

            return tableSchema;
        }
        finally
        {
            database.Schema.Semaphore.Release();
        }
    }

    /// <summary>
    /// Allows querying the current schema of a table object.
    /// </summary>
    /// <param name="database"></param>
    /// <param name="tableName"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    public TableSchema GetTableSchema(DatabaseDescriptor database, string tableName) // @todo return a snapshot instead of the schema
    {
        if (database.Schema.Tables.TryGetValue(tableName, out TableSchema? tableSchema))
            return tableSchema;

        throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{tableName}' doesn't exist");
    }

    /// <summary>
    /// Returns true if a table exists
    /// </summary>
    /// <param name="database"></param>
    /// <param name="tableName"></param>
    /// <returns></returns>
    public bool TableExists(DatabaseDescriptor database, string tableName)
    {
        return database.Schema.Tables.ContainsKey(tableName);
    }

    public static TableSchema? ApplySchemaDelta(Schema schema, SchemaChangeLogEntry entry)
    {
        TableSchema? tableSchema = entry.Op switch
        {
            SchemaOp.CreateTable => ApplyCreateTable(schema, DecodePayload<SchemaCreateTablePayload>(entry)),
            SchemaOp.DropTable => ApplyDropTable(schema, DecodePayload<SchemaDropTablePayload>(entry)),
            SchemaOp.AddColumn => ApplyAlterColumn(schema, DecodePayload<SchemaAlterColumnPayload>(entry), entry.Op),
            SchemaOp.DropColumn => ApplyAlterColumn(schema, DecodePayload<SchemaAlterColumnPayload>(entry), entry.Op),
            SchemaOp.AddIndex or SchemaOp.DropIndex or SchemaOp.SetElementState => null,
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown schema operation '{entry.Op}'")
        };

        schema.SchemaVersion = entry.ToVersion;

        return tableSchema;
    }

    private static T DecodePayload<T>(SchemaChangeLogEntry entry) where T : new()
    {
        T payload = Serializator.Unserialize<T>(entry.Payload);

        if (payload is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid payload for schema operation '{entry.Op}'");

        return payload;
    }

    private static TableSchema ApplyCreateTable(Schema schema, SchemaCreateTablePayload payload)
    {
        if (schema.Tables.ContainsKey(payload.TableName))
            throw new CamusDBException(CamusDBErrorCodes.TableAlreadyExists, $"Table '{payload.TableName}' already exists");

        TableSchema tableSchema = new()
        {
            Id = ObjectIdGenerator.Generate().ToString(),
            Version = 0,
            Name = payload.TableName,
            Columns = new(payload.Columns.Length),
            SchemaHistory = new()
        };

        foreach (SchemaColumnPayload column in payload.Columns)
        {
            tableSchema.Columns.Add(
                new TableColumnSchema(
                    id: ObjectIdGenerator.Generate().ToString(),
                    name: column.Name,
                    type: column.Type,
                    notNull: column.NotNull,
                    defaultValue: column.DefaultValue
                )
            );
        }

        // Every time a change is made to the table schema, an instance is added
        // to the history that allows reading records with old schema versions.
        TableSchemaHistory schemaHistory = new()
        {
            Version = 0,
            Columns = tableSchema.Columns,
        };

        tableSchema.SchemaHistory.Add(schemaHistory);
        schema.Tables.Add(payload.TableName, tableSchema);

        return tableSchema;
    }

    private static TableSchema? ApplyDropTable(Schema schema, SchemaDropTablePayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            return null;

        schema.Tables.Remove(payload.TableName);
        return tableSchema;
    }

    private static TableSchema ApplyAlterColumn(Schema schema, SchemaAlterColumnPayload payload, SchemaOp op)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        tableSchema.Version++;

        switch (op)
        {
            case SchemaOp.AddColumn:
                AddColumn(tableSchema, payload.Column);
                break;

            case SchemaOp.DropColumn:
                DropColumn(tableSchema, payload.Column.Name);
                break;

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown alter table operation '{op}'");
        }

        TableSchemaHistory schemaHistory = new()
        {
            Version = tableSchema.Version,
            Columns = tableSchema.Columns,
        };

        tableSchema.SchemaHistory!.Add(schemaHistory);

        return tableSchema;
    }

    private static void AddColumn(TableSchema tableSchema, SchemaColumnPayload newColumn)
    {
        bool hasColumn = false;

        List<TableColumnSchema> tableColumns = new(tableSchema.Columns!.Count);

        foreach (TableColumnSchema column in tableSchema.Columns!)
        {
            if (newColumn.Name == column.Name)
                hasColumn = true;
            else
                tableColumns.Add(column);
        }

        if (hasColumn)
            throw new CamusDBException(CamusDBErrorCodes.DuplicateColumn, $"Duplicate column '{newColumn.Name}'");

        tableColumns.Add(
            new TableColumnSchema(
                id: ObjectIdGenerator.Generate().ToString(),
                name: newColumn.Name,
                type: newColumn.Type,
                notNull: newColumn.NotNull,
                defaultValue: newColumn.DefaultValue
            )
        );

        tableSchema.Columns = tableColumns;
    }

    private static void DropColumn(TableSchema tableSchema, string columnName)
    {
        bool hasColumn = false;

        List<TableColumnSchema> tableColumns = new(tableSchema.Columns!.Count);

        foreach (TableColumnSchema column in tableSchema.Columns!)
        {
            if (columnName == column.Name)
                hasColumn = true;
            else
                tableColumns.Add(column);
        }

        if (!hasColumn)
            throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"Unknown column '{columnName}'");

        tableSchema.Columns = tableColumns;
    }

    // -----------------------------------------------------------------------
    // Schema persistence
    // -----------------------------------------------------------------------

    private static string LegacySchemaKey(string dbName) => $"{dbName}/meta/schema";
    private static string SystemKey(string dbName) => $"{dbName}/meta/system";
    private static string VersionKey(string dbName) => $"{dbName}/meta/version";
    private static string TableBucketPrefix(string dbName) => $"{dbName}/meta/table";
    private static string TableKeyPrefix(string dbName) => $"{TableBucketPrefix(dbName)}/";
    private static string TableKey(string dbName, string tableId) => $"{TableKeyPrefix(dbName)}{tableId}";
    private static string HistoryBucketPrefix(string dbName, string tableId) => $"{dbName}/meta/history/{tableId}";
    private static string HistoryKeyPrefix(string dbName, string tableId) => $"{HistoryBucketPrefix(dbName, tableId)}/";
    private static string HistoryKey(string dbName, string tableId, int version) => $"{HistoryKeyPrefix(dbName, tableId)}{version}";

    /// <summary>
    /// Persists the system schema metadata. Schema table metadata is stored per object
    /// through <see cref="PersistSchemaTableAsync"/>.
    /// </summary>
    public async Task PersistMetaAsync(DatabaseDescriptor database, KvTransaction tx)
        => await PersistSystemMetaAsync(database, tx).ConfigureAwait(false);

    public async Task PersistSystemMetaAsync(DatabaseDescriptor database, KvTransaction tx)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;

        byte[] systemBytes = MetaJsonSerializer.Serialize(database.SystemSchema, MetaJsonContext.Default.SystemSchema);

        await WriteMetaKey(kahuna, tx, SystemKey(database.Name), systemBytes).ConfigureAwait(false);
    }

    public async Task PersistSchemaTableAsync(DatabaseDescriptor database, TableSchema tableSchema, KvTransaction tx)
        => await PersistSchemaTableAsync(database, tableSchema, database.Schema.SchemaVersion, tx).ConfigureAwait(false);

    public async Task PersistSchemaTableAsync(DatabaseDescriptor database, TableSchema tableSchema, long schemaVersion, KvTransaction tx)
    {
        if (string.IsNullOrWhiteSpace(tableSchema.Id))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Table '{tableSchema.Name}' has no table id");

        IKahuna kahuna = database.Kahuna.Kahuna;

        byte[] versionBytes = MetaJsonSerializer.Serialize(schemaVersion, MetaJsonContext.Default.Int64);
        byte[] tableBytes = MetaJsonSerializer.Serialize(WithoutHistory(tableSchema), MetaJsonContext.Default.TableSchema);

        await WriteMetaKey(kahuna, tx, VersionKey(database.Name), versionBytes).ConfigureAwait(false);
        await WriteMetaKey(kahuna, tx, TableKey(database.Name, tableSchema.Id), tableBytes).ConfigureAwait(false);

        if (tableSchema.SchemaHistory is not null)
        {
            TableSchemaHistory? history = tableSchema.SchemaHistory.FirstOrDefault(x => x.Version == tableSchema.Version);
            if (history is not null)
            {
                // Schema history keys are append-only: once a table version is recorded,
                // readers may safely cache it and load it under their own read timestamp.
                byte[] historyBytes = MetaJsonSerializer.Serialize(history, MetaJsonContext.Default.TableSchemaHistory);
                await WriteMetaKey(kahuna, tx, HistoryKey(database.Name, tableSchema.Id, history.Version), historyBytes).ConfigureAwait(false);
            }
        }
    }

    public async Task PersistDroppedTableAsync(DatabaseDescriptor database, string tableId, KvTransaction tx)
        => await PersistDroppedTableAsync(database, tableId, database.Schema.SchemaVersion, tx).ConfigureAwait(false);

    public async Task PersistDroppedTableAsync(DatabaseDescriptor database, string tableId, long schemaVersion, KvTransaction tx)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;

        byte[] versionBytes = MetaJsonSerializer.Serialize(schemaVersion, MetaJsonContext.Default.Int64);

        await WriteMetaKey(kahuna, tx, VersionKey(database.Name), versionBytes).ConfigureAwait(false);
        await DeleteMetaKey(kahuna, tx, TableKey(database.Name, tableId)).ConfigureAwait(false);
    }

    /// <summary>
    /// Loads <c>Schema.Tables</c> and <c>SystemSchema</c> from Kahuna KV into the
    /// in-memory descriptor. Called once at database open time.
    /// </summary>
    public async Task LoadMetaAsync(DatabaseDescriptor database)
    {
        KvTransaction tx = await database.Transactions.BeginAsync().ConfigureAwait(false);

        try
        {
            IKahuna kahuna = database.Kahuna.Kahuna;

            (KeyValueResponseType schemaType, ReadOnlyKeyValueEntry? schemaEntry) =
                await kahuna.LocateAndTryGetValue(
                    tx.TransactionId, VersionKey(database.Name), -1,
                    KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);

            bool migratedLegacySchema = false;

            if (schemaType == KeyValueResponseType.Get && schemaEntry?.Value is not null)
            {
                database.Schema.SchemaVersion = MetaJsonSerializer.DeserializeCompat(schemaEntry.Value, MetaJsonContext.Default.Int64);
                database.Schema.Tables = await LoadTablesAsync(database, tx).ConfigureAwait(false);
            }
            else
            {
                if (database.OwnsKahuna)
                    migratedLegacySchema = await LoadAndMigrateLegacySchemaAsync(database, tx).ConfigureAwait(false);
                else
                    await LoadLegacySchemaAsync(database, tx).ConfigureAwait(false);
            }

            (KeyValueResponseType systemType, ReadOnlyKeyValueEntry? systemEntry) =
                await kahuna.LocateAndTryGetValue(
                    tx.TransactionId, SystemKey(database.Name), -1,
                    KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);

            if (systemType == KeyValueResponseType.Get && systemEntry?.Value is not null)
            {
                SystemSchema? system =
                    MetaJsonSerializer.DeserializeCompat(systemEntry.Value, MetaJsonContext.Default.SystemSchema);
                if (system is not null)
                    database.SystemSchema = system;
            }

            if (migratedLegacySchema)
                await database.Transactions.CommitAsync(tx).ConfigureAwait(false);

            logger.LogInformation(
                "Schema loaded: {Tables} table(s), {Indexes} index object(s)",
                database.Schema.Tables.Count,
                database.SystemSchema.Indexes.Count
            );
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    private async Task<Dictionary<string, TableSchema>> LoadTablesAsync(DatabaseDescriptor database, KvTransaction tx)
    {
        Dictionary<string, TableSchema> tables = new();
        IKahuna kahuna = database.Kahuna.Kahuna;
        string tableKeyPrefix = TableKeyPrefix(database.Name);

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            tx.TransactionId,
            TableBucketPrefix(database.Name),
            null, true,
            null, true,
            512,
            KeyValueDurability.Persistent,
            CancellationToken.None).ConfigureAwait(false))
        {
            if (!key.StartsWith(tableKeyPrefix, StringComparison.Ordinal) || entry.Value is null)
                continue;

            TableSchema table = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.TableSchema);
            ValidateLoadedTable(table, key);
            table.SchemaHistory = null;
            ConfigureSchemaHistoryLoader(database, table);
            tables[table.Name!] = table;
        }

        return tables;
    }

    private void ConfigureSchemaHistoryLoader(DatabaseDescriptor database, TableSchema table)
    {
        string tableId = table.Id ?? "";
        table.SchemaHistoryLoader = (txId, version) =>
            new ValueTask<TableSchemaHistory?>(LoadSchemaHistoryEntryAsync(database, tableId, txId, version));
    }

    private async Task<TableSchemaHistory?> LoadSchemaHistoryEntryAsync(DatabaseDescriptor database, string tableId, HLCTimestamp txId, int version)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) =
            await kahuna.LocateAndTryGetValue(
                txId,
                HistoryKey(database.Name, tableId, version),
                -1,
                KeyValueDurability.Persistent,
                CancellationToken.None
            ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Get || entry?.Value is null)
            return null;

        return MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.TableSchemaHistory);
    }

    private async Task<bool> LoadLegacySchemaAsync(DatabaseDescriptor database, KvTransaction tx)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;

        (KeyValueResponseType schemaType, ReadOnlyKeyValueEntry? schemaEntry) =
            await kahuna.LocateAndTryGetValue(
                tx.TransactionId, LegacySchemaKey(database.Name), -1,
                KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);

        if (schemaType != KeyValueResponseType.Get || schemaEntry?.Value is null)
            return false;

        SchemaCheckpoint checkpoint = LoadSchemaCheckpoint(schemaEntry.Value);
        database.Schema.Tables = checkpoint.Tables;
        database.Schema.SchemaVersion = checkpoint.SchemaVersion;

        return true;
    }

    private async Task<bool> LoadAndMigrateLegacySchemaAsync(DatabaseDescriptor database, KvTransaction tx)
    {
        if (!await LoadLegacySchemaAsync(database, tx).ConfigureAwait(false))
            return false;

        IKahuna kahuna = database.Kahuna.Kahuna;

        byte[] versionBytes = MetaJsonSerializer.Serialize(database.Schema.SchemaVersion, MetaJsonContext.Default.Int64);
        await WriteMetaKey(kahuna, tx, VersionKey(database.Name), versionBytes).ConfigureAwait(false);

        foreach (TableSchema table in database.Schema.Tables.Values)
        {
            ValidateLoadedTable(table, LegacySchemaKey(database.Name));
            string tableId = table.Id!;

            byte[] tableBytes = MetaJsonSerializer.Serialize(WithoutHistory(table), MetaJsonContext.Default.TableSchema);
            await WriteMetaKey(kahuna, tx, TableKey(database.Name, tableId), tableBytes).ConfigureAwait(false);

            if (table.SchemaHistory is null)
                continue;

            foreach (TableSchemaHistory history in table.SchemaHistory)
            {
                // Migration preserves the same append-only history invariant as new DDL writes.
                byte[] historyBytes = MetaJsonSerializer.Serialize(history, MetaJsonContext.Default.TableSchemaHistory);
                await WriteMetaKey(kahuna, tx, HistoryKey(database.Name, tableId, history.Version), historyBytes).ConfigureAwait(false);
            }
        }

        await DeleteMetaKey(kahuna, tx, LegacySchemaKey(database.Name)).ConfigureAwait(false);
        return true;
    }

    internal static SchemaCheckpoint LoadSchemaCheckpoint(ReadOnlySpan<byte> buffer)
    {
        string json = MetaJsonSerializer.DecodeJsonTextCompat(buffer);

        using JsonDocument document = JsonDocument.Parse(json);
        JsonElement root = document.RootElement;

        if (
            root.ValueKind == JsonValueKind.Object &&
            root.TryGetProperty(nameof(SchemaCheckpoint.FormatVersion), out JsonElement formatVersion) &&
            formatVersion.ValueKind == JsonValueKind.Number &&
            root.TryGetProperty(nameof(SchemaCheckpoint.Tables), out _)
        )
        {
            SchemaCheckpoint checkpoint = JsonSerializer.Deserialize(json, MetaJsonContext.Default.SchemaCheckpoint) ?? new();
            checkpoint.Tables ??= new();
            return checkpoint;
        }

        Dictionary<string, TableSchema> tables =
            JsonSerializer.Deserialize(json, MetaJsonContext.Default.DictionaryStringTableSchema) ?? new();

        return new()
        {
            FormatVersion = 1,
            SchemaVersion = MaxTableVersion(tables),
            Tables = tables
        };
    }

    private static long MaxTableVersion(Dictionary<string, TableSchema> tables)
    {
        long maxVersion = 0;

        foreach (TableSchema table in tables.Values)
            maxVersion = Math.Max(maxVersion, table.Version);

        return maxVersion;
    }

    private static void ValidateLoadedTable(TableSchema table, string sourceKey)
    {
        if (string.IsNullOrWhiteSpace(table.Id))
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Corrupt schema table metadata at '{sourceKey}': table id is required"
            );

        if (string.IsNullOrWhiteSpace(table.Name))
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Corrupt schema table metadata at '{sourceKey}': table name is required"
            );
    }

    private static TableSchema WithoutHistory(TableSchema tableSchema)
    {
        return new()
        {
            Id = tableSchema.Id,
            Version = tableSchema.Version,
            Name = tableSchema.Name,
            Columns = tableSchema.Columns,
            SchemaHistory = null
        };
    }

    private static async Task WriteMetaKey(IKahuna kahuna, KvTransaction tx, string key, byte[] value)
    {
        (KeyValueResponseType lockType, _, KeyValueDurability lockDurability) =
            await kahuna.LocateAndTryAcquireExclusiveLock(
                tx.TransactionId, key, 0, KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to acquire meta lock on '{key}': {lockType}"
            );

        tx.TrackLock(key, lockDurability);

        (KeyValueResponseType setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, value, null, -1,
            KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, CancellationToken.None
        ).ConfigureAwait(false);

        if (setType != KeyValueResponseType.Set)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to write meta key '{key}': {setType}"
            );

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    private static async Task DeleteMetaKey(IKahuna kahuna, KvTransaction tx, string key)
    {
        (KeyValueResponseType lockType, _, KeyValueDurability lockDurability) =
            await kahuna.LocateAndTryAcquireExclusiveLock(
                tx.TransactionId, key, 0, KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to acquire meta lock on '{key}': {lockType}"
            );

        tx.TrackLock(key, lockDurability);

        (KeyValueResponseType deleteType, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
            tx.TransactionId, key, KeyValueDurability.Persistent, CancellationToken.None
        ).ConfigureAwait(false);

        if (deleteType is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to delete meta key '{key}': {deleteType}"
            );

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }
}
