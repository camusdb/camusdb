
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Serializer;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;

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

            if (database.Schema.Tables.ContainsKey(ticket.TableName))
                throw new CamusDBException(CamusDBErrorCodes.TableAlreadyExists, $"Table '{ticket.TableName}' already exists");

            TableSchema tableSchema = new()
            {
                Id = ObjectIdGenerator.Generate().ToString(),
                Version = 0,
                Name = ticket.TableName,
                Columns = new(ticket.Columns.Length),
                SchemaHistory = new()
            };

            foreach (ColumnInfo column in ticket.Columns)
            {
                tableSchema.Columns.Add(
                    new TableColumnSchema(
                        id: ObjectIdGenerator.Generate().ToString(),
                        name: column.Name,
                        type: column.Type,
                        notNull: column.NotNull,
                        defaultValue: column.Default
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

            database.Schema.Tables.Add(ticket.TableName, tableSchema);

            await PersistMetaAsync(database, tx).ConfigureAwait(false);

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

            if (!database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? tableSchema))
                throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{ticket.TableName}' does not exist");

            tableSchema.Version++;

            switch (ticket.Operation)
            {
                case AlterTableOperation.AddColumn:
                    AddColumn(tableSchema, ticket.Column);
                    break;

                case AlterTableOperation.DropColumn:
                    DropColumn(tableSchema, ticket.Column.Name);
                    break;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown alter table operation '{ticket.Operation}'");
            }

            TableSchemaHistory schemaHistory = new()
            {
                Version = tableSchema.Version,
                Columns = tableSchema.Columns,
            };

            tableSchema.SchemaHistory!.Add(schemaHistory);

            await PersistMetaAsync(database, tx).ConfigureAwait(false);

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

    private static void AddColumn(TableSchema tableSchema, ColumnInfo newColumn)
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
                defaultValue: newColumn.Default
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

    private static string SchemaKey(string dbName) => $"{dbName}/meta/schema";
    private static string SystemKey(string dbName) => $"{dbName}/meta/system";

    /// <summary>
    /// Serializes <c>Schema.Tables</c> and <c>SystemSchema</c> to Kahuna KV within
    /// the provided transaction. Must be called while holding the appropriate semaphore.
    /// </summary>
    public async Task PersistMetaAsync(DatabaseDescriptor database, KvTransaction tx)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;

        byte[] schemaBytes = Serializator.Serialize(database.Schema.Tables);
        byte[] systemBytes = Serializator.Serialize(database.SystemSchema);

        await WriteMetaKey(kahuna, tx, SchemaKey(database.Name), schemaBytes).ConfigureAwait(false);
        await WriteMetaKey(kahuna, tx, SystemKey(database.Name), systemBytes).ConfigureAwait(false);
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
                    tx.TransactionId, SchemaKey(database.Name), -1,
                    KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);

            if (schemaType == KeyValueResponseType.Get && schemaEntry?.Value is not null)
            {
                Dictionary<string, TableSchema>? tables =
                    Serializator.Unserialize<Dictionary<string, TableSchema>>(schemaEntry.Value);
                database.Schema.Tables = tables ?? new();
            }

            (KeyValueResponseType systemType, ReadOnlyKeyValueEntry? systemEntry) =
                await kahuna.LocateAndTryGetValue(
                    tx.TransactionId, SystemKey(database.Name), -1,
                    KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);

            if (systemType == KeyValueResponseType.Get && systemEntry?.Value is not null)
            {
                SystemSchema? system =
                    Serializator.Unserialize<SystemSchema>(systemEntry.Value);
                if (system is not null)
                    database.SystemSchema = system;
            }

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
}
