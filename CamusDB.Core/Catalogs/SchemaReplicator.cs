/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.Transactions;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Catalogs;

public sealed class SchemaReplicator
{
    private readonly CatalogsManager catalogs;

    private readonly ILogger<ICamusDB> logger;

    public SchemaReplicator(CatalogsManager catalogs, ILogger<ICamusDB> logger)
    {
        this.catalogs = catalogs;
        this.logger = logger;
    }

    public void Register(DatabaseDescriptor database)
    {
        ArgumentNullException.ThrowIfNull(database);

        IDisposable subscription = database.Kahuna.RegisterSchemaApply(
            (partitionId, bytes) => ApplyAsync(database, partitionId, bytes),
            (_, bytes) => RestoreAsync(database, bytes)
        );

        database.SetSchemaReplicationSubscription(subscription);
    }

    public async Task<bool> ApplyAsync(DatabaseDescriptor database, int partitionId, byte[] bytes)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(bytes);

        SchemaChangeLogEntry entry = DecodeEntry(bytes);

        if (!string.Equals(entry.Database, database.Name, StringComparison.Ordinal))
            return true;

        await database.Schema.Semaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            if (entry.FromVersion != database.Schema.SchemaVersion)
            {
                if (entry.ToVersion <= database.Schema.SchemaVersion && WasSchemaDeltaApplied(database.Schema, entry))
                    return true;

                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Schema change for database '{database.Name}' is out of order: expected from-version {database.Schema.SchemaVersion}, got {entry.FromVersion}"
                );
            }

            if (entry.ToVersion <= database.Schema.SchemaVersion)
                return true;

            bool isLeader = await database.Kahuna.Raft.AmILeader(partitionId, CancellationToken.None).ConfigureAwait(false);

            if (isLeader)
            {
                Schema stagedSchema = CloneSchema(database.Schema);
                TableSchema? stagedTableSchema = CatalogsManager.ApplySchemaDelta(stagedSchema, entry);

                try
                {
                    await PersistCheckpointAsync(database, stagedSchema.SchemaVersion, entry, stagedTableSchema).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogError(
                        ex,
                        "Failed to persist schema checkpoint for database {DbName} at version {SchemaVersion}; in-memory schema remains at version {CurrentVersion}",
                        database.Name,
                        entry.ToVersion,
                        database.Schema.SchemaVersion
                    );

                    return true;
                }

                TableSchema? appliedTableSchema = CatalogsManager.ApplySchemaDelta(database.Schema, entry);
                InvalidateAppliedTableDescriptor(database, entry, appliedTableSchema);
            }
            else
            {
                TableSchema? appliedTableSchema = CatalogsManager.ApplySchemaDelta(database.Schema, entry);
                InvalidateAppliedTableDescriptor(database, entry, appliedTableSchema);
            }

            logger.LogInformation(
                "Applied schema change {SchemaOp} for database {DbName}: {FromVersion}->{ToVersion}",
                entry.Op,
                database.Name,
                entry.FromVersion,
                entry.ToVersion
            );

            return true;
        }
        finally
        {
            database.Schema.Semaphore.Release();
        }
    }

    private static void InvalidateAppliedTableDescriptor(
        DatabaseDescriptor database,
        SchemaChangeLogEntry entry,
        TableSchema? tableSchema
    )
    {
        if (entry.Op == SchemaOp.DropTable && tableSchema?.Name is not null)
            database.TableDescriptors.TryRemove(tableSchema.Name, out _);
    }

    public async Task<bool> RestoreAsync(DatabaseDescriptor database, byte[] bytes)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(bytes);

        SchemaChangeLogEntry entry = DecodeEntry(bytes);

        if (!string.Equals(entry.Database, database.Name, StringComparison.Ordinal))
            return true;

        await database.Schema.Semaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            if (entry.ToVersion <= database.Schema.SchemaVersion)
                return true;

            if (entry.FromVersion != database.Schema.SchemaVersion)
            {
                logger.LogError(
                    "Skipping out-of-order restored schema change for database {DbName}: expected from-version {CurrentVersion}, got {FromVersion}, target version {ToVersion}",
                    database.Name,
                    database.Schema.SchemaVersion,
                    entry.FromVersion,
                    entry.ToVersion
                );

                return true;
            }

            CatalogsManager.ApplySchemaDelta(database.Schema, entry);

            logger.LogInformation(
                "Restored schema change {SchemaOp} for database {DbName}: {FromVersion}->{ToVersion}",
                entry.Op,
                database.Name,
                entry.FromVersion,
                entry.ToVersion
            );

            return true;
        }
        finally
        {
            database.Schema.Semaphore.Release();
        }
    }

    private async Task PersistCheckpointAsync(
        DatabaseDescriptor database,
        long schemaVersion,
        SchemaChangeLogEntry entry,
        TableSchema? tableSchema
    )
    {
        KvTransaction tx = await database.Transactions.BeginAsync().ConfigureAwait(false);

        try
        {
            if (entry.Op == SchemaOp.DropTable && tableSchema?.Id is not null)
                await catalogs.PersistDroppedTableAsync(database, tableSchema.Id, schemaVersion, tx).ConfigureAwait(false);
            else if (tableSchema is not null)
                await catalogs.PersistSchemaTableAsync(database, tableSchema, schemaVersion, tx).ConfigureAwait(false);

            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    private static SchemaChangeLogEntry DecodeEntry(byte[] bytes)
    {
        SchemaChangeLogEntry? entry = Serializator.Unserialize<SchemaChangeLogEntry>(bytes);
        if (entry is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Invalid schema replication entry");

        return entry;
    }

    private static bool WasSchemaDeltaApplied(Schema schema, SchemaChangeLogEntry entry)
    {
        return entry.Op switch
        {
            SchemaOp.CreateTable => schema.Tables.ContainsKey(DecodePayload<SchemaCreateTablePayload>(entry).TableName),
            SchemaOp.DropTable => !schema.Tables.ContainsKey(DecodePayload<SchemaDropTablePayload>(entry).TableName),
            SchemaOp.AddColumn => HasColumn(schema, DecodePayload<SchemaAlterColumnPayload>(entry)),
            SchemaOp.DropColumn => !HasColumn(schema, DecodePayload<SchemaAlterColumnPayload>(entry)),
            _ => schema.SchemaVersion >= entry.ToVersion
        };
    }

    private static bool HasColumn(Schema schema, SchemaAlterColumnPayload payload)
    {
        return schema.Tables.TryGetValue(payload.TableName, out TableSchema? table) &&
               table.Columns is not null &&
               table.Columns.Any(column => column.Name == payload.Column.Name);
    }

    private static T DecodePayload<T>(SchemaChangeLogEntry entry) where T : new()
    {
        T payload = Serializator.Unserialize<T>(entry.Payload);

        if (payload is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid payload for schema operation '{entry.Op}'");

        return payload;
    }

    internal static Schema CloneSchema(Schema schema)
    {
        Schema clone = new()
        {
            SchemaVersion = schema.SchemaVersion,
            Tables = new Dictionary<string, TableSchema>(schema.Tables.Count, schema.Tables.Comparer)
        };

        foreach ((string tableName, TableSchema table) in schema.Tables)
            clone.Tables[tableName] = CloneTable(table);

        return clone;
    }

    private static TableSchema CloneTable(TableSchema table)
    {
        return new()
        {
            Id = table.Id,
            Version = table.Version,
            Name = table.Name,
            Columns = table.Columns is null ? null : [.. table.Columns],
            SchemaHistory = table.SchemaHistory is null
                ? null
                : table.SchemaHistory.Select(CloneHistory).ToList(),
            SchemaHistoryLoader = table.SchemaHistoryLoader
        };
    }

    private static TableSchemaHistory CloneHistory(TableSchemaHistory history)
    {
        return new()
        {
            Version = history.Version,
            Columns = history.Columns is null ? null : [.. history.Columns]
        };
    }
}
