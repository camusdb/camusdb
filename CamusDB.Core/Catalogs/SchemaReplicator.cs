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

/// <summary>
/// Bridges Kahuna/Kommander's replication callbacks to the catalog. <see cref="Register"/>
/// subscribes a database to its schema-log partition; from then on every committed
/// <see cref="SchemaChangeLogEntry"/> arrives at <see cref="ApplyAsync"/> (live replication)
/// or <see cref="RestoreAsync"/> (log recovery on open).
///
/// <see cref="ApplyAsync"/> enforces ordering/idempotency (skip already-applied versions,
/// throw on gaps) and mutates the in-memory schema on every node. It does <b>not</b> persist
/// the durable KV checkpoint: doing so from inside this commit-pipeline callback re-enters the
/// same Raft partition and deadlocks. The proposer persists the checkpoint after the
/// replication round-trip returns (<c>CatalogsManager.ReplicateAndWaitLocalApplyAsync</c>);
/// the committed schema log is the source of truth and the checkpoint is a load-time
/// optimization. Acks (per node, per database) are recorded only after a delta is actually
/// applied — they drive the two-version invariant gate in <see cref="SchemaAckTracker"/>.
/// See <c>docs/distributed-schema-architecture.md</c> §6.
/// </summary>
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

        database.Kahuna.RegisterLocalSchemaAckNode(database.Name);
        database.Kahuna.RecordLocalSchemaApplied(database.Name, database.Schema.SchemaVersion);

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
                {
                    database.Kahuna.RecordLocalSchemaApplied(database.Name, entry.ToVersion);
                    return true;
                }

                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Schema change for database '{database.Name}' is out of order: expected from-version {database.Schema.SchemaVersion}, got {entry.FromVersion}"
                );
            }

            if (entry.ToVersion <= database.Schema.SchemaVersion)
            {
                database.Kahuna.RecordLocalSchemaApplied(database.Name, entry.ToVersion);
                return true;
            }

            // Apply the committed delta to the in-memory schema on every node (leader and
            // follower alike). Durable checkpoint persistence is intentionally NOT performed
            // here: this callback runs inside the schema partition's commit pipeline, and
            // issuing the checkpoint's KV writes (which themselves replicate on the same
            // partition) from this context deadlocks the partition (ProposalTimeout). The
            // proposer persists the checkpoint after the replication round-trip returns —
            // see CatalogsManager.ReplicateAndWaitLocalApplyAsync. The committed schema log
            // remains the source of truth; the KV checkpoint is a load-time optimization.
            TableSchema? appliedTableSchema = CatalogsManager.ApplySchemaDelta(database.Schema, entry);
            InvalidateAppliedTableDescriptor(database, entry, appliedTableSchema);

            logger.LogInformation(
                "Applied schema change {SchemaOp} for database {DbName}: {FromVersion}->{ToVersion}",
                entry.Op,
                database.Name,
                entry.FromVersion,
                entry.ToVersion
            );

            database.Kahuna.RecordLocalSchemaApplied(database.Name, entry.ToVersion);

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
        {
            database.TableDescriptors.TryRemove(tableSchema.Name, out _);
            return;
        }

        // For index changes the table stays alive but its cached descriptor no longer
        // reflects the updated index list — evict it so the next open rebuilds from
        // the updated table.Schema.Indexes.
        if ((entry.Op == SchemaOp.AddIndex || entry.Op == SchemaOp.DropIndex) && tableSchema?.Name is not null)
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
            {
                database.Kahuna.RecordLocalSchemaApplied(database.Name, entry.ToVersion);
                return true;
            }

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

            database.Kahuna.RecordLocalSchemaApplied(database.Name, entry.ToVersion);

            return true;
        }
        finally
        {
            database.Schema.Semaphore.Release();
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
            SchemaOp.SetElementState => HasElementState(schema, DecodePayload<SchemaElementStatePayload>(entry)),
            SchemaOp.AddIndex => HasIndex(schema, DecodePayload<SchemaIndexPayload>(entry)),
            SchemaOp.DropIndex => !HasIndex(schema, DecodePayload<SchemaIndexPayload>(entry)),
            _ => schema.SchemaVersion >= entry.ToVersion
        };
    }

    private static bool HasIndex(Schema schema, SchemaIndexPayload payload)
    {
        return schema.Tables.TryGetValue(payload.TableName, out TableSchema? table) &&
               table.Indexes is not null &&
               table.Indexes.Any(ix => ix.Name == payload.IndexName);
    }

    private static bool HasColumn(Schema schema, SchemaAlterColumnPayload payload)
    {
        return schema.Tables.TryGetValue(payload.TableName, out TableSchema? table) &&
               table.Columns is not null &&
               table.Columns.Any(column => column.Name == payload.Column.Name);
    }

    private static bool HasElementState(Schema schema, SchemaElementStatePayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? table) || table.Columns is null)
            return payload.State == SchemaElementState.Absent;

        TableColumnSchema? column = table.Columns.FirstOrDefault(column => column.Name == payload.ElementName);
        return payload.State == SchemaElementState.Absent
            ? column is null
            : column?.State == payload.State;
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
            // TableIndexSchema is immutable so a shallow list copy is sufficient.
            Indexes = table.Indexes is null ? null : [.. table.Indexes],
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
