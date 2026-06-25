
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
using CamusDB.Core.Storage.Kv;
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

    /// <summary>
    /// Test hook: when non-null, thrown by <see cref="PersistSchemaCheckpointAsync"/> on every
    /// call. Use in DS11.5a tests to simulate exhausted persist retries without a real KV fault.
    /// </summary>
    internal Exception? TestPersistCheckpointException;

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
    public Task<TableSchema> CreateTable(DatabaseDescriptor database, CreateTableTicket ticket, KvTransaction tx)
        => CreateTableReplicatedAsync(database, ticket, tx);

    /// <summary>
    /// Modifies an existing table object allowing to add or remove columns.
    /// </summary>
    /// <param name="database"></param>
    /// <param name="ticket"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    public Task<TableSchema> AlterTable(DatabaseDescriptor database, AlterColumnTicket ticket, KvTransaction tx)
        => AlterTableReplicatedAsync(database, ticket, tx);

    public Task<TableSchema?> DropTableSchema(DatabaseDescriptor database, string tableName, string tableId, KvTransaction tx)
        => DropTableReplicatedAsync(database, tableName, tx);

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

    private async Task<TableSchema> CreateTableReplicatedAsync(DatabaseDescriptor database, CreateTableTicket ticket, KvTransaction tx)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = CreateTableEntry(database, ticket, tx);
            ValidateSchemaDelta(database.Schema, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
        return GetTableSchema(database, ticket.TableName);
    }

    private async Task<TableSchema> AlterTableReplicatedAsync(DatabaseDescriptor database, AlterColumnTicket ticket, KvTransaction tx)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = AlterTableEntry(database, ticket, tx);
            ValidateSchemaDelta(database.Schema, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
        return GetTableSchema(database, ticket.TableName);
    }

    private async Task<TableSchema?> DropTableReplicatedAsync(DatabaseDescriptor database, string tableName, KvTransaction tx)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = DropTableEntry(database, tableName, tx);
            ValidateSchemaDelta(database.Schema, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
        return null;
    }

    private static SchemaChangeLogEntry CreateTableEntry(DatabaseDescriptor database, CreateTableTicket ticket, KvTransaction tx)
    {
        SchemaColumnPayload[] columns = [.. ticket.Columns.Select(column =>
        {
            SchemaColumnPayload payload = SchemaColumnPayload.FromColumnInfo(column);
            payload.Id = ObjectIdGenerator.Generate().ToString();
            return payload;
        })];

        // Fold inline PRIMARY KEY / UNIQUE / INDEX constraints into this single CreateTable delta so
        // creating a table is exactly one schema version. The table is empty, so each index is born
        // at Public with nothing to backfill. Index ids and column ids are generated here and carried
        // in the payload so every node applies identical definitions.
        TableIndexSchema[]? indexes = BuildInlineIndexes(ticket, columns);

        return new()
        {
            Ts = tx.TransactionId,
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = SchemaOp.CreateTable,
            Payload = Serializator.Serialize(new SchemaCreateTablePayload
            {
                TableId = ObjectIdGenerator.Generate().ToString(),
                TableName = ticket.TableName,
                Columns = columns,
                Indexes = indexes
            })
        };
    }

    /// <summary>
    /// Translates a CREATE TABLE ticket's inline constraints into fully-resolved index definitions
    /// (Public state, generated index id, column ids resolved against <paramref name="columns"/>).
    /// Mirrors the validation the standalone AddIndex path performs (column existence, duplicate
    /// index name) since those constraints no longer flow through it.
    /// </summary>
    private static TableIndexSchema[]? BuildInlineIndexes(CreateTableTicket ticket, SchemaColumnPayload[] columns)
    {
        if (ticket.Constraints.Length == 0)
            return null;

        Dictionary<string, string> columnIdByName = new(columns.Length, StringComparer.Ordinal);
        foreach (SchemaColumnPayload column in columns)
            columnIdByName[column.Name] = column.Id!;

        List<TableIndexSchema> indexes = new(ticket.Constraints.Length);
        HashSet<string> seenNames = new(StringComparer.Ordinal);

        foreach (ConstraintInfo constraint in ticket.Constraints)
        {
            if (!seenNames.Add(constraint.Name))
            {
                string msg = constraint.Type == ConstraintType.PrimaryKey
                    ? $"Primary key already exists on table '{ticket.TableName}'"
                    : $"Index '{constraint.Name}' already exists on table '{ticket.TableName}'";
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, msg);
            }

            IndexType indexType = constraint.Type switch
            {
                ConstraintType.PrimaryKey => IndexType.Unique,
                ConstraintType.IndexUnique => IndexType.Unique,
                ConstraintType.IndexMulti => IndexType.Multi,
                _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Unknown constraint: " + constraint.Type)
            };

            string[] columnIds = new string[constraint.Columns.Length];
            for (int i = 0; i < constraint.Columns.Length; i++)
            {
                string columnName = constraint.Columns[i].Name;
                if (!columnIdByName.TryGetValue(columnName, out string? columnId))
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Column '{columnName}' does not exist on table '{ticket.TableName}'");
                columnIds[i] = columnId;
            }

            indexes.Add(new TableIndexSchema(
                ObjectIdGenerator.Generate().ToString(),
                constraint.Name,
                columnIds,
                indexType,
                SchemaElementState.Public,
                startOffset: null
            ));
        }

        return [.. indexes];
    }

    private static SchemaChangeLogEntry AlterTableEntry(DatabaseDescriptor database, AlterColumnTicket ticket, KvTransaction tx)
    {
        if (ticket.Operation == AlterTableOperation.RenameColumn)
        {
            return new()
            {
                Ts = tx.TransactionId,
                Database = database.Id,
                FromVersion = database.Schema.SchemaVersion,
                ToVersion = database.Schema.SchemaVersion + 1,
                Op = SchemaOp.RenameColumn,
                Payload = Serializator.Serialize(new SchemaRenamePayload
                {
                    TableName = ticket.TableName,
                    Kind = SchemaRenameKind.Column,
                    ElementName = ticket.Column.Name,
                    NewName = ticket.NewName ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "NewName is required for RenameColumn")
                })
            };
        }

        SchemaOp op = ticket.Operation switch
        {
            AlterTableOperation.AddColumn => SchemaOp.AddColumn,
            AlterTableOperation.DropColumn => SchemaOp.DropColumn,
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown alter table operation '{ticket.Operation}'")
        };

        SchemaColumnPayload column = SchemaColumnPayload.FromColumnInfo(ticket.Column);
        if (op == SchemaOp.AddColumn)
            column.Id = ObjectIdGenerator.Generate().ToString();

        return new()
        {
            Ts = tx.TransactionId,
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = op,
            Payload = Serializator.Serialize(new SchemaAlterColumnPayload
            {
                TableName = ticket.TableName,
                Column = column
            })
        };
    }

    private static SchemaChangeLogEntry RenameTableEntry(DatabaseDescriptor database, RenameTableTicket ticket, KvTransaction tx)
    {
        return new()
        {
            Ts = tx.TransactionId,
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = SchemaOp.RenameTable,
            Payload = Serializator.Serialize(new SchemaRenamePayload
            {
                TableName = ticket.TableName,
                Kind = SchemaRenameKind.Table,
                NewName = ticket.NewName
            })
        };
    }

    private async Task<bool> RenameTableReplicatedAsync(DatabaseDescriptor database, RenameTableTicket ticket, KvTransaction tx)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = RenameTableEntry(database, ticket, tx);
            ValidateSchemaDelta(database.Schema, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
        return true;
    }

    public Task<bool> RenameTable(DatabaseDescriptor database, RenameTableTicket ticket, KvTransaction tx)
        => RenameTableReplicatedAsync(database, ticket, tx);

    public async Task<bool> RenameIndexInTableAsync(
        DatabaseDescriptor database,
        AlterIndexTicket ticket,
        KvTransaction tx
    )
    {
        string newName = ticket.NewName ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "NewName is required for RenameIndex");
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = new()
            {
                Ts = tx.TransactionId,
                Database = database.Id,
                FromVersion = database.Schema.SchemaVersion,
                ToVersion = database.Schema.SchemaVersion + 1,
                Op = SchemaOp.RenameIndex,
                Payload = Serializator.Serialize(new SchemaRenamePayload
                {
                    TableName = ticket.TableName,
                    Kind = SchemaRenameKind.Index,
                    ElementName = ticket.IndexName,
                    NewName = newName
                })
            };

            ValidateSchemaDelta(database.Schema, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
        return true;
    }

    private static SchemaChangeLogEntry DropTableEntry(DatabaseDescriptor database, string tableName, KvTransaction tx)
    {
        return new()
        {
            Ts = tx.TransactionId,
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = SchemaOp.DropTable,
            Payload = Serializator.Serialize(new SchemaDropTablePayload { TableName = tableName })
        };
    }

    /// <summary>
    /// Replicates a completed AddIndex or DropIndex change to all cluster nodes via the
    /// schema log. Must be called AFTER the local work (backfill etc.) is done and
    /// <c>table.Schema.Indexes</c> reflects the final state. Only called when
    /// <c>isClusterMode</c>; standalone nodes need no replication.
    /// </summary>
    public async Task ReplicateIndexChangeAsync(
        DatabaseDescriptor database,
        AlterIndexTicket ticket,
        TableDescriptor table,
        KvTransaction tx
    )
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = ticket.Operation is AlterIndexOperation.DropIndex or AlterIndexOperation.DropPrimaryKey
                ? DropIndexEntry(database, ticket, tx)
                : AddIndexEntry(database, ticket, table, tx);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes an <c>AddColumn</c> delta with <paramref name="initialState"/> and replicates
    /// it to all cluster nodes via the schema log.  Used by
    /// <see cref="SchemaChangeCoordinator"/> to begin a staged add sequence in
    /// <c>DeleteOnly</c> rather than jumping straight to <c>Public</c>.
    /// Only valid on cluster nodes (<c>isClusterMode</c>).
    /// </summary>
    public async Task ReplicateAddColumnInStateAsync(
        DatabaseDescriptor database,
        string tableName,
        ColumnInfo column,
        SchemaElementState initialState
    )
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = new()
            {
                Database = database.Id,
                FromVersion = database.Schema.SchemaVersion,
                ToVersion = database.Schema.SchemaVersion + 1,
                Op = SchemaOp.AddColumn,
                Payload = Serializator.Serialize(new SchemaAlterColumnPayload
                {
                    TableName = tableName,
                    Column = new SchemaColumnPayload
                    {
                        Id = ObjectIdGenerator.Generate().ToString(),
                        Name = column.Name,
                        Type = column.Type,
                        NotNull = column.NotNull,
                        DefaultValue = column.Default,
                        State = initialState,
                    }
                })
            };
            ValidateSchemaDelta(database.Schema, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes a single <c>SetElementState</c> delta and replicates it to all cluster nodes
    /// via the schema log, then waits for every live node to ack the resulting version.
    /// Validates the state transition before proposing.
    /// Only valid on cluster nodes (<c>isClusterMode</c>).
    /// </summary>
    public async Task ReplicateElementStateAsync(
        DatabaseDescriptor database,
        string tableName,
        string elementName,
        SchemaElementState targetState,
        SchemaElementKind elementKind = SchemaElementKind.Column
    )
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = new()
            {
                Database = database.Id,
                FromVersion = database.Schema.SchemaVersion,
                ToVersion = database.Schema.SchemaVersion + 1,
                Op = SchemaOp.SetElementState,
                Payload = Serializator.Serialize(new SchemaElementStatePayload
                {
                    TableName = tableName,
                    ElementName = elementName,
                    State = targetState,
                    ElementKind = elementKind,
                })
            };
            ValidateSchemaDelta(database.Schema, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes an <c>AddIndex</c> delta with <paramref name="initialState"/> and replicates
    /// it to all cluster nodes via the schema log. Used by <see cref="SchemaChangeCoordinator"/>
    /// to begin the staged add sequence in <c>DeleteOnly</c> rather than jumping straight to
    /// <c>Public</c>. Only valid on cluster nodes (<c>isClusterMode</c>).
    /// </summary>
    public async Task ReplicateAddIndexInStateAsync(
        DatabaseDescriptor database,
        string tableName,
        IndexBuildInfo indexBuildInfo,
        SchemaElementState initialState
    )
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = new()
            {
                Database = database.Id,
                FromVersion = database.Schema.SchemaVersion,
                ToVersion = database.Schema.SchemaVersion + 1,
                Op = SchemaOp.AddIndex,
                Payload = Serializator.Serialize(new SchemaIndexPayload
                {
                    TableName = tableName,
                    IndexName = indexBuildInfo.IndexName,
                    Index = new TableIndexSchema(
                        id: indexBuildInfo.IndexId,
                        name: indexBuildInfo.IndexName,
                        columnIds: indexBuildInfo.ColumnIds,
                        type: indexBuildInfo.IndexType,
                        state: initialState,
                        startOffset: null
                    )
                })
            };
            ValidateSchemaDelta(database.Schema, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes a <c>DropIndex</c> delta and replicates it to all cluster nodes.
    /// Used to compensate a failed coordinator-driven add-index sequence: if the
    /// index was added in <c>DeleteOnly</c> or <c>WriteOnly</c> state but the sequence
    /// did not reach <c>Public</c>, this removes it cleanly on every node.
    /// Only valid on cluster nodes (<c>isClusterMode</c>). No-op if the index is
    /// already absent (idempotent).
    /// </summary>
    public async Task ReplicateDropIndexAsync(DatabaseDescriptor database, string tableName, string indexName)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = new()
            {
                Database = database.Id,
                FromVersion = database.Schema.SchemaVersion,
                ToVersion = database.Schema.SchemaVersion + 1,
                Op = SchemaOp.DropIndex,
                Payload = Serializator.Serialize(new SchemaIndexPayload
                {
                    TableName = tableName,
                    IndexName = indexName
                })
            };
            ValidateSchemaDelta(database.Schema, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    private static SchemaChangeLogEntry AddIndexEntry(
        DatabaseDescriptor database,
        AlterIndexTicket ticket,
        TableDescriptor table,
        KvTransaction tx
    )
    {
        // The completed index lives in table.Schema.Indexes (written by TableIndexAdder).
        TableIndexSchema? indexSchema = table.Schema.Indexes?.FirstOrDefault(ix => ix.Name == ticket.IndexName)
            ?? throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Index '{ticket.IndexName}' not found in table schema after local apply — cannot build replication entry"
            );

        return new()
        {
            Ts = tx.TransactionId,
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = SchemaOp.AddIndex,
            Payload = Serializator.Serialize(new SchemaIndexPayload
            {
                TableName = ticket.TableName,
                IndexName = ticket.IndexName,
                Index = indexSchema
            })
        };
    }

    private static SchemaChangeLogEntry DropIndexEntry(
        DatabaseDescriptor database,
        AlterIndexTicket ticket,
        KvTransaction tx
    )
    {
        return new()
        {
            Ts = tx.TransactionId,
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = SchemaOp.DropIndex,
            Payload = Serializator.Serialize(new SchemaIndexPayload
            {
                TableName = ticket.TableName,
                IndexName = ticket.IndexName
            })
        };
    }

    private async Task ReplicateAndWaitLocalApplyAsync(DatabaseDescriptor database, SchemaChangeLogEntry entry)
    {
        // Replicating the schema-log delta (and the checkpoint persist below) re-enters the
        // schema partition's serial, inline apply pipeline — which yields on the schema lock. Doing
        // it while the lock is held deadlocks that pipeline (the root cause). DDL proposers must
        // build/validate + apply the delta under the lock, then RELEASE before calling this. A
        // non-zero depth here is a bug.
        System.Diagnostics.Debug.Assert(
            database.Schema.LockDepth == 0,
            $"ReplicateAndWaitLocalApplyAsync called while Schema lock is held on database '{database.Name}' — no replicated write may run under a schema lock"
        );

        if (database.SchemaSubsystemDegraded)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Schema subsystem for database '{database.Name}' is degraded; DDL proposals are rejected until the node recovers"
            );

        await WaitForPreviousVersionAcksAsync(database, entry).ConfigureAwait(false);

        // For DropTable the table is removed from the in-memory schema during apply, so capture
        // its immutable id now (the checkpoint delete needs it once the table is gone).
        string? droppedTableId = entry.Op == SchemaOp.DropTable
            ? ResolveTableId(database, DecodePayload<SchemaDropTablePayload>(entry).TableName)
            : null;

        byte[] bytes = Serializator.Serialize(entry);
        SchemaReplicationResult result = await database.Kahuna.ReplicateSchemaChangeAsync(database.Id, bytes, CancellationToken.None).ConfigureAwait(false);

        if (result.Outcome != SchemaReplicationOutcome.Committed)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Schema change '{entry.Op}' for database '{database.Name}' was not committed: {result.Outcome} {result.Status}"
            );

        DateTime deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (database.Schema.SchemaVersion >= entry.ToVersion && WasSchemaDeltaApplied(database.Schema, entry))
                break;

            await Task.Delay(25).ConfigureAwait(false);
        }

        if (database.Schema.SchemaVersion < entry.ToVersion || !WasSchemaDeltaApplied(database.Schema, entry))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Timed out waiting for local schema apply for database '{database.Name}' version {entry.ToVersion}"
            );

        // Persist the durable KV checkpoint from this proposer context — NOT from the schema
        // apply callback, which runs inside the schema partition's commit pipeline and would
        // deadlock when its KV writes re-enter the same partition. The committed schema log is
        // already the source of truth; the checkpoint is a load-time optimization, so on a
        // persist failure we retry and then surface a typed error.
        await PersistSchemaCheckpointWithRetryAsync(database, entry, droppedTableId).ConfigureAwait(false);

        bool acked = await database.Kahuna.WaitForSchemaAcksAsync(
            database.Id,
            entry.ToVersion,
            database.Kahuna.SchemaAckWaitTimeout,
            cancellationToken: CancellationToken.None
        ).ConfigureAwait(false);

        if (!acked)
        {
            string timedOutLaggards = FormatLaggards(database.Kahuna.LastGateLaggards);
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Timed out waiting for live schema apply acknowledgements for database '{database.Name}' " +
                $"version {entry.ToVersion}; nodes that never acked: {timedOutLaggards}"
            );
        }

        if (database.Kahuna.LastGateOutcome == SchemaAckOutcome.QuorumBackstop)
            logger.LogWarning(
                "Schema ack post-commit gate for database '{Database}' version {Version} " +
                "completed via QuorumBackstop — these live nodes did not ack within the " +
                "backstop window ({BackstopMs}ms) and are lagging (will be fenced until they apply " +
                "the committed schema entry): {Laggards}",
                database.Name, entry.ToVersion,
                (long)database.Kahuna.SchemaAckQuorumBackstopDelay.TotalMilliseconds,
                FormatLaggards(database.Kahuna.LastGateLaggards)
            );
    }

    private static string FormatLaggards(IReadOnlyList<string> laggards)
        => laggards.Count == 0 ? "(none)" : string.Join(", ", laggards);

    private static string? ResolveTableId(DatabaseDescriptor database, string tableName)
        => database.Schema.Tables.TryGetValue(tableName, out TableSchema? table) ? table.Id : null;

    private async Task PersistSchemaCheckpointWithRetryAsync(
        DatabaseDescriptor database,
        SchemaChangeLogEntry entry,
        string? droppedTableId
    )
    {
        const int maxAttempts = 3;

        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await PersistSchemaCheckpointAsync(database, entry, droppedTableId).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts)
            {
                logger.LogWarning(
                    ex,
                    "Schema checkpoint persist attempt {Attempt} failed for database {DbName} version {Version}; retrying",
                    attempt,
                    database.Name,
                    entry.ToVersion
                );

                await Task.Delay(50 * attempt).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Persist exhausted — the Raft commit already succeeded and the change is
                // live cluster-wide, so do NOT surface this to the client. Mark this node's
                // schema subsystem degraded and request a deferred schema-partition step-down.
                // The step-down is deferred (fired after the in-flight DDL CommitAsync) because
                // in single-partition clusters the schema and KV partitions are the same: stepping
                // down before CommitAsync would invalidate the in-flight KV transaction.
                // Restart replay will recover the checkpoint on the next open.
                Log.LogSchemaCheckpointExhausted(logger, ex, maxAttempts, database.Name, entry.ToVersion);

                database.MarkSchemaSubsystemDegraded();
                database.RequestDeferredSchemaStepDown();

                return; // swallow: committed log is the source of truth; degraded flag gates future DDL
            }
        }
    }

    // Schema checkpoint commits must be bounded — an unbounded CommitAsync(CT.None)
    // hangs indefinitely when the schema partition Raft actor is stalled, converting a
    // transient cluster hiccup into a permanent 60s test timeout (or production DDL hang).
    // 30 s gives an in-process cluster sufficient headroom on a loaded CI runner;
    // the outer retry loop treats a timeout as a persist failure and eventually takes
    // the persist-exhausted path, keeping DDL liveness intact.
    private static readonly TimeSpan CheckpointCommitTimeout = TimeSpan.FromSeconds(30);

    private async Task PersistSchemaCheckpointAsync(
        DatabaseDescriptor database,
        SchemaChangeLogEntry entry,
        string? droppedTableId
    )
    {
        if (TestPersistCheckpointException is { } fault)
            throw fault;

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);

        try
        {
            if (entry.Op == SchemaOp.DropTable)
            {
                if (droppedTableId is not null)
                    await PersistDroppedTableAsync(database, droppedTableId, entry.ToVersion, tx).ConfigureAwait(false);
            }
            else
            {
                string tableName = GetEntryTableName(entry);
                if (database.Schema.Tables.TryGetValue(tableName, out TableSchema? tableSchema))
                    await PersistSchemaTableAsync(database, tableSchema, entry.ToVersion, tx).ConfigureAwait(false);
            }

            using CancellationTokenSource cts = new(CheckpointCommitTimeout);
            await database.Transactions.CommitAsync(tx, cts.Token).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    private static string GetEntryTableName(SchemaChangeLogEntry entry) => entry.Op switch
    {
        SchemaOp.CreateTable => DecodePayload<SchemaCreateTablePayload>(entry).TableName,
        SchemaOp.AddColumn or SchemaOp.DropColumn => DecodePayload<SchemaAlterColumnPayload>(entry).TableName,
        SchemaOp.SetElementState => DecodePayload<SchemaElementStatePayload>(entry).TableName,
        SchemaOp.DropTable => DecodePayload<SchemaDropTablePayload>(entry).TableName,
        SchemaOp.AddIndex or SchemaOp.DropIndex => DecodePayload<SchemaIndexPayload>(entry).TableName,
        // For RenameColumn/RenameIndex the table name is unchanged; for RenameTable the table
        // lives under the new name after apply, so use NewName so the persist path finds it.
        SchemaOp.RenameTable => DecodePayload<SchemaRenamePayload>(entry).NewName,
        SchemaOp.RenameColumn or SchemaOp.RenameIndex => DecodePayload<SchemaRenamePayload>(entry).TableName,
        _ => throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Cannot resolve table name for schema operation '{entry.Op}'"
        )
    };

    private static async Task WaitForPreviousVersionAcksAsync(DatabaseDescriptor database, SchemaChangeLogEntry entry)
    {
        // Safety: this is the PRE-PROPOSAL gate that enforces the two-version invariant.
        // The quorum backstop MUST NOT fire here — allowing quorum-only convergence on this gate
        // would let the proposer advance N→N+1 while a minority sits at N−1, breaking the
        // invariant and exposing those nodes to mis-decode. enforceFullConvergence=true disables
        // the backstop for this call while keeping it active for the post-commit gate below.
        bool acked = await database.Kahuna.WaitForSchemaAcksAsync(
            database.Id,
            entry.FromVersion,
            database.Kahuna.SchemaAckWaitTimeout,
            enforceFullConvergence: true,
            cancellationToken: CancellationToken.None
        ).ConfigureAwait(false);

        if (acked)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Timed out waiting for live schema apply acknowledgements before proposing schema change '{entry.Op}' for database '{database.Name}' from version {entry.FromVersion}"
        );
    }

    private static void ValidateSchemaDelta(Schema schema, SchemaChangeLogEntry entry)
    {
        Schema clone = SchemaReplicator.CloneSchema(schema);
        ApplySchemaDelta(clone, entry);
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
            SchemaOp.RenameTable or SchemaOp.RenameColumn or SchemaOp.RenameIndex => WasRenamed(schema, DecodePayload<SchemaRenamePayload>(entry)),
            _ => schema.SchemaVersion >= entry.ToVersion
        };
    }

    private static bool WasRenamed(Schema schema, SchemaRenamePayload payload)
    {
        return payload.Kind switch
        {
            SchemaRenameKind.Table =>
                schema.Tables.ContainsKey(payload.NewName) && !schema.Tables.ContainsKey(payload.TableName),
            SchemaRenameKind.Column =>
                schema.Tables.TryGetValue(payload.TableName, out TableSchema? ct) &&
                ct.Columns is not null &&
                ct.Columns.Any(c => c.Name == payload.NewName),
            SchemaRenameKind.Index =>
                schema.Tables.TryGetValue(payload.TableName, out TableSchema? it) &&
                it.Indexes is not null &&
                it.Indexes.Any(ix => ix.Name == payload.NewName),
            _ => false
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
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? table))
            return payload.State == SchemaElementState.Absent;

        if (payload.ElementKind == SchemaElementKind.Index)
        {
            TableIndexSchema? index = table.Indexes?.FirstOrDefault(ix => ix.Name == payload.ElementName);
            return payload.State == SchemaElementState.Absent
                ? index is null
                : index?.State == payload.State;
        }

        if (table.Columns is null)
            return payload.State == SchemaElementState.Absent;

        TableColumnSchema? column = table.Columns.FirstOrDefault(column => column.Name == payload.ElementName);
        return payload.State == SchemaElementState.Absent
            ? column is null
            : column?.State == payload.State;
    }

    public static TableSchema? ApplySchemaDelta(Schema schema, SchemaChangeLogEntry entry)
    {
        TableSchema? tableSchema = entry.Op switch
        {
            SchemaOp.CreateTable => ApplyCreateTable(schema, DecodePayload<SchemaCreateTablePayload>(entry)),
            SchemaOp.DropTable => ApplyDropTable(schema, DecodePayload<SchemaDropTablePayload>(entry)),
            SchemaOp.AddColumn => ApplyAlterColumn(schema, DecodePayload<SchemaAlterColumnPayload>(entry), entry.Op),
            SchemaOp.DropColumn => ApplyAlterColumn(schema, DecodePayload<SchemaAlterColumnPayload>(entry), entry.Op),
            SchemaOp.SetElementState => ApplyElementState(schema, DecodePayload<SchemaElementStatePayload>(entry)),
            SchemaOp.AddIndex => ApplyAddIndex(schema, DecodePayload<SchemaIndexPayload>(entry)),
            SchemaOp.DropIndex => ApplyDropIndex(schema, DecodePayload<SchemaIndexPayload>(entry)),
            SchemaOp.RenameTable => ApplyRenameTable(schema, DecodePayload<SchemaRenamePayload>(entry)),
            SchemaOp.RenameColumn => ApplyRenameColumn(schema, DecodePayload<SchemaRenamePayload>(entry)),
            SchemaOp.RenameIndex => ApplyRenameIndex(schema, DecodePayload<SchemaRenamePayload>(entry)),
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
            Id = string.IsNullOrWhiteSpace(payload.TableId) ? ObjectIdGenerator.Generate().ToString() : payload.TableId,
            Version = 0,
            Name = payload.TableName,
            Columns = new(payload.Columns.Length),
            SchemaHistory = []
        };

        foreach (SchemaColumnPayload column in payload.Columns)
        {
            tableSchema.Columns.Add(
                new TableColumnSchema(
                    id: string.IsNullOrWhiteSpace(column.Id) ? ObjectIdGenerator.Generate().ToString() : column.Id,
                    name: column.Name,
                    type: column.Type,
                    notNull: column.NotNull,
                    defaultValue: column.DefaultValue,
                    state: column.State,
                    maxLength: column.MaxLength,
                    arrayElementType: column.ArrayElementType
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

        // Inline constraints folded into the CreateTable delta (see BuildInlineIndexes). The table
        // is empty so the indexes are already Public — no backfill, no separate AddIndex delta.
        if (payload.Indexes is { Length: > 0 })
            tableSchema.Indexes = [.. payload.Indexes];

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

    /// <summary>
    /// Applies an AddIndex delta. Idempotent: if an entry with the same name already exists
    /// (e.g. the proposer already applied it locally before proposing), it is replaced.
    /// TableSchema.Version is intentionally NOT bumped — see TableSchema.Version doc.
    /// </summary>
    private static TableSchema ApplyAddIndex(Schema schema, SchemaIndexPayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        if (payload.Index is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"AddIndex payload for '{payload.IndexName}' carries no index definition");

        tableSchema.Indexes ??= [];
        tableSchema.Indexes.RemoveAll(ix => ix.Name == payload.IndexName);
        tableSchema.Indexes.Add(payload.Index);
        return tableSchema;
    }

    /// <summary>
    /// Applies a DropIndex delta. Idempotent: if the index is already absent the operation
    /// is a no-op. Returns the table even when the index was absent, because the schema
    /// version must still advance and the checkpoint must still be persisted.
    /// </summary>
    private static TableSchema? ApplyDropIndex(Schema schema, SchemaIndexPayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            return null;

        tableSchema.Indexes?.RemoveAll(ix => ix.Name == payload.IndexName);
        return tableSchema;
    }

    /// <summary>
    /// Re-keys <c>schema.Tables</c> and mutates <c>TableSchema.Name</c> in place. The same
    /// <c>TableSchema</c> instance is preserved so pin/visibility closures keep tracking it.
    /// No version bump — the re-key already invalidates pinned txns (old key gone).
    /// </summary>
    private static TableSchema ApplyRenameTable(Schema schema, SchemaRenamePayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        if (schema.Tables.ContainsKey(payload.NewName))
            throw new CamusDBException(CamusDBErrorCodes.TableAlreadyExists, $"Table '{payload.NewName}' already exists");

        // G3a: coordinator jobs are keyed by table name. If any child column or index is
        // mid-ladder (non-terminal state), renaming the table would orphan the job because
        // ResumeJobsAsync resolves Schema.Tables[oldName] → gone after the rename.
        if (tableSchema.Columns is not null)
        {
            TableColumnSchema? midLadder = tableSchema.Columns
                .FirstOrDefault(c => c.State != SchemaElementState.Public && c.State != SchemaElementState.Absent);
            if (midLadder is not null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"Cannot rename table '{payload.TableName}': column '{midLadder.Name}' has an in-flight schema change (state: {midLadder.State}). Wait for it to reach Public before renaming.");
        }

        if (tableSchema.Indexes is not null)
        {
            TableIndexSchema? midLadder = tableSchema.Indexes
                .FirstOrDefault(ix => ix.State != SchemaElementState.Public && ix.State != SchemaElementState.Absent);
            if (midLadder is not null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"Cannot rename table '{payload.TableName}': index '{midLadder.Name}' has an in-flight schema change (state: {midLadder.State}). Wait for it to reach Public before renaming.");
        }

        schema.Tables.Remove(payload.TableName);
        tableSchema.Name = payload.NewName;
        schema.Tables.Add(payload.NewName, tableSchema);

        return tableSchema;
    }

    /// <summary>
    /// Replaces a column record in the table's column list, preserving the immutable <c>Id</c>.
    /// Bumps <c>TableSchema.Version</c> and records a history entry so pins re-validate and
    /// the row decoder picks up the new name on next access.
    /// </summary>
    private static TableSchema ApplyRenameColumn(Schema schema, SchemaRenamePayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        if (string.IsNullOrEmpty(payload.ElementName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Column name is required for RenameColumn");

        if (tableSchema.Columns is null)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Table '{payload.TableName}' has no columns");

        int idx = tableSchema.Columns.FindIndex(c => c.Name == payload.ElementName);
        if (idx < 0)
            throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"Column '{payload.ElementName}' does not exist in table '{payload.TableName}'");

        if (tableSchema.Columns.Any(c => c.Name == payload.NewName))
            throw new CamusDBException(CamusDBErrorCodes.DuplicateColumn, $"Column '{payload.NewName}' already exists in table '{payload.TableName}'");

        TableColumnSchema current = tableSchema.Columns[idx];

        if (current.State != SchemaElementState.Public && current.State != SchemaElementState.Absent)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"Cannot rename column '{payload.ElementName}': it has an in-flight schema change (state: {current.State}). Wait for it to reach Public before renaming.");

        List<TableColumnSchema> columns = [.. tableSchema.Columns];
        columns[idx] = new TableColumnSchema(
            id: current.Id,
            name: payload.NewName,
            type: current.Type,
            notNull: current.NotNull,
            defaultValue: current.DefaultValue,
            state: current.State,
            maxLength: current.MaxLength,
            arrayElementType: current.ArrayElementType
        );

        tableSchema.Version++;
        tableSchema.Columns = columns;
        tableSchema.SchemaHistory ??= [];

        // Rename is purely a label change: positional encoding means every historical snapshot
        // must also reflect the new name so rows written under any previous version decode correctly.
        foreach (TableSchemaHistory history in tableSchema.SchemaHistory)
        {
            if (history.Columns is null) continue;
            int hIdx = history.Columns.FindIndex(c => c.Id == current.Id);
            if (hIdx < 0) continue;
            TableColumnSchema hCol = history.Columns[hIdx];
            history.Columns[hIdx] = new TableColumnSchema(
                id: hCol.Id, name: payload.NewName, type: hCol.Type,
                notNull: hCol.NotNull, defaultValue: hCol.DefaultValue, state: hCol.State,
                maxLength: hCol.MaxLength, arrayElementType: hCol.ArrayElementType);
        }

        tableSchema.SchemaHistory.Add(new() { Version = tableSchema.Version, Columns = tableSchema.Columns });

        return tableSchema;
    }

    /// <summary>
    /// Replaces an index entry with a renamed copy, preserving the immutable <c>Id</c>,
    /// <c>ColumnIds</c>, <c>Type</c>, <c>State</c>, and <c>StartOffset</c>.
    /// Does NOT bump <c>TableSchema.Version</c> — indexes are not part of row encoding.
    /// </summary>
    private static TableSchema ApplyRenameIndex(Schema schema, SchemaRenamePayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        if (string.IsNullOrEmpty(payload.ElementName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Index name is required for RenameIndex");

        if (tableSchema.Indexes is null || tableSchema.Indexes.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Table '{payload.TableName}' has no indexes");

        int idx = tableSchema.Indexes.FindIndex(ix => ix.Name == payload.ElementName);
        if (idx < 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Index '{payload.ElementName}' does not exist on table '{payload.TableName}'");

        if (tableSchema.Indexes.Any(ix => ix.Name == payload.NewName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Index '{payload.NewName}' already exists on table '{payload.TableName}'");

        TableIndexSchema current = tableSchema.Indexes[idx];

        if (current.State != SchemaElementState.Public && current.State != SchemaElementState.Absent)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"Cannot rename index '{payload.ElementName}': it has an in-flight schema change (state: {current.State}). Wait for it to reach Public before renaming.");

        tableSchema.Indexes[idx] = new TableIndexSchema(
            id: current.Id,
            name: payload.NewName,
            columnIds: current.ColumnIds,
            type: current.Type,
            state: current.State,
            startOffset: current.StartOffset
        );

        // TableSchema.Version is intentionally NOT bumped: indexes are not part of row encoding.
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

        tableSchema.SchemaHistory ??= [];
        tableSchema.SchemaHistory.Add(schemaHistory);

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
                id: string.IsNullOrWhiteSpace(newColumn.Id) ? ObjectIdGenerator.Generate().ToString() : newColumn.Id,
                name: newColumn.Name,
                type: newColumn.Type,
                notNull: newColumn.NotNull,
                defaultValue: newColumn.DefaultValue,
                state: newColumn.State,
                maxLength: newColumn.MaxLength,
                arrayElementType: newColumn.ArrayElementType
            )
        );

        tableSchema.Columns = tableColumns;
    }

    private static TableSchema ApplyElementState(Schema schema, SchemaElementStatePayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        if (payload.ElementKind == SchemaElementKind.Index)
            return ApplyIndexElementState(tableSchema, payload);

        if (tableSchema.Columns is null)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Table '{payload.TableName}' has no columns");

        int columnIndex = tableSchema.Columns.FindIndex(column => column.Name == payload.ElementName);
        if (columnIndex < 0)
            throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"Unknown column '{payload.ElementName}'");

        TableColumnSchema current = tableSchema.Columns[columnIndex];
        ValidateElementStateTransition(current.State, payload.State, payload.ElementName);

        if (current.State == payload.State)
            return tableSchema;

        List<TableColumnSchema> tableColumns = [.. tableSchema.Columns];

        if (payload.State == SchemaElementState.Absent)
        {
            tableColumns.RemoveAt(columnIndex);
        }
        else
        {
            tableColumns[columnIndex] = new(
                current.Id,
                current.Name,
                current.Type,
                current.NotNull,
                current.DefaultValue,
                payload.State
            );
        }

        tableSchema.Version++;
        tableSchema.Columns = tableColumns;
        tableSchema.SchemaHistory ??= [];
        tableSchema.SchemaHistory.Add(new()
        {
            Version = tableSchema.Version,
            Columns = tableSchema.Columns
        });

        return tableSchema;
    }

    /// <summary>
    /// Applies a <c>SetElementState</c> delta that targets an index. Unlike the column
    /// variant, this does NOT bump <c>tableSchema.Version</c> or write schema history —
    /// indexes are not part of the row encoding so their state changes are invisible to
    /// the row decoder.
    /// </summary>
    private static TableSchema ApplyIndexElementState(TableSchema tableSchema, SchemaElementStatePayload payload)
    {
        if (tableSchema.Indexes is null || tableSchema.Indexes.Count == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Table '{tableSchema.Name}' has no indexes — cannot apply state transition for '{payload.ElementName}'"
            );

        int indexIdx = tableSchema.Indexes.FindIndex(ix => ix.Name == payload.ElementName);
        if (indexIdx < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Unknown index '{payload.ElementName}' on table '{tableSchema.Name}'"
            );

        TableIndexSchema current = tableSchema.Indexes[indexIdx];
        ValidateElementStateTransition(current.State, payload.State, payload.ElementName);

        if (current.State == payload.State)
            return tableSchema;

        if (payload.State == SchemaElementState.Absent)
        {
            tableSchema.Indexes.RemoveAt(indexIdx);
        }
        else
        {
            tableSchema.Indexes[indexIdx] = new TableIndexSchema(
                current.Id!,
                current.Name,
                current.ColumnIds,
                current.Type,
                payload.State,
                current.StartOffset
            );
        }

        // TableSchema.Version is intentionally NOT bumped: indexes are not part of the
        // row encoding, so index state changes are invisible to the row decoder.
        return tableSchema;
    }

    private static void ValidateElementStateTransition(
        SchemaElementState current,
        SchemaElementState next,
        string elementName
    )
    {
        if (current == next)
            return;

        bool valid = (current, next) switch
        {
            (SchemaElementState.Absent, SchemaElementState.DeleteOnly) => true,
            (SchemaElementState.DeleteOnly, SchemaElementState.WriteOnly) => true,
            (SchemaElementState.WriteOnly, SchemaElementState.Public) => true,
            (SchemaElementState.Public, SchemaElementState.WriteOnly) => true,
            (SchemaElementState.WriteOnly, SchemaElementState.DeleteOnly) => true,
            (SchemaElementState.DeleteOnly, SchemaElementState.Absent) => true,
            _ => false
        };

        if (!valid)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Invalid state transition for schema element '{elementName}': {current} -> {next}"
            );
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

    // ALL meta keys share a single Kahuna routing bucket: "{dbId}/meta" — the string before the
    // last '/'. Kahuna partitions by that bucket (InversePrefixedStaticHash on the last '/'), and
    // GetByBucket matches it exactly, so every meta key must keep "{dbId}/meta" as its last-'/'
    // prefix. That is why table/history/coordinator use ':' (not '/') to separate their sub-fields:
    // it keeps them in the one bucket, so a single scan/purge of "{dbId}/meta" reaches them all.
    // (A '/' in the sub-fields would split them into per-table/per-version buckets scattered across
    // partitions, which a single "{dbId}/meta" scan cannot reach — see DatabaseDropper.)
    private static string MetaBucketPrefix(string dbId) => $"{dbId}/meta";
    private static string SystemKey(string dbId) => $"{dbId}/meta/system";
    private static string VersionKey(string dbId) => $"{dbId}/meta/version";
    private static string TableKeyPrefix(string dbId) => $"{dbId}/meta/table:";
    private static string TableKey(string dbId, string tableId) => $"{TableKeyPrefix(dbId)}{tableId}";
    private static string HistoryKeyPrefix(string dbId, string tableId) => $"{dbId}/meta/history:{tableId}:";
    private static string HistoryKey(string dbId, string tableId, int version) => $"{HistoryKeyPrefix(dbId, tableId)}{version}";
    private static string CoordinatorKeyPrefix(string dbId) => $"{dbId}/meta/coordinator:";
    // Key embeds the immutable table id, not the mutable table name.
    private static string CoordinatorKey(string dbId, string tableId, string elementName) => $"{CoordinatorKeyPrefix(dbId)}{tableId}~{elementName}";

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

        await WriteMetaKey(kahuna, tx, SystemKey(database.Id), systemBytes).ConfigureAwait(false);
    }

    public async Task PersistSchemaTableAsync(DatabaseDescriptor database, TableSchema tableSchema, KvTransaction tx)
        => await PersistSchemaTableAsync(database, tableSchema, database.Schema.SchemaVersion, tx).ConfigureAwait(false);

    public async Task PersistSchemaTableAsync(DatabaseDescriptor database, TableSchema tableSchema, long schemaVersion, KvTransaction tx)
    {
        // Replicated KV writes must never be issued while the schema lock is held.
        // A non-zero depth here means a caller violated the invariant (lock-order deadlock risk).
        System.Diagnostics.Debug.Assert(
            database.Schema.LockDepth == 0,
            $"PersistSchemaTableAsync called while Schema lock is held on database '{database.Name}' — no replicated write may run under a schema lock"
        );

        if (string.IsNullOrWhiteSpace(tableSchema.Id))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Table '{tableSchema.Name}' has no table id");

        IKahuna kahuna = database.Kahuna.Kahuna;

        byte[] versionBytes = MetaJsonSerializer.Serialize(schemaVersion, MetaJsonContext.Default.Int64);
        byte[] tableBytes = MetaJsonSerializer.Serialize(WithoutHistory(tableSchema), MetaJsonContext.Default.TableSchema);

        await WriteMetaKey(kahuna, tx, VersionKey(database.Id), versionBytes).ConfigureAwait(false);
        await WriteMetaKey(kahuna, tx, TableKey(database.Id, tableSchema.Id), tableBytes).ConfigureAwait(false);

        if (tableSchema.SchemaHistory is not null)
        {
            TableSchemaHistory? history = tableSchema.SchemaHistory.FirstOrDefault(x => x.Version == tableSchema.Version);
            if (history is not null)
            {
                // Schema history keys are append-only: once a table version is recorded,
                // readers may safely cache it and load it under their own read timestamp.
                byte[] historyBytes = MetaJsonSerializer.Serialize(history, MetaJsonContext.Default.TableSchemaHistory);
                await WriteMetaKey(kahuna, tx, HistoryKey(database.Id, tableSchema.Id, history.Version), historyBytes).ConfigureAwait(false);
            }
        }
    }

    public async Task PersistDroppedTableAsync(DatabaseDescriptor database, string tableId, KvTransaction tx)
        => await PersistDroppedTableAsync(database, tableId, database.Schema.SchemaVersion, tx).ConfigureAwait(false);

    public async Task PersistDroppedTableAsync(DatabaseDescriptor database, string tableId, long schemaVersion, KvTransaction tx)
    {
        // See PersistSchemaTableAsync above.
        System.Diagnostics.Debug.Assert(
            database.Schema.LockDepth == 0,
            $"PersistDroppedTableAsync called while Schema lock is held on database '{database.Name}' — no replicated write may run under a schema lock"
        );
        IKahuna kahuna = database.Kahuna.Kahuna;

        byte[] versionBytes = MetaJsonSerializer.Serialize(schemaVersion, MetaJsonContext.Default.Int64);

        await WriteMetaKey(kahuna, tx, VersionKey(database.Id), versionBytes).ConfigureAwait(false);
        await DeleteMetaKey(kahuna, tx, TableKey(database.Id, tableId)).ConfigureAwait(false);
    }

    /// <summary>
    /// Re-persists the complete in-memory schema (all live tables + current version) in a
    /// single KV transaction. Called after log replay completes (<c>OnRestoreFinished</c>)
    /// to bring the on-disk checkpoint up to the committed head. Respects
    /// <see cref="TestPersistCheckpointException"/> so checkpoint fault-injection tests are not
    /// accidentally fired here.
    /// </summary>
    public async Task PersistFullSchemaCheckpointAsync(DatabaseDescriptor database)
    {
        if (TestPersistCheckpointException is { } fault)
            throw fault;

        // Must not be called while Schema lock is held (deadlock risk — see class doc).
        System.Diagnostics.Debug.Assert(
            database.Schema.LockDepth == 0,
            $"PersistFullSchemaCheckpointAsync called while Schema lock is held on database '{database.Name}' — no replicated write may run under a schema lock"
        );

        IKahuna kahuna = database.Kahuna.Kahuna;
        long schemaVersion = database.Schema.SchemaVersion;

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            byte[] versionBytes = MetaJsonSerializer.Serialize(schemaVersion, MetaJsonContext.Default.Int64);
            await WriteMetaKey(kahuna, tx, VersionKey(database.Id), versionBytes).ConfigureAwait(false);

            // Snapshot the table set: callers may invoke this without holding Schema.Semaphore
            // (e.g. OnSchemaRestoreFinishedAsync, which must not hold the apply lock across these KV
            // writes — see the deadlock note there), so a concurrent live apply could otherwise
            // mutate Tables mid-iteration. A best-effort checkpoint over a point-in-time snapshot is
            // fine; the committed schema log remains the source of truth.
            foreach (TableSchema table in database.Schema.Tables.Values.ToArray())
            {
                if (string.IsNullOrWhiteSpace(table.Id))
                    continue;

                byte[] tableBytes = MetaJsonSerializer.Serialize(WithoutHistory(table), MetaJsonContext.Default.TableSchema);
                await WriteMetaKey(kahuna, tx, TableKey(database.Id, table.Id), tableBytes).ConfigureAwait(false);

                if (table.SchemaHistory is not null)
                {
                    TableSchemaHistory? current = table.SchemaHistory.FirstOrDefault(x => x.Version == table.Version);
                    if (current is not null)
                    {
                        byte[] historyBytes = MetaJsonSerializer.Serialize(current, MetaJsonContext.Default.TableSchemaHistory);
                        await WriteMetaKey(kahuna, tx, HistoryKey(database.Id, table.Id, current.Version), historyBytes).ConfigureAwait(false);
                    }
                }
            }

            using CancellationTokenSource cts = new(CheckpointCommitTimeout);
            await database.Transactions.CommitAsync(tx, cts.Token).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    // -----------------------------------------------------------------------
    // Coordinator job persistence
    // -----------------------------------------------------------------------

    public async Task PersistCoordinatorJobAsync(DatabaseDescriptor database, PersistedCoordinatorJob job)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;
        byte[] bytes = MetaJsonSerializer.Serialize(job, MetaJsonContext.Default.PersistedCoordinatorJob);

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await WriteMetaKey(kahuna, tx, CoordinatorKey(database.Id, job.TableId, job.ElementName), bytes).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    public async Task DeleteCoordinatorJobAsync(DatabaseDescriptor database, string tableId, string elementName)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await DeleteMetaKey(kahuna, tx, CoordinatorKey(database.Id, tableId, elementName)).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Deletes all persisted coordinator jobs belonging to <paramref name="tableId"/>. Called
    /// by the DROP TABLE path so orphaned job records do not survive the table they were created
    /// for, preventing stale jobs from being resumed against a new table with the same name.
    /// Because keys are anchored on the immutable table id, this prefix matches all and only
    /// the dropped table's jobs regardless of whether the table name is reused.
    /// </summary>
    public async Task DeleteCoordinatorJobsForTableAsync(DatabaseDescriptor database, string tableId)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;
        string keyPrefix = CoordinatorKeyPrefix(database.Id);
        string tableJobPrefix = $"{keyPrefix}{tableId}~";

        List<string> keysToDelete = [];

        KvTransaction scanTx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
                scanTx.TransactionId,
                MetaBucketPrefix(database.Id),
                null, true,
                null, true,
                128,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                CancellationToken.None).ConfigureAwait(false))
            {
                if (key.StartsWith(tableJobPrefix, StringComparison.Ordinal) && entry.Value is not null)
                    keysToDelete.Add(key);
            }
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(scanTx).ConfigureAwait(false);
        }

        if (keysToDelete.Count == 0)
            return;

        KvTransaction deleteTx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            foreach (string key in keysToDelete)
                await DeleteMetaKey(kahuna, deleteTx, key).ConfigureAwait(false);
            await database.Transactions.CommitAsync(deleteTx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(deleteTx).ConfigureAwait(false);
        }
    }

    public async Task<List<PersistedCoordinatorJob>> LoadCoordinatorJobsAsync(DatabaseDescriptor database)
    {
        List<PersistedCoordinatorJob> jobs = [];
        IKahuna kahuna = database.Kahuna.Kahuna;
        string keyPrefix = CoordinatorKeyPrefix(database.Id);

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
                tx.TransactionId,
                MetaBucketPrefix(database.Id),
                null, true,
                null, true,
                128,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                CancellationToken.None).ConfigureAwait(false))
            {
                if (!key.StartsWith(keyPrefix, StringComparison.Ordinal) || entry.Value is null)
                    continue;

                PersistedCoordinatorJob job = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.PersistedCoordinatorJob);
                jobs.Add(job);
            }

            return jobs;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Loads <c>Schema.Tables</c> and <c>SystemSchema</c> from Kahuna KV into the
    /// in-memory descriptor.
    /// </summary>
    public async Task LoadMetaAsync(DatabaseDescriptor database)
    {
        // Reloading metadata replaces TableSchema instances. Any open table
        // descriptors that captured the old references must be rebuilt.
        database.TableDescriptors.Clear();

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);

        try
        {
            IKahuna kahuna = database.Kahuna.Kahuna;

            (KeyValueResponseType schemaType, ReadOnlyKeyValueEntry? schemaEntry) =
                await kahuna.LocateAndTryGetValue(
                    tx.TransactionId, VersionKey(database.Id), -1,
                    HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);

            // A database with persisted schema has a version key. Absent ⟹ a fresh database with an
            // empty schema (version 0, no tables) — there is no legacy single-blob fallback to read
            // (backwards compatibility is intentionally not supported).
            if (schemaType == KeyValueResponseType.Get && schemaEntry?.Value is not null)
            {
                database.Schema.SchemaVersion = MetaJsonSerializer.DeserializeCompat(schemaEntry.Value, MetaJsonContext.Default.Int64);
                database.Schema.Tables = await LoadTablesAsync(database, tx).ConfigureAwait(false);
            }

            (KeyValueResponseType systemType, ReadOnlyKeyValueEntry? systemEntry) =
                await kahuna.LocateAndTryGetValue(
                    tx.TransactionId, SystemKey(database.Id), -1,
                    HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);

            if (systemType == KeyValueResponseType.Get && systemEntry?.Value is not null)
            {
                SystemSchema? system =
                    MetaJsonSerializer.DeserializeCompat(systemEntry.Value, MetaJsonContext.Default.SystemSchema);
                if (system is not null)
                    database.SystemSchema = system;
            }

            // Populate TableSchema.Indexes in-memory for any table that still carries
            // its indexes only in the legacy SystemSchema blob. The migration is in-memory
            // here; the next index DDL write will persist the updated TableSchema to KV via
            // PersistSchemaTableAsync (which includes Indexes via WithoutHistory).
            MigrateIndexesFromSystemSchema(database);

            Log.LogSchemaLoaded(logger, database.Schema.Tables.Count, database.SystemSchema.Indexes.Count);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            database.TableDescriptors.Clear();
        }
    }

    /// <summary>
    /// B1 migration: for every table whose <see cref="TableSchema.Indexes"/> is still null
    /// (i.e. stored only in the legacy <c>SystemSchema</c> blob), populate it in-memory from
    /// <c>database.SystemSchema.Indexes</c>. The result is used immediately by
    /// <c>TableOpener</c> so the table opens correctly; the next index DDL write will persist
    /// the populated <c>Indexes</c> list to the table's KV entry via
    /// <c>PersistSchemaTableAsync</c>.
    /// </summary>
    private static void MigrateIndexesFromSystemSchema(DatabaseDescriptor database)
    {
        if (database.SystemSchema.Indexes.Count == 0)
            return;

        foreach (TableSchema table in database.Schema.Tables.Values)
        {
            if (table.Indexes is not null && table.Indexes.Count > 0)
                continue;

            List<TableIndexSchema>? migrated = null;

            foreach (DatabaseIndexObject sysIndex in database.SystemSchema.Indexes.Values)
            {
                if (sysIndex.TableId != table.Id)
                    continue;

                migrated ??= [];
                migrated.Add(new TableIndexSchema(
                    sysIndex.Id,
                    sysIndex.Name,
                    sysIndex.ColumnIds,
                    sysIndex.Type,
                    sysIndex.State,
                    sysIndex.StartOffset
                ));
            }

            if (migrated is not null)
                table.Indexes = migrated;
        }
    }

    private async Task<Dictionary<string, TableSchema>> LoadTablesAsync(DatabaseDescriptor database, KvTransaction tx)
    {
        Dictionary<string, TableSchema> tables = new();
        IKahuna kahuna = database.Kahuna.Kahuna;
        string tableKeyPrefix = TableKeyPrefix(database.Id);

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            tx.TransactionId,
            MetaBucketPrefix(database.Id),
            null, true,
            null, true,
            512,
            HLCTimestamp.Zero,
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
                HistoryKey(database.Id, tableId, version),
                -1,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                CancellationToken.None
            ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Get || entry?.Value is null)
            return null;

        return MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.TableSchemaHistory);
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
            Indexes = tableSchema.Indexes,
            SchemaHistory = null
        };
    }

    private const int MetaKeyMaxRetries = 32;

    private static async Task WriteMetaKey(IKahuna kahuna, KvTransaction tx, string key, byte[] value)
    {
        KeyValueResponseType lockType;
        KeyValueDurability lockDurability;
        int lockRetries = 0;

        do
        {
            if (lockRetries > 0)
                await Task.Delay(lockRetries * 10).ConfigureAwait(false);

            (lockType, _, lockDurability, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                tx.TransactionId, key, 0, KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        while (lockType is KeyValueResponseType.AlreadyLocked or KeyValueResponseType.MustRetry
               && ++lockRetries < MetaKeyMaxRetries);

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to acquire meta lock on '{key}': {lockType}"
            );

        tx.TrackLock(key, lockDurability);

        KeyValueResponseType setType;
        int setRetries = 0;

        do
        {
            if (setRetries > 0)
                await Task.Delay(setRetries * 10).ConfigureAwait(false);

            (setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
                tx.TransactionId, key, value, null, -1,
                KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        while (setType is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && ++setRetries < MetaKeyMaxRetries);

        if (setType != KeyValueResponseType.Set)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to write meta key '{key}': {setType}"
            );

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    private static async Task DeleteMetaKey(IKahuna kahuna, KvTransaction tx, string key)
    {
        KeyValueResponseType lockType;
        KeyValueDurability lockDurability;
        int lockRetries = 0;

        do
        {
            if (lockRetries > 0)
                await Task.Delay(lockRetries * 10).ConfigureAwait(false);

            (lockType, _, lockDurability, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                tx.TransactionId, key, 0, KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        while (lockType is KeyValueResponseType.AlreadyLocked or KeyValueResponseType.MustRetry
               && ++lockRetries < MetaKeyMaxRetries);

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to acquire meta lock on '{key}': {lockType}"
            );

        tx.TrackLock(key, lockDurability);

        KeyValueResponseType deleteType;
        int deleteRetries = 0;

        do
        {
            if (deleteRetries > 0)
                await Task.Delay(deleteRetries * 10).ConfigureAwait(false);

            (deleteType, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
                tx.TransactionId, key, KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        while (deleteType is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && ++deleteRetries < MetaKeyMaxRetries);

        if (deleteType is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to delete meta key '{key}': {deleteType}"
            );

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }
}
