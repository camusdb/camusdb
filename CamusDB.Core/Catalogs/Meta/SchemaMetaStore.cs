
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.Catalogs.Meta;

/// <summary>
/// Persists one catalog object at a time — the system schema, a table, a view, or the tombstone of
/// a dropped table — into the caller's DDL transaction, so the object and the schema-version
/// counter commit together or not at all.
///
/// <para><b>Every write here bumps the persisted schema version alongside the object.</b> Readers
/// key their caches on that counter to decide whether what they hold is stale, so an object written
/// without its version bump is worse than an object not written at all: it is a change nobody
/// notices. Treat the pair as one write.</para>
///
/// <para><b>No method here may run while the schema lock is held.</b> These writes are replicated,
/// and a replicated write issued under the schema lock deadlocks the schema-log partition behind
/// the lock holder. Each entry point asserts <c>Schema.LockDepth == 0</c> for that reason. A caller
/// that needs the lock must release it first and then persist.</para>
///
/// <para>Every method takes the <see cref="DatabaseDescriptor"/> and reads the store from it. The
/// class holds no state: one engine serves many databases, so a captured store would bind the
/// first database and corrupt the rest.</para>
/// </summary>
internal static class SchemaMetaStore
{
    /// <summary>
    /// Persists the system schema metadata. Schema table metadata is stored per object
    /// through <see cref="PersistSchemaTableAsync"/>.
    /// </summary>
    internal static async Task PersistMetaAsync(DatabaseDescriptor database, KvTransaction tx)
        => await PersistSystemMetaAsync(database, tx).ConfigureAwait(false);

    internal static async Task PersistSystemMetaAsync(DatabaseDescriptor database, KvTransaction tx)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;

        byte[] systemBytes = MetaJsonSerializer.Serialize(database.SystemSchema, MetaJsonContext.Default.SystemSchema);

        await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.SystemKey(database.Id), systemBytes).ConfigureAwait(false);
    }

    /// <summary>
    /// Persists one view's meta blob plus the database schema-version counter, in the caller's
    /// transaction so the two commit together.
    ///
    /// <para>The version write is not incidental: a view definition that landed without its version
    /// bump would be invisible to the expansion cache, which keys on the schema version to decide
    /// whether its parsed body is stale. Both keys must move as one.</para>
    /// </summary>
    internal static async Task PersistSchemaViewAsync(DatabaseDescriptor database, ViewSchema viewSchema, KvTransaction tx)
    {
        // Same invariant as PersistSchemaTableAsync: a replicated KV write must never be issued while
        // the schema lock is held, or the schema-log partition can deadlock behind it.
        System.Diagnostics.Debug.Assert(
            database.Schema.LockDepth == 0,
            $"PersistSchemaViewAsync called while Schema lock is held on database '{database.Name}' — no replicated write may run under a schema lock"
        );

        if (string.IsNullOrWhiteSpace(viewSchema.Id))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"View '{viewSchema.Name}' has no view id");

        IKahuna kahuna = database.Kahuna.Kahuna;

        byte[] versionBytes = MetaJsonSerializer.Serialize(database.Schema.SchemaVersion, MetaJsonContext.Default.Int64);
        byte[] viewBytes = MetaJsonSerializer.Serialize(viewSchema, MetaJsonContext.Default.ViewSchema);

        await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.VersionKey(database.Id), versionBytes).ConfigureAwait(false);
        await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.ViewKey(database.Id, viewSchema.Id), viewBytes).ConfigureAwait(false);
    }

    /// <summary>
    /// Removes a dropped view's meta blob and advances the persisted schema version, in the caller's
    /// transaction. A view owns no rows and no index keyspace, so unlike a table drop there is
    /// nothing to detach or retain for recovery — deleting the definition removes the whole object.
    /// </summary>
    internal static async Task DeleteSchemaViewAsync(DatabaseDescriptor database, string viewId, KvTransaction tx)
    {
        System.Diagnostics.Debug.Assert(
            database.Schema.LockDepth == 0,
            $"DeleteSchemaViewAsync called while Schema lock is held on database '{database.Name}' — no replicated write may run under a schema lock"
        );

        IKahuna kahuna = database.Kahuna.Kahuna;

        byte[] versionBytes = MetaJsonSerializer.Serialize(database.Schema.SchemaVersion, MetaJsonContext.Default.Int64);

        await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.VersionKey(database.Id), versionBytes).ConfigureAwait(false);
        await MetaKeyWriter.DeleteMetaKey(kahuna, tx, MetaKeys.ViewKey(database.Id, viewId)).ConfigureAwait(false);
    }

    internal static async Task PersistSchemaTableAsync(DatabaseDescriptor database, TableSchema tableSchema, KvTransaction tx)
        => await PersistSchemaTableAsync(database, tableSchema, database.Schema.SchemaVersion, tx).ConfigureAwait(false);

    internal static async Task PersistSchemaTableAsync(DatabaseDescriptor database, TableSchema tableSchema, long schemaVersion, KvTransaction tx)
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

        await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.VersionKey(database.Id), versionBytes).ConfigureAwait(false);
        await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.TableKey(database.Id, tableSchema.Id), tableBytes).ConfigureAwait(false);

        if (tableSchema.SchemaHistory is not null)
        {
            TableSchemaHistory? history = tableSchema.SchemaHistory.FirstOrDefault(x => x.Version == tableSchema.Version);
            if (history is not null)
            {
                // Schema history keys are append-only: once a table version is recorded,
                // readers may safely cache it and load it under their own read timestamp.
                byte[] historyBytes = MetaJsonSerializer.Serialize(history, MetaJsonContext.Default.TableSchemaHistory);
                await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.HistoryKey(database.Id, tableSchema.Id, history.Version), historyBytes).ConfigureAwait(false);
            }
        }

        // Update the grow-only keyspace catalog to track all index ids ever used by this table.
        await WriteKeyspaceCatalogAsync(kahuna, tx, database.Id, tableSchema).ConfigureAwait(false);
    }

    internal static async Task PersistDroppedTableAsync(DatabaseDescriptor database, string tableId, KvTransaction tx)
        => await PersistDroppedTableAsync(database, tableId, database.Schema.SchemaVersion, tx).ConfigureAwait(false);

    internal static Task PersistDroppedTableAsync(DatabaseDescriptor database, string tableId, long schemaVersion, KvTransaction tx)
        => PersistDroppedTableAsync(database, tableId, schemaVersion, tx, deferred: false, formerName: "", droppedAt: default);

    /// <summary>
    /// Persists the checkpoint side of a <c>DROP TABLE</c>: bumps the schema version and deletes the
    /// per-table meta key. When <paramref name="deferred"/> is <c>true</c> it <b>also</b> writes the
    /// table's orphan record in the same transaction — reading the table's current persisted schema
    /// (still present at this point) to capture it — so the detach and the recovery record are one
    /// atomic commit. Idempotent on replay: if the meta key is already gone the orphan was already
    /// written and this is a no-op for that key.
    /// </summary>
    internal static async Task PersistDroppedTableAsync(
        DatabaseDescriptor database, string tableId, long schemaVersion, KvTransaction tx,
        bool deferred, string formerName, HLCTimestamp droppedAt)
    {
        // See PersistSchemaTableAsync above.
        System.Diagnostics.Debug.Assert(
            database.Schema.LockDepth == 0,
            $"PersistDroppedTableAsync called while Schema lock is held on database '{database.Name}' — no replicated write may run under a schema lock"
        );
        IKahuna kahuna = database.Kahuna.Kahuna;

        byte[] versionBytes = MetaJsonSerializer.Serialize(schemaVersion, MetaJsonContext.Default.Int64);
        await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.VersionKey(database.Id), versionBytes).ConfigureAwait(false);

        if (deferred)
        {
            // Capture the table's persisted schema before deleting it, and write the orphan record in the
            // same transaction so a crash can never leave the table detached without a recovery record.
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? tableEntry) = await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, MetaKeys.TableKey(database.Id, tableId), -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false);

            if (getType == KeyValueResponseType.Get && tableEntry?.Value is { Length: > 0 })
            {
                TableSchema captured = MetaJsonSerializer.Deserialize(tableEntry.Value, MetaJsonContext.Default.TableSchema);
                byte[] orphanBytes = MetaJsonSerializer.Serialize(new OrphanTableRecord
                {
                    TableId = tableId,
                    FormerName = formerName,
                    DroppedAt = droppedAt,
                    Schema = captured,
                }, MetaJsonContext.Default.OrphanTableRecord);

                await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.OrphanKey(database.Id, tableId), orphanBytes).ConfigureAwait(false);
            }
            // else: meta key already gone (idempotent replay) — the orphan record was written on the
            // original checkpoint; nothing more to do.
        }
        else
        {
            // Immediate (FORCE) drop destroys the keyspace for good. Remove any stale orphan record for
            // this id so the destroyed table can never be relinked to an empty/partial keyspace.
            await MetaKeyWriter.DeleteMetaKey(kahuna, tx, MetaKeys.OrphanKey(database.Id, tableId)).ConfigureAwait(false);
        }

        await MetaKeyWriter.DeleteMetaKey(kahuna, tx, MetaKeys.TableKey(database.Id, tableId)).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes or updates the persisted keyspace catalog entry for <paramref name="tableSchema"/>.
    /// The catalog records every index id ever allocated for this table — it grows monotonically
    /// and never shrinks on DropIndex or DropTable, so DROP DATABASE can purge orphaned
    /// overlay entries for indexes dropped before the database was dropped.
    /// </summary>
    internal static async Task WriteKeyspaceCatalogAsync(
        IKahuna kahuna, KvTransaction tx, string dbId, TableSchema tableSchema)
    {
        if (string.IsNullOrEmpty(tableSchema.Id))
            return;

        string storageId = tableSchema.EffectiveStorageId;
        string catalogKey = MetaKeys.KeyspaceCatalogKey(dbId, storageId);

        // Load existing catalog to accumulate ids from previously dropped indexes.
        // This read uses HLCTimestamp.Zero (non-transactional) because the caller holds
        // Schema.Semaphore, which serializes all writes to this key on a single node. The
        // read is not read-your-writes with tx; it sees the last committed value, which is
        // correct here since the semaphore ensures no concurrent write races this read.
        HashSet<string> allIndexIds = [];
        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? existingEntry) =
            await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, catalogKey, -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);

        if (readType == KeyValueResponseType.Get && existingEntry?.Value is { Length: > 0 } existingBytes)
        {
            string[]? existingIds = MetaJsonSerializer.Deserialize(existingBytes, MetaJsonContext.Default.StringArray);
            foreach (string id in existingIds)
                allIndexIds.Add(id);
        }
        else if (!string.Equals(storageId, tableSchema.Id, StringComparison.Ordinal))
        {
            // Compatibility: relations swapped before the catalog was keyed by storage generation
            // have their only entry under the relation id. Read it once and let this write copy it
            // forward to the storage-keyed entry.
            (KeyValueResponseType legacyType, ReadOnlyKeyValueEntry? legacyEntry) =
                await kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, MetaKeys.KeyspaceCatalogKey(dbId, tableSchema.Id), -1, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);

            if (legacyType == KeyValueResponseType.Get && legacyEntry?.Value is { Length: > 0 } legacyBytes)
            {
                foreach (string id in MetaJsonSerializer.Deserialize(legacyBytes, MetaJsonContext.Default.StringArray))
                    allIndexIds.Add(id);
            }
        }

        // Union with live indexes in the current schema.
        if (tableSchema.Indexes is not null)
            foreach (TableIndexSchema idx in tableSchema.Indexes)
                if (!string.IsNullOrEmpty(idx.Id))
                    allIndexIds.Add(idx.Id);

        // Always write the catalog entry even when the index list is empty. The catalog is
        // keyed by tableId, so its presence alone tells DROP DATABASE to purge the row bucket
        // {dbId}:{tableId}:r for tables that were dropped before the database was dropped,
        // regardless of whether the table had any indexes.
        byte[] catalogBytes = MetaJsonSerializer.Serialize(allIndexIds.ToArray(), MetaJsonContext.Default.StringArray);
        await MetaKeyWriter.WriteMetaKey(kahuna, tx, catalogKey, catalogBytes).ConfigureAwait(false);
    }

    /// <summary>
    /// The form of a table's schema that is written to its meta key: everything except the retained
    /// column history, which lives in its own per-version keys.
    /// </summary>
    /// <remarks>
    /// This projection is the durable record of the relation, so every field that must survive a
    /// reopen has to be listed here — a field omitted is silently lost on the next restart while
    /// looking perfectly correct in memory for the whole life of the process. That is why the
    /// materialized-view fields are here: without them a materialized view would come back from disk
    /// as an ordinary table, readable and writable and with no way left to refresh it.
    /// </remarks>
    internal static TableSchema WithoutHistory(TableSchema tableSchema)
    {
        return new()
        {
            Id = tableSchema.Id,
            Version = tableSchema.Version,
            Name = tableSchema.Name,
            Columns = tableSchema.Columns,
            Indexes = tableSchema.Indexes,
            CheckConstraints = tableSchema.CheckConstraints,
            Settings = tableSchema.Settings,
            Comment = tableSchema.Comment,
            StorageId = tableSchema.StorageId,
            ContentsGeneration = tableSchema.ContentsGeneration,
            MetadataGeneration = tableSchema.MetadataGeneration,
            Kind = tableSchema.Kind,
            ViewDefinition = tableSchema.ViewDefinition,
            IsPopulated = tableSchema.IsPopulated,
            RefreshedAt = tableSchema.RefreshedAt,
            SchemaHistory = null
        };
    }
}
