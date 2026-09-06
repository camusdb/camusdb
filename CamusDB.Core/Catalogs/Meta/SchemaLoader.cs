
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Catalogs.Meta;

/// <summary>
/// Rebuilds a database's in-memory schema from its persisted metadata on open. Runs once per open
/// and shares nothing with the DDL write path except the key builders.
///
/// <para><b>Tables load before views, and the order is required.</b> A view records the ids of the
/// relations it depends on, and those must already be in the map when the view is decoded. A view
/// is only ever a consumer of tables and of earlier views, never the reverse, so this single
/// ordering is sufficient — there is no dependency sort.</para>
///
/// <para><b>Loading replaces every <c>TableSchema</c> instance</b>, so any open table descriptor
/// that captured the old references is stale. <see cref="LoadMetaAsync"/> clears the descriptor
/// cache on entry <i>and</i> in its <c>finally</c>, because a partial load leaves the same hazard
/// as a complete one.</para>
///
/// <para><b>The absence of the version key means a fresh database, not a corrupt one.</b> There is
/// no legacy single-blob fallback to read; backwards compatibility with that format is
/// deliberately not supported. Treating a missing key as an error would make every new database
/// fail to open.</para>
/// </summary>
internal static class SchemaLoader
{
    /// <summary>
    /// Loads <c>Schema.Tables</c> and <c>SystemSchema</c> from Kahuna KV into the
    /// in-memory descriptor.
    /// </summary>
    internal static async Task LoadMetaAsync(DatabaseDescriptor database, ILogger<ICamusDB> logger)
    {
        // Reloading metadata replaces TableSchema instances. Any open table
        // descriptors that captured the old references must be rebuilt.
        database.TableDescriptors.Clear();

        try
        {
            SchemaSnapshot snapshot = await LoadSnapshotAsync(database).ConfigureAwait(false);

            if (snapshot.HasPersistedSchema)
            {
                database.Schema.SchemaVersion = snapshot.SchemaVersion;
                database.Schema.Tables = snapshot.Tables;
                database.Schema.Views = snapshot.Views;
            }

            // Both maps were replaced wholesale above, so the id index has to be built from them
            // here: after this point readers resolve relation references through it and must never
            // walk the maps themselves.
            database.Schema.RebuildRelationNameIndex();

            // Seed the Raft schema fence from the on-disk version so HeadSchemaVersion ≥
            // SchemaVersion holds immediately after a load or reopen.  Without this, the fence
            // starts at 0 each session and HeadSchemaVersion != SchemaVersion would incorrectly
            // appear true for any database that has had prior DDL, breaking the branch stability
            // gate and any other check that compares the two fields after a process restart.
            database.ObserveSchemaEntryHead(database.Schema.SchemaVersion);

            if (snapshot.System is not null)
                database.SystemSchema = snapshot.System;

            // Populate TableSchema.Indexes in-memory for any table that still carries
            // its indexes only in the legacy SystemSchema blob. The migration is in-memory
            // here; the next index DDL write will persist the updated TableSchema to KV via
            // PersistSchemaTableAsync (which includes Indexes via WithoutHistory).
            MigrateIndexesFromSystemSchema(database);

            Log.LogSchemaLoaded(logger, database.Schema.Tables.Count, database.SystemSchema.Indexes.Count);
        }
        finally
        {
            database.TableDescriptors.Clear();
        }
    }

    /// <summary>
    /// Reads only the persisted schema version key. This is the cheap probe the freshness
    /// reconciler runs before it pays for a full snapshot load: one KV read that answers
    /// "did the durable checkpoint move past what this node has in memory". Returns null when
    /// the key is absent, which means a fresh database that never persisted schema.
    /// </summary>
    internal static async Task<long?> TryReadPersistedVersionAsync(DatabaseDescriptor database)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);

        try
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) =
                await database.Kahuna.Kahuna.LocateAndTryGetValue(
                    tx.TransactionId, MetaKeys.VersionKey(database.Id), -1,
                    HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);

            if (type != KeyValueResponseType.Get || entry?.Value is null)
                return null;

            return MetaJsonSerializer.DeserializeCompat(entry.Value, MetaJsonContext.Default.Int64);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Reads a complete, mutually consistent copy of the database's persisted schema — version,
    /// tables, views and system blob — in one transaction, without touching the descriptor.
    /// <see cref="LoadMetaAsync"/> installs the result on a fresh descriptor;
    /// <see cref="SchemaFreshnessReconciler"/> installs it on a live one, under the schema lock,
    /// only when the persisted version is ahead of memory.
    /// </summary>
    internal static async Task<SchemaSnapshot> LoadSnapshotAsync(DatabaseDescriptor database)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);

        try
        {
            IKahuna kahuna = database.Kahuna.Kahuna;
            SchemaSnapshot snapshot = new();

            (KeyValueResponseType schemaType, ReadOnlyKeyValueEntry? schemaEntry) =
                await kahuna.LocateAndTryGetValue(
                    tx.TransactionId, MetaKeys.VersionKey(database.Id), -1,
                    HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);

            // A database with persisted schema has a version key. Absent ⟹ a fresh database with an
            // empty schema (version 0, no tables) — there is no legacy single-blob fallback to read
            // (backwards compatibility is intentionally not supported).
            if (schemaType == KeyValueResponseType.Get && schemaEntry?.Value is not null)
            {
                snapshot.HasPersistedSchema = true;
                snapshot.SchemaVersion = MetaJsonSerializer.DeserializeCompat(schemaEntry.Value, MetaJsonContext.Default.Int64);
                snapshot.Tables = await LoadTablesAsync(database, tx).ConfigureAwait(false);

                // Views load after tables so a view's recorded dependency ids can be checked against
                // relations that are already in the map; a view is only ever a consumer of tables and
                // of earlier views, never the reverse, so this one ordering is sufficient.
                snapshot.Views = await LoadViewsAsync(database, tx).ConfigureAwait(false);
            }

            (KeyValueResponseType systemType, ReadOnlyKeyValueEntry? systemEntry) =
                await kahuna.LocateAndTryGetValue(
                    tx.TransactionId, MetaKeys.SystemKey(database.Id), -1,
                    HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);

            if (systemType == KeyValueResponseType.Get && systemEntry?.Value is not null)
                snapshot.System = MetaJsonSerializer.DeserializeCompat(systemEntry.Value, MetaJsonContext.Default.SystemSchema);

            return snapshot;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    private static async Task<Dictionary<string, TableSchema>> LoadTablesAsync(DatabaseDescriptor database, KvTransaction tx)
    {
        Dictionary<string, TableSchema> tables = new(StringComparer.OrdinalIgnoreCase);
        IKahuna kahuna = database.Kahuna.Kahuna;
        string tableKeyPrefix = MetaKeys.TableKeyPrefix(database.Id);

        string versionKey = MetaKeys.VersionKey(database.Id);
        bool sawVersionKey = false;

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            tx.TransactionId,
            MetaKeys.MetaBucketPrefix(database.Id),
            null, true,
            null, true,
            512,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            CancellationToken.None).ConfigureAwait(false))
        {
            if (string.Equals(key, versionKey, StringComparison.Ordinal))
                sawVersionKey = true;

            if (!key.StartsWith(tableKeyPrefix, StringComparison.Ordinal) || entry.Value is null)
                continue;

            TableSchema table = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.TableSchema);
            ValidateLoadedTable(table, key);
            table.SchemaHistory = null;
            SchemaHistoryStore.ConfigureSchemaHistoryLoader(database, table);
            ConstraintDeltaApplier.ParseCheckConstraintAsts(table);
            tables[table.Name!] = table;
        }

        RequireCompleteMetaScan(database, sawVersionKey, "tables");

        return tables;
    }

    /// <summary>
    /// Fails a metadata scan that cannot have reached the end of the bucket.
    ///
    /// <para>The caller has already read <c>{dbId}/meta/version</c> with a point read that reported
    /// success, so that key exists. It lives in the same bucket this scan walks, and it sorts after
    /// every <c>table:</c> key, so a scan that reached the tables must also have yielded it. Not
    /// seeing it means the scan ended early — and an early end is indistinguishable from an empty
    /// range at the enumerable's surface, which is how a node once loaded a schema at a real version
    /// holding no tables at all and then re-persisted that emptiness over the shared checkpoint.</para>
    ///
    /// <para>The check is one ordinal comparison per scanned key and needs nothing persisted. It is
    /// complete for tables. Views sort after the version key, so a scan that dies partway through
    /// them still passes this check; that gap needs the scan itself to report failure.</para>
    /// </summary>
    private static void RequireCompleteMetaScan(DatabaseDescriptor database, bool sawVersionKey, string what)
    {
        if (sawVersionKey)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Metadata scan for database '{database.Name}' ended before reaching the schema version key, " +
            $"so the {what} it returned are incomplete. The schema was not loaded; retry the open."
        );
    }

    /// <summary>
    /// Loads every persisted <see cref="ViewSchema"/> for the database, keyed by name.
    /// </summary>
    /// <remarks>
    /// Shares the single <c>{dbId}/meta</c> bucket scan pattern with <see cref="LoadTablesAsync"/>
    /// and filters on the view key prefix, which is why view keys must use ':' rather than '/' as
    /// their sub-field separator — a '/' would scatter them into per-view buckets this scan cannot
    /// reach.
    /// </remarks>
    private static async Task<Dictionary<string, ViewSchema>> LoadViewsAsync(DatabaseDescriptor database, KvTransaction tx)
    {
        Dictionary<string, ViewSchema> views = new(StringComparer.OrdinalIgnoreCase);
        IKahuna kahuna = database.Kahuna.Kahuna;
        string viewKeyPrefix = MetaKeys.ViewKeyPrefix(database.Id);

        string versionKey = MetaKeys.VersionKey(database.Id);
        bool sawVersionKey = false;

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            tx.TransactionId,
            MetaKeys.MetaBucketPrefix(database.Id),
            null, true,
            null, true,
            512,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            CancellationToken.None).ConfigureAwait(false))
        {
            if (string.Equals(key, versionKey, StringComparison.Ordinal))
                sawVersionKey = true;

            if (!key.StartsWith(viewKeyPrefix, StringComparison.Ordinal) || entry.Value is null)
                continue;

            ViewSchema view = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.ViewSchema);

            if (string.IsNullOrWhiteSpace(view.Name) || string.IsNullOrWhiteSpace(view.Id))
                throw new CamusDBException(
                    CamusDBErrorCodes.SystemSpaceCorrupt,
                    $"View meta key '{key}' decoded without a name or id");

            views[view.Name!] = view;
        }

        RequireCompleteMetaScan(database, sawVersionKey, "views");

        return views;
    }

    /// <summary>
    /// For every table whose <see cref="TableSchema.Indexes"/> is still null
    /// (i.e. stored only in the legacy <c>SystemSchema</c> blob), populate it in-memory from
    /// <c>database.SystemSchema.Indexes</c>. The result is used immediately by
    /// <c>TableOpener</c> so the table opens correctly; the next index DDL write will persist
    /// the populated <c>Indexes</c> list to the table's KV entry via
    /// <c>PersistSchemaTableAsync</c>.
    /// </summary>
    internal static void MigrateIndexesFromSystemSchema(DatabaseDescriptor database)
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
            // JSON deserialization builds an ordinal-comparer dictionary; rebuild it with a
            // case-insensitive comparer so loaded table names match SQL identifiers case-insensitively.
            checkpoint.Tables = ToCaseInsensitiveTables(checkpoint.Tables);
            return checkpoint;
        }

        Dictionary<string, TableSchema> tables = ToCaseInsensitiveTables(
            JsonSerializer.Deserialize(json, MetaJsonContext.Default.DictionaryStringTableSchema));

        return new()
        {
            FormatVersion = 1,
            SchemaVersion = MaxTableVersion(tables),
            Tables = tables
        };
    }

    /// <summary>
    /// Rebuilds a table dictionary with a case-insensitive comparer. JSON deserialization always
    /// produces an ordinal-comparer dictionary, so any table set loaded from a KV checkpoint must
    /// be re-keyed here to match the case-insensitive lookup semantics of <see cref="Schema.Tables"/>.
    /// </summary>
    private static Dictionary<string, TableSchema> ToCaseInsensitiveTables(Dictionary<string, TableSchema>? tables)
    {
        if (tables is null)
            return new(StringComparer.OrdinalIgnoreCase);

        return new(tables, StringComparer.OrdinalIgnoreCase);
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
}
