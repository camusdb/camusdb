
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.Catalogs.Apply;

/// <summary>
/// Applies the committed deltas that act on a relation as a whole: create, relink, drop, rename,
/// settings, comments, and the contents swap behind TRUNCATE.
///
/// <para><b>A rename is metadata-only.</b> Rows and index entries are keyed by the relation's
/// immutable id, never by its name, so renaming rewrites the schema map and the stored bodies of
/// dependent views and touches no data at all.</para>
///
/// <para><b>A truncate swaps the storage id and leaves <c>TableSchema.Version</c> alone.</b> It
/// verifies the generation it was proposed against is still the live one and refuses otherwise,
/// rather than applying to a generation it does not describe. The replaced key-space is made
/// recoverable afterwards by <see cref="ContentsRetirementStore"/>, from an intent captured during
/// apply — this class writes nothing.</para>
/// </summary>
internal static class TableDeltaApplier
{
    internal static TableSchema ApplyCreateTable(Schema schema, SchemaCreateTablePayload payload)
    {
        if (schema.Tables.ContainsKey(payload.TableName))
            throw new CamusDBException(CamusDBErrorCodes.TableAlreadyExists, $"Table '{payload.TableName}' already exists");

        // One table id maps to at most one live name. A relink reuses the orphan's id; reject if that id
        // is already live under another name so a stale-orphan relink cannot mint a second alias for one
        // physical keyspace. (A fresh CREATE always allocates an unregistered id, so this never fires.)
        if (!string.IsNullOrWhiteSpace(payload.TableId))
        {
            foreach (TableSchema live in schema.Tables.Values)
                if (string.Equals(live.Id, payload.TableId, StringComparison.Ordinal))
                    throw new CamusDBException(
                        CamusDBErrorCodes.TableAlreadyExists,
                        $"Table id '{payload.TableId}' is already live under name '{live.Name}'");
        }

        TableSchema tableSchema = new()
        {
            Id = string.IsNullOrWhiteSpace(payload.TableId) ? ObjectIdGenerator.Generate().ToString() : payload.TableId,
            Version = 0,
            Name = payload.TableName,
            Columns = new(payload.Columns.Length),
            Comment = payload.Comment,
            Settings = payload.Settings is null
                ? null
                : new Dictionary<string, string>(payload.Settings, StringComparer.Ordinal),
            // A materialized view is created through this same op — it is a relation — and carries
            // its defining query with it so every node can render SHOW CREATE and refuse DML on it
            // without a second round of replication.
            Kind = payload.Kind,
            ViewDefinition = payload.ViewDefinition,
            IsPopulated = payload.IsPopulated,
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
                    arrayElementType: column.ArrayElementType,
                    defaultFunction: column.DefaultFunction,
                    notNullConstraintName: column.NotNullConstraintName,
                    comment: column.Comment
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

        // CHECK constraints folded in by BuildInlineCheckConstraints. Rebuild AST cache so
        // enforcement is immediately available on the node that created the table.
        if (payload.CheckConstraints is { Length: > 0 })
        {
            tableSchema.CheckConstraints = [.. payload.CheckConstraints];
            ConstraintDeltaApplier.ParseCheckConstraintAsts(tableSchema);
        }

        schema.Tables.Add(payload.TableName, tableSchema);

        return tableSchema;
    }

    /// <summary>
    /// Reattaches an orphaned table to the live schema, preserving its original id, column ids, index
    /// definitions, check constraints, and — critically — its real schema <see cref="TableSchema.Version"/>.
    /// Unlike <see cref="ApplyCreateTable"/> it does <b>not</b> synthesize a version-0 history (which
    /// would both mislabel post-<c>ALTER</c> rows and overwrite the retained version-0 history key);
    /// instead it leaves <see cref="TableSchema.SchemaHistory"/> null and configures the lazy loader that
    /// reads the retained on-disk history keys, exactly like a table loaded from disk. Runs on every node
    /// (proposer + followers) through the replicated apply, so all reconstruct an identical table.
    /// </summary>
    internal static TableSchema ApplyRelinkTable(Schema schema, DatabaseDescriptor database, SchemaRelinkTablePayload payload)
    {
        if (schema.Tables.ContainsKey(payload.TableName))
            throw new CamusDBException(CamusDBErrorCodes.TableAlreadyExists, $"Table '{payload.TableName}' already exists");

        // One table id maps to at most one live name — reject a stale-orphan relink that would alias.
        if (!string.IsNullOrWhiteSpace(payload.TableId))
        {
            foreach (TableSchema live in schema.Tables.Values)
                if (string.Equals(live.Id, payload.TableId, StringComparison.Ordinal))
                    throw new CamusDBException(
                        CamusDBErrorCodes.TableAlreadyExists,
                        $"Table id '{payload.TableId}' is already live under name '{live.Name}'");
        }

        TableSchema tableSchema = new()
        {
            Id = string.IsNullOrWhiteSpace(payload.TableId) ? ObjectIdGenerator.Generate().ToString() : payload.TableId,
            Version = payload.Version,   // preserve the real version so rows decode against the right layout
            Name = payload.TableName,
            Columns = new(payload.Columns.Length),
            Comment = payload.Comment,
            StorageId = payload.StorageId,
            Kind = payload.Kind,
            ViewDefinition = payload.ViewDefinition,
            IsPopulated = payload.IsPopulated,
            RefreshedAt = payload.RefreshedAt,
            SchemaHistory = null,        // lazy-loaded from the retained {dbId}/meta/history:{tableId}:* keys
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
                    arrayElementType: column.ArrayElementType,
                    defaultFunction: column.DefaultFunction,
                    notNullConstraintName: column.NotNullConstraintName,
                    comment: column.Comment
                )
            );
        }

        if (payload.Indexes is { Length: > 0 })
            tableSchema.Indexes = [.. payload.Indexes];

        if (payload.CheckConstraints is { Length: > 0 })
        {
            tableSchema.CheckConstraints = [.. payload.CheckConstraints];
            ConstraintDeltaApplier.ParseCheckConstraintAsts(tableSchema);
        }

        if (payload.Settings is { Count: > 0 })
            tableSchema.Settings = new Dictionary<string, string>(payload.Settings, StringComparer.Ordinal);

        // Configure the lazy history loader (as for a disk-loaded table) so rows written under versions
        // older than the current one decode against their retained on-disk history layout.
        SchemaHistoryStore.ConfigureSchemaHistoryLoader(database, tableSchema);

        schema.Tables.Add(payload.TableName, tableSchema);

        return tableSchema;
    }

    internal static TableSchema? ApplyDropTable(Schema schema, SchemaDropTablePayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            return null;

        schema.Tables.Remove(payload.TableName);
        return tableSchema;
    }

    /// <summary>
    /// Re-keys <c>schema.Tables</c> and mutates <c>TableSchema.Name</c> in place. The same
    /// <c>TableSchema</c> instance is preserved so pin/visibility closures keep tracking it.
    /// No version bump — the re-key already invalidates pinned txns (old key gone).
    /// </summary>
    internal static TableSchema ApplyRenameTable(Schema schema, SchemaRenamePayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        if (schema.Tables.ContainsKey(payload.NewName))
            throw new CamusDBException(CamusDBErrorCodes.TableAlreadyExists, $"Table '{payload.NewName}' already exists");

        // Coordinator jobs are keyed by table name. If any child column or index is
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

        // Same apply as the rename: no node ever sees a dependent body naming the old relation, and
        // there is no interval in which the freed name could be claimed by something else and read
        // through a stale body.
        SchemaDeltaApplier.ApplyDependentViewRewrites(schema, payload);

        return tableSchema;
    }

    /// <summary>
    /// Applies a <see cref="SchemaOp.SetTableSettings"/> delta by merging the payload's settings into
    /// <see cref="TableSchema.Settings"/> and removing the payload's reset keys. Idempotent; does not
    /// bump <see cref="TableSchema.Version"/>.
    /// </summary>
    internal static TableSchema ApplySetTableSettings(Schema schema, SchemaSetTableSettingsPayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        Dictionary<string, string> merged = tableSchema.Settings is null
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            : new Dictionary<string, string>(tableSchema.Settings, StringComparer.Ordinal);

        // Defensive: only apply recognized, lowercase-canonical keys, so a malformed log entry from an
        // older/other proposer cannot inject divergent settings semantics on apply. The *value* is
        // stored as proposed — the proposer already canonicalized it, and case is significant for a
        // value that names a column, so lowercasing here would silently break a camelCase column name.
        foreach (KeyValuePair<string, string> kv in payload.Settings)
        {
            string key = (kv.Key ?? "").Trim().ToLowerInvariant();
            if (TableSettings.Recognized.ContainsKey(key))
                merged[key] = (kv.Value ?? "").Trim();
        }

        foreach (string rawKey in payload.RemovedKeys)
            merged.Remove((rawKey ?? "").Trim().ToLowerInvariant());

        tableSchema.Settings = merged;
        return tableSchema;
    }

    /// <summary>
    /// Applies a <see cref="SchemaOp.SetComment"/> delta: attaches or removes a free-text comment on
    /// the table, one of its columns, or one of its indexes. A null <c>payload.Comment</c> means
    /// remove, and is deliberately not conflated with the empty string.
    ///
    /// <para>Replay-safe by construction: every branch is a pure overwrite, and a column or index
    /// that no longer exists is a silent no-op rather than a throw. That matters because a
    /// re-delivered entry (Raft redelivery, WAL replay) can arrive after a later DROP COLUMN /
    /// DROP INDEX has already been applied — throwing there would wedge the apply pipeline on an
    /// operation that carries no data.</para>
    ///
    /// <para>Does not bump <see cref="TableSchema.Version"/>: comments do not affect row encoding.
    /// The database schema version is advanced centrally by <see cref="ApplySchemaDelta"/>.</para>
    /// </summary>
    internal static TableSchema? ApplySetComment(Schema schema, SchemaSetCommentPayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        switch (payload.Target)
        {
            case CommentTarget.Table:
                tableSchema.Comment = payload.Comment;
                break;

            case CommentTarget.Column:
                {
                    if (tableSchema.Columns is null)
                        break;

                    int idx = tableSchema.Columns.FindIndex(
                        c => string.Equals(c.Name, payload.ElementName, StringComparison.OrdinalIgnoreCase));

                    if (idx < 0)
                        break;

                    TableColumnSchema old = tableSchema.Columns[idx];
                    tableSchema.Columns[idx] = new TableColumnSchema(
                        id: old.Id,
                        name: old.Name,
                        type: old.Type,
                        notNull: old.NotNull,
                        defaultValue: old.DefaultValue,
                        state: old.State,
                        maxLength: old.MaxLength,
                        arrayElementType: old.ArrayElementType,
                        defaultFunction: old.DefaultFunction,
                        notNullConstraintName: old.NotNullConstraintName,
                        comment: payload.Comment
                    );
                    break;
                }

            case CommentTarget.Index:
                {
                    if (tableSchema.Indexes is null)
                        break;

                    int idx = tableSchema.Indexes.FindIndex(
                        ix => string.Equals(ix.Name, payload.ElementName, StringComparison.OrdinalIgnoreCase));

                    if (idx < 0)
                        break;

                    TableIndexSchema old = tableSchema.Indexes[idx];
                    tableSchema.Indexes[idx] = new TableIndexSchema(
                        id: old.Id,
                        name: old.Name,
                        columnIds: old.ColumnIds,
                        type: old.Type,
                        state: old.State,
                        startOffset: old.StartOffset,
                        columnDirections: old.ColumnDirections,
                        includeColumnIds: old.IncludeColumnIds,
                        comment: payload.Comment
                    );
                    break;
                }

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unsupported comment target '{payload.Target}' in the schema log");
        }

        return tableSchema;
    }

    /// <summary>
    /// Applies a TruncateTable delta: moves the relation onto the fresh, empty key-space the payload
    /// names, advances its contents generation, and records the timestamp that generation is valid
    /// from. Nothing else about the relation changes — not its id, not its name, not its columns,
    /// indexes, constraints or schema version.
    /// </summary>
    /// <remarks>
    /// <para><b>Compare-and-swap, in both directions.</b> A relation already sitting on the new
    /// storage id is a re-delivered entry: returning without touching it is what stops a replay from
    /// retiring a second key-space. A relation sitting on neither the expected nor the new id lost a
    /// race with another contents change, and applying anyway would move it off a generation this
    /// entry never described — so that is refused rather than forced.</para>
    ///
    /// <para>A relation that is no longer in the schema was dropped after the entry was proposed. That
    /// is a no-op rather than a failure: a delta that cannot find its target must never wedge the
    /// apply pipeline.</para>
    ///
    /// <para>Pure by construction. This runs inside the schema partition's commit pipeline on every
    /// node, so it reads no KV and writes none — everything it needs is in the payload. The retired
    /// key-space is made recoverable afterwards, from the intent captured by
    /// <see cref="CaptureContentsRetirementIntent"/>.</para>
    /// </remarks>
    internal static TableSchema? ApplyTruncateTable(Schema schema, SchemaTruncateTablePayload payload)
    {
        TableSchema? live = SchemaDeltaApplier.FindRelationById(schema, payload.TableId);

        if (live is null)
            return null;

        if (string.Equals(live.EffectiveStorageId, payload.NewStorageId, StringComparison.Ordinal))
            return null;

        if (!string.Equals(live.EffectiveStorageId, payload.ExpectedStorageId, StringComparison.Ordinal) ||
            live.ContentsGeneration != payload.ExpectedContentsGeneration)
            throw new CamusDBException(
                CamusDBErrorCodes.ConcurrentSchemaChange,
                $"The contents of '{live.Name}' changed while it was being truncated, so this truncate was " +
                "discarded rather than applied to a generation it does not describe. Retry it.");

        live.StorageId = payload.NewStorageId;
        live.ContentsGeneration++;
        live.ContentsValidFrom = payload.ContentsValidFrom;

        return live;
    }
}
