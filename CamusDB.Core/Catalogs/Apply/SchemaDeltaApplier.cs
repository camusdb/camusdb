
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
/// Shared machinery for applying a committed <see cref="SchemaChangeLogEntry"/> to in-memory
/// schema: payload decoding, relation lookup by immutable id, the dry-run validator, and the
/// idempotency predicates that answer "has this delta already landed?".
///
/// <para><b>Nothing under <c>Catalogs.Apply</c> may touch KV.</b> Apply runs inside the schema
/// partition's commit pipeline on every node, and a KV write from there re-enters the same
/// partition and deadlocks it. That rule used to live in a doc comment; it is now structural —
/// no type in this namespace takes an <c>IKahuna</c> or a <c>KvTransaction</c> in any signature,
/// so the compiler refuses the mistake.</para>
///
/// <para><b>The idempotency predicates check structure, not the version counter.</b> A re-delivered
/// entry has to be recognised by what it did to the schema — is the column there, did the storage
/// id move — because several ops leave the version untouched. A truncate is the clearest case: it
/// does not move <c>TableSchema.Version</c>, so a version comparison would call an unrelated later
/// DDL "the same truncate". Only the storage id proves it.</para>
///
/// <para><b>A delta that cannot find its target is a no-op, never an error.</b> The apply pipeline
/// is serial and shared; an exception there wedges every subsequent delta for the database.</para>
/// </summary>
internal static class SchemaDeltaApplier
{
    /// <summary>
    /// Applies <paramref name="entry"/> to <paramref name="schema"/> without a database context. Valid
    /// for every op except <see cref="SchemaOp.RelinkTable"/>, which needs a database to configure the
    /// reattached table's history loader. Used by unit tests that exercise the pure apply logic on a bare
    /// schema.
    /// </summary>
    public static TableSchema? ApplySchemaDelta(Schema schema, SchemaChangeLogEntry entry)
    {
        if (entry.Op == SchemaOp.RelinkTable)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "RelinkTable requires a database context to apply; use the 3-argument overload.");

        return ApplySchemaDelta(schema, database: null!, entry);
    }

    /// <summary>
    /// Applies <paramref name="entry"/> to <paramref name="schema"/> (which is either the live
    /// <c>database.Schema</c> for a real apply, or a throwaway clone for dry-run validation).
    /// <paramref name="database"/> is used only by <see cref="SchemaOp.RelinkTable"/> to configure the
    /// reattached table's lazy history loader; it must not be treated as the mutation target.
    /// </summary>
    public static TableSchema? ApplySchemaDelta(Schema schema, DatabaseDescriptor database, SchemaChangeLogEntry entry)
    {
        TableSchema? tableSchema = entry.Op switch
        {
            SchemaOp.CreateTable => TableDeltaApplier.ApplyCreateTable(schema, SchemaDeltaApplier.DecodePayload<SchemaCreateTablePayload>(entry)),
            SchemaOp.RelinkTable => TableDeltaApplier.ApplyRelinkTable(schema, database, SchemaDeltaApplier.DecodePayload<SchemaRelinkTablePayload>(entry)),
            SchemaOp.DropTable => TableDeltaApplier.ApplyDropTable(schema, SchemaDeltaApplier.DecodePayload<SchemaDropTablePayload>(entry)),
            SchemaOp.AddColumn => ColumnDeltaApplier.ApplyAlterColumn(schema, SchemaDeltaApplier.DecodePayload<SchemaAlterColumnPayload>(entry), entry.Op),
            SchemaOp.DropColumn => ColumnDeltaApplier.ApplyAlterColumn(schema, SchemaDeltaApplier.DecodePayload<SchemaAlterColumnPayload>(entry), entry.Op),
            SchemaOp.SetElementState => ElementStateApplier.ApplyElementState(schema, SchemaDeltaApplier.DecodePayload<SchemaElementStatePayload>(entry)),
            SchemaOp.AddIndex => IndexDeltaApplier.ApplyAddIndex(schema, SchemaDeltaApplier.DecodePayload<SchemaIndexPayload>(entry)),
            SchemaOp.DropIndex => IndexDeltaApplier.ApplyDropIndex(schema, SchemaDeltaApplier.DecodePayload<SchemaIndexPayload>(entry)),
            SchemaOp.RenameTable => TableDeltaApplier.ApplyRenameTable(schema, SchemaDeltaApplier.DecodePayload<SchemaRenamePayload>(entry)),
            SchemaOp.RenameColumn => ColumnDeltaApplier.ApplyRenameColumn(schema, SchemaDeltaApplier.DecodePayload<SchemaRenamePayload>(entry)),
            SchemaOp.RenameIndex => IndexDeltaApplier.ApplyRenameIndex(schema, SchemaDeltaApplier.DecodePayload<SchemaRenamePayload>(entry)),
            SchemaOp.AddCheckConstraint => ConstraintDeltaApplier.ApplyAddCheckConstraint(schema, SchemaDeltaApplier.DecodePayload<SchemaCheckConstraintPayload>(entry)),
            SchemaOp.DropCheckConstraint => ConstraintDeltaApplier.ApplyDropCheckConstraint(schema, SchemaDeltaApplier.DecodePayload<SchemaCheckConstraintPayload>(entry)),
            SchemaOp.SetColumnNotNull => ConstraintDeltaApplier.ApplySetColumnNotNull(schema, SchemaDeltaApplier.DecodePayload<SchemaSetColumnNotNullPayload>(entry)),
            SchemaOp.SetTableSettings => TableDeltaApplier.ApplySetTableSettings(schema, SchemaDeltaApplier.DecodePayload<SchemaSetTableSettingsPayload>(entry)),
            SchemaOp.SetComment => TableDeltaApplier.ApplySetComment(schema, SchemaDeltaApplier.DecodePayload<SchemaSetCommentPayload>(entry)),
            SchemaOp.CreateView => ViewDeltaApplier.ApplyCreateView(schema, SchemaDeltaApplier.DecodePayload<SchemaViewPayload>(entry), replacing: false),
            SchemaOp.ReplaceView => ViewDeltaApplier.ApplyCreateView(schema, SchemaDeltaApplier.DecodePayload<SchemaViewPayload>(entry), replacing: true),
            SchemaOp.DropView => ViewDeltaApplier.ApplyDropView(schema, SchemaDeltaApplier.DecodePayload<SchemaDropViewPayload>(entry)),
            SchemaOp.RenameView => ViewDeltaApplier.ApplyRenameView(schema, SchemaDeltaApplier.DecodePayload<SchemaRenamePayload>(entry)),
            SchemaOp.SetViewDefinition => ViewDeltaApplier.ApplySetViewDefinition(schema, SchemaDeltaApplier.DecodePayload<SchemaSetViewDefinitionPayload>(entry)),
            SchemaOp.SetMaterializedViewState => ViewDeltaApplier.ApplySetMaterializedViewState(schema, SchemaDeltaApplier.DecodePayload<SchemaSetMatViewStatePayload>(entry)),
            SchemaOp.TruncateTable => TableDeltaApplier.ApplyTruncateTable(schema, SchemaDeltaApplier.DecodePayload<SchemaTruncateTablePayload>(entry)),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown schema operation '{entry.Op}'")
        };

        // Same reasoning as the generation bump below, and for the same reason it lives here: every
        // op that can create, drop or rename a relation lands in this method, and a stored view body
        // resolves the relations it reads through this index. Rebuilt before the version advances so
        // a lock-free reader sees either the pre-delta index or the post-delta one, never a torn one.
        schema.RebuildRelationNameIndex();

        schema.SchemaVersion = entry.ToVersion;

        // One place, deliberately. Every op that touches a relation lands here, so a generation bump
        // cannot be forgotten by an apply arm — and a forgotten bump is indistinguishable from "nothing
        // changed" to anything doing a compare-and-swap against it.
        if (tableSchema is not null)
            tableSchema.MetadataGeneration++;

        return tableSchema;
    }

    internal static T DecodePayload<T>(SchemaChangeLogEntry entry) where T : new()
        => entry.GetPayload<T>();

    /// <summary>
    /// Finds a live relation by its immutable id. Used where a name would be the wrong key because the
    /// operation can outlive a concurrent rename.
    /// </summary>
    internal static TableSchema? FindRelationById(Schema schema, string tableId)
    {
        foreach (TableSchema candidate in schema.Tables.Values)
        {
            if (string.Equals(candidate.Id, tableId, StringComparison.Ordinal))
                return candidate;
        }

        return null;
    }

    /// <summary>
    /// Applies a RenameView delta by swapping the map key. The view's id is deliberately unchanged,
    /// so dependents that recorded it keep resolving across the rename.
    /// </summary>
    /// <summary>
    /// Applies the dependent-view conversions carried by a rename, in the same apply as the rename
    /// itself. Idempotent: overwriting a definition with the one already stored is a no-op, so a
    /// re-delivered entry replays harmlessly. A view that is no longer present is skipped rather than
    /// failing — it was dropped after the entry was proposed, and a rename must not wedge on that.
    /// </summary>
    internal static void ApplyDependentViewRewrites(Schema schema, SchemaRenamePayload payload)
    {
        if (payload.DependentViewDefinitions is not { Count: > 0 } rewrites)
            return;

        foreach ((string viewName, ViewDefinition definition) in rewrites)
        {
            if (schema.Views.TryGetValue(viewName, out ViewSchema? view))
                view.Definition = definition;
        }
    }

    internal static void ValidateSchemaDelta(DatabaseDescriptor database, SchemaChangeLogEntry entry)
    {
        // Dry-run: apply to a throwaway clone so validation has no side effects on the live schema.
        Schema clone = SchemaReplicator.CloneSchema(database.Schema);
        ApplySchemaDelta(clone, database, entry);
    }

    internal static bool WasSchemaDeltaApplied(Schema schema, SchemaChangeLogEntry entry)
    {
        return entry.Op switch
        {
            SchemaOp.CreateTable => schema.Tables.ContainsKey(DecodePayload<SchemaCreateTablePayload>(entry).TableName),
            SchemaOp.RelinkTable => schema.Tables.ContainsKey(DecodePayload<SchemaRelinkTablePayload>(entry).TableName),
            SchemaOp.DropTable => !schema.Tables.ContainsKey(DecodePayload<SchemaDropTablePayload>(entry).TableName),
            SchemaOp.AddColumn => HasColumn(schema, DecodePayload<SchemaAlterColumnPayload>(entry)),
            SchemaOp.DropColumn => !HasColumn(schema, DecodePayload<SchemaAlterColumnPayload>(entry)),
            SchemaOp.SetElementState => HasElementState(schema, DecodePayload<SchemaElementStatePayload>(entry)),
            SchemaOp.AddIndex => HasIndex(schema, DecodePayload<SchemaIndexPayload>(entry)),
            SchemaOp.DropIndex => !HasIndex(schema, DecodePayload<SchemaIndexPayload>(entry)),
            SchemaOp.RenameTable or SchemaOp.RenameColumn or SchemaOp.RenameIndex => WasRenamed(schema, DecodePayload<SchemaRenamePayload>(entry)),
            // View ops check the view map, not the table map — WasRenamed above would look in the
            // wrong place for a RenameView and report "not applied" forever.
            SchemaOp.CreateView or SchemaOp.ReplaceView => schema.Views.ContainsKey(DecodePayload<SchemaViewPayload>(entry).ViewName),
            SchemaOp.DropView => !schema.Views.ContainsKey(DecodePayload<SchemaDropViewPayload>(entry).ViewName),
            SchemaOp.RenameView => schema.Views.ContainsKey(DecodePayload<SchemaRenamePayload>(entry).NewName),
            // A truncate does not move TableSchema.Version, so the fallback below would answer
            // "applied" for any unrelated DDL that did. The storage id is the only proof.
            SchemaOp.TruncateTable => WasTruncateApplied(schema, DecodePayload<SchemaTruncateTablePayload>(entry)),
            _ => schema.SchemaVersion >= entry.ToVersion
        };
    }

    internal static bool WasRenamed(Schema schema, SchemaRenamePayload payload)
    {
        return payload.Kind switch
        {
            SchemaRenameKind.Table =>
                schema.Tables.ContainsKey(payload.NewName) && !schema.Tables.ContainsKey(payload.TableName),
            SchemaRenameKind.Column =>
                schema.Tables.TryGetValue(payload.TableName, out TableSchema? ct) &&
                ct.Columns is not null &&
                ct.Columns.Any(c => string.Equals(c.Name, payload.NewName, StringComparison.OrdinalIgnoreCase)),
            SchemaRenameKind.Index =>
                schema.Tables.TryGetValue(payload.TableName, out TableSchema? it) &&
                it.Indexes is not null &&
                it.Indexes.Any(ix => string.Equals(ix.Name, payload.NewName, StringComparison.OrdinalIgnoreCase)),
            _ => false
        };
    }

    internal static bool HasIndex(Schema schema, SchemaIndexPayload payload)
    {
        return schema.Tables.TryGetValue(payload.TableName, out TableSchema? table) &&
               table.Indexes is not null &&
               table.Indexes.Any(ix => string.Equals(ix.Name, payload.IndexName, StringComparison.OrdinalIgnoreCase));
    }

    internal static bool HasColumn(Schema schema, SchemaAlterColumnPayload payload)
    {
        return schema.Tables.TryGetValue(payload.TableName, out TableSchema? table) &&
               table.Columns is not null &&
               table.Columns.Any(column => string.Equals(column.Name, payload.Column.Name, StringComparison.OrdinalIgnoreCase));
    }

    internal static bool HasElementState(Schema schema, SchemaElementStatePayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? table))
            return payload.State == SchemaElementState.Absent;

        if (payload.ElementKind == SchemaElementKind.Index)
        {
            TableIndexSchema? index = table.Indexes?.FirstOrDefault(ix => string.Equals(ix.Name, payload.ElementName, StringComparison.OrdinalIgnoreCase));
            return payload.State == SchemaElementState.Absent
                ? index is null
                : index?.State == payload.State;
        }

        if (table.Columns is null)
            return payload.State == SchemaElementState.Absent;

        TableColumnSchema? column = table.Columns.FirstOrDefault(column => string.Equals(column.Name, payload.ElementName, StringComparison.OrdinalIgnoreCase));
        return payload.State == SchemaElementState.Absent
            ? column is null
            : column?.State == payload.State;
    }

    /// <summary>
    /// Whether <paramref name="payload"/>'s contents swap is present in <paramref name="schema"/>.
    /// </summary>
    /// <remarks>
    /// A truncate deliberately does not bump <see cref="TableSchema.Version"/>, so the generic
    /// "the version moved" test would report success the moment any other DDL advanced it. The target
    /// storage id is the only thing that proves this particular delta landed. A relation that is gone
    /// counts as applied, matching the no-op arm of <see cref="ApplyTruncateTable"/> — otherwise a
    /// proposer would wait for a transition that can never happen.
    /// </remarks>
    internal static bool WasTruncateApplied(Schema schema, SchemaTruncateTablePayload payload)
    {
        TableSchema? live = FindRelationById(schema, payload.TableId);
        return live is null || string.Equals(live.EffectiveStorageId, payload.NewStorageId, StringComparison.Ordinal);
    }
}
