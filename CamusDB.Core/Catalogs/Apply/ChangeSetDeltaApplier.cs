
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Replication;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Apply;

/// <summary>
/// Applies the three schema-log ops of a transactional change set: <see cref="SchemaOp.PendingChangeSet"/>,
/// <see cref="SchemaOp.PublishChangeSet"/> and <see cref="SchemaOp.AbortChangeSet"/>. In-memory only, as
/// every type in this namespace.
///
/// <para><b>The pending apply is a reservation.</b> It refuses the change set when the live schema moved
/// after the transaction copied it, dry-runs every delta on a copy of the live schema, and records the
/// change set. From then on <see cref="SchemaDeltaApplier.ApplySchemaDelta(Schema, DatabaseDescriptor, SchemaChangeLogEntry)"/>
/// refuses every other op until a publish or an abort ends it. So the publish, which runs after the
/// transaction committed its rows in KV, applies onto the exact schema the dry run used and cannot be
/// refused.</para>
///
/// <para><b>The deltas take the version of the entry that applies them.</b> A delta of the change set
/// carries the versions of the transaction's private schema. The pending apply and the publish give
/// every delta the from and to versions of their own entry, so the database version moves by one per
/// entry, as for any other op, and all the deltas of a publish appear together under the schema lock.
/// Table layout versions (<see cref="TableSchema.Version"/>) are counted per table and move the same way
/// on the private schema and on the live one, so the rows the transaction wrote decode with the
/// published layouts.</para>
///
/// <para><b>Publish and abort are idempotent.</b> A decision for a change set that is no longer pending
/// does nothing except consume its version. The committing node and a resolver may both propose one,
/// and a replay delivers each again.</para>
/// </summary>
internal static class ChangeSetDeltaApplier
{
    /// <summary>True for the three ops of a transactional change set.</summary>
    internal static bool IsChangeSetOp(SchemaOp op) =>
        op is SchemaOp.PendingChangeSet or SchemaOp.PublishChangeSet or SchemaOp.AbortChangeSet;

    /// <summary>
    /// The refusal of a delta proposed while a change set is pending. Retryable: the change set ends
    /// within one KV commit and one schema-log round trip, or within the resolver's delay after a crash.
    /// </summary>
    internal static SchemaDeltaRefusedException RefuseWhilePending(PendingSchemaChangeSet pending, SchemaOp op) =>
        new(
            CamusDBErrorCodes.TransactionMustRetry,
            $"Schema change '{op}' was refused: the schema changes of a committing transaction " +
            $"(change set {pending.ChangeSetId}) are not yet published; retry the statement");

    /// <summary>
    /// Decodes the deltas of a change set and gives each the versions of <paramref name="applying"/>.
    /// </summary>
    internal static List<SchemaChangeLogEntry> DecodeDeltas(SchemaChangeSetPayload payload, SchemaChangeLogEntry applying)
    {
        List<SchemaChangeLogEntry> deltas = new(payload.Deltas.Count);

        foreach (byte[] bytes in payload.Deltas)
        {
            SchemaChangeLogEntry delta = SchemaChangeLogEntryCodec.Decode(bytes);
            delta.FromVersion = applying.FromVersion;
            delta.ToVersion = applying.ToVersion;
            deltas.Add(delta);
        }

        return deltas;
    }

    /// <summary>
    /// Applies a <see cref="SchemaOp.PendingChangeSet"/> entry. Refuses it when another change set is
    /// pending, when the live schema is not at the change set's base version, or when a delta does not
    /// apply to the live schema. Otherwise records the change set and changes nothing else.
    /// </summary>
    internal static void ApplyPending(Schema schema, DatabaseDescriptor database, SchemaChangeLogEntry entry)
    {
        SchemaChangeSetPayload payload = entry.GetPayload<SchemaChangeSetPayload>();

        if (schema.PendingChangeSet is { } other)
        {
            // The same change set again: a replay of an entry this schema already holds as a record
            // loaded from the checkpoint. Nothing to do.
            if (string.Equals(other.ChangeSetId, payload.ChangeSetId, StringComparison.Ordinal))
                return;

            throw RefuseWhilePending(other, entry.Op);
        }

        if (payload.BaseVersion != entry.FromVersion)
            throw new SchemaDeltaRefusedException(
                CamusDBErrorCodes.TransactionConflict,
                $"The schema changes of the transaction were planned against schema version {payload.BaseVersion}, " +
                $"but the schema is at version {entry.FromVersion}: another schema change landed while the " +
                "transaction was open; the transaction was rolled back, retry it from BEGIN");

        if (payload.Deltas.Count == 0)
            throw new SchemaDeltaRefusedException(CamusDBErrorCodes.InvalidInternalOperation, "A pending change set must carry at least one delta");

        HashSet<string> affected = DryRun(schema, database, entry, payload, refuseOnFailure: true);

        schema.PendingChangeSet = new PendingSchemaChangeSet(payload.ChangeSetId, entry, payload, Environment.TickCount64, affected);
    }

    /// <summary>
    /// Records <paramref name="entry"/> as the pending change set without the checks of
    /// <see cref="ApplyPending"/>. The schema load uses it for a pending record read from the checkpoint;
    /// that record was checked when its entry applied. The affected tables are computed again from the
    /// loaded schema, which is the change set's base.
    /// </summary>
    internal static void RecordPending(Schema schema, SchemaChangeLogEntry entry)
    {
        SchemaChangeSetPayload payload = entry.GetPayload<SchemaChangeSetPayload>();
        HashSet<string> affected = DryRun(schema, database: null, entry, payload, refuseOnFailure: false);

        schema.PendingChangeSet = new PendingSchemaChangeSet(payload.ChangeSetId, entry, payload, Environment.TickCount64, affected);
    }

    /// <summary>
    /// Applies the deltas of a change set, in order, to a copy of <paramref name="schema"/>, and returns the
    /// ids of the live tables the change set touches: every table a delta changes, and every table that a
    /// foreign key of the change set references, on a created table or added to an existing one. The
    /// pending change set fences those tables (see <see cref="PendingSchemaChangeSet.AffectedTableIds"/>),
    /// and the publish copies them.
    /// With <paramref name="refuseOnFailure"/> a delta that does not apply refuses the change set; without
    /// it the delta is skipped, which only the load of an already checked record uses.
    /// </summary>
    private static HashSet<string> DryRun(
        Schema schema, DatabaseDescriptor? database, SchemaChangeLogEntry entry, SchemaChangeSetPayload payload, bool refuseOnFailure)
    {
        HashSet<string> live = new(schema.Tables.Count, StringComparer.Ordinal);
        foreach (TableSchema table in schema.Tables.Values)
        {
            if (table.Id is { Length: > 0 } id)
                live.Add(id);
        }

        HashSet<string> affected = new(StringComparer.Ordinal);

        // No table is copied up front: the dry run changes the tables of its deltas only, and each is copied
        // right before its first delta.
        Schema dryRun = SchemaReplicator.CloneSchema(schema, static (_, _) => false);
        dryRun.PendingChangeSet = null;

        HashSet<TableSchema> ownTables = new(ReferenceEqualityComparer.Instance);

        foreach (SchemaChangeLogEntry delta in DecodeDeltas(payload, entry))
        {
            if (IsChangeSetOp(delta.Op))
            {
                if (refuseOnFailure)
                    throw new SchemaDeltaRefusedException(
                        CamusDBErrorCodes.InvalidInternalOperation, $"A change set cannot contain the operation '{delta.Op}'");
                continue;
            }

            CopyTableOnWrite(dryRun, delta, ownTables);

            if (TargetTableName(delta) is { } targetName
                && dryRun.Tables.TryGetValue(targetName, out TableSchema? target)
                && target.Id is { } targetId
                && live.Contains(targetId))
                affected.Add(targetId);

            // A parent of a new foreign key gets a delete-side check at the publish; a delete planned
            // without it could remove a parent the transaction's new rows reference.
            if (delta.Op == SchemaOp.CreateTable && delta.GetPayload<SchemaCreateTablePayload>().ForeignKeys is { } foreignKeys)
            {
                foreach (ForeignKeySchema foreignKey in foreignKeys)
                {
                    if (foreignKey.ReferencedTableId is { } parentId && live.Contains(parentId))
                        affected.Add(parentId);
                }
            }

            if (delta.Op == SchemaOp.AddForeignKey
                && delta.GetPayload<SchemaAddForeignKeyPayload>().ForeignKey?.ReferencedTableId is { } addedParentId
                && live.Contains(addedParentId))
                affected.Add(addedParentId);

            TableSchema? applied;
            try
            {
                applied = SchemaDeltaApplier.ApplySchemaDelta(dryRun, database!, delta);
            }
            catch (CamusDBException) when (!refuseOnFailure)
            {
                continue;
            }
            catch (SchemaDeltaRefusedException)
            {
                throw;
            }
            catch (CamusDBException ex)
            {
                // Every node runs the same dry run on the same schema, so this answer is the same
                // everywhere; a refusal keeps the log moving where an exception would wedge it.
                throw new SchemaDeltaRefusedException(ex.Code, ex.Message);
            }

            if (applied is not null)
                ownTables.Add(applied);

            if (applied?.Id is { } appliedId && live.Contains(appliedId))
                affected.Add(appliedId);
        }

        return affected;
    }

    /// <summary>
    /// Copies the table that <paramref name="delta"/> is about to change in <paramref name="schema"/>, a copy
    /// of a catalog that shares its tables with the original, unless the copy owns that table already.
    /// <paramref name="ownTables"/> holds the tables the copy owns: the ones copied here, and the ones its
    /// deltas created, which the caller adds.
    ///
    /// <para>For the deltas of a change set only. Each of them changes at most the table that
    /// <see cref="TargetTableName"/> names, besides views and sequences, which every copy of a schema owns;
    /// the publish relies on the same fact when it copies the affected tables.</para>
    /// </summary>
    internal static void CopyTableOnWrite(Schema schema, SchemaChangeLogEntry delta, HashSet<TableSchema> ownTables)
    {
        if (TargetTableName(delta) is not { } name || !schema.Tables.TryGetValue(name, out TableSchema? table) || ownTables.Contains(table))
            return;

        TableSchema copy = SchemaReplicator.CloneTable(table);
        schema.Tables[name] = copy;
        ownTables.Add(copy);
    }

    /// <summary>
    /// Which tables a copy of the schema must hold as copies of its own before <paramref name="entry"/>
    /// applies to it, for <see cref="SchemaReplicator.CloneSchema"/>: the table the entry names, or none for
    /// an entry that changes no table. Null, which copies every table, for an operation this method does not
    /// know, so a new operation is safe until somebody decides what it changes.
    /// </summary>
    internal static Func<string, TableSchema, bool>? TableChangedBy(SchemaChangeLogEntry entry)
    {
        if (TargetTableName(entry) is { } target)
            return (name, _) => string.Equals(name, target, StringComparison.OrdinalIgnoreCase);

        return entry.Op switch
        {
            SchemaOp.CreateTable when entry.GetPayload<SchemaCreateTablePayload>().AdoptStagingTableId is null or "" => NoTable,

            SchemaOp.CreateView or SchemaOp.ReplaceView or SchemaOp.DropView or SchemaOp.RenameView or SchemaOp.SetViewDefinition
                or SchemaOp.CreateSequence or SchemaOp.DropSequence or SchemaOp.RenameSequence or SchemaOp.AlterSequence => NoTable,

            // The pending entry and the publish work on copies of their own, and the abort changes no table.
            SchemaOp.PendingChangeSet or SchemaOp.PublishChangeSet or SchemaOp.AbortChangeSet => NoTable,

            _ => null
        };
    }

    private static readonly Func<string, TableSchema, bool> NoTable = static (_, _) => false;

    /// <summary>
    /// The name of the table <paramref name="entry"/> changes, as it is before the delta applies; null for
    /// a delta on no table.
    /// </summary>
    internal static string? TargetTableName(SchemaChangeLogEntry entry) => entry.Op switch
    {
        SchemaOp.DropTable => entry.GetPayload<SchemaDropTablePayload>().TableName,
        SchemaOp.AddColumn or SchemaOp.DropColumn => entry.GetPayload<SchemaAlterColumnPayload>().TableName,
        SchemaOp.RenameTable or SchemaOp.RenameColumn or SchemaOp.RenameIndex => entry.GetPayload<SchemaRenamePayload>().TableName,
        SchemaOp.AddIndex or SchemaOp.DropIndex => entry.GetPayload<SchemaIndexPayload>().TableName,
        SchemaOp.SetTableSettings => entry.GetPayload<SchemaSetTableSettingsPayload>().TableName,
        SchemaOp.SetComment => entry.GetPayload<SchemaSetCommentPayload>().TableName,
        SchemaOp.AddCheckConstraint => entry.GetPayload<SchemaCheckConstraintPayload>().TableName,
        SchemaOp.SetColumnNotNull => entry.GetPayload<SchemaSetColumnNotNullPayload>().TableName,
        SchemaOp.AddForeignKey => entry.GetPayload<SchemaAddForeignKeyPayload>().TableName,
        SchemaOp.SetElementState => entry.GetPayload<SchemaElementStatePayload>().TableName,
        _ => null
    };

    /// <summary>
    /// The pending change set that a publish or an abort entry ends, or null when that change set is no
    /// longer pending (an earlier decision ended it, or it never applied).
    /// </summary>
    internal static PendingSchemaChangeSet? FindDecided(Schema schema, SchemaChangeLogEntry entry)
    {
        SchemaChangeSetDecisionPayload decision = entry.GetPayload<SchemaChangeSetDecisionPayload>();

        return schema.PendingChangeSet is { } pending
               && string.Equals(pending.ChangeSetId, decision.ChangeSetId, StringComparison.Ordinal)
            ? pending
            : null;
    }

    /// <summary>
    /// Applies a <see cref="SchemaOp.PublishChangeSet"/> entry: makes the pending change set visible in one
    /// step. The deltas apply, in order, to a copy of the schema in which only the affected tables are
    /// copied and every other table is the live object; the copy's maps are then installed into
    /// <paramref name="schema"/> at once (<see cref="Schema.InstallPublishedCatalog"/>). A reader that reads
    /// the schema without its lock therefore sees the catalog before the change set or after it, never a
    /// state between two of its deltas, such as the middle name of two renames.
    ///
    /// <para>The affected tables get new <see cref="TableSchema"/> objects. The live apply evicts their
    /// descriptors and fences the writers that pinned the old objects (<c>SchemaReplicator</c>). A publish
    /// for a change set that is no longer pending changes nothing.</para>
    /// </summary>
    internal static void ApplyPublish(Schema schema, DatabaseDescriptor database, SchemaChangeLogEntry entry)
    {
        if (FindDecided(schema, entry) is not { } pending)
            return;

        IReadOnlySet<string> affected = pending.AffectedTableIds;

        Schema published = SchemaReplicator.CloneSchema(schema, (_, table) => table.Id is { } id && affected.Contains(id));
        published.PendingChangeSet = null;

        foreach (SchemaChangeLogEntry delta in DecodeDeltas(pending.Payload, entry))
            SchemaDeltaApplier.ApplyOrRefuse(published, database, delta);

        schema.InstallPublishedCatalog(published);
    }

    /// <summary>Applies a <see cref="SchemaOp.AbortChangeSet"/> entry: drops the pending change set it names.</summary>
    internal static void ApplyAbort(Schema schema, SchemaChangeLogEntry entry)
    {
        if (FindDecided(schema, entry) is not null)
            schema.PendingChangeSet = null;
    }
}
