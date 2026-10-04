/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Apply;

/// <summary>
/// The tables whose write shape a schema delta changes. <see cref="Tables"/> is null when the delta
/// adds no write obligation. <see cref="UseAppliedTable"/> asks the caller to stamp the table that the
/// apply returns, for an operation these rules do not know.
/// </summary>
internal readonly record struct WriteShapeTargets(IReadOnlyList<TableSchema>? Tables, bool UseAppliedTable)
{
    public static readonly WriteShapeTargets None = new(null, false);

    public static readonly WriteShapeTargets AppliedTable = new(null, true);

    public static WriteShapeTargets Of(TableSchema table) => new([table], false);

    /// <summary>A foreign key: the child, and the parent when it resolves and is another table.</summary>
    public static WriteShapeTargets Of(TableSchema child, TableSchema? parent) =>
        parent is null || ReferenceEquals(parent, child) ? Of(child) : new([child, parent], false);
}

/// <summary>
/// Decides which tables a committed schema delta fences: the tables that gain a <b>write
/// obligation</b>, so that a write planned before the delta is now incomplete. See
/// <see cref="WriteShapeClock"/> for what the fence does with the answer.
///
/// <para><b>Only additions fence.</b> A dropped index or constraint leaves earlier writes correct;
/// they did more than is needed now. A column change moves the table's layout version, which the
/// schema-version pin already watches. A step to <c>Public</c> changes who may read, not what a
/// write must do. Fencing those would refuse transactions for no gain.</para>
///
/// <para><b>An operation this class does not list is fenced.</b> A refused commit costs a retry. A
/// commit that should have been refused costs a row without its index entry, found weeks later. So
/// a new <see cref="SchemaOp"/> lands on the safe side until somebody decides that it adds nothing.
/// </para>
///
/// <para><b>Resolve before the apply, stamp after it.</b> The answer needs the state the delta is
/// about to replace: an index that the proposer already keeps in memory with the same write duty
/// (the single-node build adds it locally and replicates afterwards) gains nothing from its delta.
/// Every table named here exists before the apply, and the apply mutates it in place, so the
/// instance resolved early is the one to stamp.</para>
/// </summary>
internal static class WriteShapeDeltaRules
{
    internal static WriteShapeTargets Resolve(Schema schema, SchemaChangeLogEntry entry)
    {
        switch (entry.Op)
        {
            case SchemaOp.AddIndex:
            {
                SchemaIndexPayload payload = SchemaDeltaApplier.DecodePayload<SchemaIndexPayload>(entry);

                if (payload.Index is null || !schema.Tables.TryGetValue(payload.TableName, out TableSchema? table))
                    return WriteShapeTargets.None;

                TableIndexSchema? existing = table.Indexes?.Find(
                    ix => string.Equals(ix.Name, payload.IndexName, StringComparison.OrdinalIgnoreCase));

                if (existing is not null && WriteDuty(existing.State) >= WriteDuty(payload.Index.State))
                    return WriteShapeTargets.None;

                return WriteShapeTargets.Of(table);
            }

            case SchemaOp.SetElementState:
            {
                SchemaElementStatePayload payload = SchemaDeltaApplier.DecodePayload<SchemaElementStatePayload>(entry);

                // A step that takes duties away, and the step to Public, add nothing.
                if (payload.State is not (SchemaElementState.DeleteOnly or SchemaElementState.WriteOnly))
                    return WriteShapeTargets.None;

                if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? table))
                    return WriteShapeTargets.None;

                return payload.ElementKind switch
                {
                    // Moves TableSchema.Version; the schema-version pin refuses the commit.
                    SchemaElementKind.Column => WriteShapeTargets.None,
                    SchemaElementKind.Index => WriteShapeTargets.Of(table),
                    SchemaElementKind.ForeignKey => WriteShapeTargets.Of(table, ParentOf(schema, table, payload.ElementName)),
                    _ => WriteShapeTargets.AppliedTable
                };
            }

            case SchemaOp.AddForeignKey:
            {
                SchemaAddForeignKeyPayload payload = SchemaDeltaApplier.DecodePayload<SchemaAddForeignKeyPayload>(entry);

                if (payload.ForeignKey is null || !schema.Tables.TryGetValue(payload.TableName, out TableSchema? table))
                    return WriteShapeTargets.None;

                // Both sides: the child must find its parent, and the parent must not remove a
                // referenced key. A parent DELETE planned before the constraint runs no check.
                return WriteShapeTargets.Of(table, SchemaDeltaApplier.FindRelationById(schema, payload.ForeignKey.ReferencedTableId));
            }

            case SchemaOp.AddCheckConstraint:
            {
                SchemaCheckConstraintPayload payload = SchemaDeltaApplier.DecodePayload<SchemaCheckConstraintPayload>(entry);

                return schema.Tables.TryGetValue(payload.TableName, out TableSchema? table)
                    ? WriteShapeTargets.Of(table)
                    : WriteShapeTargets.None;
            }

            case SchemaOp.SetColumnNotNull:
            {
                SchemaSetColumnNotNullPayload payload = SchemaDeltaApplier.DecodePayload<SchemaSetColumnNotNullPayload>(entry);

                return payload.NotNull && schema.Tables.TryGetValue(payload.TableName, out TableSchema? table)
                    ? WriteShapeTargets.Of(table)
                    : WriteShapeTargets.None;
            }

            case SchemaOp.CreateTable:
            {
                // The new table has no earlier writer. Its parents do: each foreign key declared
                // with the table is enforced on the parent from this delta on, and a parent DELETE
                // staged before it ran no check for children.
                SchemaCreateTablePayload payload = SchemaDeltaApplier.DecodePayload<SchemaCreateTablePayload>(entry);

                if (payload.ForeignKeys is not { Length: > 0 } foreignKeys)
                    return WriteShapeTargets.None;

                List<TableSchema>? parents = null;

                foreach (ForeignKeySchema foreignKey in foreignKeys)
                {
                    TableSchema? parent = SchemaDeltaApplier.FindRelationById(schema, foreignKey.ReferencedTableId);

                    if (parent is not null && parents?.Contains(parent) != true)
                        (parents ??= []).Add(parent);
                }

                return parents is null ? WriteShapeTargets.None : new(parents, false);
            }

            // A re-linked relation comes back without its foreign keys and has no earlier writer. A
            // dropped, renamed or truncated one is caught by the schema-version pin and the contents pin.
            case SchemaOp.RelinkTable:
            case SchemaOp.DropTable:
            case SchemaOp.RenameTable:
            case SchemaOp.TruncateTable:
            // Column changes move TableSchema.Version; the schema-version pin refuses the commit.
            case SchemaOp.AddColumn:
            case SchemaOp.DropColumn:
            case SchemaOp.RenameColumn:
            // These take a duty away or change none.
            case SchemaOp.DropIndex:
            case SchemaOp.RenameIndex:
            case SchemaOp.DropCheckConstraint:
            case SchemaOp.SetColumnStorage:
            case SchemaOp.SetTableSettings:
            case SchemaOp.SetComment:
            // Not tables that a user statement writes.
            case SchemaOp.CreateView:
            case SchemaOp.ReplaceView:
            case SchemaOp.DropView:
            case SchemaOp.RenameView:
            case SchemaOp.SetViewDefinition:
            case SchemaOp.SetMaterializedViewState:
            case SchemaOp.CreateSequence:
            case SchemaOp.DropSequence:
            case SchemaOp.RenameSequence:
            case SchemaOp.AlterSequence:
                return WriteShapeTargets.None;

            default:
                return WriteShapeTargets.AppliedTable;
        }
    }

    /// <summary>
    /// How much a write must do for an index in <paramref name="state"/>: nothing, remove entries, or
    /// keep entries. <c>WriteOnly</c> and <c>Public</c> ask the same of a write.
    /// </summary>
    private static int WriteDuty(SchemaElementState state) => state switch
    {
        SchemaElementState.Absent => 0,
        SchemaElementState.DeleteOnly => 1,
        _ => 2
    };

    private static TableSchema? ParentOf(Schema schema, TableSchema child, string constraintName)
    {
        ForeignKeySchema? foreignKey = child.ForeignKeys?.Find(
            fk => string.Equals(fk.Name, constraintName, StringComparison.OrdinalIgnoreCase));

        return foreignKey is null ? null : SchemaDeltaApplier.FindRelationById(schema, foreignKey.ReferencedTableId);
    }
}
