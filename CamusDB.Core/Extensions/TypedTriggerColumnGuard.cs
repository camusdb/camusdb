/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Extensions;

/// <summary>
/// The proposer-side check that keeps a typed trigger's row mapping valid: a column that a typed
/// trigger (<c>ICamusTrigger&lt;TRow&gt;</c>) maps cannot be dropped or renamed while the trigger exists,
/// so a trigger that works cannot start failing because of a schema change. The same holds for an
/// inline trigger whose C# body names the column (<see cref="Inline.InlineTriggerBodyReferences"/>):
/// after the drop or the rename the body would no longer compile, and every write to the table would
/// fail.
///
/// <para><b>Catalog only.</b> The check reads the columns each trigger record keeps
/// (<see cref="TriggerDefinition.RowColumns"/>), not the trigger's assembly, so it gives the same answer
/// on a node that does not load the extension.</para>
///
/// <para><b>Both directions of the race.</b> <c>CREATE TRIGGER</c> checks the row class against the
/// table before it takes the schema lock, so a <c>DROP COLUMN</c> can commit in between. The check
/// therefore also runs for the trigger's own delta and refuses it when a mapped column is no longer a
/// readable column of the table. Proposals are ordered by schema version, so whichever delta comes
/// second is checked against the first.</para>
///
/// <para><b>Where it runs.</b> In <c>SchemaDeltaApplier.ValidateSchemaDelta</c>, which every proposer
/// calls under the schema lock against the live schema just before it proposes. Never in the replicated
/// apply, which must not fail.</para>
/// </summary>
internal static class TypedTriggerColumnGuard
{
    /// <summary>Throws when <paramref name="entry"/> would break a typed trigger of <paramref name="schema"/>.</summary>
    public static void Check(Schema schema, SchemaChangeLogEntry entry)
    {
        switch (entry.Op)
        {
            case SchemaOp.DropColumn:
            {
                SchemaAlterColumnPayload payload = SchemaDeltaApplier.DecodePayload<SchemaAlterColumnPayload>(entry);
                RefuseIfMapped(schema, payload.TableName, payload.Column.Name, "drop");
                return;
            }

            case SchemaOp.RenameColumn:
            {
                SchemaRenamePayload payload = SchemaDeltaApplier.DecodePayload<SchemaRenamePayload>(entry);

                if (payload.Kind == SchemaRenameKind.Column && payload.ElementName is { } column)
                    RefuseIfMapped(schema, payload.TableName, column, "rename");

                return;
            }

            // The staged DROP COLUMN starts by moving the column out of Public. A mapped column is
            // Public when its trigger is created, so any other target state is the start of a drop.
            case SchemaOp.SetElementState:
            {
                SchemaElementStatePayload payload = SchemaDeltaApplier.DecodePayload<SchemaElementStatePayload>(entry);

                if (payload.ElementKind == SchemaElementKind.Column && payload.State != SchemaElementState.Public)
                    RefuseIfMapped(schema, payload.TableName, payload.ElementName, "drop", inlineBodies: IsPublic(schema, payload.TableName, payload.ElementName));

                return;
            }

            case SchemaOp.CreateTrigger:
            {
                SchemaCreateTriggerPayload payload = SchemaDeltaApplier.DecodePayload<SchemaCreateTriggerPayload>(entry);

                if (payload.Trigger?.RowColumns is not { Count: > 0 } mapped || !schema.Tables.TryGetValue(payload.TableName, out TableSchema? table))
                    return;

                foreach (string column in mapped)
                {
                    if (TypedTriggerAdapter.FindColumn(table.Columns ?? [], column) is null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.TriggerRowMappingInvalid,
                            $"Trigger '{payload.Trigger.Name}' maps column '{column}', which table '{table.Name}' no longer has; the column was dropped while the statement ran");
                }

                return;
            }
        }
    }

    /// <param name="inlineBodies">
    /// False for the steps of a staged column change that do not start a drop: an <c>ADD COLUMN</c>
    /// moves a new column through the same non-public states, and a trigger body may hold a local
    /// with the new column's name, which must not block the add.
    /// </param>
    private static void RefuseIfMapped(Schema schema, string tableName, string column, string action, bool inlineBodies = true)
    {
        if (!schema.Tables.TryGetValue(tableName, out TableSchema? table) || table.Triggers is not { Count: > 0 } triggers)
            return;

        List<string>? users = null;
        List<string>? bodies = null;

        foreach (TriggerDefinition trigger in triggers)
        {
            if (trigger.RowColumns is { } mapped && mapped.Contains(column, StringComparer.OrdinalIgnoreCase))
                (users ??= []).Add(trigger.Name);
            else if (inlineBodies && trigger.IsInline && Inline.InlineTriggerBodyReferences.NamesColumn(trigger.Source, column))
                (bodies ??= []).Add(trigger.Name);
        }

        if (users is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.DependentObjectsExist,
                $"Cannot {action} column '{column}' of table '{table.Name}': typed trigger(s) {string.Join(", ", users)} map it to a row property; drop the trigger(s) first");

        if (bodies is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.DependentObjectsExist,
                $"Cannot {action} column '{column}' of table '{table.Name}': the C# body of trigger(s) {string.Join(", ", bodies)} names it, " +
                "and would no longer compile; drop the trigger(s) first, or create them again without the name");
    }

    /// <summary>True when the table has the column and the column is public: a move out of that state starts a drop.</summary>
    private static bool IsPublic(Schema schema, string tableName, string column)
    {
        if (!schema.Tables.TryGetValue(tableName, out TableSchema? table) || table.Columns is not { } columns)
            return false;

        foreach (TableColumnSchema candidate in columns)
        {
            if (string.Equals(candidate.Name, column, StringComparison.OrdinalIgnoreCase))
                return candidate.State == SchemaElementState.Public;
        }

        return false;
    }
}
