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
/// Applies the committed deltas that add and remove row triggers (<see cref="SchemaOp.CreateTrigger"/>,
/// <see cref="SchemaOp.DropTrigger"/>).
///
/// <para><b>Metadata only.</b> The apply never looks at the extension registry: the proposer already
/// resolved the type and named its installed extension, and each node binds the record to its own
/// loaded code at the first DML. An apply that consulted the local registry could fail on a node that
/// lacks the extension, and a committed schema delta must apply everywhere. The apply checks only the
/// replicated catalog: the named extension must be installed. An inline trigger names no extension, and
/// its body is compiled by each node at the first fire, never by the apply.</para>
///
/// <para><b>Idempotent.</b> A re-delivered create finds its own trigger (same name and type) and returns;
/// a re-delivered drop finds nothing to remove and returns. The checks that can fail — the table was
/// replaced, the name is taken by a different trigger — run before the first change, in log order, so
/// a concurrent <c>DROP TABLE</c> or <c>RENAME TABLE</c> is ordered against the trigger DDL.</para>
///
/// <para>The list is replaced, never edited in place, so a lock-free reader that holds the old list
/// keeps a consistent one. Never persists: <c>SchemaReplicator.ApplyAsync</c> runs inside the commit
/// pipeline; the proposer persists the checkpoint afterwards.</para>
/// </summary>
internal static class TriggerDeltaApplier
{
    internal static TableSchema ApplyCreateTrigger(Schema schema, SchemaCreateTriggerPayload payload)
    {
        TableSchema table = RequireTable(schema, payload.TableName, payload.TableId);

        TriggerDefinition trigger = payload.Trigger
            ?? throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"A CreateTrigger delta for table '{payload.TableName}' carries no trigger");

        TriggerDefinition? existing = Find(table, trigger.Name);

        if (existing is not null)
        {
            if (SameBody(existing, trigger) &&
                existing.Timing == trigger.Timing &&
                existing.Events == trigger.Events)
                return table;

            throw new CamusDBException(
                CamusDBErrorCodes.TriggerAlreadyExists,
                $"Table '{table.Name}' already has a trigger named '{trigger.Name}'");
        }

        if (table.Kind != RelationKind.Table)
            throw new CamusDBException(
                CamusDBErrorCodes.TriggerTargetInvalid,
                $"'{table.Name}' is not a plain table; triggers can be created only on tables");

        // In log order against DROP EXTENSION: a trigger must name an installed extension, and a drop
        // proposed in between would otherwise leave it pointing at nothing. An inline trigger names
        // no extension: its code is its own body.
        if (!trigger.IsInline && !schema.Extensions.ContainsKey(trigger.ExtensionName))
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionNotInstalled,
                $"Trigger '{trigger.Name}' names extension '{trigger.ExtensionName}', which is not installed in this database");

        table.Triggers = table.Triggers is null ? [trigger] : [.. table.Triggers, trigger];
        return table;
    }

    internal static TableSchema ApplyDropTrigger(Schema schema, SchemaDropTriggerPayload payload)
    {
        TableSchema table = RequireTable(schema, payload.TableName, payload.TableId);

        if (Find(table, payload.TriggerName) is null)
            return table;

        TriggerDefinition removed = Find(table, payload.TriggerName)!;

        List<TriggerDefinition> remaining = table.Triggers!
            .Where(t => !ReferenceEquals(t, removed))
            .ToList();

        table.Triggers = remaining.Count == 0 ? null : remaining;

        // An inline trigger's binding holds a reference on its compiled code; give it back now, not
        // when the engine closes. A statement that still fires it keeps the code alive until it ends.
        CamusDB.Core.Extensions.TriggerRuntime.Retire(removed);
        return table;
    }

    /// <summary>
    /// True when two records of one trigger name have the same code: the same type for the DLL form,
    /// the same body for the inline form.
    /// </summary>
    private static bool SameBody(TriggerDefinition existing, TriggerDefinition trigger) =>
        string.Equals(existing.TypeName, trigger.TypeName, StringComparison.Ordinal) &&
        string.Equals(existing.SourceSha256, trigger.SourceSha256, StringComparison.Ordinal);

    internal static bool WasCreateApplied(Schema schema, SchemaCreateTriggerPayload payload) =>
        payload.Trigger is { } trigger
        && schema.Tables.TryGetValue(payload.TableName, out TableSchema? table)
        && Find(table, trigger.Name) is { } existing
        && SameBody(existing, trigger);

    internal static bool WasDropApplied(Schema schema, SchemaDropTriggerPayload payload) =>
        !schema.Tables.TryGetValue(payload.TableName, out TableSchema? table)
        || Find(table, payload.TriggerName) is null;

    /// <summary>The trigger of the table with this name, compared case-insensitively.</summary>
    internal static TriggerDefinition? Find(TableSchema table, string name)
    {
        if (table.Triggers is not { } triggers)
            return null;

        foreach (TriggerDefinition trigger in triggers)
        {
            if (string.Equals(trigger.Name, name, StringComparison.OrdinalIgnoreCase))
                return trigger;
        }

        return null;
    }

    private static TableSchema RequireTable(Schema schema, string tableName, string tableId)
    {
        if (!schema.Tables.TryGetValue(tableName, out TableSchema? table)
            || !string.Equals(table.Id, tableId, StringComparison.Ordinal))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{tableName}' does not exist");

        return table;
    }
}
