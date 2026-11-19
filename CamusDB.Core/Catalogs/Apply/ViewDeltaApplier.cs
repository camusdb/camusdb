
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.Catalogs.Apply;

/// <summary>
/// Applies the committed deltas that create, replace, drop, rename or redefine a view, and the
/// state transitions of a materialized view.
///
/// <para><b>View operations act on the view map, not the table map.</b> The two are separate, and a
/// delta that looked for a view among the tables would find nothing and conclude the change had not
/// landed — forever. Every arm here resolves through <c>Schema.Views</c>.</para>
///
/// <para><b>A view body is bound to the relations it reads by their immutable ids.</b> Renaming a
/// table therefore rewrites dependent view bodies rather than invalidating them, which is why
/// <c>SchemaDeltaApplier.ApplyDependentViewRewrites</c> is shared between the table-rename and
/// view-rename arms.</para>
///
/// <para><b>Adopting a refreshed materialized view swaps its contents but keeps its identity.</b>
/// The replaced key-space is left with nothing naming it, so <c>ContentsRetirementStore</c> makes it
/// recoverable afterwards from an intent captured during apply. This class writes no KV.</para>
/// </summary>
internal static class ViewDeltaApplier
{
    /// <summary>
    /// Applies a CreateView or ReplaceView delta. Returns null because a view is not a relation and
    /// there is no <see cref="TableSchema"/> checkpoint to persist from this apply — the proposer
    /// persists the view's own meta key afterward.
    /// </summary>
    /// <remarks>
    /// The two ops differ only in how they treat an existing name, and that difference is the whole
    /// reason they are separate ops: a create must refuse a taken name, while a replace must
    /// overwrite it while <b>preserving the existing view's id</b>. Minting a fresh id on replace
    /// would silently break every dependent that recorded the old one, and every dependency check
    /// that scans for it.
    /// </remarks>
    internal static TableSchema? ApplyCreateView(Schema schema, SchemaViewPayload payload, bool replacing)
    {
        schema.Views.TryGetValue(payload.ViewName, out ViewSchema? existing);

        if (existing is null)
        {
            // A name held by a table (or a materialized view) is not ours to take, on either op.
            if (schema.Tables.ContainsKey(payload.ViewName))
                throw new SchemaDeltaRefusedException(
                    CamusDBErrorCodes.TableAlreadyExists,
                    $"Relation '{payload.ViewName}' already exists");

            // A replace validated against a view that a delta committed ahead of it dropped. Creating
            // the view afresh would bring back a name its dropper meant to remove, under the old id.
            if (replacing && payload.ExpectedGeneration is not null)
                throw new SchemaDeltaRefusedException(
                    CamusDBErrorCodes.ConcurrentSchemaChange,
                    $"View '{payload.ViewName}' was dropped while it was being replaced; nothing was changed");
        }
        else if (!replacing)
        {
            // Idempotent replay of a create: the same id landing twice is a re-delivery, not a
            // conflict, and must not wedge the apply pipeline.
            if (string.Equals(existing.Id, payload.ViewId, StringComparison.Ordinal))
                return null;

            throw new SchemaDeltaRefusedException(
                CamusDBErrorCodes.ViewAlreadyExists,
                $"Relation '{payload.ViewName}' already exists");
        }
        else if (payload.ExpectedGeneration is { } expected
            && (!string.Equals(existing.Id, payload.ViewId, StringComparison.Ordinal) || existing.Generation != expected))
        {
            // The replacement's shape check, its owner and its cycle check were all computed from the
            // definition at that generation. Two replaces that are each compatible with the old shape
            // need not be compatible with each other, so the later one is refused.
            throw new SchemaDeltaRefusedException(
                CamusDBErrorCodes.ConcurrentSchemaChange,
                $"View '{payload.ViewName}' was changed by another statement while this one was replacing it; " +
                "nothing was changed. Retry the statement.");
        }

        string viewId = existing?.Id ?? payload.ViewId!;

        if (payload.ValidateDependencies && payload.Definition is { } definition)
            RequireDependenciesHold(schema, viewId, payload.ViewName, definition);

        schema.Views[payload.ViewName] = new ViewSchema
        {
            Id = viewId,
            Name = payload.ViewName,
            Definition = payload.Definition,
            Comment = payload.Comment ?? existing?.Comment,
            Generation = existing is null ? 0 : existing.Generation + 1,
        };

        schema.ReplaceViewDependencyEdges(viewId, existing?.Definition, payload.Definition);

        return null;
    }

    /// <summary>
    /// Applies a DropView delta. Idempotent: a view that is already gone is a no-op rather than a
    /// failure, so a re-delivered entry — or one that arrives after the view was dropped by a later,
    /// already-applied entry — cannot wedge the apply pipeline.
    /// </summary>
    /// <remarks>
    /// Entries written with preconditions also refuse a name that now belongs to a different view,
    /// and refuse while anything still reads the view — a dependent created after the statement's own
    /// check, or after a <c>CASCADE</c> listed what to drop first, would otherwise be left reading
    /// nothing. The table-drop rule <see cref="ViewDependencyRules"/> is the same check one level
    /// down.
    /// </remarks>
    internal static TableSchema? ApplyDropView(Schema schema, SchemaDropViewPayload payload)
    {
        if (!schema.Views.TryGetValue(payload.ViewName, out ViewSchema? view))
            return null;

        if (payload.ExpectedViewId is { } expectedId && !string.Equals(view.Id, expectedId, StringComparison.Ordinal))
            throw new SchemaDeltaRefusedException(
                CamusDBErrorCodes.ConcurrentSchemaChange,
                $"View '{payload.ViewName}' was dropped and created again by another statement; the new view was not dropped");

        if (payload.RequireNoDependents && view.Id is not null)
        {
            List<string> dependents = ViewDependencyGraph.DirectDependentsOfView(schema, view.Id);

            if (dependents.Count > 0)
            {
                dependents.Sort(StringComparer.OrdinalIgnoreCase);

                throw new SchemaDeltaRefusedException(
                    CamusDBErrorCodes.DependentObjectsExist,
                    $"Cannot drop view '{payload.ViewName}' because other objects depend on it: " +
                    $"{string.Join(", ", dependents)}. Use DROP VIEW ... CASCADE to drop them too.");
            }
        }

        schema.Views.Remove(payload.ViewName);
        schema.ReplaceViewDependencyEdges(view.Id, view.Definition, null);
        return null;
    }

    internal static TableSchema? ApplyRenameView(Schema schema, SchemaRenamePayload payload)
    {
        if (!schema.Views.TryGetValue(payload.TableName, out ViewSchema? view))
        {
            // Already renamed by an earlier delivery of this same entry.
            if (schema.Views.ContainsKey(payload.NewName))
                return null;

            throw new SchemaDeltaRefusedException(
                CamusDBErrorCodes.ViewDoesntExist,
                $"View '{payload.TableName}' does not exist");
        }

        if (schema.Tables.ContainsKey(payload.NewName) || schema.Views.ContainsKey(payload.NewName)
            || schema.Sequences.ContainsKey(payload.NewName))
            throw new SchemaDeltaRefusedException(
                CamusDBErrorCodes.ViewAlreadyExists,
                $"Relation '{payload.NewName}' already exists");

        schema.Views.Remove(payload.TableName);
        view.Name = payload.NewName;
        view.Generation++;
        schema.Views[payload.NewName] = view;

        SchemaDeltaApplier.ApplyDependentViewRewrites(schema, payload);

        return null;
    }

    /// <summary>
    /// Applies a SetViewDefinition delta: <c>ALTER VIEW … OWNER TO</c>, <c>COMMENT ON VIEW</c>, or a
    /// rewrite of a view's stored body. Idempotent: replaying it simply writes the same body again,
    /// and, for an entry without preconditions, a view that no longer exists is a no-op rather than a
    /// failure.
    /// </summary>
    /// <remarks>
    /// An entry that carries <see cref="SchemaSetViewDefinitionPayload.ExpectedGeneration"/> is a
    /// change computed from the view as it stood; it is refused if the view has moved on since, so it
    /// cannot put back a body a concurrent <c>CREATE OR REPLACE</c> replaced.
    /// </remarks>
    internal static TableSchema? ApplySetViewDefinition(Schema schema, SchemaSetViewDefinitionPayload payload)
    {
        if (!schema.Views.TryGetValue(payload.ViewName, out ViewSchema? view))
        {
            if (payload.ExpectedGeneration is not null)
                throw new SchemaDeltaRefusedException(
                    CamusDBErrorCodes.ConcurrentSchemaChange,
                    $"View '{payload.ViewName}' was dropped or renamed by another statement; nothing was changed");

            return null;
        }

        if (payload.ExpectedGeneration is { } expected && view.Generation != expected)
            throw new SchemaDeltaRefusedException(
                CamusDBErrorCodes.ConcurrentSchemaChange,
                $"View '{payload.ViewName}' was changed by another statement while this one was altering it; " +
                "nothing was changed. Retry the statement.");

        if (payload.Definition is not null)
        {
            ViewDefinition? before = view.Definition;
            view.Definition = payload.Definition;
            schema.ReplaceViewDependencyEdges(view.Id, before, payload.Definition);
        }

        if (payload.SetComment)
            view.Comment = payload.Comment;

        view.Generation++;

        return null;
    }

    /// <summary>
    /// Refuses a view definition that reads a relation which no longer exists, or that would make the
    /// view reachable from itself, against the schema the entry is applied to.
    /// </summary>
    /// <remarks>
    /// The statement checked both before it proposed, but against the schema as it stood then. A drop
    /// committed in between — on another node, or by a <c>CASCADE</c> — would otherwise leave this
    /// view reading something gone, and a concurrent replace of a view it reads could close a cycle
    /// neither statement saw.
    /// </remarks>
    internal static void RequireDependenciesHold(Schema schema, string viewId, string viewName, ViewDefinition definition)
    {
        // Resolved through the id index, so the check costs one lookup per dependency the
        // definition lists.
        foreach (string tableId in definition.DependsOnTableIds ?? [])
        {
            if (!schema.TryGetRelationNameById(tableId, out string? tableName)
                || !schema.Tables.TryGetValue(tableName, out TableSchema? relation)
                || !string.Equals(relation.Id, tableId, StringComparison.Ordinal))
                throw new SchemaDeltaRefusedException(
                    CamusDBErrorCodes.ConcurrentSchemaChange,
                    $"A relation that '{viewName}' reads was dropped by another statement; nothing was changed");
        }

        foreach (string dependencyId in definition.DependsOnViewIds ?? [])
        {
            if (!schema.TryGetRelationNameById(dependencyId, out string? dependencyName)
                || !schema.Views.TryGetValue(dependencyName, out ViewSchema? dependency)
                || !string.Equals(dependency.Id, dependencyId, StringComparison.Ordinal))
                throw new SchemaDeltaRefusedException(
                    CamusDBErrorCodes.ConcurrentSchemaChange,
                    $"A view that '{viewName}' reads was dropped by another statement; nothing was changed");
        }

        try
        {
            ViewDependencyGraph.RequireAcyclic(schema, viewId, viewName, definition);
        }
        catch (CamusDBException cycle) when (cycle is not SchemaDeltaRefusedException)
        {
            throw new SchemaDeltaRefusedException(cycle.Code, cycle.Message);
        }
    }

    /// <summary>
    /// Applies a SetMaterializedViewState delta: the populated flag, the snapshot the contents are
    /// consistent as of, and — for the swap half of a build-and-swap refresh — the id of the freshly
    /// built relation the live name should now point at.
    /// </summary>
    /// <remarks>
    /// Keyed by table id, not name, so a refresh that raced a rename still marks the relation it
    /// actually built. Returns the affected <see cref="TableSchema"/> so the proposer persists the
    /// updated checkpoint; a relation whose id is not found is a no-op (it was dropped) rather than
    /// a failure.
    /// </remarks>
    internal static TableSchema? ApplySetMaterializedViewState(Schema schema, SchemaSetMatViewStatePayload payload)
    {
        TableSchema? live = SchemaDeltaApplier.FindRelationById(schema, payload.TableId);

        // Not found means the relation was dropped, or this entry is a replay of a swap that already
        // moved the name onto a different id. Both are no-ops: the flags this entry carries describe
        // a relation that is no longer the one answering to the name.
        if (live is null)
            return null;

        if (string.IsNullOrEmpty(payload.SwapToTableId))
        {
            live.IsPopulated = payload.IsPopulated;
            live.RefreshedAt = payload.RefreshedAt;
            return live;
        }

        // Compare-and-swap on the relation's metadata. The staging relation was built from a copy of
        // this layout taken when the rebuild started; publishing that copy over a definition that has
        // since changed would silently erase the change — an index created during the rebuild would
        // vanish from the schema, and its entries were never written to the new key-space either.
        // Refused rather than merged: a heuristic merge would have to guess which side of a conflict
        // the user meant, and guessing wrong here loses data rather than an operation.
        if (payload.ExpectedMetadataGeneration is { } expected && live.MetadataGeneration != expected)
            throw new SchemaDeltaRefusedException(
                CamusDBErrorCodes.ConcurrentSchemaChange,
                $"The definition of materialized view '{live.Name}' changed while it was being refreshed, so the " +
                "rebuild was discarded rather than published over it. Retry the refresh.");

        // Contents may only move forward in time. Two runs that both got past the fence — a lease that
        // lapsed under a stalled node, say — would otherwise race at the swap, and the one that finished
        // last would win regardless of which read the newer source. Ordering by the source snapshot
        // makes the outcome depend on the data rather than on scheduling.
        if (payload.RefreshedAt is { } incoming && live.RefreshedAt is { } published
            && incoming < published)
            throw new SchemaDeltaRefusedException(
                CamusDBErrorCodes.ConcurrentSchemaChange,
                $"A newer refresh of materialized view '{live.Name}' has already been published, so this " +
                "older rebuild was discarded rather than overwriting it.");

        TableSchema? built = SchemaDeltaApplier.FindRelationById(schema, payload.SwapToTableId);

        if (built is null)
            throw new SchemaDeltaRefusedException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Cannot swap materialized view '{live.Name}': the rebuilt relation '{payload.SwapToTableId}' is not in the schema");

        // What moves is the *contents*: the key-space the rows were written into, and the column and
        // index definitions they were encoded against. What deliberately stays is the materialized
        // view's identity — its id, and with it every privilege grant, dependency edge, cached result
        // and statistic that names it. A refresh replaces what a materialized view holds; it does not
        // replace the materialized view, and treating it as a new object would silently revoke every
        // grant on it.
        live.StorageId = built.EffectiveStorageId;
        live.Columns = built.Columns;
        live.Indexes = built.Indexes;
        live.CheckConstraints = built.CheckConstraints;
        // A materialized view can never take part in a foreign key, so this is null on both sides.
        // Copied anyway so that the swap stays a complete transfer of the definition.
        live.ForeignKeys = built.ForeignKeys;
        live.Version = built.Version;
        live.SchemaHistory = built.SchemaHistory;
        live.SchemaHistoryLoader = built.SchemaHistoryLoader;
        live.IsPopulated = payload.IsPopulated;
        live.RefreshedAt = payload.RefreshedAt;

        // Bumped on every node as part of the same apply, so the generation is identical everywhere
        // rather than depending on which node happened to propose the refresh.
        live.ContentsGeneration++;

        // The relation the rebuild was staged under has served its purpose; only the materialized view
        // answers for that key-space now.
        schema.Tables.Remove(built.Name!);

        return live;
    }
}
