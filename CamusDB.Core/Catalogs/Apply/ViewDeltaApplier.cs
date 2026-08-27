
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
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
                throw new CamusDBException(
                    CamusDBErrorCodes.TableAlreadyExists,
                    $"Relation '{payload.ViewName}' already exists");
        }
        else if (!replacing)
        {
            // Idempotent replay of a create: the same id landing twice is a re-delivery, not a
            // conflict, and must not wedge the apply pipeline.
            if (string.Equals(existing.Id, payload.ViewId, StringComparison.Ordinal))
                return null;

            throw new CamusDBException(
                CamusDBErrorCodes.ViewAlreadyExists,
                $"Relation '{payload.ViewName}' already exists");
        }

        schema.Views[payload.ViewName] = new ViewSchema
        {
            Id = existing?.Id ?? payload.ViewId,
            Name = payload.ViewName,
            Definition = payload.Definition,
            Comment = payload.Comment ?? existing?.Comment,
        };

        return null;
    }

    /// <summary>
    /// Applies a DropView delta. Idempotent: a view that is already gone is a no-op rather than a
    /// failure, so a re-delivered entry — or one that arrives after the view was dropped by a later,
    /// already-applied entry — cannot wedge the apply pipeline.
    /// </summary>
    internal static TableSchema? ApplyDropView(Schema schema, SchemaDropViewPayload payload)
    {
        schema.Views.Remove(payload.ViewName);
        return null;
    }

    internal static TableSchema? ApplyRenameView(Schema schema, SchemaRenamePayload payload)
    {
        if (!schema.Views.TryGetValue(payload.TableName, out ViewSchema? view))
        {
            // Already renamed by an earlier delivery of this same entry.
            if (schema.Views.ContainsKey(payload.NewName))
                return null;

            throw new CamusDBException(
                CamusDBErrorCodes.ViewDoesntExist,
                $"View '{payload.TableName}' does not exist");
        }

        if (schema.Tables.ContainsKey(payload.NewName) || schema.Views.ContainsKey(payload.NewName))
            throw new CamusDBException(
                CamusDBErrorCodes.ViewAlreadyExists,
                $"Relation '{payload.NewName}' already exists");

        schema.Views.Remove(payload.TableName);
        view.Name = payload.NewName;
        schema.Views[payload.NewName] = view;

        SchemaDeltaApplier.ApplyDependentViewRewrites(schema, payload);

        return null;
    }

    /// <summary>
    /// Applies a SetViewDefinition delta — the dependent-view rewrite that rides along with a base
    /// table or column rename. Idempotent: replaying it simply writes the same body again, and a
    /// view that no longer exists is a no-op rather than a failure, so a rename entry re-delivered
    /// after a later DROP VIEW cannot wedge apply.
    /// </summary>
    internal static TableSchema? ApplySetViewDefinition(Schema schema, SchemaSetViewDefinitionPayload payload)
    {
        if (schema.Views.TryGetValue(payload.ViewName, out ViewSchema? view))
            view.Definition = payload.Definition;

        return null;
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
            throw new CamusDBException(
                CamusDBErrorCodes.ConcurrentSchemaChange,
                $"The definition of materialized view '{live.Name}' changed while it was being refreshed, so the " +
                "rebuild was discarded rather than published over it. Retry the refresh.");

        // Contents may only move forward in time. Two runs that both got past the fence — a lease that
        // lapsed under a stalled node, say — would otherwise race at the swap, and the one that finished
        // last would win regardless of which read the newer source. Ordering by the source snapshot
        // makes the outcome depend on the data rather than on scheduling.
        if (payload.RefreshedAt is { } incoming && live.RefreshedAt is { } published
            && incoming < published)
            throw new CamusDBException(
                CamusDBErrorCodes.ConcurrentSchemaChange,
                $"A newer refresh of materialized view '{live.Name}' has already been published, so this " +
                "older rebuild was discarded rather than overwriting it.");

        TableSchema? built = SchemaDeltaApplier.FindRelationById(schema, payload.SwapToTableId);

        if (built is null)
            throw new CamusDBException(
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
