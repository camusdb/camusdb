
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Keeps stored view bodies valid across changes to the relations they read: refuses a drop that
/// would orphan one, and converts a body that still names its relations so a rename cannot strand it.
///
/// <para><b>Why a rename used to need anything at all.</b> A body written before relation ids were
/// stored names its sources in text, so renaming a table left that text pointing at something that
/// no longer existed. Keeping such a view working meant finding every dependent and editing its
/// body — and correctness then depended on that edit reaching all of them, which is a class of bug
/// this codebase has already paid for twice.</para>
///
/// <para>Bodies are now bound to immutable relation ids (see <see cref="StoredBodyBinder"/>), so a
/// rename is metadata-only and there is nothing to reach. What remains here is the bridge for the
/// older form: when a rename would have stranded a name-bound body, that body is rebound to ids and
/// rides the rename in the same replicated change. It converts exactly once, at the only moment it
/// could otherwise break, and is immune afterwards.</para>
/// </summary>
internal static class ViewDependencyMaintainer
{
    /// <summary>
    /// Refuses a drop that would leave a view reading a relation that no longer exists.
    /// </summary>
    /// <remarks>
    /// This makes <c>DROP TABLE</c> stricter than it was before views existed, which is deliberate
    /// and matches PostgreSQL: the alternative is a table drop that silently converts every dependent
    /// view into a delayed error for whoever reads it next. <c>CASCADE</c> is the escape hatch, and it
    /// is spelled out in the message.
    /// </remarks>
    public static void RequireNoDependentViews(Schema schema, string relationName, string relationId, bool cascade)
    {
        if (cascade)
            return;

        List<string> dependents = ViewDependencyGraph.DirectDependentsOfTable(schema, relationId);

        if (dependents.Count == 0)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.DependentObjectsExist,
            $"Cannot drop table '{relationName}' because other objects depend on it: " +
            $"{string.Join(", ", dependents)}. Use DROP TABLE ... CASCADE to drop them too.");
    }

    /// <summary>
    /// Refuses a column change that would leave a view reading a column that no longer exists, or
    /// that answers to a different name than the body asks for.
    /// </summary>
    /// <remarks>
    /// <para>Same reasoning as the table-drop rule above, applied one level down: without it a
    /// <c>DROP COLUMN</c> or <c>RENAME COLUMN</c> succeeds and the dependent view fails at its next
    /// read, with nothing at the time of the change to say so. There is no <c>CASCADE</c> form of
    /// either statement, so the escape hatch is to drop or replace the view first — which the
    /// message says.</para>
    ///
    /// <para><b>The two arms differ.</b> A drop is refused by any dependent, because the column is
    /// about to stop existing and no form of reference survives that. A rename is refused only by a
    /// dependent that still names the column in <em>text</em>: a body that refers to it by id is
    /// unaffected — the id does not move — so refusing on its behalf would block a change that
    /// cannot break it.</para>
    ///
    /// <para>Rests on <see cref="ViewDefinition.DependsOnColumnIds"/>, which is a lower bound: a
    /// view whose definition predates column analysis, or whose reference could not be resolved with
    /// certainty, is not protected here. That is the same exposure as before this check existed.</para>
    /// </remarks>
    /// <param name="renaming">
    /// True for <c>RENAME COLUMN</c>, which only bodies that still spell the column out can object
    /// to; false for <c>DROP COLUMN</c>, which every dependent objects to.
    /// </param>
    public static void RequireNoDependentViewsOnColumn(
        Schema schema, string tableName, string columnName, string columnId, bool renaming)
    {
        string operation = renaming ? "rename" : "drop";
        string token = StoredColumnRef.Format(columnId);

        List<string> dependents = [];

        foreach ((string viewName, ViewSchema view) in schema.Views)
        {
            if (Objects(view.Definition))
                dependents.Add(viewName);
        }

        // Materialized views keep their definition on the relation rather than in the view map, and
        // a body that would break on its next refresh has the same claim on the column as one that
        // would break on its next read.
        foreach ((string relationName, TableSchema relation) in schema.Tables)
        {
            if (relation.IsMaterializedView && Objects(relation.ViewDefinition))
                dependents.Add(relationName);
        }

        bool Objects(ViewDefinition? definition)
        {
            if (!Reads(definition, columnId))
                return false;

            // Bound to the id: a rename cannot reach it. A drop still can.
            return !renaming || definition!.Sql?.Contains(token, StringComparison.OrdinalIgnoreCase) != true;
        }

        if (dependents.Count == 0)
            return;

        dependents.Sort(StringComparer.OrdinalIgnoreCase);

        throw new CamusDBException(
            CamusDBErrorCodes.DependentObjectsExist,
            $"Cannot {operation} column '{columnName}' of table '{tableName}' because other objects " +
            $"depend on it: {string.Join(", ", dependents)}. Drop or replace them first.");

        static bool Reads(ViewDefinition? definition, string columnId) =>
            definition?.DependsOnColumnIds?.Contains(columnId, StringComparer.Ordinal) == true;
    }

    /// <summary>
    /// Builds the id-bound body of every view reading <paramref name="relationId"/> whose stored
    /// body still names its relations, so a rename can carry the conversion in its own delta.
    /// Returns null when there is nothing to convert — which is the steady state, and is what makes
    /// a rename metadata-only.
    /// </summary>
    /// <remarks>
    /// <para>Computed against one schema snapshot, while the relation still answers to its old name,
    /// and applied together with the rename as a single transition. Converting afterwards instead
    /// would leave a window in which the stored body names a relation that no longer exists — and
    /// because the old name is free the moment the rename commits, anything created under it during
    /// that window would make the body resolve to <em>that</em> relation and return its rows. A
    /// wrong answer, not an outage.</para>
    ///
    /// <para>Both edge kinds are consulted. A dependent records what it reads in
    /// <c>DependsOnTableIds</c> or in <c>DependsOnViewIds</c> depending on what that relation is, and
    /// reading only one list silently skips every view that reads a renamed <em>view</em> — a bug
    /// this code shipped once already.</para>
    ///
    /// <para>Idempotent: a body already bound to ids binds to itself, renders identically, and is
    /// skipped, so a replay or a partially-completed conversion re-runs harmlessly.</para>
    /// </remarks>
    public static Dictionary<string, ViewDefinition>? BuildRenameConversions(
        Schema schema,
        string relationId,
        Func<string, NodeAst> parse)
    {
        Dictionary<string, ViewDefinition>? rewrites = null;

        IEnumerable<string> dependents = ViewDependencyGraph
            .DirectDependentsOfTable(schema, relationId)
            .Concat(ViewDependencyGraph.DirectDependentsOfView(schema, relationId))
            .Distinct(StringComparer.OrdinalIgnoreCase);

        foreach (string viewName in dependents)
        {
            if (!schema.Views.TryGetValue(viewName, out ViewSchema? view) || view.Definition is null)
                continue;

            NodeAst body = parse(view.Definition.Sql);
            NodeAst rewritten = StoredBodyBinder.BindStoredForm(schema, body);

            // Same instance means every relation was already bound by id, so the rename cannot
            // disturb this body and there is nothing to carry.
            if (ReferenceEquals(rewritten, body))
                continue;

            rewrites ??= new(StringComparer.OrdinalIgnoreCase);
            rewrites[viewName] = new ViewDefinition
            {
                Sql = ViewBodyRenderer.RenderSelect(rewritten),
                Columns = view.Definition.Columns,
                DependsOnTableIds = view.Definition.DependsOnTableIds,
                DependsOnViewIds = view.Definition.DependsOnViewIds,
                // Carried like every other field: a conversion rewrites how the body names its
                // relations, not what it depends on, and dropping this would silently un-protect
                // every column the view reads.
                DependsOnColumnIds = view.Definition.DependsOnColumnIds,
                CheckOption = view.Definition.CheckOption,
                Owner = view.Definition.Owner,
                // Carried explicitly: dropping it would silently strip the view's ownership, and an
                // owner that no longer resolves fails every read of the view.
                OwnerId = view.Definition.OwnerId,
            };
        }

        return rewrites;
    }
}
