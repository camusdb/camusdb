/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// The <c>CASCADE</c> half of <c>DROP VIEW</c> and <c>DROP MATERIALIZED VIEW</c>: removes every view
/// and materialized view that reads a relation, transitively, before the relation itself goes.
///
/// <para><b>Both kinds of dependent, through both kinds of edge.</b> A plain view may read a
/// materialized view and a materialized view may read a plain view, so a chain alternates between the
/// two maps. Each cascade used to drop only the kind it was named after: <c>DROP VIEW … CASCADE</c>
/// skipped a materialized view that read the view, leaving it with a body that names something gone,
/// and <c>DROP MATERIALIZED VIEW … CASCADE</c> dropped only direct plain-view dependents, so a view over
/// a view over the materialized view was left behind. Each dependent is now dropped by the path that
/// owns its kind — a materialized view under its refresh fence.</para>
///
/// <para><b>Authorized first, dropped after.</b> <c>CASCADE</c> removes objects the caller did not
/// name, so each is checked on its own. All of them are checked before any is dropped, so a refusal
/// half-way down the chain cannot leave the chain half-removed.</para>
///
/// <para><b>Dependents first.</b> The order comes from
/// <see cref="ViewDependencyGraph.TransitiveDependentsOfView"/>, which lists every object after
/// everything that reads it, so no intermediate state — on this node or any other applying the log —
/// contains a view whose source is already gone.</para>
/// </summary>
internal static class DependentRelationDropper
{
    internal static async Task DropTransitiveDependentsAsync(
        CommandExecutor executor,
        CatalogsManager catalogs,
        DatabaseRegistry registry,
        DatabaseDescriptor database,
        string relationId,
        ILogger<ICamusDB> logger)
    {
        List<(string Name, string Id, bool Materialized)> ordered = [];

        foreach (string dependent in ViewDependencyGraph.TransitiveDependentsOfView(database.Schema, relationId))
        {
            if (database.Schema.Views.TryGetValue(dependent, out ViewSchema? view))
            {
                ViewAuthorization.Require(database, dependent, view, Privilege.Drop);
                ordered.Add((dependent, view.Id!, false));
                continue;
            }

            if (database.Schema.Tables.TryGetValue(dependent, out TableSchema? relation) && relation.IsMaterializedView)
            {
                MaterializedViewRefresher.RequireTargetPrivilege(database, dependent, relation);
                ordered.Add((dependent, relation.Id!, true));
            }
        }

        foreach ((string name, string id, bool materialized) in ordered)
        {
            if (materialized)
            {
                await MaterializedViewCreator.DropFencedAsync(
                    executor, catalogs, registry, database, name, id, logger).ConfigureAwait(false);
                continue;
            }

            // Already gone is fine: an earlier step of this same cascade, or a concurrent drop, got
            // there first, and the outcome the caller asked for holds either way.
            if (CatalogsManager.ViewExists(database, name))
                await catalogs.DropViewAsync(database, name, expectedViewId: id).ConfigureAwait(false);
        }
    }
}
