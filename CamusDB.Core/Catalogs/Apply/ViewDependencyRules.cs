/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Apply;

/// <summary>
/// The DDL guard of stored view bodies: a relation that a view or a materialized view reads must not
/// be dropped. The rule reads <see cref="ViewDefinition.DependsOnTableIds"/> through
/// <see cref="ViewDependencyGraph"/>, so a rename between the view's creation and the drop cannot hide
/// the edge.
///
/// <para><b>Two callers, like <see cref="ForeignKeyDependencyRules"/>.</b> The DDL service calls the
/// rule under the DDL semaphore, before the statement changes anything, so every entry point into
/// <c>DROP TABLE</c> (the SQL statement, the ticket API, and a statement forwarded from a follower)
/// meets it with the schema unchanged. The delta apply calls it again in log order on every node: a
/// view is created without the DDL semaphore and may be proposed from any node, so a view created
/// after the service's check and before the drop's delta still stops the drop, and every node reaches
/// the same answer.</para>
///
/// <para><c>DROP MATERIALIZED VIEW</c> has its own form of this rule with a <c>CASCADE</c> escape
/// hatch (<see cref="ViewDependencyMaintainer.RequireNoDependentViews"/>); it drops the dependents
/// first, so by the time its relation's drop delta is applied there is nothing left for this rule to
/// object to.</para>
/// </summary>
internal static class ViewDependencyRules
{
    /// <summary>
    /// Refuses to drop <paramref name="table"/> while a view or a materialized view reads it. Without
    /// this, the drop would turn every dependent into a delayed error for whoever reads it next, with
    /// nothing at the time of the drop to say so.
    /// </summary>
    internal static void RequireNotReadByViews(Schema schema, TableSchema table)
    {
        if (table.Id is null)
            return;

        List<string> dependents = ViewDependencyGraph.DirectDependentsOfTable(schema, table.Id);

        if (dependents.Count == 0)
            return;

        dependents.Sort(StringComparer.OrdinalIgnoreCase);

        throw new CamusDBException(
            CamusDBErrorCodes.DependentObjectsExist,
            $"Cannot drop table '{table.Name}' because other objects depend on it: " +
            $"{string.Join(", ", dependents)}. Drop them first.");
    }
}
