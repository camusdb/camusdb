
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Names the short-lived relation a materialized-view refresh builds into before it takes over the
/// view's name.
///
/// <para>The staging relation is a real, registered relation — that is the whole point, because it
/// is what gives the rebuild ordinary index maintenance, ordinary transactions, and ordinary
/// deferred-drop cleanup when a refresh is abandoned. Being registered means it needs a name, and
/// that name must be one no user can collide with: it is generated, it appears in no statement, and
/// a table that shadowed it would be silently overwritten by the next refresh. So it is built from
/// characters an identifier cannot contain — the SQL lexer accepts only <c>[A-Za-z_][A-Za-z0-9_]*</c>
/// and the ticket validator rejects everything outside that set, so no <c>CREATE TABLE</c> through
/// any entry point can produce one of these names.</para>
///
/// <para>The name embeds <b>both</b> ids on purpose. The view's id is what makes abandoned staging
/// relations for one view findable without touching another view's in-flight rebuild; the staging
/// relation's own id is what keeps two attempts distinct, so a retry never inherits the half-built
/// contents of the attempt before it.</para>
/// </summary>
internal static class MaterializedViewNaming
{
    /// <summary>Prefix shared by every staging relation, for any materialized view.</summary>
    internal const string StagingPrefix = "~mv:";

    /// <summary>The name a refresh of <paramref name="viewTableId"/> stages into.</summary>
    internal static string StagingRelationName(string viewTableId, string stagingTableId)
        => $"{StagingPrefix}{viewTableId}:{stagingTableId}";

    /// <summary>Matches every staging relation belonging to one materialized view.</summary>
    internal static string StagingPrefixFor(string viewTableId)
        => $"{StagingPrefix}{viewTableId}:";

    /// <summary>
    /// True for a relation that only exists mid-refresh. Every catalog listing must exclude these:
    /// they are engine bookkeeping with a name no client can act on, and reporting one as a table
    /// would invite a user to query or drop something that is about to stop existing.
    /// </summary>
    internal static bool IsStagingRelation(string relationName)
        => relationName.StartsWith(StagingPrefix, StringComparison.Ordinal);
}
