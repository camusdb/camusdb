/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// The authenticated identity and authorization snapshot for one request, resolved by the transport
/// from a bearer token and threaded into <c>CommandExecutor</c> via the ticket. Immutable for the
/// lifetime of the request.
///
/// <para>Enforcement is centralized in <see cref="HasPrivilege"/>: a superuser passes everything;
/// otherwise a grant satisfies a check when it holds the required privilege at the requested scope or
/// any broader one (global ⊃ database ⊃ table). Matching is by immutable id, never by name, so a
/// dropped-and-recreated object cannot inherit an old grant.</para>
/// </summary>
public sealed class Principal
{
    public string UserName { get; }

    public bool IsSuperuser { get; }

    private readonly IReadOnlyList<GrantRecord> grants;

    public Principal(string userName, bool isSuperuser, IReadOnlyList<GrantRecord> grants)
    {
        UserName = userName;
        IsSuperuser = isSuperuser;
        this.grants = grants;
    }

    /// <summary>
    /// True when this principal may perform <paramref name="required"/> on the object identified by
    /// <paramref name="databaseId"/> / <paramref name="tableId"/>. A superuser always passes. Otherwise
    /// a grant passes when its mask includes <paramref name="required"/> at a scope that covers the
    /// object: a global (<c>*.*</c>) grant covers everything, a database grant covers any table in that
    /// database, and a table grant covers only that table.
    /// </summary>
    public bool HasPrivilege(Privilege required, string databaseId, string? tableId)
    {
        if (IsSuperuser)
            return true;

        foreach (GrantRecord grant in grants)
        {
            if ((grant.Privileges & required) != required)
                continue;

            switch (grant.Scope.Kind)
            {
                case GrantScopeKind.Global:
                    return true;

                case GrantScopeKind.Database:
                    if (grant.Scope.DatabaseId == databaseId)
                        return true;
                    break;

                case GrantScopeKind.Table:
                    if (tableId is not null && grant.Scope.DatabaseId == databaseId && grant.Scope.TableId == tableId)
                        return true;
                    break;
            }
        }

        return false;
    }

    /// <summary>True when this principal may administer users and grants (superuser only, for now).</summary>
    public bool CanAdministerUsers => IsSuperuser;

    /// <summary>
    /// True when this principal holds <b>at least one</b> privilege on the object identified by
    /// <paramref name="databaseId"/> / <paramref name="tableId"/>. This is the visibility test used by
    /// the catalog-listing surfaces (<c>SHOW TABLES</c>): a name is listed when the caller could do
    /// *something* with the object, which is weaker than the access checks in
    /// <see cref="HasPrivilege"/> — being able to see that a table exists does not imply being able to
    /// read it.
    ///
    /// <para>Scope covering follows <see cref="HasPrivilege"/>: a global grant makes everything
    /// visible, a database grant makes every table in that database visible. A null
    /// <paramref name="tableId"/> therefore only matches global/database grants, so an object whose id
    /// is unknown is listed only when the caller has database-wide (or broader) access.</para>
    /// </summary>
    public bool HasAnyPrivilege(string databaseId, string? tableId)
    {
        if (IsSuperuser)
            return true;

        foreach (GrantRecord grant in grants)
        {
            if (grant.Privileges == Privilege.None)
                continue;

            switch (grant.Scope.Kind)
            {
                case GrantScopeKind.Global:
                    return true;

                case GrantScopeKind.Database:
                    if (grant.Scope.DatabaseId == databaseId)
                        return true;
                    break;

                case GrantScopeKind.Table:
                    if (tableId is not null && grant.Scope.DatabaseId == databaseId && grant.Scope.TableId == tableId)
                        return true;
                    break;
            }
        }

        return false;
    }

    /// <summary>
    /// True when this principal should see <paramref name="databaseId"/> in <c>SHOW DATABASES</c>.
    ///
    /// <para>Unlike <see cref="HasAnyPrivilege"/> this deliberately looks *upward* as well as
    /// downward: a grant on a single table inside the database makes the database itself visible,
    /// because a caller who may touch <c>db.t</c> has to be able to discover <c>db</c> to reach it.
    /// Only the database's identity is revealed — which tables it contains is still filtered
    /// per-table by <c>SHOW TABLES</c>.</para>
    /// </summary>
    public bool CanSeeDatabase(string databaseId)
    {
        if (IsSuperuser)
            return true;

        foreach (GrantRecord grant in grants)
        {
            if (grant.Privileges == Privilege.None)
                continue;

            if (grant.Scope.Kind == GrantScopeKind.Global)
                return true;

            // Database- and table-scoped grants both name their database, and either one is enough
            // to make that database discoverable.
            if (grant.Scope.DatabaseId == databaseId)
                return true;
        }

        return false;
    }
}
