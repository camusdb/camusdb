
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// The per-object privilege check for a <b>plain view</b>.
///
/// <para>Every other relation is checked at one chokepoint — <c>TableOpener.Open</c> — because every
/// path that touches one has to open it. A view has no such moment. A read expands it into a derived
/// table <em>before</em> binding, so by the time anything opens a relation the view's name is gone;
/// and <c>DROP</c>, <c>ALTER</c>, <c>SHOW CREATE</c> and <c>SHOW COLUMNS</c> read
/// <see cref="Schema.Views"/> directly and never open anything at all. Mapping those statements to a
/// privilege therefore enforced nothing: with no opener to consume the ambient requirement, the check
/// simply never ran, and a caller with no grant on a view could read it, describe it, and drop it.</para>
///
/// <para>So views are checked here instead, explicitly, at each path that names one. This is a
/// caller's-rights check on the view object only. It is <b>not</b> the owner's-rights model: the
/// view's base tables are still checked against the caller, so a view cannot yet be used to grant
/// access to a table the caller cannot otherwise read.</para>
/// </summary>
internal static class ViewAuthorization
{
    /// <summary>
    /// Throws unless the caller holds <paramref name="required"/> on the view.
    /// </summary>
    /// <remarks>
    /// Grants are keyed by immutable relation id, and a view's id comes from the same sequence tables
    /// use, so a view is grantable exactly like a table and a dropped-and-recreated view inherits
    /// nothing from its predecessor.
    /// </remarks>
    internal static void Require(
        DatabaseDescriptor database, string viewName, ViewSchema view, Privilege required)
    {
        if (!database.Options.AuthenticationEnabled)
            return;

        Principal? principal = AuthorizationContext.Current.Principal;

        if (principal is not null && !principal.HasPrivilege(required, database.Id, view.Id))
            throw new CamusDBException(
                CamusDBErrorCodes.InsufficientPrivilege,
                $"Missing {required} privilege on view '{database.Name}.{viewName}'");
    }

    /// <summary>
    /// Checks the ambient requirement the statement was mapped to, for paths where that mapping is
    /// already the right answer (a read expands the view, a drop is mapped to Drop, and so on).
    /// </summary>
    internal static void RequireAmbient(DatabaseDescriptor database, string viewName, ViewSchema view)
    {
        if (AuthorizationContext.Current.RequiredPrivilege is { } required)
            Require(database, viewName, view, required);
    }

    /// <summary>
    /// True when <paramref name="principal"/> may know the view exists at all.
    /// </summary>
    /// <remarks>
    /// Listings omit what the caller cannot touch rather than refusing the statement, matching
    /// <c>SHOW TABLES</c>: revealing a name the caller has no access to is a disclosure in itself, and
    /// erroring instead of omitting would reveal it just as effectively.
    /// </remarks>
    internal static bool IsVisible(DatabaseDescriptor database, ViewSchema view, Principal? principal)
        => principal is null || principal.HasAnyPrivilege(database.Id, view.Id);
}
