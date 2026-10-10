
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Auth;

/// <summary>
/// The one rule for a database the caller may not know about: it is reported exactly as a name that
/// is not registered at all.
///
/// <para><b>Why the two answers must be identical.</b> Any difference between "this database does not
/// exist" and "you may not use this database" — a different error code, a different message, even a
/// different casing of the name inside the message — tells a caller with no grant that the name is
/// registered, so every statement that names a database becomes an existence oracle. That is why
/// every refusal of an invisible database is built here, from the name the caller sent, and why the
/// not-registered path builds its error here too: two copies of the message would drift.</para>
///
/// <para><b>What "visible" means.</b> <see cref="Principal.CanSeeDatabase"/>: a superuser, a global
/// grant, or any grant that names the database, including a grant on one table inside it. It is the
/// same test <c>SHOW DATABASES</c> filters with, so a database is hidden from exactly the callers
/// that listing hides it from. Whether a visible database also lets the caller run a given statement
/// is a separate, later check, which may still refuse with <c>CADB0517</c>.</para>
/// </summary>
internal static class DatabaseVisibility
{
    /// <summary>
    /// The error for a database that is not registered, or that the caller may not see. Always pass
    /// the name as the caller sent it, never a registry or descriptor name, which can differ in case.
    /// </summary>
    internal static CamusDBException DoesNotExist(string requestedName)
        => new(CamusDBErrorCodes.DatabaseDoesntExist, $"Database '{requestedName}' does not exist");

    /// <summary>
    /// True when <paramref name="principal"/> must be told that <paramref name="databaseId"/> does not
    /// exist. A null principal is engine work or authentication off, and sees everything.
    /// </summary>
    internal static bool IsHiddenFrom(Principal? principal, string databaseId)
        => principal is not null && !principal.CanSeeDatabase(databaseId);
}
