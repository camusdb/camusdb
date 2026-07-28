/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// The per-request authorization state consulted by the per-table privilege check in
/// <c>TableOpener.Open</c>: the authenticated <see cref="Principal"/> and the privilege the current
/// statement requires on each table it touches (its <c>MapRequiredPrivilege</c> result).
/// </summary>
public readonly record struct AuthorizationScope(Principal? Principal, Privilege? RequiredPrivilege);

/// <summary>
/// Ambient (per-async-flow) carrier for the current request's <see cref="AuthorizationScope"/>.
///
/// <para>The engine's table-access chokepoint (<c>TableOpener.Open</c>) is reached through many layers
/// — the query binder, every subquery/semi-join executor, DML, DDL — none of which thread request
/// context. Rather than add a principal/access-kind parameter to all of them, the entry points
/// (<c>ExecuteSQLQuery</c>/<c>ExecuteNonSQLQuery</c>/<c>ExecuteDDLSQL</c>) set this
/// <see cref="AsyncLocal{T}"/> once. An <see cref="AsyncLocal{T}"/> value flows <b>down</b> into
/// awaited callees but is copy-on-write, so a value set inside one request's flow is invisible both to
/// that request's caller and to any concurrently-executing request on the singleton executor — exactly
/// the request isolation needed.</para>
/// </summary>
public static class AuthorizationContext
{
    private static readonly AsyncLocal<AuthorizationScope> current = new();

    /// <summary>The authorization scope for the current async flow (default = no principal, no requirement).</summary>
    public static AuthorizationScope Current
    {
        get => current.Value;
        set => current.Value = value;
    }
}
