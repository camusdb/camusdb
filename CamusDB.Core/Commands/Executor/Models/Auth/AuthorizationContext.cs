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
///
/// <para><b>With authentication on, a principal with a null <see cref="RequiredPrivilege"/> is refused,
/// not waved through.</b> A null requirement means an entry point forgot to say what the access needs,
/// and treating that as "no check" once turned two whole transports into full bypasses. Work that
/// genuinely must open a table no grant can name says so explicitly with
/// <see cref="TableCheckSuspended"/>, through <see cref="AuthorizationContext.SuspendTableCheck"/>.</para>
///
/// <para>A scope with no principal is engine work — a background sweep, a replicated apply, a peer's
/// forwarded request — and is not checked. A user request with authentication on always carries a
/// principal: every transport refuses a request without a valid token before the engine runs.</para>
/// </summary>
public readonly record struct AuthorizationScope(Principal? Principal, Privilege? RequiredPrivilege)
{
    /// <summary>
    /// True while the running statement opens a relation it created itself, or opens a table only to
    /// resolve its id. See <see cref="AuthorizationContext.SuspendTableCheck"/> for the exact cases.
    /// The statement's authority was already checked before this was set.
    /// </summary>
    public bool TableCheckSuspended { get; init; }
}

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

    /// <summary>
    /// Temporarily swaps the required privilege, keeping the same principal, until the returned
    /// value is disposed.
    ///
    /// <para>A statement that both reads and writes needs two different privileges on two different
    /// sets of tables — <c>INSERT … SELECT</c> requires Insert on its target but only Select on its
    /// sources. Since the per-table check reads this ambient scope at
    /// <c>TableOpener.Open</c>, the caller narrows the requirement around the phase that resolves
    /// the source tables, and every source — including join and subquery sources it never names
    /// itself — is then checked for Select rather than for the statement's write privilege.</para>
    /// </summary>
    ///
    /// <para>The parameter is not nullable on purpose: a null requirement is refused by the per-table
    /// check, so it can never mean "skip". Narrowing also ends any suspension in effect, because a
    /// phase that names a privilege is reading tables a user named.</para>
    public static PrivilegeSwap WithRequiredPrivilege(Privilege required) =>
        new(Current with { RequiredPrivilege = required, TableCheckSuspended = false });

    /// <summary>
    /// Suspends the per-table privilege check, keeping the same principal, until the returned value is
    /// disposed.
    ///
    /// <para>Only for two kinds of access, and only after the statement's own authority was checked:
    /// a relation the running statement itself creates, fills or discards, which no grant can name
    /// (the relation a materialized-view statement builds, or its staging relation); and an open that
    /// only resolves a table's id and reads no row (<c>GRANT … ON db.table</c>, already gated to user
    /// administrators). Reading a table a user named inside the suspension would bypass its grants, so
    /// any such phase narrows again with <see cref="WithRequiredPrivilege"/>.</para>
    /// </summary>
    public static PrivilegeSwap SuspendTableCheck() =>
        new(Current with { RequiredPrivilege = null, TableCheckSuspended = true });

    /// <summary>Restores the previous <see cref="AuthorizationScope"/> when disposed.</summary>
    public readonly struct PrivilegeSwap : IDisposable
    {
        private readonly AuthorizationScope previous;

        internal PrivilegeSwap(AuthorizationScope next)
        {
            previous = Current;
            Current = next;
        }

        public void Dispose() => Current = previous;
    }
}
