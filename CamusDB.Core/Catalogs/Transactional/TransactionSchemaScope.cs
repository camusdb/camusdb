
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.Catalogs.Transactional;

/// <summary>
/// Names, for the current async flow, the explicit transaction whose statement is running, so that
/// <see cref="DatabaseDescriptor.Schema"/> and the other schema-shaped members of the descriptor answer
/// from that transaction's private schema (<see cref="TransactionSchemaOverlay"/>) instead of the shared
/// one.
///
/// <para><b>Why a flow value and not a parameter.</b> More than a hundred lookups of
/// <c>database.Schema</c> sit on statement paths: the binder, the planner, the table opener, view
/// expansion, the authorizer, the constraint checks and every DDL builder. A transaction that created a
/// table must find it through all of them. Redirecting the one member they all read covers them in one
/// place; threading a parameter through each would leave the next new lookup reading the shared schema
/// and seeing nothing.</para>
///
/// <para><b>Set it only in the async method that owns the statement.</b> An assignment to an async-local
/// value inside an async method is undone when that method returns, so the scope lasts exactly as long
/// as the statement. An assignment in a synchronous helper would stay in its caller's flow after the
/// statement ends. A cursor that the caller pulls later re-enters the scope for each row
/// (<see cref="TransactionSchemaScopedCursor"/>).</para>
///
/// <para><b>Only an open overlay redirects.</b> A flow that outlives its statement, such as a background
/// task forked from it, still names the transaction, but once the transaction ends its overlay is
/// closed and every lookup falls back to the shared schema. The check costs one volatile read while no
/// overlay is open anywhere in the process.</para>
///
/// <para><b>Schema-log apply and the commit of a change set run with the scope cleared</b>
/// (<see cref="Suppress"/>): they change the shared schema and must never see a private one.</para>
/// </summary>
internal static class TransactionSchemaScope
{
    private static readonly AsyncLocal<KvTransaction?> current = new();

    // The number of overlays open in the process. While it is zero, no lookup reads the async-local
    // value at all.
    private static int openOverlays;

    internal static void OverlayOpened() => Interlocked.Increment(ref openOverlays);

    internal static void OverlayClosed() => Interlocked.Decrement(ref openOverlays);

    /// <summary>True while at least one overlay is open anywhere in the process.</summary>
    internal static bool AnyOpen => Volatile.Read(ref openOverlays) > 0;

    /// <summary>
    /// Names <paramref name="tx"/> as the transaction of the current flow, or clears the name when it has
    /// no schema changes. Call it at the top of the async method that runs a statement. Also clears a
    /// name that an earlier statement's cursor left in the flow, so a statement of another transaction
    /// never reads a private schema.
    /// </summary>
    internal static void EnterStatement(KvTransaction? tx)
    {
        KvTransaction? named = tx?.SchemaChanges is TransactionSchemaOverlay { IsOpen: true } ? tx : null;

        if (named is null && current.Value is null)
            return;

        current.Value = named;
    }

    /// <summary>
    /// Names <paramref name="tx"/> as the transaction of the current flow whether or not it has an
    /// overlay yet. The DDL dispatcher calls it before the first schema change of the transaction
    /// creates the overlay, so the rest of that statement already reads it.
    /// </summary>
    internal static void Enter(KvTransaction tx) => current.Value = tx;

    /// <summary>
    /// Clears the name for the current flow. Same placement rule as <see cref="EnterStatement"/>: call it
    /// at the top of an async method, which restores the caller's value when it returns.
    /// </summary>
    internal static void Suppress()
    {
        if (current.Value is not null)
            current.Value = null;
    }

    /// <summary>The transaction named for the current flow, or null.</summary>
    internal static KvTransaction? CurrentTransaction => current.Value;

    /// <summary>
    /// Sets the name and returns the previous one, for a synchronous caller that must restore it before
    /// it returns (<see cref="TransactionSchemaScopedCursor"/>, the pin check at commit).
    /// </summary>
    internal static KvTransaction? Swap(KvTransaction? tx)
    {
        KvTransaction? previous = current.Value;

        if (!ReferenceEquals(previous, tx))
            current.Value = tx;

        return previous;
    }

    /// <summary>
    /// The open overlay of the current flow's transaction for <paramref name="database"/>, or null. Matched
    /// by database id, so a descriptor that was evicted and opened again between two statements of the
    /// transaction still finds it.
    /// </summary>
    internal static TransactionSchemaOverlay? OverlayFor(DatabaseDescriptor database)
    {
        if (Volatile.Read(ref openOverlays) == 0)
            return null;

        if (current.Value?.SchemaChanges is not TransactionSchemaOverlay overlay)
            return null;

        return overlay.IsOpen && string.Equals(overlay.DatabaseId, database.Id, StringComparison.Ordinal)
            ? overlay
            : null;
    }
}
