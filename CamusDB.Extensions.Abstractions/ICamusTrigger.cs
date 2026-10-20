/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Tasks;

namespace CamusDB.Extensions;

/// <summary>
/// A row trigger. <c>CREATE TRIGGER name ON table {BEFORE|AFTER} event[, event...] EXTERNAL NAME
/// 'Namespace.Type'</c> binds a public class that implements this interface to a table. The class
/// needs a public parameterless constructor.
///
/// <para><b>Firing.</b> A trigger fires once for each affected row, on the node that runs the
/// statement, inside the statement's transaction.</para>
/// <list type="bullet">
/// <item>A BEFORE trigger fires before the row is validated and written. In INSERT and UPDATE it
/// can change <see cref="ITriggerContext.New"/>, and the changed row must still pass NOT NULL,
/// length and CHECK rules. In BEFORE INSERT the row id does not exist yet.</item>
/// <item>An AFTER trigger fires after the whole statement wrote its rows, once per row, in
/// statement order. It sees the final rows and cannot change them.</item>
/// </list>
///
/// <para><b>Errors and rollback.</b> An exception from the trigger ends the statement, and nothing
/// the statement wrote persists. Inside an explicit transaction the whole transaction rolls back.
/// SQL that the trigger runs through <see cref="ITriggerContext.Sql"/> commits or rolls back with
/// the statement.</para>
///
/// <para><b>Retries.</b> The server can run a statement again (a schema change in progress, or a
/// serializable conflict in autocommit mode). Effects inside the transaction occur once. Effects
/// outside the database occur at least once.</para>
///
/// <para><b>What does not fire.</b> Engine-internal row work fires no trigger: dropping or
/// truncating a table, TTL expiry, column backfills and storage rewrites.</para>
///
/// <para><b>Threading.</b> The server creates one instance per node and shares it between
/// concurrent statements. Keep no per-statement state in fields.</para>
/// </summary>
public interface ICamusTrigger
{
    /// <summary>Handles one row event.</summary>
    /// <param name="context">The event, the OLD and NEW rows, the SQL session and the
    /// cancellation token.</param>
    Task FireAsync(ITriggerContext context);
}
