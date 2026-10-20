/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Tasks;

namespace CamusDB.Extensions;

/// <summary>
/// A stored procedure. Mark the implementing class with <see cref="CamusProcedureAttribute"/>.
///
/// <para><b>Transactions.</b> The procedure runs inside the caller's transaction. SQL that it runs
/// through <see cref="IProcedureContext.Sql"/> commits or rolls back with the <c>CALL</c>. An
/// exception from the procedure rolls back everything the <c>CALL</c> wrote.</para>
///
/// <para><b>Retries.</b> The server can run a whole statement again, for example after a
/// serializable conflict in autocommit mode. Effects inside the transaction then occur once,
/// because the failed attempt rolls back. Effects outside the database (a network call, a file)
/// occur at least once.</para>
///
/// <para>The contract uses <see cref="Task"/> rather than <c>ValueTask</c> so that it has no
/// package dependency on netstandard2.0. Return <see cref="Task.CompletedTask"/> from a
/// procedure that completes synchronously.</para>
/// </summary>
public interface ICamusProcedure
{
    /// <summary>Runs the procedure.</summary>
    /// <param name="context">The arguments, the SQL session over the caller's transaction, and
    /// the cancellation token.</param>
    Task ExecuteAsync(IProcedureContext context);
}
