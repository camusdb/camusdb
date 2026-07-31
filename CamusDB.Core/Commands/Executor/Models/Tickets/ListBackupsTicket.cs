/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Ticket for listing every backup in the node's catalog. A privileged server operation — the executor
/// requires a superuser <see cref="Principal"/> when authentication is on.
/// </summary>
public readonly struct ListBackupsTicket
{
    /// <summary>The authenticated caller. Null when authentication is disabled; otherwise must be a superuser.</summary>
    public Principal? Principal { get; }

    public ListBackupsTicket(Principal? principal = null)
    {
        Principal = principal;
    }
}
