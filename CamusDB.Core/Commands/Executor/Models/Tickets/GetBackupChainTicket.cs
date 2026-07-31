/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Ticket for resolving and validating the backup chain ending at <see cref="LeafBackupId"/>. A
/// privileged server operation — the executor requires a superuser <see cref="Principal"/> when
/// authentication is on.
/// </summary>
public readonly struct GetBackupChainTicket
{
    public Guid LeafBackupId { get; }

    /// <summary>The authenticated caller. Null when authentication is disabled; otherwise must be a superuser.</summary>
    public Principal? Principal { get; }

    public GetBackupChainTicket(Guid leafBackupId, Principal? principal = null)
    {
        LeafBackupId = leafBackupId;
        Principal = principal;
    }
}
