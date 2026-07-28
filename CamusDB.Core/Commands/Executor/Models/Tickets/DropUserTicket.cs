/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Ticket for <c>DROP USER [IF EXISTS] u</c>. Removing the user removes all its grants in the same
/// catalog transaction. <see cref="IfExists"/> makes an unknown user a no-op rather than an error.
/// </summary>
public readonly struct DropUserTicket
{
    public string UserName { get; }

    public bool IfExists { get; }

    public DropUserTicket(string userName, bool ifExists)
    {
        UserName = userName;
        IfExists = ifExists;
    }
}
