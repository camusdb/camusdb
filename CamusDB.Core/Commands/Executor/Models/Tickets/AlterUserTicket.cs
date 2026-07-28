/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Ticket for <c>ALTER USER u IDENTIFIED [WITH plugin] BY secret</c> — rotate a user's password. A
/// password clause is always required (unlike <see cref="CreateUserTicket"/>). <see cref="Password"/>
/// is the cleartext, carried no further than the executor, which hashes it and drops it.
/// </summary>
public readonly struct AlterUserTicket
{
    public string UserName { get; }

    public string Plugin { get; }

    public string Password { get; }

    public AlterUserTicket(string userName, string plugin, string password)
    {
        UserName = userName;
        Plugin = plugin;
        Password = password;
    }
}
