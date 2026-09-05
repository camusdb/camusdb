/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Ticket for <c>ALTER USER u IDENTIFIED [WITH plugin] BY secret [REPLACE current]</c> — rotate a
/// user's password. A password clause is always required (unlike <see cref="CreateUserTicket"/>).
/// <see cref="Password"/> is the cleartext, carried no further than the executor, which hashes it and
/// drops it.
/// </summary>
public readonly struct AlterUserTicket
{
    public string UserName { get; }

    public string Plugin { get; }

    public string Password { get; }

    /// <summary>
    /// The cleartext password the account holds now, from the <c>REPLACE</c> clause, or null when the
    /// statement carried none.
    ///
    /// <para>Present means the executor must verify it before the rotation applies. Null does
    /// <b>not</b> mean the check is optional: the statement-level gate decides whether the clause was
    /// required and refuses the statement when it is missing, so a null reaching the executor is either
    /// a superuser resetting another account or a node with authentication switched off. The executor
    /// verifies what it is given and never decides who may omit it — that decision needs the caller's
    /// identity, which the ticket does not carry.</para>
    /// </summary>
    public string? CurrentPassword { get; }

    public AlterUserTicket(string userName, string plugin, string password, string? currentPassword = null)
    {
        UserName = userName;
        Plugin = plugin;
        Password = password;
        CurrentPassword = currentPassword;
    }
}
