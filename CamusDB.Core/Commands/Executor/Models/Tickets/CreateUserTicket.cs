/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Ticket for <c>CREATE USER u [IF NOT EXISTS] [IDENTIFIED [WITH plugin] BY secret]</c>.
///
/// <para><see cref="Password"/> is <c>null</c> when the statement had no auth clause — the user is
/// created without a password and cannot authenticate. When present it is the <b>cleartext</b>
/// password, carried no further than the executor, which hashes it immediately (see
/// <see cref="Auth.PasswordHasher"/>) and never persists or logs it. <see cref="Plugin"/> is the
/// authentication plugin name (defaulted to <c>sha256_password</c> for the <c>IDENTIFIED BY</c> form);
/// it is null exactly when <see cref="Password"/> is null.</para>
/// </summary>
public readonly struct CreateUserTicket
{
    public string UserName { get; }

    public bool IfNotExists { get; }

    public string? Plugin { get; }

    public string? Password { get; }

    public CreateUserTicket(string userName, bool ifNotExists, string? plugin, string? password)
    {
        UserName = userName;
        IfNotExists = ifNotExists;
        Plugin = plugin;
        Password = password;
    }
}
