/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

/// <summary>
/// Shape/bounds checks for <c>CREATE USER</c> — a valid identifier, a supported auth plugin, and a
/// password within the byte cap. Existence (already-exists) is decided at execution time against the
/// auth catalog, not here.
/// </summary>
internal sealed class CreateUserValidator : ValidatorBase
{
    public CreateUserValidator(CamusDBOptions options) : base(options) { }

    public void Validate(CreateUserTicket ticket)
    {
        ValidateIdentifier(ticket.UserName, "User");

        if (ticket.Password is not null)
            AuthClauseValidator.Validate(ticket.Plugin, ticket.Password);
    }
}

/// <summary>
/// Shared validation for the <c>IDENTIFIED [WITH plugin] BY secret</c> clause: only
/// <c>sha256_password</c> is accepted, and the password may not exceed
/// <see cref="CamusDBConstants.MaxPasswordBytes"/> UTF-8 bytes (caps per-hash work).
/// </summary>
internal static class AuthClauseValidator
{
    public const string Sha256Password = "sha256_password";

    public static void Validate(string? plugin, string password)
    {
        if (plugin is not null && plugin != Sha256Password)
            throw new CamusDBException(
                CamusDBErrorCodes.UnsupportedAuthPlugin,
                $"Unsupported authentication plugin '{plugin}'; only '{Sha256Password}' is supported");

        if (Encoding.UTF8.GetByteCount(password) > CamusDBConstants.MaxPasswordBytes)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Password exceeds the maximum of {CamusDBConstants.MaxPasswordBytes} bytes");
    }
}
