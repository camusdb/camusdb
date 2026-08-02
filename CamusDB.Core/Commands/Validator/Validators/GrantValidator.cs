/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

/// <summary>
/// Shape/bounds checks for <c>GRANT</c> / <c>REVOKE</c>: a valid user identifier, a non-empty privilege
/// set, and valid database/table identifiers for the scoped forms. Whether the user and object exist
/// is decided at execution time (the object is resolved to an immutable id there).
/// </summary>
internal sealed class GrantValidator : ValidatorBase
{
    public GrantValidator(CamusDBOptions options) : base(options) { }

    public void Validate(GrantTicket ticket)
    {
        ValidateIdentifier(ticket.UserName, "User");

        if (ticket.Privileges == Privilege.None)
            throw new CamusDBException(CamusDBErrorCodes.InvalidPrivilege, "No privileges specified");

        if (ticket.ScopeKind is GrantScopeKind.Database or GrantScopeKind.Table)
            ValidateIdentifier(ticket.DatabaseName, "Database");

        if (ticket.ScopeKind == GrantScopeKind.Table)
            ValidateIdentifier(ticket.TableName, "Table");
    }
}
