/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

/// <summary>Shape/bounds checks for <c>ALTER USER … IDENTIFIED …</c> (password rotation).</summary>
internal sealed class AlterUserValidator : ValidatorBase
{
    public AlterUserValidator(CamusDBOptions options) : base(options) { }

    public void Validate(AlterUserTicket ticket)
    {
        ValidateIdentifier(ticket.UserName, "User");
        AuthClauseValidator.Validate(ticket.Plugin, ticket.Password);
    }
}
