/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

/// <summary>Shape check for <c>DROP USER [IF EXISTS]</c>. Existence is decided at execution time.</summary>
internal sealed class DropUserValidator : ValidatorBase
{
    public DropUserValidator(CamusDBOptions options) : base(options) { }

    public void Validate(DropUserTicket ticket)
    {
        ValidateIdentifier(ticket.UserName, "User");
    }
}
