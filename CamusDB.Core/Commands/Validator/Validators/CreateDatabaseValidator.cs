
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.RegularExpressions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

internal sealed class CreateDatabaseValidator : ValidatorBase
{
    public void Validate(CreateDatabaseTicket ticket)
    {
        ValidateIdentifier(ticket.DatabaseName, "Database");

        if (ticket.BranchFrom is not null)
            ValidateIdentifier(ticket.BranchFrom, "Source database");
    }
}
