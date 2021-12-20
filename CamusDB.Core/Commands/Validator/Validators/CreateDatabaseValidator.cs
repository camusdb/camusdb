
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
        if (string.IsNullOrWhiteSpace(ticket.DatabaseName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Database name is required"
            );

        if (ticket.DatabaseName.Length > 255)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Database name is too long"
            );

        if (!HasValidCharacters(ticket.DatabaseName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Database name has invalid characters"
            );
    }
}
