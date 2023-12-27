
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

internal sealed class DropDatabaseValidator : ValidatorBase
{
    public void Validate(DropDatabaseTicket ticket)
    {
        if (string.IsNullOrWhiteSpace(ticket.DatabaseName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Database name is required"
            );       
    }
}