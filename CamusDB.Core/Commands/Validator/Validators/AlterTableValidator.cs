
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

internal sealed class AlterTableValidator : ValidatorBase
{
    public void Validate(AlterTableTicket ticket)
    {
        if (string.IsNullOrWhiteSpace(ticket.DatabaseName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Database name is required"
            );

        if (string.IsNullOrWhiteSpace(ticket.TableName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Table name is required"
            );

        if (string.IsNullOrWhiteSpace(ticket.Column.Name))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Column name is required"
            );

        if (ticket.Column.Name.Length > 255)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Column name is too long"
            );

        if (!HasValidCharacters(ticket.Column.Name))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Column name has invalid characters"
            );
    }
}
