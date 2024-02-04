
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

internal sealed class UpdateValidator : ValidatorBase
{
    public void Validate(UpdateTicket ticket)
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

        if (ticket.PlainValues is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Values are required"
            );

        foreach (KeyValuePair<string, ColumnValue> columnValue in ticket.PlainValues)
        {
            switch (columnValue.Value.Type)
            {
                case ColumnType.Id: // @todo validate alphanumeric digits
                    if (!string.IsNullOrEmpty(columnValue.Value.StrValue) && columnValue.Value.StrValue.Length != 24)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInput,
                            $"Invalid id value for field '{columnValue.Key}'"
                        );
                    break;                
            }
        }
    }
}
