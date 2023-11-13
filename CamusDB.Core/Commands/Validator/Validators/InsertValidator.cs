
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

internal sealed class InsertValidator : ValidatorBase
{
    public void Validate(InsertTicket ticket)
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

        if (ticket.Values is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Values are required"
            );

        foreach (KeyValuePair<string, ColumnValue> columnValue in ticket.Values)
        {
            switch (columnValue.Value.Type)
            {
                case ColumnType.Id: // @todo validate alphanumeric digits
                    if (!string.IsNullOrEmpty(columnValue.Value.Value) && columnValue.Value.Value.Length != 24)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInput,
                            "Invalid id value for field '" + columnValue.Key + "'"
                        );
                    break;

                case ColumnType.Integer64:
                    if (!long.TryParse(columnValue.Value.Value, out long _))
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInput,
                            "Invalid numeric integer format for field '" + columnValue.Key + "'"
                        );
                    break;

                case ColumnType.Bool:
                    string boolValue = columnValue.Value.Value.ToLowerInvariant();
                    if (boolValue != "true" && boolValue != "false")
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInput,
                            "Invalid bool value for field '" + columnValue.Key + "'"
                        );
                    break;
            }
        }
    }
}
