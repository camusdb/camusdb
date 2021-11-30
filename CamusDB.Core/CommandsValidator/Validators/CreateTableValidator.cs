
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

internal sealed class CreateTableValidator
{
    public void Validate(CreateTableTicket ticket)
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

        if (ticket.Columns.Length == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Table requires at least one column"
            );

        HashSet<string> existingColumns = new();

        for (int i = 0; i < ticket.Columns.Length; i++)
        {
            ColumnInfo columnInfo = ticket.Columns[i];

            if (!existingColumns.Add(columnInfo.Name.ToLowerInvariant()))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Duplicate column name: " + columnInfo.Name
                );
        }
    }
}
