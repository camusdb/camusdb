
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

internal sealed class CreateTableValidator : ValidatorBase
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

        if (ticket.TableName.Length > 255)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Table name is too long"
            );

        if (!HasValidCharacters(ticket.TableName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Table name has invalid characters"
            );

        if (ticket.Columns.Length == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Table requires at least one column"
            );

        ColumnInfo? primaryKey = null;
        HashSet<string> existingColumns = new();

        for (int i = 0; i < ticket.Columns.Length; i++)
        {
            ColumnInfo columnInfo = ticket.Columns[i];

            if (!HasValidCharacters(columnInfo.Name))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Column name has invalid characters"
                );

            if (IsReservedName(columnInfo.Name))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Reserved column name: " + columnInfo.Name
                );

            if (!existingColumns.Add(columnInfo.Name.ToLowerInvariant()))
                throw new CamusDBException(
                    CamusDBErrorCodes.DuplicateColumn,
                    "Duplicate column name: " + columnInfo.Name
                );

            if (columnInfo.Primary)
            {
                if (primaryKey != null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.DuplicatePrimaryKey,
                        "Multiple primary key defined"
                    );

                primaryKey = columnInfo;
            }
        }

        if (primaryKey is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "A primary key column is mandatory in the table"
            );
    }
}
