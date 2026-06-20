
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

internal sealed class CreateTableValidator : ValidatorBase
{
    public void Validate(CreateTableTicket ticket)
    {
        if (string.IsNullOrWhiteSpace(ticket.DatabaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Database name is required");

        ValidateIdentifier(ticket.TableName, "Table");

        if (ticket.Columns.Length == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Table requires at least one column");

        int maxCols = CamusDB.Core.CamusDBConfig.MaxColumnsPerTable;
        if (maxCols > 0 && ticket.Columns.Length > maxCols)
            throw new CamusDBException(
                CamusDBErrorCodes.SchemaLimitExceeded,
                $"Table '{ticket.TableName}' declares {ticket.Columns.Length} columns, which exceeds the maximum of {maxCols}");

        HashSet<string> existingColumns = new();

        for (int i = 0; i < ticket.Columns.Length; i++)
        {
            ColumnInfo columnInfo = ticket.Columns[i];

            ValidateIdentifier(columnInfo.Name, "Column");

            if (IsReservedName(columnInfo.Name))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Reserved column name: " + columnInfo.Name);

            if (!existingColumns.Add(columnInfo.Name.ToLowerInvariant()))
                throw new CamusDBException(CamusDBErrorCodes.DuplicateColumn, "Duplicate column name: " + columnInfo.Name);

            if (columnInfo.Type == ColumnType.Null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Column type cannot be null");
        }

        bool havePrimaryKey = false;

        for (int i = 0; i < ticket.Constraints.Length; i++)
        {
            ConstraintInfo constraint = ticket.Constraints[i];

            if (constraint.Type == ConstraintType.PrimaryKey)
            {
                havePrimaryKey = true;
                break;
            }
        }

        if (!havePrimaryKey)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "A primary key column is mandatory in the table"
            );
    }
}
