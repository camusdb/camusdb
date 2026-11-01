
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

internal sealed class AlterIndexValidator : ValidatorBase
{
    public AlterIndexValidator(CamusDBOptions options) : base(options) { }

    public void Validate(AlterIndexTicket ticket)
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

        if (ticket.Operation != AlterIndexOperation.AddPrimaryKey && ticket.Operation != AlterIndexOperation.DropPrimaryKey && string.IsNullOrWhiteSpace(ticket.IndexName))            
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Index name is required"
            );


        if (ticket.Operation is AlterIndexOperation.AddIndex or AlterIndexOperation.AddUniqueIndex or AlterIndexOperation.AddPrimaryKey or AlterIndexOperation.AddInvertedIndex)
        {
            foreach (ColumnIndexInfo column in ticket.Columns)
            {
                if (string.IsNullOrWhiteSpace(column.Name))
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        "Column name is required"
                    );
            }
        }

        if (ticket.Operation is AlterIndexOperation.AddIndex or AlterIndexOperation.AddUniqueIndex or AlterIndexOperation.AddInvertedIndex)
        {
            ValidateIdentifier(ticket.IndexName ?? "", "Index");
        }

        // A GIN index has one key column, a jsonb document, and no INCLUDE payload: its entries are
        // paths and values, not a copy of the row. The column's type is checked where the table is known.
        if (ticket.Operation == AlterIndexOperation.AddInvertedIndex)
        {
            if (ticket.Columns.Length != 1)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"A GIN index has exactly one column, got {ticket.Columns.Length}");

            if (ticket.IncludeColumns.Length > 0)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "A GIN index cannot have INCLUDE columns");

            if (ticket.Columns[0].Order == OrderType.Descending)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "A GIN index has no order; remove DESC");
        }

        if (ticket.Operation == AlterIndexOperation.RenameIndex)
        {
            ValidateIdentifier(ticket.NewName ?? "", "New index");

            if (ticket.NewName!.StartsWith('~'))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"New name '{ticket.NewName}' is reserved for internal use");

            if (ticket.NewName == ticket.IndexName)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"New index name '{ticket.NewName}' is the same as the current name");
        }
    }
}
