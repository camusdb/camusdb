
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


        if (ticket.Operation == AlterIndexOperation.AddIndex || ticket.Operation == AlterIndexOperation.AddUniqueIndex || ticket.Operation == AlterIndexOperation.AddPrimaryKey)
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

        if (ticket.Operation == AlterIndexOperation.AddIndex || ticket.Operation == AlterIndexOperation.AddUniqueIndex)
        {
            if (string.IsNullOrWhiteSpace(ticket.IndexName))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Index name is required"
                );

            if (string.IsNullOrWhiteSpace(ticket.IndexName))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Index name is required"
                );

            if (ticket.IndexName.Length > 255)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Index name is too long"
                );

            if (!HasValidCharacters(ticket.IndexName))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Index name has invalid characters"
                );
        }
    }
}
