
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

internal sealed class AlterTableValidator : ValidatorBase
{
    public AlterTableValidator(CamusDBOptions options) : base(options) { }

    public void Validate(AlterTableTicket ticket)
    {
        if (string.IsNullOrWhiteSpace(ticket.DatabaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Database name is required");

        if (string.IsNullOrWhiteSpace(ticket.TableName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Table name is required");

        ValidateIdentifier(ticket.Column.Name, "Column");

        // Covers ADD COLUMN … COMMENT '…'; other operations carry no comment and pass a null.
        ValidateCommentLength(ticket.Column.Comment, $"Column '{ticket.Column.Name}'");

        // ADD COLUMN … DEFAULT(…) is the other way a default enters the schema, and it must meet the
        // same type/length rules as one declared at CREATE TABLE.
        if (ticket.Operation is AlterTableOperation.AddColumn)
            ValidateColumnDefault(ticket.Column, $"Column '{ticket.Column.Name}'");

        if (ticket.Operation == AlterTableOperation.RenameColumn)
        {
            ValidateIdentifier(ticket.NewName ?? "", "New column");

            if (ticket.NewName!.StartsWith('~'))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"New name '{ticket.NewName}' is reserved for internal use");

            if (IsReservedName(ticket.NewName))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"'{ticket.NewName}' is a reserved column name");

            if (ticket.NewName == ticket.Column.Name)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"New column name '{ticket.NewName}' is the same as the current name");
        }
    }
}
