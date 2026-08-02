
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

internal sealed class AlterConstraintValidator : ValidatorBase
{
    public AlterConstraintValidator(CamusDBOptions options) : base(options) { }

    public void Validate(AlterConstraintTicket ticket)
    {
        if (string.IsNullOrWhiteSpace(ticket.DatabaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Database name is required");

        if (string.IsNullOrWhiteSpace(ticket.TableName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Table name is required");

        bool needsConstraintName = ticket.Operation == AlterConstraintOperation.AddCheck ||
                                    ticket.Operation == AlterConstraintOperation.DropConstraint;
        if (needsConstraintName && string.IsNullOrWhiteSpace(ticket.ConstraintName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Constraint name is required");

        if (ticket.Operation == AlterConstraintOperation.AddCheck &&
            string.IsNullOrWhiteSpace(ticket.Expression))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Expression is required for ADD CONSTRAINT CHECK");

        if ((ticket.Operation == AlterConstraintOperation.SetNotNull ||
             ticket.Operation == AlterConstraintOperation.DropNotNull) &&
            string.IsNullOrWhiteSpace(ticket.ColumnName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Column name is required for SET/DROP NOT NULL");
    }
}
