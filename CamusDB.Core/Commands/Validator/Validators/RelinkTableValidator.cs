
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

internal sealed class RelinkTableValidator : ValidatorBase
{
    public RelinkTableValidator(CamusDBOptions options) : base(options) { }

    public void Validate(RelinkTableTicket ticket)
    {
        ValidateIdentifier(ticket.DatabaseName, "Database");
        ValidateIdentifier(ticket.NewTableName, "Table");
        // Orphan ids are alphanumeric (short base-62 or legacy 24-hex) with no key separators.
        ValidateIdentifier(ticket.OrphanTableId, "Orphan table id");
    }
}
