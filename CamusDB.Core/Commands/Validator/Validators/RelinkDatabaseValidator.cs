
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

internal sealed class RelinkDatabaseValidator : ValidatorBase
{
    public RelinkDatabaseValidator(CamusDBOptions options) : base(options) { }

    public void Validate(RelinkDatabaseTicket ticket)
    {
        ValidateIdentifier(ticket.NewName, "Database");
        // Orphan ids are alphanumeric (short base-62 or legacy 24-hex) with no key separators,
        // so the identifier character rules apply to them too.
        ValidateIdentifier(ticket.OrphanId, "Orphan database id");
    }
}
