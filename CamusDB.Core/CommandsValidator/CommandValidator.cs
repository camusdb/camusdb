
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator.Validators;

namespace CamusDB.Core.CommandsValidator;

public sealed class CommandValidator
{
    public void Validate(CreateDatabaseTicket ticket)
    {
        CreateDatabaseValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(CreateTableTicket ticket)
    {
        CreateTableValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(CloseDatabaseTicket ticket)
    {
        CloseDatabaseValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(InsertTicket ticket)
    {
        InsertValidator validator = new();
        validator.Validate(ticket);
    }
}
