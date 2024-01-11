
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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

    public void Validate(DropDatabaseTicket ticket)
    {
        DropDatabaseValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(CreateTableTicket ticket)
    {
        CreateTableValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(AlterTableTicket ticket)
    {
        AlterTableValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(AlterIndexTicket ticket)
    {
        AlterIndexValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(CloseDatabaseTicket ticket)
    {
        CloseDatabaseValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(DropTableTicket ticket)
    {
        DropTableValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(InsertTicket ticket)
    {
        InsertValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(UpdateTicket ticket)
    {
        UpdateValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(UpdateByIdTicket ticket)
    {
        UpdateByIdValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(DeleteByIdTicket ticket)
    {
        DeleteByIdValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(DeleteTicket ticket)
    {
        DeleteValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(QueryTicket ticket)
    {
        QueryValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(QueryByIdTicket ticket)
    {
        QueryByIdValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(ExecuteSQLTicket ticket)
    {
        ExecuteSQLValidator validator = new();
        validator.Validate(ticket);
    }
}
