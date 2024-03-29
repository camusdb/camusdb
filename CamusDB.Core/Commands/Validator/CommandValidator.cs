
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
    private readonly CreateDatabaseValidator createDatabaseValidator = new();

    private readonly DropDatabaseValidator dropDatabaseValidator = new();

    private readonly CreateTableValidator createTableValidator = new();

    private readonly InsertValidator insertValidator = new();

    private readonly AlterTableValidator alterTableValidator = new();

    private readonly AlterIndexValidator alterIndexValidator = new();

    private readonly CloseDatabaseValidator closeDatabaseValidator = new();

    public void Validate(CreateDatabaseTicket ticket)
    {
        createDatabaseValidator.Validate(ticket);
    }

    public void Validate(DropDatabaseTicket ticket)
    {
        dropDatabaseValidator.Validate(ticket);
    }

    public void Validate(CreateTableTicket ticket)
    {
        createTableValidator.Validate(ticket);
    }

    public void Validate(AlterTableTicket ticket)
    {
        alterTableValidator.Validate(ticket);
    }

    public void Validate(AlterIndexTicket ticket)
    {
        alterIndexValidator.Validate(ticket);
    }

    public void Validate(CloseDatabaseTicket ticket)
    {
        closeDatabaseValidator.Validate(ticket);
    }

    public void Validate(DropTableTicket ticket)
    {
        DropTableValidator validator = new();
        validator.Validate(ticket);
    }

    public void Validate(InsertTicket ticket)
    {        
        insertValidator.Validate(ticket);
    }

    public void Validate(UpdateTicket ticket)
    {
        UpdateValidator validator = new();
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
