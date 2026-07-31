
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
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

    private readonly AlterConstraintValidator alterConstraintValidator = new();

    private readonly CommentValidator commentValidator = new();

    private readonly CreateUserValidator createUserValidator = new();

    private readonly AlterUserValidator alterUserValidator = new();

    private readonly DropUserValidator dropUserValidator = new();

    private readonly GrantValidator grantValidator = new();

    private readonly CloseDatabaseValidator closeDatabaseValidator = new();

    private readonly RelinkDatabaseValidator relinkDatabaseValidator = new();

    private readonly RelinkTableValidator relinkTableValidator = new();

    private readonly TakeBackupValidator takeBackupValidator = new();

    private readonly RestoreBackupValidator restoreBackupValidator = new();

    public void Validate(TakeBackupTicket ticket)
    {
        takeBackupValidator.Validate(ticket);
    }

    public void Validate(RestoreBackupTicket ticket)
    {
        restoreBackupValidator.Validate(ticket);
    }

    public void Validate(GetBackupChainTicket ticket)
    {
        if (ticket.LeafBackupId == Guid.Empty)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "A backup-chain lookup requires a non-empty backup id");
    }

    public void Validate(CreateDatabaseTicket ticket)
    {
        createDatabaseValidator.Validate(ticket);
    }

    public void Validate(DropDatabaseTicket ticket)
    {
        dropDatabaseValidator.Validate(ticket);
    }

    public void Validate(RelinkDatabaseTicket ticket)
    {
        relinkDatabaseValidator.Validate(ticket);
    }

    public void Validate(RelinkTableTicket ticket)
    {
        relinkTableValidator.Validate(ticket);
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

    public void Validate(AlterConstraintTicket ticket)
    {
        alterConstraintValidator.Validate(ticket);
    }

    public void Validate(CommentTicket ticket)
    {
        commentValidator.Validate(ticket);
    }

    public void Validate(CreateUserTicket ticket)
    {
        createUserValidator.Validate(ticket);
    }

    public void Validate(AlterUserTicket ticket)
    {
        alterUserValidator.Validate(ticket);
    }

    public void Validate(DropUserTicket ticket)
    {
        dropUserValidator.Validate(ticket);
    }

    public void Validate(GrantTicket ticket)
    {
        grantValidator.Validate(ticket);
    }

    public void Validate(RenameTableTicket ticket)
    {
        RenameValidator.Validate(new SchemaRenamePayload
        {
            Kind = SchemaRenameKind.Table,
            TableName = ticket.TableName,
            NewName = ticket.NewName,
        });
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
