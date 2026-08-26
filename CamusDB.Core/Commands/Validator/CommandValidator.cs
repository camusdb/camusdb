
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
    private readonly CreateDatabaseValidator createDatabaseValidator;

    private readonly DropDatabaseValidator dropDatabaseValidator;

    private readonly CreateTableValidator createTableValidator;

    private readonly InsertValidator insertValidator;

    private readonly InsertSelectValidator insertSelectValidator;

    private readonly AlterTableValidator alterTableValidator;

    private readonly AlterIndexValidator alterIndexValidator;

    private readonly AlterConstraintValidator alterConstraintValidator;

    private readonly CommentValidator commentValidator;

    private readonly CreateUserValidator createUserValidator;

    private readonly AlterUserValidator alterUserValidator;

    private readonly DropUserValidator dropUserValidator;

    private readonly GrantValidator grantValidator;

    private readonly CloseDatabaseValidator closeDatabaseValidator;

    private readonly RelinkDatabaseValidator relinkDatabaseValidator;

    private readonly RelinkTableValidator relinkTableValidator;

    private readonly TakeBackupValidator takeBackupValidator;

    private readonly RestoreBackupValidator restoreBackupValidator;

    /// <summary>
    /// Builds the validator set for one engine. Limits such as identifier length and the per-table
    /// column ceiling are operator-settable, so each validator is given this engine's configuration
    /// rather than reading a process-wide value.
    /// </summary>
    /// <summary>Configuration of the engine whose commands this validates.</summary>
    private CamusDBOptions options;

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Reference assignment is atomic and the
    /// record itself stays immutable; readers pin the field once at the top of an operation, so an
    /// in-flight operation keeps the snapshot it started with and a change takes effect at the
    /// next operation boundary.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    public CommandValidator(CamusDBOptions options)
    {
        this.options = options;

        createDatabaseValidator = new(options);
        dropDatabaseValidator = new(options);
        createTableValidator = new(options);
        insertValidator = new(options);
        insertSelectValidator = new(options);
        alterTableValidator = new(options);
        alterIndexValidator = new(options);
        alterConstraintValidator = new(options);
        commentValidator = new(options);
        createUserValidator = new(options);
        alterUserValidator = new(options);
        dropUserValidator = new(options);
        grantValidator = new(options);
        closeDatabaseValidator = new(options);
        relinkDatabaseValidator = new(options);
        relinkTableValidator = new(options);
        takeBackupValidator = new(options);
        restoreBackupValidator = new(options);
    }

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
        }, options);
    }

    public void Validate(CloseDatabaseTicket ticket)
    {
        closeDatabaseValidator.Validate(ticket);
    }

    public void Validate(DropTableTicket ticket)
    {
        DropTableValidator validator = new(options);
        validator.Validate(ticket);
    }

    public void Validate(TruncateTableTicket ticket)
    {
        TruncateTableValidator validator = new(options);
        validator.Validate(ticket);
    }

    public void Validate(InsertTicket ticket)
    {
        insertValidator.Validate(ticket);
    }

    public void Validate(InsertSelectTicket ticket)
    {
        insertSelectValidator.Validate(ticket);
    }

    public void Validate(UpdateTicket ticket)
    {
        UpdateValidator validator = new(options);
        validator.Validate(ticket);
    }

    public void Validate(DeleteTicket ticket)
    {
        DeleteValidator validator = new(options);
        validator.Validate(ticket);
    }

    public void Validate(QueryTicket ticket)
    {
        QueryValidator validator = new(options);
        validator.Validate(ticket);
    }

    public void Validate(QueryByIdTicket ticket)
    {
        QueryByIdValidator validator = new(options);
        validator.Validate(ticket);
    }

    public void Validate(ExecuteSQLTicket ticket)
    {
        ExecuteSQLValidator validator = new(options);
        validator.Validate(ticket);
    }
}
