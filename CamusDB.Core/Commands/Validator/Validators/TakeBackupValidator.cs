/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

/// <summary>
/// Validates a <see cref="TakeBackupTicket"/>: an incremental backup requires a non-empty parent id,
/// and full/coordinated backups must not carry one. Authorization (superuser) and the backup-configured
/// gate are enforced later in the executor, not here.
/// </summary>
internal sealed class TakeBackupValidator : ValidatorBase
{
    public TakeBackupValidator(CamusDBOptions options) : base(options) { }

    public void Validate(TakeBackupTicket ticket)
    {
        if (ticket.Kind == BackupKind.Incremental)
        {
            if (ticket.ParentBackupId is null || ticket.ParentBackupId == Guid.Empty)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "An incremental backup requires a non-empty 'parentBackupId'");
        }
        else if (ticket.ParentBackupId is not null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"A {ticket.Kind.ToString().ToLowerInvariant()} backup must not specify a 'parentBackupId'");
        }
    }
}
