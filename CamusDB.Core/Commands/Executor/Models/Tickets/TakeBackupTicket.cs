/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Ticket for taking a node-wide backup (full, incremental, or coordinated). A privileged server
/// operation — the executor requires a superuser <see cref="Principal"/> when authentication is on.
///
/// <para><see cref="ParentBackupId"/> is required exactly when <see cref="Kind"/> is
/// <see cref="BackupKind.Incremental"/> (the parent the increment chains onto) and must be null
/// otherwise; the validator enforces this.</para>
/// </summary>
public readonly struct TakeBackupTicket
{
    public BackupKind Kind { get; }

    public Guid? ParentBackupId { get; }

    /// <summary>
    /// The authenticated caller. Null when authentication is disabled; when enabled it must be a
    /// superuser or the executor rejects the request.
    /// </summary>
    public Principal? Principal { get; }

    public TakeBackupTicket(BackupKind kind, Guid? parentBackupId, Principal? principal = null)
    {
        Kind = kind;
        ParentBackupId = parentBackupId;
        Principal = principal;
    }
}
