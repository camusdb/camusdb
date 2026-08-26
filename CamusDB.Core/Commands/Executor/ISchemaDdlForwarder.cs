/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;


namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// Production schema-DDL forwarding boundary. A non-leader CamusDB node forwards
/// the original command ticket to the schema leader before opening a local DDL
/// transaction, preserving command validation and response semantics on the
/// canonical DDL path.
/// </summary>
public interface ISchemaDdlForwarder
{
    Task<bool?> ForwardCreateTableAsync(string leader, CreateTableTicket ticket, string operationId, CancellationToken cancellationToken);

    Task<bool?> ForwardAlterTableAsync(string leader, AlterTableTicket ticket, string operationId, CancellationToken cancellationToken);

    Task<bool?> ForwardAlterIndexAsync(string leader, AlterIndexTicket ticket, string operationId, CancellationToken cancellationToken);

    Task<bool?> ForwardDropTableAsync(string leader, DropTableTicket ticket, string operationId, CancellationToken cancellationToken);

    Task<bool?> ForwardRelinkTableAsync(string leader, RelinkTableTicket ticket, string operationId, CancellationToken cancellationToken);

    /// <summary>
    /// Forwards <c>TRUNCATE</c>. Only the schema leader may run it: it allocates the new storage id
    /// and holds the exclusive fence over the old row key-space while the delta commits, and a
    /// follower holding a local fence while waiting on the leader would fence nothing.
    /// </summary>
    Task<bool?> ForwardTruncateTableAsync(string leader, TruncateTableTicket ticket, string operationId, CancellationToken cancellationToken);

    Task<bool?> ForwardRenameTableAsync(string leader, RenameTableTicket ticket, string operationId, CancellationToken cancellationToken);

    Task<bool?> ForwardAlterConstraintAsync(string leader, AlterConstraintTicket ticket, string operationId, CancellationToken cancellationToken);

    /// <summary>
    /// Forwards <c>COMMENT ON TABLE|COLUMN|INDEX</c>. <c>COMMENT ON DATABASE</c> never comes through
    /// here — it writes the cross-database registry directly and needs no schema leader.
    /// </summary>
    Task<bool?> ForwardCommentAsync(string leader, CommentTicket ticket, string operationId, CancellationToken cancellationToken);
}
