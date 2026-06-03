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
    Task<bool?> ForwardCreateTableAsync(string leader, CreateTableTicket ticket, CancellationToken cancellationToken);

    Task<bool?> ForwardAlterTableAsync(string leader, AlterTableTicket ticket, CancellationToken cancellationToken);

    Task<bool?> ForwardAlterIndexAsync(string leader, AlterIndexTicket ticket, CancellationToken cancellationToken);

    Task<bool?> ForwardDropTableAsync(string leader, DropTableTicket ticket, CancellationToken cancellationToken);
}
