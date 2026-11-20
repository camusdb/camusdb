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

    /// <summary>
    /// Forwards a schema change a follower built and checked itself: its op, payload and timestamp.
    /// The leader wraps the payload at its own schema version, checks it again under its DDL gate and
    /// schema lock, and publishes it. Returns the version the change was committed at, or null when
    /// the target does not lead the database's schema log or could not be reached.
    /// </summary>
    /// <remarks>
    /// Used for the changes that have no statement-level forward of their own: view DDL, the
    /// relations a materialized view is created with, a refresh's publish, and table settings. Each
    /// payload carries the preconditions its apply checks, so a payload built against an older schema
    /// is refused in log order rather than applied over a newer one. A forwarder that does not support
    /// it refuses with an error that names the leader.
    /// </remarks>
    Task<long?> ForwardSchemaDeltaAsync(string leader, SchemaDeltaForward delta, string operationId, CancellationToken cancellationToken)
        => throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"This node cannot forward a '{delta.Op}' schema change to the schema leader '{leader}' of database " +
            $"'{delta.DatabaseName}'; run the statement on the schema leader");
}

/// <summary>
/// One schema change forwarded to the schema leader, as <see cref="ISchemaDdlForwarder.ForwardSchemaDeltaAsync"/>
/// carries it. <paramref name="Payload"/> is the encoded payload exactly as the follower built it.
/// </summary>
public sealed record SchemaDeltaForward(
    string DatabaseName,
    Catalogs.Models.SchemaOp Op,
    byte[] Payload,
    Kommander.Time.HLCTimestamp Ts);
