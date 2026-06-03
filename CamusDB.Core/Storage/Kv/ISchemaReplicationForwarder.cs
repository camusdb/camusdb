/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Internal test harness hook for forwarding raw schema-log entries between
/// in-memory <see cref="EmbeddedKahuna"/> instances. Production CamusDB DDL
/// forwarding is ticket-level through <c>ISchemaDdlForwarder</c>, so command
/// validation, retries, and user-visible results stay on one path.
/// </summary>
internal interface ISchemaReplicationForwarder
{
    Task<SchemaReplicationResult?> ForwardSchemaChangeAsync(
        string leader,
        string db,
        byte[] entry,
        CancellationToken cancellationToken
    );
}
