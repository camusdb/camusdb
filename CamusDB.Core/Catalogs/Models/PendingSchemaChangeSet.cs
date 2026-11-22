
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// The change set of an explicit transaction that the schema log announced with a
/// <see cref="SchemaOp.PendingChangeSet"/> entry and that has no publish or abort yet. Part of the
/// replicated schema state: every node records it at the same log position, and a reopen restores it
/// from the pending record in the checkpoint or from the log.
///
/// <para>While one exists, the schema apply refuses every other change of the database. That is what
/// lets the publish apply the deltas onto the exact schema that the pending apply dry-ran them
/// against, after the transaction already committed its rows in KV.</para>
/// </summary>
/// <param name="ChangeSetId">The id that names the decision key and the pending record.</param>
/// <param name="PendingEntry">The committed pending entry. The checkpoint stores its framed bytes as the pending record.</param>
/// <param name="Payload">The decoded payload of <paramref name="PendingEntry"/>.</param>
/// <param name="RecordedAtTicks">
/// <see cref="Environment.TickCount64"/> on this node when the change set was recorded. Local and
/// monotonic: the resolver measures with it how long the change set has waited, which is a local
/// question, and a restart starts the wait again.
/// </param>
/// <param name="AffectedTableIds">
/// The ids of the live tables the change set changes, and of the tables that a foreign key it creates
/// references. While the change set is pending, a statement that opens one of them waits for the publish
/// or the abort (<c>TableOpener</c>), and the pending apply stamped their write shape, so a writer that
/// planned before cannot commit against the old shape. Between the transaction's KV commit and the
/// publish, its rows exist while the shared schema does not describe them yet; the fence keeps every
/// other statement from acting in that window on what the change set changes, such as deleting a parent
/// row that a new foreign key protects, or reading through an index the change set drops.
/// </param>
public sealed record PendingSchemaChangeSet(
    string ChangeSetId,
    SchemaChangeLogEntry PendingEntry,
    SchemaChangeSetPayload Payload,
    long RecordedAtTicks,
    IReadOnlySet<string> AffectedTableIds)
{
    /// <summary>True when the change set touches the table <paramref name="tableId"/>; see <see cref="AffectedTableIds"/>.</summary>
    public bool Affects(string? tableId) => tableId is not null && AffectedTableIds.Contains(tableId);
}
