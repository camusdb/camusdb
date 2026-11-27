/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Work outside the schema that a schema change needs after it took effect: data to remove that the
/// change made unreachable, or the counter of a sequence to bring to its new definition
/// (<see cref="DeferredPurgeKind.SequenceCounter"/>). Stored under <c>{dbId}/meta/purge:{PurgeId}</c>.
/// Two kinds of statement write one.
///
/// <para><b>A schema change in an explicit transaction</b> writes its records in the transaction's own
/// KV commit, next to its decision key, so a record exists exactly when the transaction committed. A
/// committed transaction always ends in a publish, so a record never protects data that an abort would
/// still need. The data stays until the publish, because until then the shared schema still uses it: a
/// rollback, or a transaction that never commits, leaves it untouched and writes no record. Such a
/// record has a <see cref="ChangeSetId"/>, and its <see cref="PurgeId"/> is the change set id, <c>~</c>,
/// and the record's position in it.</para>
///
/// <para><b>A <c>DROP INDEX</c> outside a transaction</b> writes its record when the index is gone from
/// the schema of every node (<see cref="ForDroppedIndex"/>). The coordinator writes the record and
/// deletes the job of the drop in one KV transaction, so a crash leaves one of the two and never
/// neither. Such a record has no change set, its <see cref="PurgeId"/> names the storage id and the
/// index id, which are never used again, and it is due from <see cref="DueVersion"/> on.</para>
///
/// <para><b>When a run may remove the data.</b> Only after the change ended on the node that runs. For a
/// change set: the node's schema has no pending change set with <see cref="ChangeSetId"/> and its version
/// is past the pending entry (<see cref="BaseVersion"/> + 2 or later). For a dropped index: the node's
/// schema version is <see cref="DueVersion"/> or later. A node whose schema lags waits for a later run.
/// The removal is idempotent and the record is deleted last, so an interrupted run is finished by a
/// later one, on any node.</para>
/// </summary>
public sealed class DeferredPurgeRecord
{
    /// <summary>
    /// The key segment of the record. For a change set: the change set id, <c>~</c>, and the record's
    /// position in it. For a dropped index: see <see cref="DroppedIndexPurgeId"/>.
    /// </summary>
    public string PurgeId { get; set; } = "";

    /// <summary>What the record removes.</summary>
    public DeferredPurgeKind Kind { get; set; }

    /// <summary>The change set whose publish makes the data unreachable; empty for a record of a dropped index.</summary>
    public string ChangeSetId { get; set; } = "";

    /// <summary>
    /// The schema version the change set's pending entry is proposed from, set when the commit writes the
    /// record. The pending entry moves the version to this plus one, and no other schema change applies
    /// while it is pending, so a version of this plus two or later means the change set ended. Zero for a
    /// record of a dropped index.
    /// </summary>
    public long BaseVersion { get; set; }

    /// <summary>
    /// For a record with no <see cref="ChangeSetId"/>: the schema version from which a run may remove the
    /// data. A dropped index is gone from the schema at this version on every node, so a node whose schema
    /// is older still reads or writes the index and must wait. Zero for a record of a change set.
    /// </summary>
    public long DueVersion { get; set; }

    /// <summary>The relation id of the table the data belonged to, for diagnostics.</summary>
    public string TableId { get; set; } = "";

    /// <summary>The storage id whose key spaces hold the data (<c>TableSchema.EffectiveStorageId</c>).</summary>
    public string StorageId { get; set; } = "";

    /// <summary>
    /// The stable ids of the index key spaces to remove (<c>TableIndexSchema.KvId</c>): the dropped index
    /// for <see cref="DeferredPurgeKind.IndexEntries"/>, and every index of the table for
    /// <see cref="DeferredPurgeKind.TableData"/>.
    /// </summary>
    public List<string> IndexIds { get; set; } = [];

    /// <summary>For <see cref="DeferredPurgeKind.SequenceCounter"/>, the immutable id of the sequence.</summary>
    public string SequenceId { get; set; } = "";

    /// <summary>
    /// For <see cref="DeferredPurgeKind.SequenceCounter"/>, true when the statement changed the start value,
    /// which the counter also keeps.
    /// </summary>
    public bool SetsStartValue { get; set; }

    /// <summary>
    /// The purge id of the record of a dropped index: <c>index~{storageId}~{indexId}</c>. It needs no change
    /// set, so it cannot collide with one, whose ids hold one <c>~</c>. A storage id and an index id are
    /// never used again, so a resume that writes the record a second time writes the same key, and a
    /// <c>CREATE INDEX</c> of the same name gets another id and another record.
    /// </summary>
    public static string DroppedIndexPurgeId(string storageId, string indexId) => $"index~{storageId}~{indexId}";

    /// <summary>
    /// The record that removes the entries of an index that a <c>DROP INDEX</c> outside a transaction took
    /// out of the schema: the key space of <paramref name="indexId"/> under <paramref name="storageId"/>,
    /// due from schema version <paramref name="dueVersion"/>.
    /// </summary>
    public static DeferredPurgeRecord ForDroppedIndex(string tableId, string storageId, string indexId, long dueVersion) => new()
    {
        PurgeId = DroppedIndexPurgeId(storageId, indexId),
        Kind = DeferredPurgeKind.IndexEntries,
        TableId = tableId,
        StorageId = storageId,
        IndexIds = [indexId],
        DueVersion = dueVersion,
    };
}

/// <summary>The kinds of work a <see cref="DeferredPurgeRecord"/> stands for.</summary>
public enum DeferredPurgeKind
{
    /// <summary>Every entry of one dropped index: the key space <c>{dbId}:{storageId}|i:{indexId}</c>.</summary>
    IndexEntries = 0,

    /// <summary>
    /// Every row, out-of-line value and index entry of a table dropped without retention
    /// (<c>DROP TABLE ... FORCE</c>, or <c>DROP TABLE</c> in a branch database), with its statistics and its
    /// key-space records. In a branch database these are the branch's own entries only: the inherited rows
    /// live under the ancestors, and nothing reads them once the branch schema no longer names the table.
    /// </summary>
    TableData = 1,

    /// <summary>
    /// The Kahuna counter of a sequence whose definition the change set altered. The record carries no
    /// definition: a run brings the counter to the definition the schema holds then, so a run that a later
    /// <c>ALTER SEQUENCE</c> overtook changes nothing, and running twice has the same result. A sequence
    /// that was dropped since has its counter removed.
    /// </summary>
    SequenceCounter = 2,
}
