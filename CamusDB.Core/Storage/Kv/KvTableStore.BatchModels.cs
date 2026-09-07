/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// The request shapes callers build to drive the batched write paths. They stay nested in
/// <see cref="KvTableStore"/> because they are that type's published vocabulary; the code that acts
/// on them lives in <see cref="KvBatchWriter"/>.
///
/// <para>All five are <see langword="readonly struct"/>s so a per-row descriptor lives in the batch
/// list's backing array rather than as its own heap object. Every index-entry collection is nullable
/// rather than empty, so a row with no applicable index allocates no per-row collection at all. A
/// producer builds such a list lazily and must never mutate it afterwards — these are value types,
/// and mutating a copied struct's list would be a bug.</para>
/// </summary>
public sealed partial class KvTableStore
{
    /// <summary>
    /// A single row plus its secondary-index entries, to be written as part of a batch.
    /// </summary>
    public readonly struct RowWrite
    {
        public required ObjectIdValue RowId { get; init; }

        /// <summary>
        /// The row's final Kahuna storage value — the <see cref="BranchKvKind.Value"/>-enveloped row
        /// bytes as produced by <see cref="RowEncoder.EncodeStorageValue"/>. It is written to KV
        /// verbatim (no further enveloping), so the whole write costs one allocation, not raw-row plus
        /// a re-enveloped copy.
        /// </summary>
        public required byte[] RowData { get; init; }

        /// <summary>
        /// The row's secondary-index entries, or <see langword="null"/> when the table has no writable
        /// index applicable to this row.
        /// </summary>
        public IReadOnlyList<IndexWrite>? IndexEntries { get; init; }
    }

    /// <summary>
    /// One secondary-index entry for a row in a batch write.
    /// <para>
    /// <see cref="IncludeTuple"/> carries the serialized stored/payload (INCLUDE) column values for a
    /// covering index (see <see cref="IndexIncludeValueCodec"/>); it is null/empty for a plain index,
    /// keeping the entry value byte-identical to the historical rowId-only form.
    /// </para>
    /// <para>
    /// <see cref="Overwrite"/> forces the write to use <c>Set</c> even for a unique index. It is set
    /// only when an UPDATE changed an included column but not the key, so the entry key already exists
    /// and belongs to this same row: the value must be overwritten in place, and <c>SetIfNotExists</c>
    /// (the normal unique-insert flag) would wrongly no-op. It never bypasses duplicate detection for a
    /// genuinely new key.
    /// </para>
    /// </summary>
    public readonly record struct IndexWrite(string IndexId, CompositeColumnValue Key, bool Unique, byte[]? IncludeTuple = null, bool Overwrite = false);

    /// <summary>
    /// A single row update: the new row data plus index entries to remove (old) and to add (new).
    /// Only entries whose indexed columns actually changed should be included; unchanged entries
    /// are intentionally omitted so the batch skips needless delete+put round-trips and avoids
    /// the branch tombstone-replace correctness trap (see <see cref="UpdateRowsBatch"/>).
    /// </summary>
    public readonly struct RowUpdate
    {
        public required ObjectIdValue RowId { get; init; }

        /// <summary>
        /// The row's final Kahuna storage value — the <see cref="BranchKvKind.Value"/>-enveloped row
        /// bytes from <see cref="RowEncoder.EncodeStorageValue"/>. Written verbatim (no re-enveloping).
        /// </summary>
        public required byte[] NewRowData { get; init; }

        /// <summary>
        /// Old secondary-index entries to remove (only for changed keys), or <see langword="null"/>
        /// when no indexed value changed.
        /// </summary>
        public IReadOnlyList<IndexDelete>? OldIndexEntries { get; init; }

        /// <summary>
        /// New secondary-index entries to put (only for changed keys), or <see langword="null"/> when
        /// no indexed value changed.
        /// </summary>
        public IReadOnlyList<IndexWrite>? NewIndexEntries { get; init; }
    }

    /// <summary>
    /// A single row plus its secondary-index entries, to be deleted as part of a batch.
    /// </summary>
    public readonly struct RowDelete
    {
        public required ObjectIdValue RowId { get; init; }

        /// <summary>
        /// The row's secondary-index entries to remove, or <see langword="null"/> when the table has no
        /// writable index applicable to this row.
        /// </summary>
        public IReadOnlyList<IndexDelete>? IndexEntries { get; init; }
    }

    /// <summary>One secondary-index entry for a row in a batch delete.</summary>
    public readonly record struct IndexDelete(string IndexId, CompositeColumnValue Key, ObjectIdValue RowId, bool Unique);
}
