
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
/// A zero-copy, borrowed view over one stored row's bytes. It holds the row id, the row's
/// <see cref="ReadOnlyMemory{T}"/> payload (a slice of the Kahuna-owned scan buffer — see
/// <see cref="BranchKvCodec"/>), and the <see cref="CompiledRowCodec"/> for its stored schema version.
/// No cell is decoded until a typed accessor is called, and fixed scalars are read straight out of the
/// borrowed bytes with no allocation, so a scanned row that is filtered out — or whose value columns are
/// never touched — allocates nothing for its payload.
///
/// <para>
/// <b>Ordinals are stored ordinals</b> (the codec's column order for the row's schema version), not
/// query-output ordinals. Callers that work in output-ordinal space (e.g. a projected <c>QueryRow</c>)
/// map output → stored before calling these accessors.
/// </para>
///
/// <para>
/// <b>Lifetime.</b> A <see cref="RowView"/> is only valid while its backing <see cref="Storage"/> array
/// is alive. CamusDB never mutates a stored buffer in place and Kahuna hands out a fresh array per
/// scanned entry (verified for both the disk and in-memory paths), so a view may be retained across scan
/// iterations without its bytes changing — the reference keeps the array rooted. A view must still not be
/// handed to code that assumes an owned/compacted row; retained rows copy the cells they keep.
/// </para>
/// </summary>
internal readonly struct RowView
{
    /// <summary>The KV row identifier, taken from the key — never stored in the payload.</summary>
    public readonly ObjectIdValue RowId;

    private readonly ReadOnlyMemory<byte> storage;
    private readonly CompiledRowCodec codec;

    public RowView(ObjectIdValue rowId, ReadOnlyMemory<byte> storage, CompiledRowCodec codec)
    {
        RowId = rowId;
        this.storage = storage;
        this.codec = codec;
    }

    /// <summary>The borrowed payload (envelope already stripped). Backs every accessor.</summary>
    public ReadOnlyMemory<byte> Storage => storage;

    /// <summary>The codec for the stored schema version these bytes were written under.</summary>
    public CompiledRowCodec Codec => codec;

    /// <summary>Runs the one-time frame bounds check; call once before any unchecked typed read.</summary>
    public void ValidateFrame() => codec.ValidateFrame(storage.Span);

    public bool IsNull(int storedOrdinal) => codec.IsNull(storage.Span, storedOrdinal);

    public long GetInt64(int storedOrdinal) => codec.GetInt64(storage.Span, storedOrdinal);

    public double GetDouble(int storedOrdinal) => codec.GetDouble(storage.Span, storedOrdinal);

    public float GetFloat(int storedOrdinal) => codec.GetFloat(storage.Span, storedOrdinal);

    public bool GetBool(int storedOrdinal) => codec.GetBool(storage.Span, storedOrdinal);

    public ObjectIdValue GetId(int storedOrdinal) => codec.GetId(storage.Span, storedOrdinal);

    public (long High, long Low) GetUuid(int storedOrdinal) => codec.GetUuid(storage.Span, storedOrdinal);

    /// <summary>Raw UTF-8 / bytes slice of a variable column (zero-copy). Empty for NULL or empty.</summary>
    public ReadOnlySpan<byte> GetVariableSlice(int storedOrdinal) => codec.GetVariableSlice(storage.Span, storedOrdinal);

    /// <summary>Materializes the column as a <see cref="ValueSlot"/> (strings/bytes/arrays allocate here).</summary>
    public ValueSlot GetSlot(int storedOrdinal) => codec.GetSlot(storage.Span, storedOrdinal);
}
