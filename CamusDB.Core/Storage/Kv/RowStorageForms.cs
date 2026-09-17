/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
using System.IO.Hashing;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// The storage-form extension of the positional row layout: how a row records that a variable-length
/// cell is compressed, or holds a pointer to a value stored under its own key, and the schema-free
/// helpers that read and rewrite those marks.
///
/// <para><b>Layout.</b> Bit 31 of the leading schema-version word says whether the row carries a
/// storage-form trailer. The other 31 bits are the stored schema version, unchanged. A row with no
/// compressed and no out-of-line cell keeps bit 31 clear and has no trailer, so it is byte-identical
/// to a row written before this extension and grows by zero bytes. A row with the bit set appends,
/// after the variable area:</para>
/// <code>
///   outOfLineBitmap  : ceil(V / 8) bytes — set = the cell holds an out-of-line pointer
///   compressedBitmap : ceil(V / 8) bytes — set = the cell's stored bytes are an LZ4 block
///   V                : u32 — the variable column count of the row's layout
///   offsetsOffset    : u32 — payload offset of the variable end-offset directory
/// </code>
/// <para>The trailer goes at the end, not after the version word, so every fixed offset and every
/// variable offset of the compiled layout stays exactly where it is: a reader of an unmarked cell
/// does no extra arithmetic. The last two fields make the trailer self-describing, so a store can
/// find and resolve marked cells without loading the table schema.</para>
///
/// <para><b>Cell forms.</b> The two bits are independent. A compressed inline cell stores a
/// <c>u32</c> uncompressed length followed by the LZ4 block. An out-of-line cell stores a fixed
/// 16-byte pointer — <c>u32</c> uncompressed length, <c>u32</c> stored length, <c>u64</c> XxHash64 of
/// the stored bytes — and the stored bytes (raw, or an LZ4 block when the compressed bit is also set)
/// live under the key <see cref="KvKeyBuilder.BuildLargeValueKey"/> derives from the row id and the
/// cell's variable ordinal. The checksum lets a read that saw a row and its large value at two
/// different moments detect the mismatch and retry, instead of pairing a new value with an old row.</para>
///
/// <para><b>Version compatibility.</b> A binary that predates this extension cannot read a row with
/// bit 31 set. Rows with the bit clear are unchanged, so old data needs no migration.</para>
/// </summary>
internal static class RowStorageForms
{
    /// <summary>Bit 31 of the schema-version word: the row carries a storage-form trailer.</summary>
    internal const uint TrailerFlag = 0x8000_0000u;

    /// <summary>Mask that recovers the stored schema version from the schema-version word.</summary>
    internal const uint VersionMask = 0x7FFF_FFFFu;

    /// <summary>Bytes of the fixed tail of the trailer (<c>V</c> plus <c>offsetsOffset</c>).</summary>
    internal const int TrailerTailSize = 8;

    /// <summary>Bytes of an out-of-line pointer cell.</summary>
    internal const int PointerSize = 16;

    /// <summary>Bytes of the uncompressed-length prefix of a compressed inline cell.</summary>
    internal const int CompressedPrefixSize = 4;

    /// <summary>
    /// Highest variable ordinal that may move out of line. The ordinal is written into the key as four
    /// hex digits; a column past this bound keeps its value inline.
    /// </summary>
    internal const int MaxOutOfLineOrdinal = 0xFFFF;

    /// <summary>True when the row payload carries a storage-form trailer (bit 31 of the version word is set).</summary>
    internal static bool HasTrailer(ReadOnlySpan<byte> payload) => payload.Length >= 4 && (payload[3] & 0x80) != 0;

    /// <summary>The stored schema version with the trailer flag removed.</summary>
    internal static int StoredVersion(uint versionWord) => (int)(versionWord & VersionMask);

    /// <summary>
    /// Geometry of a row that carries a trailer, as plain offsets so it can be kept across an await.
    /// Every offset is absolute within the payload (the envelope byte already stripped).
    /// </summary>
    internal readonly record struct TrailerLayout(
        int VariableCount,
        int OffsetsOffset,
        int VariableAreaOffset,
        int OutOfLineBitmapOffset,
        int CompressedBitmapOffset,
        int BitmapBytes)
    {
        internal bool IsOutOfLine(ReadOnlySpan<byte> payload, int variableOrdinal) =>
            (payload[OutOfLineBitmapOffset + (variableOrdinal >> 3)] & (1 << (variableOrdinal & 7))) != 0;

        internal bool IsCompressed(ReadOnlySpan<byte> payload, int variableOrdinal) =>
            (payload[CompressedBitmapOffset + (variableOrdinal >> 3)] & (1 << (variableOrdinal & 7))) != 0;

        internal bool IsMarked(ReadOnlySpan<byte> payload, int variableOrdinal) =>
            IsOutOfLine(payload, variableOrdinal) || IsCompressed(payload, variableOrdinal);

        /// <summary>The stored bytes of one variable cell: raw, a compressed inline cell, or a pointer.</summary>
        internal ReadOnlySpan<byte> Cell(ReadOnlySpan<byte> payload, int variableOrdinal)
        {
            int start = variableOrdinal == 0 ? 0 : (int)BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(OffsetsOffset + (variableOrdinal - 1) * 4, 4));
            int end = (int)BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(OffsetsOffset + variableOrdinal * 4, 4));
            return payload.Slice(VariableAreaOffset + start, end - start);
        }
    }

    /// <summary>
    /// Reads the trailer geometry of a marked row and checks it for internal consistency. Throws
    /// <see cref="CamusDBErrorCodes.SystemSpaceCorrupt"/> when the trailer does not describe the
    /// payload it ends.
    /// </summary>
    internal static TrailerLayout ReadLayout(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 4 + TrailerTailSize)
            throw Corrupt($"row payload of {payload.Length} bytes is too short for a storage-form trailer");

        int tail = payload.Length - TrailerTailSize;
        uint variableCount = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(tail, 4));
        uint offsetsOffset = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(tail + 4, 4));

        if (variableCount == 0 || variableCount > int.MaxValue / 4)
            throw Corrupt($"storage-form trailer records {variableCount} variable columns");

        int bitmapBytes = (int)((variableCount + 7) / 8);
        long variableAreaOffset = (long)offsetsOffset + variableCount * 4L;
        long outOfLineBitmapOffset = (long)tail - 2L * bitmapBytes;

        if (offsetsOffset < 4 || variableAreaOffset > outOfLineBitmapOffset)
            throw Corrupt("storage-form trailer does not fit the row payload");

        int count = (int)variableCount;
        long lastEnd = BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice((int)offsetsOffset + (count - 1) * 4, 4));
        if (variableAreaOffset + lastEnd != outOfLineBitmapOffset)
            throw Corrupt($"variable area end {variableAreaOffset + lastEnd} does not meet the storage-form trailer at {outOfLineBitmapOffset}");

        return new TrailerLayout(
            count,
            (int)offsetsOffset,
            (int)variableAreaOffset,
            (int)outOfLineBitmapOffset,
            (int)outOfLineBitmapOffset + bitmapBytes,
            bitmapBytes);
    }

    /// <summary>Byte length of a trailer for <paramref name="variableCount"/> variable columns.</summary>
    internal static int TrailerSize(int variableCount) => 2 * ((variableCount + 7) / 8) + TrailerTailSize;

    /// <summary>A decoded out-of-line pointer cell.</summary>
    internal readonly record struct Pointer(int RawLength, int StoredLength, ulong Checksum);

    internal static Pointer ReadPointer(ReadOnlySpan<byte> cell)
    {
        if (cell.Length != PointerSize)
            throw Corrupt($"out-of-line pointer cell is {cell.Length} bytes, expected {PointerSize}");

        return new Pointer(
            (int)BinaryPrimitives.ReadUInt32LittleEndian(cell),
            (int)BinaryPrimitives.ReadUInt32LittleEndian(cell[4..]),
            BinaryPrimitives.ReadUInt64LittleEndian(cell[8..]));
    }

    internal static void WritePointer(Span<byte> dest, int rawLength, ReadOnlySpan<byte> stored)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(dest, (uint)rawLength);
        BinaryPrimitives.WriteUInt32LittleEndian(dest[4..], (uint)stored.Length);
        BinaryPrimitives.WriteUInt64LittleEndian(dest[8..], XxHash64.HashToUInt64(stored));
    }

    /// <summary>True when <paramref name="stored"/> is the exact value the pointer names.</summary>
    internal static bool Matches(Pointer pointer, ReadOnlySpan<byte> stored) =>
        stored.Length == pointer.StoredLength && XxHash64.HashToUInt64(stored) == pointer.Checksum;

    /// <summary>
    /// Variable ordinals of every out-of-line cell of a stored row, or null when it has none. The
    /// delete and update paths use this to name the keys to remove without fetching the values.
    /// </summary>
    internal static List<int>? OutOfLineOrdinals(ReadOnlySpan<byte> payload)
    {
        if (!HasTrailer(payload))
            return null;

        TrailerLayout layout = ReadLayout(payload);
        List<int>? ordinals = null;
        for (int v = 0; v < layout.VariableCount; v++)
        {
            if (layout.IsOutOfLine(payload, v))
                (ordinals ??= []).Add(v);
        }

        return ordinals;
    }

    /// <summary>
    /// The decoded size of every marked cell of a stored row, read from the lengths the row records: the
    /// length prefix of a compressed inline cell and the raw length of a pointer. Nothing is fetched or
    /// decompressed. Zero for a row with no trailer. Readers use it to bound how many decoded bytes one
    /// resolution holds; it is an upper bound on what a resolution of a subset of the cells allocates.
    /// </summary>
    internal static long MarkedRawBytes(ReadOnlySpan<byte> payload)
    {
        if (!HasTrailer(payload))
            return 0;

        TrailerLayout layout = ReadLayout(payload);
        long total = 0;
        for (int v = 0; v < layout.VariableCount; v++)
        {
            if (!layout.IsMarked(payload, v))
                continue;

            ReadOnlySpan<byte> cell = layout.Cell(payload, v);
            if (cell.Length >= sizeof(uint))
                total += BinaryPrimitives.ReadUInt32LittleEndian(cell);
        }

        return total;
    }

    /// <summary>
    /// Returns the raw value bytes of one marked cell when they can be produced without I/O: a
    /// compressed inline cell is decompressed here. An out-of-line cell needs its fetched stored bytes,
    /// passed as <paramref name="fetched"/>; the bytes are checked against the pointer first and a
    /// mismatch returns null so the caller can retry the read.
    /// </summary>
    internal static byte[]? ResolveCell(ReadOnlySpan<byte> payload, in TrailerLayout layout, int variableOrdinal, ReadOnlySpan<byte> fetched)
    {
        ReadOnlySpan<byte> cell = layout.Cell(payload, variableOrdinal);
        bool compressed = layout.IsCompressed(payload, variableOrdinal);

        if (layout.IsOutOfLine(payload, variableOrdinal))
        {
            Pointer pointer = ReadPointer(cell);
            if (!Matches(pointer, fetched))
                return null;

            return compressed
                ? LargeValueCompression.Decompress(fetched, pointer.RawLength)
                : fetched.ToArray();
        }

        if (cell.Length < CompressedPrefixSize)
            throw new CamusDBException(CamusDBErrorCodes.LargeValueCorrupt, $"compressed cell is {cell.Length} bytes, shorter than its length prefix");

        int rawLength = (int)BinaryPrimitives.ReadUInt32LittleEndian(cell);
        return LargeValueCompression.Decompress(cell[CompressedPrefixSize..], rawLength);
    }

    /// <summary>
    /// Rebuilds a marked row with some cells replaced by their raw values. <paramref name="replacements"/>
    /// has one entry per variable ordinal; a non-null entry becomes a plain cell with its marks cleared,
    /// and a null entry keeps the stored cell and its marks. The trailer is dropped, and bit 31 cleared,
    /// when no marked cell remains — the result is then byte-identical to an unmarked row, so any reader,
    /// including a binary that predates this extension, can decode it.
    /// </summary>
    internal static byte[] Rebuild(ReadOnlySpan<byte> payload, in TrailerLayout layout, byte[]?[] replacements)
    {
        int count = layout.VariableCount;
        long variableSize = 0;
        bool anyMarkLeft = false;

        for (int v = 0; v < count; v++)
        {
            if (replacements[v] is byte[] raw)
            {
                variableSize += raw.Length;
                continue;
            }

            variableSize += layout.Cell(payload, v).Length;
            anyMarkLeft |= layout.IsMarked(payload, v);
        }

        long total = layout.VariableAreaOffset + variableSize + (anyMarkLeft ? TrailerSize(count) : 0);
        if (total > Array.MaxLength)
            throw Corrupt($"resolved row would be {total} bytes");

        byte[] output = new byte[total];
        Span<byte> dest = output;

        // Fixed prefix (version word, bitmaps, fixed area) is copied as is; the offset directory is rewritten below.
        payload[..layout.OffsetsOffset].CopyTo(dest);

        uint versionWord = BinaryPrimitives.ReadUInt32LittleEndian(payload);
        versionWord = anyMarkLeft ? versionWord | TrailerFlag : versionWord & VersionMask;
        BinaryPrimitives.WriteUInt32LittleEndian(dest, versionWord);

        int cursor = 0;
        for (int v = 0; v < count; v++)
        {
            ReadOnlySpan<byte> source = replacements[v] is byte[] raw ? raw : layout.Cell(payload, v);
            source.CopyTo(dest[(layout.VariableAreaOffset + cursor)..]);
            cursor += source.Length;
            BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(layout.OffsetsOffset + v * 4, 4), (uint)cursor);
        }

        if (anyMarkLeft)
        {
            int trailer = layout.VariableAreaOffset + cursor;
            int bitmapBytes = layout.BitmapBytes;
            Span<byte> outOfLine = dest.Slice(trailer, bitmapBytes);
            Span<byte> compressedBits = dest.Slice(trailer + bitmapBytes, bitmapBytes);

            for (int v = 0; v < count; v++)
            {
                if (replacements[v] is not null)
                    continue;

                if (layout.IsOutOfLine(payload, v))
                    outOfLine[v >> 3] |= (byte)(1 << (v & 7));
                if (layout.IsCompressed(payload, v))
                    compressedBits[v >> 3] |= (byte)(1 << (v & 7));
            }

            int tail = trailer + 2 * bitmapBytes;
            BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(tail, 4), (uint)count);
            BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(tail + 4, 4), (uint)layout.OffsetsOffset);
        }

        return output;
    }

    private static CamusDBException Corrupt(string message) => new(CamusDBErrorCodes.SystemSpaceCorrupt, message);
}
