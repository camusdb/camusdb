/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.IO;
using System.Text;
using System.Buffers.Binary;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;

/// <summary>
/// Schema-less binary codec for <see cref="QueryResultRow"/>.
///
/// Each record is framed as <c>[int32 byteLen][payload]</c> where
/// <c>payload = [12-byte RowId][int32 columnCount][col…]</c>
/// and each column is <c>[int32 nameUtf8Len][utf8 name][1-byte ColumnType tag][value bytes]</c>.
///
/// Value bytes per type:
/// <list type="bullet">
/// <item>Null — nothing</item>
/// <item>Id — 12 bytes (three little-endian int32)</item>
/// <item>Integer64, Date, DateTime — 8 bytes (little-endian int64)</item>
/// <item>Float64 — 8 bytes (little-endian double)</item>
/// <item>Float32 — 4 bytes (little-endian float)</item>
/// <item>Bool — 1 byte (0 or 1)</item>
/// <item>String — [int32 utf8Len][utf8 bytes]</item>
/// <item>Bytes — [int32 len][raw bytes]</item>
/// <item>Uuid, Numeric — 16 bytes: <c>UuidHigh</c> then <c>LongValue</c> (little-endian int64 each)</item>
/// <item>Array — [1-byte elementType][int32 count][element…] where each
///   element is [1-byte isNull flag: 0=null, 1=present][value bytes for elementType if present]</item>
/// </list>
///
/// A top-level cell and an array element write the same value bytes through one scalar switch per
/// operation (write, read, measure). A new <see cref="ColumnType"/> member must get an arm in each of
/// the three, or a query that carries the type fails when it spills. <c>TestColumnTypeCoverage</c>
/// round-trips every member, alone and as an array element, and fails on a member with no arm.
///
/// Column ordering is preserved on decode (insertion order of the source dictionary is maintained).
/// Callers that depend on positional column order must not rely on dictionary enumeration order from
/// other sources.
///
/// <para>
/// <b>Allocation:</b> the stream-writing encoders serialize each record into a stack buffer (small
/// frames) or a pooled <see cref="ArrayPool{T}"/> buffer (large frames) and write it synchronously,
/// so steady-state spill writing allocates no per-row frame array. The wire format is byte-identical
/// to the earlier per-row-<c>new byte[]</c> implementation — spill files remain compatible.
/// </para>
/// </summary>
public static class SpillRowCodec
{
    /// <summary>
    /// Frames at or below this size are serialized on the stack; larger frames rent from
    /// <see cref="ArrayPool{T}"/>. Kept a small compile-time constant so the <c>stackalloc</c> can
    /// never be driven past a safe bound by a wide row — the pool covers everything above it.
    /// </summary>
    private const int StackFrameThreshold = 512;

    // ──────────────────────────────────────────────────────────────────────────
    // Encode
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Encodes <paramref name="row"/> as a framed record: <c>[int32 payloadLen][payload]</c>.
    /// Allocates the returned array; the hot path is <see cref="EncodeToStream"/>, which does not.
    /// </summary>
    public static byte[] Encode(QueryResultRow row)
    {
        int payloadSize = MeasurePayload(row);
        byte[] frame = new byte[4 + payloadSize];
        int pos = 0;

        WriteInt32(frame, payloadSize, ref pos);
        WritePayload(frame, row, ref pos);

        return frame;
    }

    /// <summary>
    /// Appends the framed record to <paramref name="stream"/> without allocating a per-row frame
    /// array: the frame is built in a stack buffer (small rows) or a pooled buffer (large rows) and
    /// written synchronously.
    /// </summary>
    public static void EncodeToStream(Stream stream, QueryResultRow row)
    {
        int payloadSize = MeasurePayload(row);
        int total = 4 + payloadSize;

        byte[]? rented = null;
        Span<byte> frame = total <= StackFrameThreshold
            ? stackalloc byte[total]
            : (rented = ArrayPool<byte>.Shared.Rent(total)).AsSpan(0, total);
        try
        {
            int pos = 0;
            WriteInt32(frame, payloadSize, ref pos);
            WritePayload(frame, row, ref pos);
            stream.Write(frame);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Decode
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Reads one record from <paramref name="data"/> starting at <paramref name="offset"/>,
    /// advancing <paramref name="offset"/> past the record.
    /// </summary>
    public static QueryResultRow Decode(byte[] data, ref int offset)
    {
        int payloadLen = ReadInt32(data, ref offset);
        if (offset + payloadLen > data.Length)
            throw new InvalidDataException(
                $"SpillRowCodec: truncated record — expected {payloadLen} payload bytes but only {data.Length - offset} available");

        int start = offset;
        QueryResultRow row = ReadPayload(data, ref offset);

        if (offset != start + payloadLen)
            throw new InvalidDataException(
                $"SpillRowCodec: payload length mismatch — declared {payloadLen}, consumed {offset - start}");

        return row;
    }

    /// <summary>
    /// Decodes a row from a pre-read payload span (no leading frame-length prefix).
    /// Used by <see cref="SpillRunReader"/>, which strips the 4-byte length header from
    /// the stream separately and reads exactly that many bytes into a reusable buffer — so the
    /// span may cover only a prefix of a larger backing array. Strings and byte arrays are copied
    /// out into owned <see cref="ColumnValue"/> storage, so the decoded row keeps no reference into
    /// <paramref name="payload"/> and the caller may immediately reuse the buffer.
    /// </summary>
    public static QueryResultRow DecodePayload(ReadOnlySpan<byte> payload)
    {
        int offset = 0;
        return ReadPayload(payload, ref offset);
    }

    /// <summary>Decodes all framed records from <paramref name="data"/>.</summary>
    public static List<QueryResultRow> DecodeAll(byte[] data)
    {
        var result = new List<QueryResultRow>();
        int offset = 0;
        while (offset < data.Length)
            result.Add(Decode(data, ref offset));
        return result;
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Payload write
    // ──────────────────────────────────────────────────────────────────────────

    private static void WritePayload(Span<byte> buf, QueryResultRow row, ref int pos)
    {
        WriteRowId(buf, row.RowId, ref pos);

        IReadOnlyDictionary<string, ColumnValue> columns = row.Row;
        WriteInt32(buf, columns.Count, ref pos);

        foreach (KeyValuePair<string, ColumnValue> kv in columns)
        {
            WriteStringUtf8(buf, kv.Key, ref pos);
            WriteColumnValue(buf, kv.Value, ref pos);
        }
    }

    private static void WriteRowId(Span<byte> buf, ObjectIdValue id, ref int pos)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buf[pos..], id.a); pos += 4;
        BinaryPrimitives.WriteInt32LittleEndian(buf[pos..], id.b); pos += 4;
        BinaryPrimitives.WriteInt32LittleEndian(buf[pos..], id.c); pos += 4;
    }

    private static void WriteColumnValue(Span<byte> buf, ColumnValue cv, ref int pos)
    {
        buf[pos++] = (byte)cv.Type;

        switch (cv.Type)
        {
            case ColumnType.Null:
                break;

            case ColumnType.Array:
            {
                buf[pos++] = (byte)cv.ArrayElementType;
                IReadOnlyList<ColumnValue> elements = cv.ArrayValues ?? [];
                WriteInt32(buf, elements.Count, ref pos);
                foreach (ColumnValue el in elements)
                    WriteArrayElement(buf, cv.ArrayElementType, el, ref pos);
                break;
            }

            default:
                WriteScalar(buf, cv.Type, cv, ref pos);
                break;
        }
    }

    private static void WriteArrayElement(Span<byte> buf, ColumnType elementType, ColumnValue el, ref int pos)
    {
        if (el.Type == ColumnType.Null)
        {
            buf[pos++] = 0; // isNull flag
            return;
        }

        buf[pos++] = 1; // isNull flag (non-null)
        WriteScalar(buf, elementType, el, ref pos);
    }

    /// <summary>
    /// Writes the value bytes of one non-null scalar, with no tag. A top-level cell and an array
    /// element share this switch, so a type is supported in both places or in neither.
    /// <see cref="ReadScalar"/> and <see cref="MeasureScalar"/> hold the matching arms.
    /// </summary>
    private static void WriteScalar(Span<byte> buf, ColumnType type, ColumnValue cv, ref int pos)
    {
        switch (type)
        {
            case ColumnType.Id:
            case ColumnType.String:
                WriteStringUtf8(buf, cv.StrValue!, ref pos);
                break;

            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                BinaryPrimitives.WriteInt64LittleEndian(buf[pos..], cv.LongValue);
                pos += 8;
                break;

            case ColumnType.Float64:
                BinaryPrimitives.WriteDoubleLittleEndian(buf[pos..], cv.FloatValue);
                pos += 8;
                break;

            case ColumnType.Float32:
                BinaryPrimitives.WriteSingleLittleEndian(buf[pos..], (float)cv.FloatValue);
                pos += 4;
                break;

            case ColumnType.Bool:
                buf[pos++] = cv.BoolValue ? (byte)1 : (byte)0;
                break;

            case ColumnType.Bytes:
            {
                byte[] bytes = cv.BytesValue ?? [];
                WriteInt32(buf, bytes.Length, ref pos);
                bytes.CopyTo(buf[pos..]);
                pos += bytes.Length;
                break;
            }

            case ColumnType.Uuid:
            case ColumnType.Numeric:
                BinaryPrimitives.WriteInt64LittleEndian(buf[pos..], cv.UuidHigh);
                BinaryPrimitives.WriteInt64LittleEndian(buf[(pos + 8)..], cv.LongValue);
                pos += 16;
                break;

            default:
                throw new InvalidOperationException($"SpillRowCodec: unsupported ColumnType {type}");
        }
    }

    private static void WriteStringUtf8(Span<byte> buf, string s, ref int pos)
    {
        int byteCount = Encoding.UTF8.GetByteCount(s);
        WriteInt32(buf, byteCount, ref pos);
        Encoding.UTF8.GetBytes(s, buf[pos..]);
        pos += byteCount;
    }

    private static void WriteInt32(Span<byte> buf, int value, ref int pos)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buf[pos..], value);
        pos += 4;
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Payload read
    // ──────────────────────────────────────────────────────────────────────────

    private static QueryResultRow ReadPayload(ReadOnlySpan<byte> data, ref int pos)
    {
        ObjectIdValue rowId = ReadRowId(data, ref pos);
        int count = ReadInt32(data, ref pos);

        var row = new Dictionary<string, ColumnValue>(count, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < count; i++)
        {
            string name = ReadStringUtf8(data, ref pos);
            ColumnValue cv = ReadColumnValue(data, ref pos);
            row[name] = cv;
        }

        return new QueryResultRow(rowId, row);
    }

    private static ObjectIdValue ReadRowId(ReadOnlySpan<byte> data, ref int pos)
    {
        int a = BinaryPrimitives.ReadInt32LittleEndian(data[pos..]); pos += 4;
        int b = BinaryPrimitives.ReadInt32LittleEndian(data[pos..]); pos += 4;
        int c = BinaryPrimitives.ReadInt32LittleEndian(data[pos..]); pos += 4;
        return new ObjectIdValue(a, b, c);
    }

    private static ColumnValue ReadColumnValue(ReadOnlySpan<byte> data, ref int pos)
    {
        ColumnType type = (ColumnType)data[pos++];

        switch (type)
        {
            case ColumnType.Null:
                return ColumnValue.Null;

            case ColumnType.Array:
            {
                ColumnType elementType = (ColumnType)data[pos++];
                int count = ReadInt32(data, ref pos);
                var elements = new List<ColumnValue>(count);
                for (int i = 0; i < count; i++)
                    elements.Add(ReadArrayElement(elementType, data, ref pos));
                return ColumnValue.FromArray(elementType, elements);
            }

            default:
                return ReadScalar(type, data, ref pos);
        }
    }

    private static ColumnValue ReadArrayElement(ColumnType elementType, ReadOnlySpan<byte> data, ref int pos)
    {
        byte isNonNull = data[pos++];
        if (isNonNull == 0)
            return ColumnValue.Null;

        return ReadScalar(elementType, data, ref pos);
    }

    /// <summary>
    /// Reads the value bytes of one non-null scalar written by <see cref="WriteScalar"/>. Strings and
    /// byte arrays are copied out, so the value keeps no reference into <paramref name="data"/>.
    /// </summary>
    private static ColumnValue ReadScalar(ColumnType type, ReadOnlySpan<byte> data, ref int pos)
    {
        switch (type)
        {
            case ColumnType.Id:
            case ColumnType.String:
                return new ColumnValue(type, ReadStringUtf8(data, ref pos));

            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
            {
                long v = BinaryPrimitives.ReadInt64LittleEndian(data[pos..]); pos += 8;
                return new ColumnValue(type, v);
            }

            case ColumnType.Float64:
            {
                double v = BinaryPrimitives.ReadDoubleLittleEndian(data[pos..]); pos += 8;
                return new ColumnValue(ColumnType.Float64, v);
            }

            case ColumnType.Float32:
            {
                float v = BinaryPrimitives.ReadSingleLittleEndian(data[pos..]); pos += 4;
                return new ColumnValue(ColumnType.Float32, (double)v);
            }

            case ColumnType.Bool:
                return ColumnValue.FromBool(data[pos++] != 0);

            case ColumnType.Bytes:
            {
                int len = ReadInt32(data, ref pos);
                byte[] bytes = new byte[len];
                data.Slice(pos, len).CopyTo(bytes);
                pos += len;
                return new ColumnValue(bytes);
            }

            case ColumnType.Uuid:
            case ColumnType.Numeric:
            {
                long high = BinaryPrimitives.ReadInt64LittleEndian(data[pos..]);
                long low = BinaryPrimitives.ReadInt64LittleEndian(data[(pos + 8)..]);
                pos += 16;
                return new ColumnValue(type, high, low);
            }

            default:
                throw new InvalidDataException($"SpillRowCodec: unknown ColumnType tag {(int)type}");
        }
    }

    private static string ReadStringUtf8(ReadOnlySpan<byte> data, ref int pos)
    {
        int len = ReadInt32(data, ref pos);
        string s = Encoding.UTF8.GetString(data.Slice(pos, len));
        pos += len;
        return s;
    }

    private static int ReadInt32(ReadOnlySpan<byte> data, ref int pos)
    {
        int v = BinaryPrimitives.ReadInt32LittleEndian(data[pos..]);
        pos += 4;
        return v;
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Layout-header (value-only) encode / decode
    //
    // When all rows in a spill run are QueryRow, column names are written once
    // per run (carried in the SpillRun record, not in the file itself) and each
    // row record contains only RowId + values — no per-row name strings.
    // Wire format per record: [int32 payloadLen][12-byte RowId][value0]…[valueN-1].
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Appends a value-only framed record to <paramref name="stream"/>, without a per-row frame
    /// array (stack buffer for small rows, pooled buffer for large ones).
    /// Encodes only the RowId and column values (in layout ordinal order) — column names are
    /// omitted because the <see cref="RowLayout"/> is carried alongside the run path and passed
    /// to <see cref="DecodeValueOnlyPayload"/> at read time.
    /// </summary>
    public static void EncodeValueOnlyToStream(Stream stream, QueryRow row)
    {
        int payloadSize = MeasureValueOnlyPayload(row);
        int total = 4 + payloadSize;

        byte[]? rented = null;
        Span<byte> frame = total <= StackFrameThreshold
            ? stackalloc byte[total]
            : (rented = ArrayPool<byte>.Shared.Rent(total)).AsSpan(0, total);
        try
        {
            int pos = 0;
            WriteInt32(frame, payloadSize, ref pos);
            WriteRowId(frame, row.RowId, ref pos);
            for (int i = 0; i < row.Values.Length; i++)
                WriteColumnValue(frame, row.Values[i], ref pos);
            stream.Write(frame);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Decodes a value-only payload (produced by <see cref="EncodeValueOnlyToStream"/>) using
    /// the supplied <paramref name="layout"/> to reconstruct column names and ordinal positions.
    /// Returns a <see cref="QueryRow"/> that shares <paramref name="layout"/> with every other
    /// row in the same run. Strings and byte arrays are copied out, so the row keeps no reference
    /// into <paramref name="payload"/> and the caller may immediately reuse the buffer.
    /// </summary>
    public static QueryRow DecodeValueOnlyPayload(ReadOnlySpan<byte> payload, RowLayout layout)
    {
        int pos = 0;
        ObjectIdValue rowId = ReadRowId(payload, ref pos);

        ColumnValue[] values = new ColumnValue[layout.Count];
        for (int i = 0; i < layout.Count; i++)
            values[i] = ReadColumnValue(payload, ref pos);

        return new QueryRow(rowId, layout, values);
    }

    private static int MeasureValueOnlyPayload(QueryRow row)
    {
        int size = 12; // RowId
        for (int i = 0; i < row.Values.Length; i++)
            size += MeasureColumnValue(row.Values[i]);
        return size;
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Size measurement
    // ──────────────────────────────────────────────────────────────────────────

    private static int MeasurePayload(QueryResultRow row)
    {
        int size = 12; // RowId
        size += 4;     // column count

        foreach (KeyValuePair<string, ColumnValue> kv in row.Row)
        {
            int nameBytes = Encoding.UTF8.GetByteCount(kv.Key);
            size += 4 + nameBytes;       // nameLen + name
            size += MeasureColumnValue(kv.Value);
        }

        return size;
    }

    private static int MeasureColumnValue(ColumnValue cv)
    {
        int size = 1; // type tag

        switch (cv.Type)
        {
            case ColumnType.Null:
                break;

            case ColumnType.Array:
            {
                size += 1; // elementType
                size += 4; // count
                IReadOnlyList<ColumnValue> elements = cv.ArrayValues ?? [];
                foreach (ColumnValue el in elements)
                    size += MeasureArrayElement(cv.ArrayElementType, el);
                break;
            }

            default:
                size += MeasureScalar(cv.Type, cv);
                break;
        }

        return size;
    }

    private static int MeasureArrayElement(ColumnType elementType, ColumnValue el)
    {
        int size = 1; // isNull flag
        if (el.Type == ColumnType.Null)
            return size;

        return size + MeasureScalar(elementType, el);
    }

    /// <summary>The number of bytes <see cref="WriteScalar"/> writes for one non-null scalar.</summary>
    private static int MeasureScalar(ColumnType type, ColumnValue cv)
    {
        return type switch
        {
            ColumnType.Id or ColumnType.String => 4 + Encoding.UTF8.GetByteCount(cv.StrValue!),
            ColumnType.Integer64 or ColumnType.Date or ColumnType.DateTime or ColumnType.Float64 => 8,
            ColumnType.Float32 => 4,
            ColumnType.Bool => 1,
            ColumnType.Bytes => 4 + (cv.BytesValue?.Length ?? 0),
            ColumnType.Uuid or ColumnType.Numeric => 16,
            _ => throw new InvalidOperationException($"SpillRowCodec: unsupported ColumnType {type}"),
        };
    }
}
