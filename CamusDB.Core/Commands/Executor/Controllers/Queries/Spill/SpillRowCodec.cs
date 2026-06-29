/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using System.Text;
using System.Buffers.Binary;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
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
/// <item>Array — [1-byte elementType][int32 count][element…] where each
///   element is [1-byte isNull flag: 0=null, 1=present][value bytes for elementType if present]</item>
/// </list>
///
/// Column ordering is preserved on decode (insertion order of the source dictionary is maintained).
/// Callers that depend on positional column order must not rely on dictionary enumeration order from
/// other sources.
/// </summary>
public static class SpillRowCodec
{
    // ──────────────────────────────────────────────────────────────────────────
    // Encode
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Encodes <paramref name="row"/> as a framed record: <c>[int32 payloadLen][payload]</c>.
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

    /// <summary>Appends the framed record to <paramref name="stream"/>.</summary>
    public static void EncodeToStream(Stream stream, QueryResultRow row)
    {
        byte[] frame = Encode(row);
        stream.Write(frame, 0, frame.Length);
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

    private static void WritePayload(byte[] buf, QueryResultRow row, ref int pos)
    {
        WriteRowId(buf, row.RowId, ref pos);

        Dictionary<string, ColumnValue> columns = row.Row;
        WriteInt32(buf, columns.Count, ref pos);

        foreach (KeyValuePair<string, ColumnValue> kv in columns)
        {
            byte[] nameBytes = Encoding.UTF8.GetBytes(kv.Key);
            WriteInt32(buf, nameBytes.Length, ref pos);
            Buffer.BlockCopy(nameBytes, 0, buf, pos, nameBytes.Length);
            pos += nameBytes.Length;

            WriteColumnValue(buf, kv.Value, ref pos);
        }
    }

    private static void WriteRowId(byte[] buf, ObjectIdValue id, ref int pos)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), id.a); pos += 4;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), id.b); pos += 4;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), id.c); pos += 4;
    }

    private static void WriteColumnValue(byte[] buf, ColumnValue cv, ref int pos)
    {
        buf[pos++] = (byte)cv.Type;

        switch (cv.Type)
        {
            case ColumnType.Null:
                break;

            case ColumnType.Id:
                WriteStringUtf8(buf, cv.StrValue!, ref pos);
                break;

            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), cv.LongValue);
                pos += 8;
                break;

            case ColumnType.Float64:
                BinaryPrimitives.WriteDoubleLittleEndian(buf.AsSpan(pos), cv.FloatValue);
                pos += 8;
                break;

            case ColumnType.Float32:
                BinaryPrimitives.WriteSingleLittleEndian(buf.AsSpan(pos), (float)cv.FloatValue);
                pos += 4;
                break;

            case ColumnType.Bool:
                buf[pos++] = cv.BoolValue ? (byte)1 : (byte)0;
                break;

            case ColumnType.String:
                WriteStringUtf8(buf, cv.StrValue!, ref pos);
                break;

            case ColumnType.Bytes:
            {
                byte[] bytes = cv.BytesValue ?? [];
                WriteInt32(buf, bytes.Length, ref pos);
                Buffer.BlockCopy(bytes, 0, buf, pos, bytes.Length);
                pos += bytes.Length;
                break;
            }

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
                throw new InvalidOperationException($"SpillRowCodec: unsupported ColumnType {cv.Type}");
        }
    }

    private static void WriteArrayElement(byte[] buf, ColumnType elementType, ColumnValue el, ref int pos)
    {
        if (el.Type == ColumnType.Null)
        {
            buf[pos++] = 0; // isNull flag
            return;
        }

        buf[pos++] = 1; // isNull flag (non-null)

        switch (elementType)
        {
            case ColumnType.Id:
                WriteStringUtf8(buf, el.StrValue!, ref pos);
                break;

            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(pos), el.LongValue);
                pos += 8;
                break;

            case ColumnType.Float64:
                BinaryPrimitives.WriteDoubleLittleEndian(buf.AsSpan(pos), el.FloatValue);
                pos += 8;
                break;

            case ColumnType.Float32:
                BinaryPrimitives.WriteSingleLittleEndian(buf.AsSpan(pos), (float)el.FloatValue);
                pos += 4;
                break;

            case ColumnType.Bool:
                buf[pos++] = el.BoolValue ? (byte)1 : (byte)0;
                break;

            case ColumnType.String:
                WriteStringUtf8(buf, el.StrValue!, ref pos);
                break;

            case ColumnType.Bytes:
            {
                byte[] bytes = el.BytesValue ?? [];
                WriteInt32(buf, bytes.Length, ref pos);
                Buffer.BlockCopy(bytes, 0, buf, pos, bytes.Length);
                pos += bytes.Length;
                break;
            }

            default:
                throw new InvalidOperationException($"SpillRowCodec: unsupported array element type {elementType}");
        }
    }

    private static void WriteStringUtf8(byte[] buf, string s, ref int pos)
    {
        int byteCount = Encoding.UTF8.GetByteCount(s);
        WriteInt32(buf, byteCount, ref pos);
        Encoding.UTF8.GetBytes(s, buf.AsSpan(pos));
        pos += byteCount;
    }

    private static void WriteInt32(byte[] buf, int value, ref int pos)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(pos), value);
        pos += 4;
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Payload read
    // ──────────────────────────────────────────────────────────────────────────

    private static QueryResultRow ReadPayload(byte[] data, ref int pos)
    {
        ObjectIdValue rowId = ReadRowId(data, ref pos);
        int count = ReadInt32(data, ref pos);

        var row = new Dictionary<string, ColumnValue>(count, StringComparer.Ordinal);
        for (int i = 0; i < count; i++)
        {
            int nameLen = ReadInt32(data, ref pos);
            string name = Encoding.UTF8.GetString(data, pos, nameLen);
            pos += nameLen;

            ColumnValue cv = ReadColumnValue(data, ref pos);
            row[name] = cv;
        }

        return new QueryResultRow(rowId, row);
    }

    private static ObjectIdValue ReadRowId(byte[] data, ref int pos)
    {
        int a = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(pos)); pos += 4;
        int b = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(pos)); pos += 4;
        int c = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(pos)); pos += 4;
        return new ObjectIdValue(a, b, c);
    }

    private static ColumnValue ReadColumnValue(byte[] data, ref int pos)
    {
        ColumnType type = (ColumnType)data[pos++];

        switch (type)
        {
            case ColumnType.Null:
                return ColumnValue.Null;

            case ColumnType.Id:
                return new ColumnValue(ColumnType.Id, ReadStringUtf8(data, ref pos));

            case ColumnType.Integer64:
            {
                long v = BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(pos)); pos += 8;
                return new ColumnValue(ColumnType.Integer64, v);
            }

            case ColumnType.Date:
            {
                long v = BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(pos)); pos += 8;
                return new ColumnValue(ColumnType.Date, v);
            }

            case ColumnType.DateTime:
            {
                long v = BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(pos)); pos += 8;
                return new ColumnValue(ColumnType.DateTime, v);
            }

            case ColumnType.Float64:
            {
                double v = BinaryPrimitives.ReadDoubleLittleEndian(data.AsSpan(pos)); pos += 8;
                return new ColumnValue(ColumnType.Float64, v);
            }

            case ColumnType.Float32:
            {
                float v = BinaryPrimitives.ReadSingleLittleEndian(data.AsSpan(pos)); pos += 4;
                return new ColumnValue(ColumnType.Float32, (double)v);
            }

            case ColumnType.Bool:
                return ColumnValue.FromBool(data[pos++] != 0);

            case ColumnType.String:
                return new ColumnValue(ColumnType.String, ReadStringUtf8(data, ref pos));

            case ColumnType.Bytes:
            {
                int len = ReadInt32(data, ref pos);
                byte[] bytes = new byte[len];
                Buffer.BlockCopy(data, pos, bytes, 0, len);
                pos += len;
                return new ColumnValue(bytes);
            }

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
                throw new InvalidDataException($"SpillRowCodec: unknown ColumnType tag {(int)type}");
        }
    }

    private static ColumnValue ReadArrayElement(ColumnType elementType, byte[] data, ref int pos)
    {
        byte isNonNull = data[pos++];
        if (isNonNull == 0)
            return ColumnValue.Null;

        switch (elementType)
        {
            case ColumnType.Id:
                return new ColumnValue(ColumnType.Id, ReadStringUtf8(data, ref pos));

            case ColumnType.Integer64:
            {
                long v = BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(pos)); pos += 8;
                return new ColumnValue(ColumnType.Integer64, v);
            }

            case ColumnType.Date:
            {
                long v = BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(pos)); pos += 8;
                return new ColumnValue(ColumnType.Date, v);
            }

            case ColumnType.DateTime:
            {
                long v = BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(pos)); pos += 8;
                return new ColumnValue(ColumnType.DateTime, v);
            }

            case ColumnType.Float64:
            {
                double v = BinaryPrimitives.ReadDoubleLittleEndian(data.AsSpan(pos)); pos += 8;
                return new ColumnValue(ColumnType.Float64, v);
            }

            case ColumnType.Float32:
            {
                float v = BinaryPrimitives.ReadSingleLittleEndian(data.AsSpan(pos)); pos += 4;
                return new ColumnValue(ColumnType.Float32, (double)v);
            }

            case ColumnType.Bool:
                return ColumnValue.FromBool(data[pos++] != 0);

            case ColumnType.String:
                return new ColumnValue(ColumnType.String, ReadStringUtf8(data, ref pos));

            case ColumnType.Bytes:
            {
                int len = ReadInt32(data, ref pos);
                byte[] bytes = new byte[len];
                Buffer.BlockCopy(data, pos, bytes, 0, len);
                pos += len;
                return new ColumnValue(bytes);
            }

            default:
                throw new InvalidDataException($"SpillRowCodec: unknown array element type {(int)elementType}");
        }
    }

    private static string ReadStringUtf8(byte[] data, ref int pos)
    {
        int len = ReadInt32(data, ref pos);
        string s = Encoding.UTF8.GetString(data, pos, len);
        pos += len;
        return s;
    }

    private static int ReadInt32(byte[] data, ref int pos)
    {
        int v = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(pos));
        pos += 4;
        return v;
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

            case ColumnType.Id:
                size += 4 + Encoding.UTF8.GetByteCount(cv.StrValue!);
                break;

            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                size += 8;
                break;

            case ColumnType.Float64:
                size += 8;
                break;

            case ColumnType.Float32:
                size += 4;
                break;

            case ColumnType.Bool:
                size += 1;
                break;

            case ColumnType.String:
                size += 4 + Encoding.UTF8.GetByteCount(cv.StrValue!);
                break;

            case ColumnType.Bytes:
                size += 4 + (cv.BytesValue?.Length ?? 0);
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
                throw new InvalidOperationException($"SpillRowCodec: unsupported ColumnType {cv.Type}");
        }

        return size;
    }

    private static int MeasureArrayElement(ColumnType elementType, ColumnValue el)
    {
        int size = 1; // isNull flag
        if (el.Type == ColumnType.Null)
            return size;

        switch (elementType)
        {
            case ColumnType.Id:
                size += 4 + Encoding.UTF8.GetByteCount(el.StrValue!);
                break;

            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
            case ColumnType.Float64:
                size += 8;
                break;

            case ColumnType.Float32:
                size += 4;
                break;

            case ColumnType.Bool:
                size += 1;
                break;

            case ColumnType.String:
                size += 4 + Encoding.UTF8.GetByteCount(el.StrValue!);
                break;

            case ColumnType.Bytes:
                size += 4 + (el.BytesValue?.Length ?? 0);
                break;

            default:
                throw new InvalidOperationException($"SpillRowCodec: unsupported array element type {elementType}");
        }

        return size;
    }
}
