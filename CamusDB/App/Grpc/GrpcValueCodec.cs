
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
using Google.Protobuf;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;

using CoreColumnType = CamusDB.Core.Catalogs.Models.ColumnType;
using ProtoValue     = CamusDB.Grpc.Value;
using ProtoArray     = CamusDB.Grpc.ArrayValue;
using ProtoColType   = CamusDB.Grpc.ColumnType;
using ProtoNullValue = CamusDB.Grpc.NullValue;

namespace CamusDB.App.Grpc;

/// <summary>
/// Single mapping point between <see cref="ColumnValue"/> and the Protobuf <c>Value</c> message.
/// All twelve <see cref="CoreColumnType"/> values are covered here; no other code may perform the
/// conversion so the REST and gRPC wire formats cannot drift.
///
/// Encoding decisions (compact-raw, matching the REST positional codec):
///   Date / DateTime — raw <c>DateTime.Ticks</c> (UTC) as <c>int64</c>.
///   Uuid — 16 big-endian bytes (high-half || low-half), reconstructed from
///           <see cref="ColumnValue.UuidHigh"/> and <see cref="ColumnValue.LongValue"/>.
///   Id  — carried in <c>id_value</c>, not <c>string_value</c>, so <see cref="CoreColumnType.Id"/>
///          and <see cref="CoreColumnType.String"/> stay distinct across the wire.
///   Array — wraps an <c>ArrayValue</c> that carries the element type so an empty array still
///            round-trips its <see cref="ColumnValue.ArrayElementType"/>.
/// </summary>
public static class GrpcValueCodec
{
    /// <summary>
    /// Converts a <see cref="ColumnValue"/> to its Protobuf <c>Value</c> representation.
    /// A null reference is treated as a typed NULL, mirroring the REST positional encoder
    /// (<c>CompactRowEncoder.EncodeValue</c>), so a row cell that is absent/null never faults.
    /// </summary>
    public static ProtoValue ToProto(ColumnValue? cv)
    {
        if (cv is null)
            return new ProtoValue { NullValue = ProtoNullValue.Unset };

        return cv.Type switch
        {
            CoreColumnType.Null     => new ProtoValue { NullValue = ProtoNullValue.Unset },
            CoreColumnType.Id       => new ProtoValue { IdValue = cv.StrValue ?? "" },
            CoreColumnType.Integer64 => new ProtoValue { Int64Value = cv.LongValue },
            CoreColumnType.String   => new ProtoValue { StringValue = cv.StrValue ?? "" },
            CoreColumnType.Bool     => new ProtoValue { BoolValue = cv.BoolValue },
            CoreColumnType.Float64  => new ProtoValue { Float64Value = cv.FloatValue },
            CoreColumnType.Float32  => new ProtoValue { Float32Value = (float)cv.FloatValue },
            CoreColumnType.Bytes    => new ProtoValue { BytesValue = ByteString.CopyFrom(cv.BytesValue ?? []) },
            CoreColumnType.Date     => new ProtoValue { DateValue = cv.LongValue },
            CoreColumnType.DateTime => new ProtoValue { DatetimeValue = cv.LongValue },
            CoreColumnType.Array    => new ProtoValue { ArrayValue = ToProtoArray(cv) },
            CoreColumnType.Uuid     => new ProtoValue { UuidValue = UuidToBytes(cv.UuidHigh, cv.LongValue) },

            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"GrpcValueCodec: unsupported ColumnType {cv.Type}")
        };
    }

    /// <summary>
    /// Converts an internal <see cref="ValueSlot"/> straight to its Protobuf <c>Value</c> — the
    /// slot-direct sink path: a projected cell of a slot- or view-backed row reaches the wire without
    /// ever materializing a <see cref="ColumnValue"/>. Must produce a message identical to
    /// <see cref="ToProto(ColumnValue?)"/> over <see cref="ValueSlot.ToColumnValue"/>'s result; every
    /// case stays in lockstep with that method.
    /// </summary>
    internal static ProtoValue ToProto(in ValueSlot slot)
    {
        return slot.Type switch
        {
            CoreColumnType.Null     => new ProtoValue { NullValue = ProtoNullValue.Unset },
            CoreColumnType.Id       => new ProtoValue { IdValue = slot.AsString ?? "" },
            CoreColumnType.Integer64 => new ProtoValue { Int64Value = slot.AsLong },
            CoreColumnType.String   => new ProtoValue { StringValue = slot.AsString ?? "" },
            CoreColumnType.Bool     => new ProtoValue { BoolValue = slot.AsBool },
            CoreColumnType.Float64  => new ProtoValue { Float64Value = slot.AsDouble },
            CoreColumnType.Float32  => new ProtoValue { Float32Value = (float)slot.AsDouble },
            CoreColumnType.Bytes    => new ProtoValue { BytesValue = ByteString.CopyFrom(slot.AsBytes ?? []) },
            CoreColumnType.Date     => new ProtoValue { DateValue = slot.AsLong },
            CoreColumnType.DateTime => new ProtoValue { DatetimeValue = slot.AsLong },
            CoreColumnType.Array    => new ProtoValue { ArrayValue = ToProtoArray(in slot) },
            CoreColumnType.Uuid     => new ProtoValue { UuidValue = UuidToBytes(slot.UuidHigh, slot.UuidLow) },

            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"GrpcValueCodec: unsupported ColumnType {slot.Type}")
        };
    }

    /// <summary>
    /// Converts a Protobuf <c>Value</c> message back to a <see cref="ColumnValue"/>.
    /// Returns <see cref="ColumnValue.Null"/> when the oneof is not set.
    /// Throws <see cref="CamusDBException"/> for unrecognised oneof cases.
    /// </summary>
    public static ColumnValue FromProto(ProtoValue v) => v.KindCase switch
    {
        ProtoValue.KindOneofCase.NullValue    => ColumnValue.Null,
        ProtoValue.KindOneofCase.IdValue      => new ColumnValue(CoreColumnType.Id, v.IdValue),
        ProtoValue.KindOneofCase.Int64Value   => new ColumnValue(CoreColumnType.Integer64, v.Int64Value),
        ProtoValue.KindOneofCase.StringValue  => new ColumnValue(CoreColumnType.String, v.StringValue),
        ProtoValue.KindOneofCase.BoolValue    => ColumnValue.FromBool(v.BoolValue),
        ProtoValue.KindOneofCase.Float64Value => new ColumnValue(CoreColumnType.Float64, v.Float64Value),
        ProtoValue.KindOneofCase.Float32Value => new ColumnValue(CoreColumnType.Float32, (double)v.Float32Value),
        ProtoValue.KindOneofCase.BytesValue   => new ColumnValue(v.BytesValue.ToByteArray()),
        ProtoValue.KindOneofCase.DateValue    => new ColumnValue(CoreColumnType.Date, v.DateValue),
        ProtoValue.KindOneofCase.DatetimeValue => new ColumnValue(CoreColumnType.DateTime, v.DatetimeValue),
        ProtoValue.KindOneofCase.ArrayValue   => FromProtoArray(v.ArrayValue),
        ProtoValue.KindOneofCase.UuidValue    => UuidFromBytes(v.UuidValue),
        ProtoValue.KindOneofCase.None         => ColumnValue.Null,

        _ => throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"GrpcValueCodec: unrecognised Value.KindCase {v.KindCase}")
    };

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static ProtoArray ToProtoArray(ColumnValue cv)
    {
        ProtoArray proto = new()
        {
            ElementType = (ProtoColType)(int)cv.ArrayElementType,
        };

        IReadOnlyList<ColumnValue> items = cv.ArrayValues ?? [];
        foreach (ColumnValue item in items)
            proto.Items.Add(ToProto(item));

        return proto;
    }

    private static ProtoArray ToProtoArray(in ValueSlot slot)
    {
        ProtoArray proto = new()
        {
            ElementType = (ProtoColType)(int)slot.ArrayElementType,
        };

        ValueSlot[] elements = slot.ArrayElements;
        for (int i = 0; i < elements.Length; i++)
            proto.Items.Add(ToProto(in elements[i]));

        return proto;
    }

    private static ColumnValue FromProtoArray(ProtoArray av)
    {
        CoreColumnType elementType = (CoreColumnType)(int)av.ElementType;
        List<ColumnValue> items = new(av.Items.Count);
        foreach (ProtoValue item in av.Items)
            items.Add(FromProto(item));

        return ColumnValue.FromArray(elementType, items);
    }

    /// <summary>
    /// Packs the two big-endian int64 halves of a UUID into a 16-byte <see cref="ByteString"/>.
    /// Big-endian byte order equals the canonical string order (RFC 4122 bytes 0..15).
    /// </summary>
    private static ByteString UuidToBytes(long high, long low)
    {
        byte[] buf = new byte[16];
        BinaryPrimitives.WriteUInt64BigEndian(buf.AsSpan(0, 8), (ulong)high);
        BinaryPrimitives.WriteUInt64BigEndian(buf.AsSpan(8, 8), (ulong)low);
        return ByteString.CopyFrom(buf);
    }

    /// <summary>
    /// Reconstructs a <see cref="ColumnValue"/> of type <see cref="CoreColumnType.Uuid"/> from a
    /// 16-byte big-endian <see cref="ByteString"/>.
    /// </summary>
    private static ColumnValue UuidFromBytes(ByteString bs)
    {
        if (bs.Length != 16)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"GrpcValueCodec: Uuid bytes must be exactly 16, got {bs.Length}");

        ReadOnlySpan<byte> span = bs.Span;
        long high = (long)BinaryPrimitives.ReadUInt64BigEndian(span[..8]);
        long low  = (long)BinaryPrimitives.ReadUInt64BigEndian(span[8..]);
        return new ColumnValue(CoreColumnType.Uuid, high, low);
    }
}
