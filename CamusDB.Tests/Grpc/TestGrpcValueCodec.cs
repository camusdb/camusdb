
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.App.Grpc;
using CamusDB.Core.CommandsExecutor.Models;

using CoreColumnType = CamusDB.Core.Catalogs.Models.ColumnType;
using ProtoValue     = CamusDB.Grpc.Value;
using ProtoColType   = CamusDB.Grpc.ColumnType;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// Round-trips every <see cref="CoreColumnType"/> through <see cref="GrpcValueCodec"/> to prove the
/// gRPC wire mapping is lossless and that type identity is preserved — in particular that
/// <see cref="CoreColumnType.Id"/> stays distinct from <see cref="CoreColumnType.String"/>, an empty
/// array still carries its element type, and a Uuid survives as its two big-endian halves.
/// </summary>
[Parallelizable(ParallelScope.All)]
public sealed class TestGrpcValueCodec
{
    private static ColumnValue RoundTrip(ColumnValue original)
    {
        ProtoValue proto = GrpcValueCodec.ToProto(original);
        return GrpcValueCodec.FromProto(proto);
    }

    [Test]
    public void Null_RoundTrips()
    {
        ProtoValue proto = GrpcValueCodec.ToProto(ColumnValue.Null);
        Assert.AreEqual(ProtoValue.KindOneofCase.NullValue, proto.KindCase);

        ColumnValue back = GrpcValueCodec.FromProto(proto);
        Assert.AreEqual(CoreColumnType.Null, back.Type);
    }

    [Test]
    public void NullReference_EncodesAsTypedNull()
    {
        // A null cell reference must not fault — it maps to a typed NULL like the REST encoder.
        ProtoValue proto = GrpcValueCodec.ToProto(null);
        Assert.AreEqual(ProtoValue.KindOneofCase.NullValue, proto.KindCase);
        Assert.AreEqual(CoreColumnType.Null, GrpcValueCodec.FromProto(proto).Type);
    }

    [Test]
    public void UnsetOneof_DecodesAsNull()
    {
        // A Value with no oneof set (default message) decodes to a typed NULL rather than throwing.
        Assert.AreEqual(CoreColumnType.Null, GrpcValueCodec.FromProto(new ProtoValue()).Type);
    }

    [Test]
    public void Id_RoundTrips_AndUsesIdField()
    {
        ColumnValue original = new(CoreColumnType.Id, "6849f3a1c2e7d50b4f8a91d3");

        ProtoValue proto = GrpcValueCodec.ToProto(original);
        Assert.AreEqual(ProtoValue.KindOneofCase.IdValue, proto.KindCase, "Id must use id_value, not string_value");

        ColumnValue back = GrpcValueCodec.FromProto(proto);
        Assert.AreEqual(CoreColumnType.Id, back.Type);
        Assert.AreEqual("6849f3a1c2e7d50b4f8a91d3", back.StrValue);
    }

    [Test]
    public void String_RoundTrips_AndUsesStringField()
    {
        ColumnValue original = new(CoreColumnType.String, "hello");

        ProtoValue proto = GrpcValueCodec.ToProto(original);
        Assert.AreEqual(ProtoValue.KindOneofCase.StringValue, proto.KindCase);

        ColumnValue back = GrpcValueCodec.FromProto(proto);
        Assert.AreEqual(CoreColumnType.String, back.Type);
        Assert.AreEqual("hello", back.StrValue);
    }

    [Test]
    public void IdAndString_StayDistinctAcrossTheWire()
    {
        // The engine treats Id and String as different types; identical text must not collapse them.
        ProtoValue asId     = GrpcValueCodec.ToProto(new ColumnValue(CoreColumnType.Id, "abc"));
        ProtoValue asString = GrpcValueCodec.ToProto(new ColumnValue(CoreColumnType.String, "abc"));

        Assert.AreEqual(ProtoValue.KindOneofCase.IdValue, asId.KindCase);
        Assert.AreEqual(ProtoValue.KindOneofCase.StringValue, asString.KindCase);
        Assert.AreEqual(CoreColumnType.Id, GrpcValueCodec.FromProto(asId).Type);
        Assert.AreEqual(CoreColumnType.String, GrpcValueCodec.FromProto(asString).Type);
    }

    [Test]
    public void Integer64_RoundTrips()
    {
        ColumnValue back = RoundTrip(new ColumnValue(CoreColumnType.Integer64, 9_223_372_036_854_775_123L));
        Assert.AreEqual(CoreColumnType.Integer64, back.Type);
        Assert.AreEqual(9_223_372_036_854_775_123L, back.LongValue);
    }

    [Test]
    public void Bool_RoundTrips()
    {
        Assert.IsTrue(RoundTrip(ColumnValue.FromBool(true)).BoolValue);
        Assert.IsFalse(RoundTrip(ColumnValue.FromBool(false)).BoolValue);
        Assert.AreEqual(CoreColumnType.Bool, RoundTrip(ColumnValue.FromBool(true)).Type);
    }

    [Test]
    public void Float64_RoundTrips()
    {
        ColumnValue back = RoundTrip(new ColumnValue(CoreColumnType.Float64, 3.5));
        Assert.AreEqual(CoreColumnType.Float64, back.Type);
        Assert.AreEqual(3.5, back.FloatValue);
    }

    [Test]
    public void Float32_RoundTrips_AtSinglePrecision()
    {
        ColumnValue back = RoundTrip(new ColumnValue(CoreColumnType.Float32, 1.25));
        Assert.AreEqual(CoreColumnType.Float32, back.Type);
        Assert.AreEqual(1.25f, (float)back.FloatValue);
    }

    [Test]
    public void Bytes_RoundTrips()
    {
        byte[] raw = { 0x00, 0x01, 0xFF, 0x7F };
        ColumnValue back = RoundTrip(new ColumnValue(raw));
        Assert.AreEqual(CoreColumnType.Bytes, back.Type);
        CollectionAssert.AreEqual(raw, back.BytesValue);
    }

    [Test]
    public void Bytes_Empty_RoundTrips()
    {
        ColumnValue back = RoundTrip(new ColumnValue(Array.Empty<byte>()));
        Assert.AreEqual(CoreColumnType.Bytes, back.Type);
        CollectionAssert.AreEqual(Array.Empty<byte>(), back.BytesValue);
    }

    [Test]
    public void Date_RoundTrips_AsRawTicks()
    {
        long ticks = new DateTime(2026, 7, 16, 0, 0, 0, DateTimeKind.Utc).Ticks;
        ColumnValue back = RoundTrip(new ColumnValue(CoreColumnType.Date, ticks));
        Assert.AreEqual(CoreColumnType.Date, back.Type);
        Assert.AreEqual(ticks, back.LongValue);
    }

    [Test]
    public void DateTime_RoundTrips_AsRawTicks()
    {
        long ticks = new DateTime(2026, 7, 16, 13, 45, 12, DateTimeKind.Utc).Ticks;
        ColumnValue back = RoundTrip(new ColumnValue(CoreColumnType.DateTime, ticks));
        Assert.AreEqual(CoreColumnType.DateTime, back.Type);
        Assert.AreEqual(ticks, back.LongValue);
    }

    [Test]
    public void Uuid_RoundTrips_AsBigEndianHalves()
    {
        Guid guid = Guid.Parse("11223344-5566-7788-99aa-bbccddeeff00");
        ColumnValue original = ColumnValue.FromUuid(guid);

        ProtoValue proto = GrpcValueCodec.ToProto(original);
        Assert.AreEqual(ProtoValue.KindOneofCase.UuidValue, proto.KindCase);
        Assert.AreEqual(16, proto.UuidValue.Length, "Uuid must serialize as exactly 16 bytes");

        ColumnValue back = GrpcValueCodec.FromProto(proto);
        Assert.AreEqual(CoreColumnType.Uuid, back.Type);
        Assert.AreEqual(original.UuidHigh, back.UuidHigh);
        Assert.AreEqual(original.LongValue, back.LongValue);
        Assert.AreEqual(guid, back.ToGuid());
    }

    [Test]
    public void Uuid_RejectsWrongByteLength()
    {
        ProtoValue bad = new() { UuidValue = Google.Protobuf.ByteString.CopyFrom(new byte[8]) };
        Assert.Throws<CamusDB.Core.CamusDBException>(() => GrpcValueCodec.FromProto(bad));
    }

    [Test]
    public void Array_RoundTrips_WithElementValues()
    {
        ColumnValue original = ColumnValue.FromArray(CoreColumnType.Integer64, new List<ColumnValue>
        {
            new(CoreColumnType.Integer64, 1L),
            new(CoreColumnType.Integer64, 2L),
            new(CoreColumnType.Integer64, 3L),
        });

        ColumnValue back = RoundTrip(original);
        Assert.AreEqual(CoreColumnType.Array, back.Type);
        Assert.AreEqual(CoreColumnType.Integer64, back.ArrayElementType);
        Assert.IsNotNull(back.ArrayValues);
        Assert.AreEqual(3, back.ArrayValues!.Count);
        Assert.AreEqual(1L, back.ArrayValues[0].LongValue);
        Assert.AreEqual(2L, back.ArrayValues[1].LongValue);
        Assert.AreEqual(3L, back.ArrayValues[2].LongValue);
    }

    [Test]
    public void Array_Empty_PreservesElementType()
    {
        // The element type must survive even with zero items — the whole reason ArrayValue carries it.
        ColumnValue original = ColumnValue.FromArray(CoreColumnType.String, new List<ColumnValue>());

        ProtoValue proto = GrpcValueCodec.ToProto(original);
        Assert.AreEqual(ProtoColType.String, proto.ArrayValue.ElementType);

        ColumnValue back = GrpcValueCodec.FromProto(proto);
        Assert.AreEqual(CoreColumnType.Array, back.Type);
        Assert.AreEqual(CoreColumnType.String, back.ArrayElementType);
        Assert.AreEqual(0, back.ArrayValues!.Count);
    }

    [Test]
    public void Array_Nested_RoundTrips()
    {
        ColumnValue inner = ColumnValue.FromArray(CoreColumnType.Integer64, new List<ColumnValue>
        {
            new(CoreColumnType.Integer64, 7L),
        });
        ColumnValue original = ColumnValue.FromArray(CoreColumnType.Array, new List<ColumnValue> { inner });

        ColumnValue back = RoundTrip(original);
        Assert.AreEqual(CoreColumnType.Array, back.Type);
        Assert.AreEqual(CoreColumnType.Array, back.ArrayElementType);
        Assert.AreEqual(1, back.ArrayValues!.Count);
        Assert.AreEqual(CoreColumnType.Integer64, back.ArrayValues[0].ArrayElementType);
        Assert.AreEqual(7L, back.ArrayValues[0].ArrayValues![0].LongValue);
    }
}
