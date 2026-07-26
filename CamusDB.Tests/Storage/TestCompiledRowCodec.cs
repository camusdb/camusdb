
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Text;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Round-trip, size, projection, array, and corruption tests for the positional
/// <see cref="CompiledRowCodec"/> (the schema-driven replacement for the self-describing row format).
/// </summary>
[TestFixture]
public sealed class TestCompiledRowCodec
{
    private static TableColumnSchema Col(string name, ColumnType type, ColumnType? elementType = null) =>
        new(name, name, type, notNull: false, defaultValue: null, arrayElementType: elementType);

    private static CompiledRowCodec CodecFor(int version, params TableColumnSchema[] cols) =>
        CompiledRowCodec.Build(version, cols);

    /// <summary>params-friendly wrapper so tests can pass loose <see cref="ValueSlot"/> args.</summary>
    private static byte[] Enc(CompiledRowCodec codec, params ValueSlot[] values) => codec.Encode(values);

    /// <summary>Encodes, validates the frame, decodes, and asserts each decoded slot equals the input.</summary>
    private static void AssertRoundTrip(CompiledRowCodec codec, params ValueSlot[] values)
    {
        byte[] payload = Enc(codec, values);
        codec.ValidateFrame(payload);

        ValueSlot[] decoded = codec.DecodeToSlots(payload);
        Assert.AreEqual(values.Length, decoded.Length);

        for (int i = 0; i < values.Length; i++)
            AssertSlotEqual(values[i], decoded[i], $"column {i}");

        // The storage form must carry the Value envelope and decode identically past byte 0.
        byte[] storage = codec.EncodeStorageValue(values);
        Assert.AreEqual((byte)BranchKvKind.Value, storage[0]);
        CollectionAssert.AreEqual(payload, storage[1..]);
    }

    private static void AssertSlotEqual(ValueSlot expected, ValueSlot actual, string because)
    {
        if (expected.IsNull)
        {
            Assert.IsTrue(actual.IsNull, $"{because}: expected NULL");
            return;
        }

        Assert.IsFalse(actual.IsNull, $"{because}: unexpected NULL");
        Assert.AreEqual(expected.Type, actual.Type, $"{because}: type");
        Assert.AreEqual(0, expected.CompareTo(actual), $"{because}: value (compare)");
    }

    private static ObjectIdValue AnId(int a = 0x11223344, int b = 0x55667788, int c = unchecked((int)0x99aabbcc)) => new(a, b, c);

    // ─────────────────────────────── Fixed scalars ───────────────────────────────

    [Test]
    public void RoundTrip_Integer64_Extremes()
    {
        CompiledRowCodec codec = CodecFor(0, Col("n", ColumnType.Integer64));
        foreach (long v in new[] { long.MinValue, -1L, 0L, 1L, long.MaxValue })
            AssertRoundTrip(codec, ValueSlot.FromLong(ColumnType.Integer64, v));
    }

    [Test]
    public void RoundTrip_Float64_EdgeCases()
    {
        CompiledRowCodec codec = CodecFor(0, Col("f", ColumnType.Float64));
        foreach (double v in new[] { 0.0, -0.0, double.NaN, double.PositiveInfinity, double.NegativeInfinity, double.Epsilon, double.MaxValue, double.MinValue })
        {
            byte[] payload = Enc(codec, ValueSlot.FromDouble(ColumnType.Float64, v));
            codec.ValidateFrame(payload);
            double got = codec.GetDouble(payload, 0);
            // BitConverter comparison so NaN and -0.0 round-trip bit-exactly.
            Assert.AreEqual(BitConverter.DoubleToInt64Bits(v), BitConverter.DoubleToInt64Bits(got), $"double {v}");
        }
    }

    [Test]
    public void RoundTrip_Float32_EdgeCases()
    {
        CompiledRowCodec codec = CodecFor(0, Col("f", ColumnType.Float32));
        foreach (float v in new[] { 0f, -0f, float.NaN, float.PositiveInfinity, float.NegativeInfinity, float.MaxValue, float.MinValue })
        {
            byte[] payload = Enc(codec, ValueSlot.FromDouble(ColumnType.Float32, v));
            codec.ValidateFrame(payload);
            float got = codec.GetFloat(payload, 0);
            Assert.AreEqual(BitConverter.SingleToInt32Bits(v), BitConverter.SingleToInt32Bits(got), $"float {v}");
        }
    }

    [Test]
    public void RoundTrip_DateAndDateTime()
    {
        CompiledRowCodec codec = CodecFor(0, Col("d", ColumnType.Date), Col("dt", ColumnType.DateTime));
        AssertRoundTrip(codec, ValueSlot.FromLong(ColumnType.Date, 20260725), ValueSlot.FromLong(ColumnType.DateTime, long.MaxValue));
    }

    [Test]
    public void RoundTrip_Id()
    {
        CompiledRowCodec codec = CodecFor(0, Col("id", ColumnType.Id));
        ObjectIdValue id = AnId();
        byte[] payload = Enc(codec, ValueSlot.FromId(id.ToString()));
        codec.ValidateFrame(payload);
        Assert.AreEqual(id, codec.GetId(payload, 0));
        Assert.AreEqual(id.ToString(), codec.GetSlot(payload, 0).AsString);
    }

    [Test]
    public void RoundTrip_Uuid_PreservesOrderingHalves()
    {
        CompiledRowCodec codec = CodecFor(0, Col("u", ColumnType.Uuid));
        ValueSlot slot = ValueSlot.FromUuid(unchecked((long)0xFFEEDDCCBBAA9988), 0x0011223344556677);
        byte[] payload = Enc(codec, slot);
        codec.ValidateFrame(payload);
        (long high, long low) = codec.GetUuid(payload, 0);
        Assert.AreEqual(slot.UuidHigh, high);
        Assert.AreEqual(slot.UuidLow, low);
        AssertRoundTrip(codec, slot);
    }

    [Test]
    public void RoundTrip_Bool_NullTrueFalse()
    {
        CompiledRowCodec codec = CodecFor(0, Col("a", ColumnType.Bool), Col("b", ColumnType.Bool), Col("c", ColumnType.Bool));
        AssertRoundTrip(codec, ValueSlot.True, ValueSlot.False, ValueSlot.Null);
    }

    // ─────────────────────────────── Strings / bytes (UTF-8) ───────────────────────────────

    [Test]
    public void RoundTrip_String_NullVsEmptyVsAscii()
    {
        CompiledRowCodec codec = CodecFor(0, Col("s", ColumnType.String));
        AssertRoundTrip(codec, ValueSlot.Null);
        AssertRoundTrip(codec, ValueSlot.FromString(""));
        AssertRoundTrip(codec, ValueSlot.FromString("hello world"));

        // Null and empty must decode differently despite both being zero-length payloads.
        byte[] nul = Enc(codec, ValueSlot.Null);
        byte[] empty = Enc(codec, ValueSlot.FromString(""));
        Assert.IsTrue(codec.IsNull(nul, 0));
        Assert.IsFalse(codec.IsNull(empty, 0));
        Assert.AreEqual("", codec.GetSlot(empty, 0).AsString);
    }

    [Test]
    public void RoundTrip_String_NonAscii_Utf8()
    {
        CompiledRowCodec codec = CodecFor(0, Col("s", ColumnType.String));
        string text = "café — 日本語 — Ω — 🚀";
        byte[] payload = Enc(codec, ValueSlot.FromString(text));
        codec.ValidateFrame(payload);
        Assert.AreEqual(text, codec.GetSlot(payload, 0).AsString);
        // The stored slice must be UTF-8, not UTF-16.
        CollectionAssert.AreEqual(Encoding.UTF8.GetBytes(text), codec.GetVariableSlice(payload, 0).ToArray());
    }

    [Test]
    public void RoundTrip_Bytes_NullEmptyAndData()
    {
        CompiledRowCodec codec = CodecFor(0, Col("b", ColumnType.Bytes));
        AssertRoundTrip(codec, ValueSlot.Null);
        AssertRoundTrip(codec, ValueSlot.FromBytes([]));
        AssertRoundTrip(codec, ValueSlot.FromBytes([0, 1, 2, 255, 254]));
    }

    // ─────────────────────────────── Mixed / wide rows ───────────────────────────────

    [Test]
    public void RoundTrip_MixedRow_FixedBoolVariableInterleaved()
    {
        CompiledRowCodec codec = CodecFor(0,
            Col("i", ColumnType.Integer64),
            Col("s", ColumnType.String),
            Col("flag", ColumnType.Bool),
            Col("f", ColumnType.Float64),
            Col("b", ColumnType.Bytes),
            Col("id", ColumnType.Id));

        AssertRoundTrip(codec,
            ValueSlot.FromLong(ColumnType.Integer64, -42),
            ValueSlot.FromString("mixed"),
            ValueSlot.True,
            ValueSlot.FromDouble(ColumnType.Float64, 3.14159),
            ValueSlot.FromBytes([9, 8, 7]),
            ValueSlot.FromId(AnId().ToString()));

        // Null-heavy variant of the same schema.
        AssertRoundTrip(codec,
            ValueSlot.Null, ValueSlot.Null, ValueSlot.Null, ValueSlot.Null, ValueSlot.Null, ValueSlot.Null);
    }

    // ─────────────────────────────── Fixed-only exact size ───────────────────────────────

    [Test]
    public void FixedOnlyRow_HasConstantSize_RegardlessOfValues()
    {
        CompiledRowCodec codec = CodecFor(0, Col("a", ColumnType.Integer64), Col("b", ColumnType.Bool), Col("c", ColumnType.Uuid));
        Assert.IsTrue(codec.IsFixedOnly);

        byte[] one = Enc(codec, ValueSlot.FromLong(ColumnType.Integer64, 1), ValueSlot.True, ValueSlot.FromUuid(1, 2));
        byte[] two = Enc(codec, ValueSlot.FromLong(ColumnType.Integer64, long.MinValue), ValueSlot.Null, ValueSlot.Null);
        Assert.AreEqual(one.Length, two.Length, "fixed-only rows must be the same size");

        codec.ValidateFrame(one);
        codec.ValidateFrame(two);
    }

    // ─────────────────────────────── Arrays ───────────────────────────────

    [Test]
    public void RoundTrip_IntArray_WithNullElementAndEmpty()
    {
        CompiledRowCodec codec = CodecFor(0, Col("arr", ColumnType.Array, ColumnType.Integer64));

        AssertRoundTrip(codec, ValueSlot.FromArray(ColumnType.Integer64, System.Array.Empty<ValueSlot>()));
        AssertRoundTrip(codec, ValueSlot.FromArray(ColumnType.Integer64, new[]
        {
            ValueSlot.FromLong(ColumnType.Integer64, 10),
            ValueSlot.Null,
            ValueSlot.FromLong(ColumnType.Integer64, -30),
        }));
        AssertRoundTrip(codec, ValueSlot.Null);
    }

    [Test]
    public void RoundTrip_StringArray_WithNullsAndUnicode()
    {
        CompiledRowCodec codec = CodecFor(0, Col("arr", ColumnType.Array, ColumnType.String));
        AssertRoundTrip(codec, ValueSlot.FromArray(ColumnType.String, new[]
        {
            ValueSlot.FromString("a"),
            ValueSlot.Null,
            ValueSlot.FromString(""),
            ValueSlot.FromString("日本語"),
        }));
    }

    [Test]
    public void RoundTrip_BoolArray()
    {
        CompiledRowCodec codec = CodecFor(0, Col("arr", ColumnType.Array, ColumnType.Bool));
        AssertRoundTrip(codec, ValueSlot.FromArray(ColumnType.Bool, new[]
        {
            ValueSlot.True, ValueSlot.False, ValueSlot.Null, ValueSlot.True,
        }));
    }

    // ─────────────────────────────── Projection is O(1) / borrowed ───────────────────────────────

    [Test]
    public void Projection_ReadsSelectedColumnWithoutDecodingOthers()
    {
        CompiledRowCodec codec = CodecFor(0,
            Col("big", ColumnType.String),
            Col("n", ColumnType.Integer64),
            Col("tail", ColumnType.String));

        string big = new('x', 4096);
        byte[] payload = Enc(codec, ValueSlot.FromString(big), ValueSlot.FromLong(ColumnType.Integer64, 777), ValueSlot.FromString("t"));
        codec.ValidateFrame(payload);

        // Read only the fixed column and the tail variable column; never touch 'big'.
        Assert.AreEqual(777, codec.GetInt64(payload, 1));
        Assert.AreEqual("t", Encoding.UTF8.GetString(codec.GetVariableSlice(payload, 2)));
    }

    // ─────────────────────────────── Corruption / bounds ───────────────────────────────

    [Test]
    public void ValidateFrame_RejectsTruncatedHeader()
    {
        CompiledRowCodec codec = CodecFor(0, Col("n", ColumnType.Integer64), Col("s", ColumnType.String));
        byte[] payload = Enc(codec, ValueSlot.FromLong(ColumnType.Integer64, 1), ValueSlot.FromString("abc"));

        CamusDBException ex = Assert.Throws<CamusDBException>(() => codec.ValidateFrame(payload[..3]));
        Assert.AreEqual(CamusDBErrorCodes.SystemSpaceCorrupt, ex.Code);
    }

    [Test]
    public void ValidateFrame_RejectsWrongSchemaVersion()
    {
        CompiledRowCodec v0 = CodecFor(0, Col("n", ColumnType.Integer64));
        CompiledRowCodec v5 = CodecFor(5, Col("n", ColumnType.Integer64));
        byte[] payload = Enc(v0, ValueSlot.FromLong(ColumnType.Integer64, 1));

        CamusDBException ex = Assert.Throws<CamusDBException>(() => v5.ValidateFrame(payload));
        Assert.AreEqual(CamusDBErrorCodes.SystemSpaceCorrupt, ex.Code);
    }

    [Test]
    public void ValidateFrame_RejectsNonMonotonicOffsets()
    {
        CompiledRowCodec codec = CodecFor(0, Col("a", ColumnType.String), Col("b", ColumnType.String));
        byte[] payload = Enc(codec, ValueSlot.FromString("xxxx"), ValueSlot.FromString("y"));

        // Corrupt the payload length by appending a stray byte so the directory no longer terminates
        // exactly at the payload end.
        byte[] longer = new byte[payload.Length + 1];
        payload.CopyTo(longer, 0);

        CamusDBException ex = Assert.Throws<CamusDBException>(() => codec.ValidateFrame(longer));
        Assert.AreEqual(CamusDBErrorCodes.SystemSpaceCorrupt, ex.Code);
    }

    [Test]
    public void ValidateFrame_RejectsFixedOnlyLengthMismatch()
    {
        CompiledRowCodec codec = CodecFor(0, Col("n", ColumnType.Integer64));
        byte[] payload = Enc(codec, ValueSlot.FromLong(ColumnType.Integer64, 1));
        byte[] padded = new byte[payload.Length + 4];
        payload.CopyTo(padded, 0);

        CamusDBException ex = Assert.Throws<CamusDBException>(() => codec.ValidateFrame(padded));
        Assert.AreEqual(CamusDBErrorCodes.SystemSpaceCorrupt, ex.Code);
    }

    [Test]
    public void Encode_RejectsWrongValueCount()
    {
        CompiledRowCodec codec = CodecFor(0, Col("a", ColumnType.Integer64), Col("b", ColumnType.Integer64));
        CamusDBException ex = Assert.Throws<CamusDBException>(() => Enc(codec, ValueSlot.FromLong(ColumnType.Integer64, 1)));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    // ── Array-blob count corruption: a hostile/corrupt count must fail with SystemSpaceCorrupt, never a
    //    framework OverflowException / negative-length allocation. ──

    [Test]
    public void DecodeArrayBlob_ShortBlob_Rejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => CompiledRowCodec.DecodeArrayBlob(new byte[] { 1, 2, 3 }, ColumnType.Integer64));
        Assert.AreEqual(CamusDBErrorCodes.SystemSpaceCorrupt, ex.Code);
    }

    [TestCase(0x80000000u)]   // negative when cast to int
    [TestCase(0xFFFFFFFFu)]   // uint.MaxValue
    [TestCase(0x20000000u)]   // 536M: count*8 (fixed) and count*4 (variable) overflow int32
    public void DecodeArrayBlob_HugeCount_FixedElement_Rejected(uint count)
    {
        byte[] blob = new byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(blob, count);
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => CompiledRowCodec.DecodeArrayBlob(blob, ColumnType.Integer64));
        Assert.AreEqual(CamusDBErrorCodes.SystemSpaceCorrupt, ex.Code);
    }

    [TestCase(0x80000000u)]
    [TestCase(0xFFFFFFFFu)]
    [TestCase(0x20000000u)]
    public void DecodeArrayBlob_HugeCount_VariableElement_Rejected(uint count)
    {
        byte[] blob = new byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(blob, count);
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => CompiledRowCodec.DecodeArrayBlob(blob, ColumnType.String));
        Assert.AreEqual(CamusDBErrorCodes.SystemSpaceCorrupt, ex.Code);
    }

    [Test]
    public void ValidateFrame_ShorterThanHeader_RejectedNotFrameworkException([Values(0, 1, 2, 3)] int length)
    {
        CompiledRowCodec codec = CodecFor(0, Col("n", ColumnType.Integer64));
        CamusDBException ex = Assert.Throws<CamusDBException>(() => codec.ValidateFrame(new byte[length]));
        Assert.AreEqual(CamusDBErrorCodes.SystemSpaceCorrupt, ex.Code);
    }
}
