
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies <see cref="CompactRowEncoder"/> emits the exact compact-raw JSON-native shape the
/// positional wire format promises for every <see cref="ColumnType"/>, and that
/// <see cref="CompactRowEncoder.EncodeRow"/> aligns values to the schema by name.
/// </summary>
[Parallelizable(ParallelScope.All)]
public sealed class TestCompactRowEncoder
{
    [Test]
    public void Null_EncodesAsNull()
    {
        Assert.IsNull(CompactRowEncoder.EncodeValue(null));
        Assert.IsNull(CompactRowEncoder.EncodeValue(new ColumnValue(ColumnType.Null, false)));
    }

    [Test]
    public void Id_EncodesAsString()
    {
        Assert.AreEqual("6849f3a1c2e7d50b4f8a91d3",
            CompactRowEncoder.EncodeValue(new ColumnValue(ColumnType.Id, "6849f3a1c2e7d50b4f8a91d3")));
    }

    [Test]
    public void String_EncodesAsString()
    {
        Assert.AreEqual("hello", CompactRowEncoder.EncodeValue(new ColumnValue(ColumnType.String, "hello")));
    }

    [Test]
    public void Integer64_EncodesAsLong()
    {
        object? encoded = CompactRowEncoder.EncodeValue(new ColumnValue(ColumnType.Integer64, 9_223_372_036_854_775_123L));
        Assert.AreEqual(9_223_372_036_854_775_123L, encoded);
        Assert.IsInstanceOf<long>(encoded);
    }

    [Test]
    public void Bool_EncodesAsBool()
    {
        Assert.AreEqual(true, CompactRowEncoder.EncodeValue(new ColumnValue(ColumnType.Bool, true)));
        Assert.AreEqual(false, CompactRowEncoder.EncodeValue(new ColumnValue(ColumnType.Bool, false)));
    }

    [Test]
    public void Float64_EncodesAsDouble()
    {
        object? encoded = CompactRowEncoder.EncodeValue(new ColumnValue(ColumnType.Float64, 3.5));
        Assert.AreEqual(3.5, encoded);
        Assert.IsInstanceOf<double>(encoded);
    }

    [Test]
    public void Float32_EncodesAsFloat()
    {
        object? encoded = CompactRowEncoder.EncodeValue(new ColumnValue(ColumnType.Float32, 1.25));
        Assert.AreEqual(1.25f, encoded);
        Assert.IsInstanceOf<float>(encoded);
    }

    [Test]
    public void Bytes_EncodesAsBase64()
    {
        byte[] raw = { 0x00, 0x01, 0xFF, 0x7F };
        Assert.AreEqual(Convert.ToBase64String(raw), CompactRowEncoder.EncodeValue(new ColumnValue(raw)));
    }

    [Test]
    public void Date_EncodesAsRawTicks()
    {
        long ticks = new DateTime(2026, 7, 16, 0, 0, 0, DateTimeKind.Utc).Ticks;
        object? encoded = CompactRowEncoder.EncodeValue(new ColumnValue(ColumnType.Date, ticks));
        Assert.AreEqual(ticks, encoded);
        Assert.IsInstanceOf<long>(encoded);
    }

    [Test]
    public void DateTime_EncodesAsRawTicks()
    {
        long ticks = new DateTime(2026, 7, 16, 13, 45, 12, DateTimeKind.Utc).Ticks;
        object? encoded = CompactRowEncoder.EncodeValue(new ColumnValue(ColumnType.DateTime, ticks));
        Assert.AreEqual(ticks, encoded);
        Assert.IsInstanceOf<long>(encoded);
    }

    [Test]
    public void Uuid_EncodesAsHighLowPair()
    {
        Guid guid = Guid.Parse("11223344-5566-7788-99aa-bbccddeeff00");
        ColumnValue value = ColumnValue.FromUuid(guid);

        object? encoded = CompactRowEncoder.EncodeValue(value);
        Assert.IsInstanceOf<long[]>(encoded);

        long[] halves = (long[])encoded!;
        Assert.AreEqual(2, halves.Length);
        Assert.AreEqual(value.UuidHigh, halves[0], "high half");
        Assert.AreEqual(value.LongValue, halves[1], "low half");

        // The pair must reconstruct the original Guid via the same big-endian halves.
        Assert.AreEqual(guid, new ColumnValue(ColumnType.Uuid, halves[0], halves[1]).ToGuid());
    }

    [Test]
    public void Array_EncodesAsNestedElementValues()
    {
        ColumnValue value = ColumnValue.FromArray(ColumnType.Integer64, new List<ColumnValue>
        {
            new(ColumnType.Integer64, 1L),
            new(ColumnType.Integer64, 2L),
            new(ColumnType.Integer64, 3L),
        });

        object? encoded = CompactRowEncoder.EncodeValue(value);
        Assert.IsInstanceOf<object?[]>(encoded);
        CollectionAssert.AreEqual(new object?[] { 1L, 2L, 3L }, (object?[])encoded!);
    }

    [Test]
    public void Array_Empty_EncodesAsEmptyArray()
    {
        ColumnValue value = ColumnValue.FromArray(ColumnType.String, new List<ColumnValue>());
        object? encoded = CompactRowEncoder.EncodeValue(value);
        Assert.IsInstanceOf<object?[]>(encoded);
        Assert.AreEqual(0, ((object?[])encoded!).Length);
    }

    [Test]
    public void EncodeRow_AlignsValuesToSchemaOrder()
    {
        IReadOnlyList<DerivedColumnSchema> schema =
        [
            new("id", ColumnType.Integer64),
            new("name", ColumnType.String),
            new("active", ColumnType.Bool),
        ];

        // Dictionary insertion order intentionally differs from schema order.
        Dictionary<string, ColumnValue> row = new()
        {
            { "active", new ColumnValue(ColumnType.Bool, true) },
            { "id", new ColumnValue(ColumnType.Integer64, 42L) },
            { "name", new ColumnValue(ColumnType.String, "abc") },
        };

        object?[] encoded = CompactRowEncoder.EncodeRow(row, schema);

        CollectionAssert.AreEqual(new object?[] { 42L, "abc", true }, encoded);
    }

    [Test]
    public void EncodeRow_MissingColumn_EncodesAsNull()
    {
        IReadOnlyList<DerivedColumnSchema> schema =
        [
            new("present", ColumnType.String),
            new("absent", ColumnType.String),
        ];

        Dictionary<string, ColumnValue> row = new()
        {
            { "present", new ColumnValue(ColumnType.String, "here") },
        };

        object?[] encoded = CompactRowEncoder.EncodeRow(row, schema);
        Assert.AreEqual("here", encoded[0]);
        Assert.IsNull(encoded[1]);
    }
}
