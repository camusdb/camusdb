/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

using NUnit.Framework;

using CamusDB.App.Grpc;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Sends every <see cref="ColumnType"/> member through each place that has one switch arm per type
/// and must handle every type. A codec with no arm for a type throws only when a query carries that
/// type down the path, for example when a sort spills to disk or a fragment runs on another node. A
/// hash with no arm does not throw: it puts every value of the type in one bucket, and the operator
/// slows toward quadratic work. Neither shows up in an ordinary test, so this fixture goes through
/// <see cref="Enum.GetValues{TEnum}()"/> and fails for a member that any of these paths misses:
/// <list type="bullet">
/// <item><see cref="SpillRowCodec"/>: the named-row, stream and value-only encodings, as a cell and
///   as an array element.</item>
/// <item><see cref="ColumnValueWireCodec"/>, the encoding of decoded cells between nodes.</item>
/// <item><see cref="GrpcValueCodec"/>, from a value and from a <see cref="ValueSlot"/>.</item>
/// <item><see cref="ValueSlot.FromColumnValue"/> and <see cref="ValueSlot.ToColumnValue"/>.</item>
/// <item><see cref="ColumnValueHash"/> and the comparers that use it.</item>
/// </list>
/// A new member first fails in <see cref="Samples"/>. Add samples for it there, then add its arms.
/// </summary>
[TestFixture]
[Parallelizable(ParallelScope.All)]
public sealed class TestColumnTypeCoverage
{
    private const int SampleCount = 64;

    private static IEnumerable<ColumnType> AllTypes() => Enum.GetValues<ColumnType>();

    /// <summary>Every type that an array can hold: a scalar, so neither NULL nor an array.</summary>
    private static IEnumerable<ColumnType> ElementTypes() =>
        Enum.GetValues<ColumnType>().Where(t => t is not ColumnType.Null and not ColumnType.Array);

    private static IEnumerable<ColumnType> HashedTypes() =>
        Enum.GetValues<ColumnType>().Where(t => t is not ColumnType.Null);

    /// <summary>
    /// Values of <paramref name="type"/> that are all different under <see cref="ColumnValue.CompareTo"/>.
    /// The Uuid and Numeric values share one low half, so a path that keeps only the low 64 bits
    /// fails. An array sample holds every element type, a NULL element included.
    /// </summary>
    private static List<ColumnValue> Samples(ColumnType type)
    {
        List<ColumnValue> values = new(SampleCount);

        switch (type)
        {
            case ColumnType.Null:
                values.Add(ColumnValue.Null);
                break;

            case ColumnType.Bool:
                values.Add(ColumnValue.FromBool(false));
                values.Add(ColumnValue.FromBool(true));
                break;

            case ColumnType.Array:
                foreach (ColumnType element in ElementTypes())
                {
                    List<ColumnValue> items = Samples(element);
                    values.Add(ColumnValue.FromArray(element, [items[0], ColumnValue.Null, items[1]]));
                    values.Add(ColumnValue.FromArray(element, [items[1], items[0]]));
                }
                break;

            default:
                for (int i = 0; i < SampleCount; i++)
                    values.Add(Sample(type, i));
                break;
        }

        return values;
    }

    private static ColumnValue Sample(ColumnType type, int i) => type switch
    {
        ColumnType.Id => new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()),
        ColumnType.Integer64 => new ColumnValue(ColumnType.Integer64, i switch { 0 => long.MinValue, 1 => long.MaxValue, _ => i * 1_000_003L - 31 }),
        ColumnType.String => new ColumnValue(ColumnType.String, i switch { 0 => "", 1 => "ñandú ☃", _ => "s" + i }),
        ColumnType.Float64 => new ColumnValue(ColumnType.Float64, i switch { 0 => double.MinValue, 1 => 1e-300, _ => i * 0.5 - 10.25 }),
        ColumnType.Float32 => new ColumnValue(ColumnType.Float32, (double)(i * 0.25f - 3f)),
        ColumnType.Bytes => new ColumnValue(i == 0 ? Array.Empty<byte>() : new byte[] { (byte)i, (byte)(i * 3), 0xFF }),
        ColumnType.Date => new ColumnValue(ColumnType.Date, new DateTime(1999, 12, 31, 0, 0, 0, DateTimeKind.Utc).AddDays(i).Ticks),
        ColumnType.DateTime => new ColumnValue(ColumnType.DateTime, new DateTime(2026, 10, 9, 0, 0, 0, DateTimeKind.Utc).AddSeconds(i * 1003.5).Ticks),
        ColumnType.Uuid => new ColumnValue(ColumnType.Uuid, i == 0 ? long.MinValue : i, 5),
        ColumnType.Numeric => ColumnValue.FromNumeric(i switch
        {
            0 => NumericMath.MaxUnscaled,
            1 => NumericMath.MinUnscaled,
            _ => (i % 2 == 0 ? 1 : -1) * (((Int128)i << 64) + 5),
        }),
        _ => throw new AssertionException(
            $"ColumnType.{type} has no samples. Add samples here. Then give the type an arm in SpillRowCodec " +
            "(WriteScalar, ReadScalar, MeasureScalar), ColumnValueHash, ColumnValueWireCodec, GrpcValueCodec " +
            "(both ToProto overloads and FromProto) and ValueSlot."),
    };

    private static void AssertSameValue(ColumnValue expected, ColumnValue actual, string path)
    {
        Assert.AreEqual(expected.Type, actual.Type, $"{path}: type of {expected}");

        if (expected.Type == ColumnType.Array)
            Assert.AreEqual(expected.ArrayElementType, actual.ArrayElementType, $"{path}: element type of {expected}");

        Assert.AreEqual(0, expected.CompareTo(actual), $"{path}: {expected} came back as {actual}");
    }

    /// <summary>The value, and arrays of the value when the type can be an array element.</summary>
    private static IEnumerable<ColumnValue> ValuesAndArraysOf(ColumnType type)
    {
        List<ColumnValue> samples = Samples(type);
        foreach (ColumnValue value in samples)
            yield return value;

        if (type is ColumnType.Null or ColumnType.Array)
            yield break;

        yield return ColumnValue.FromArray(type, []);
        yield return ColumnValue.FromArray(type, [samples[0], ColumnValue.Null, samples[^1]]);
    }

    // ─── Codecs ───────────────────────────────────────────────────────────────

    [TestCaseSource(nameof(AllTypes))]
    public void SpillRowCodec_RoundTripsTheType_AsACellAndAsAnArrayElement(ColumnType type)
    {
        RowLayout layout = new(["c"]);

        foreach (ColumnValue value in ValuesAndArraysOf(type))
        {
            QueryResultRow named = new(ObjectIdValue.Empty, new Dictionary<string, ColumnValue> { ["c"] = value });

            byte[] frame = SpillRowCodec.Encode(named);
            int offset = 0;
            AssertSameValue(value, SpillRowCodec.Decode(frame, ref offset).Row["c"], "spill named row");

            using MemoryStream stream = new();
            SpillRowCodec.EncodeToStream(stream, named);
            AssertSameValue(value, SpillRowCodec.DecodePayload(stream.ToArray().AsSpan(4)).Row["c"], "spill stream");

            using MemoryStream valueOnly = new();
            SpillRowCodec.EncodeValueOnlyToStream(valueOnly, new QueryRow(ObjectIdValue.Empty, layout, [value]));
            AssertSameValue(value, SpillRowCodec.DecodeValueOnlyPayload(valueOnly.ToArray().AsSpan(4), layout).Values[0], "spill value-only");
        }
    }

    [TestCaseSource(nameof(AllTypes))]
    public void ColumnValueWireCodec_RoundTripsTheType(ColumnType type)
    {
        foreach (ColumnValue value in ValuesAndArraysOf(type))
        {
            using MemoryStream stream = new();
            using (Utf8JsonWriter writer = new(stream))
                ColumnValueWireCodec.Write(writer, value);

            using JsonDocument doc = JsonDocument.Parse(stream.ToArray());
            AssertSameValue(value, ColumnValueWireCodec.Read(doc.RootElement), "fragment wire codec");
        }
    }

    [TestCaseSource(nameof(AllTypes))]
    public void GrpcValueCodec_RoundTripsTheType_FromAValueAndFromASlot(ColumnType type)
    {
        foreach (ColumnValue value in ValuesAndArraysOf(type))
        {
            CamusDB.Grpc.Value fromValue = GrpcValueCodec.ToProto(value);
            AssertSameValue(value, GrpcValueCodec.FromProto(fromValue), "gRPC from a value");

            ValueSlot slot = ValueSlot.FromColumnValue(value);
            AssertSameValue(value, slot.ToColumnValue(), "value slot");
            Assert.AreEqual(fromValue, GrpcValueCodec.ToProto(in slot), $"gRPC from a slot: {value}");
        }
    }

    // ─── Hashes ───────────────────────────────────────────────────────────────

    private static readonly (string Name, Func<ColumnValue, int> Hash)[] Hashes =
    [
        ("ColumnValueHash", ColumnValueHash.Of),
        ("SqlColumnValueComparer", v => SqlColumnValueComparer.Instance.GetHashCode(v)),
        ("QueryDistincter.DistinctValueHash", QueryDistincter.DistinctValueHash),
        ("CompositeColumnValueComparer", v => CompositeColumnValueComparer.Instance.GetHashCode(new CompositeColumnValue([v]))),
        ("GROUP BY key hash", v => QueryAggregator.GroupPartitionIndex(new CompositeColumnValue([v]), 1 << 30)),
    ];

    [TestCaseSource(nameof(HashedTypes))]
    public void EveryHash_SpreadsDistinctValuesOfTheType(ColumnType type)
    {
        List<ColumnValue> samples = Samples(type);

        foreach ((string name, Func<ColumnValue, int> hash) in Hashes)
        {
            int distinct = samples.Select(hash).Distinct().Count();

            // A hash that ignores the payload gives 1. A random collision among 64 values is rare,
            // so half is a safe floor that never flakes.
            Assert.GreaterOrEqual(distinct, Math.Max(2, samples.Count / 2),
                $"{name} gives {distinct} hashes for {samples.Count} distinct {type} values. Add a {type} arm to ColumnValueHash.");
        }
    }

    [TestCaseSource(nameof(ElementTypes))]
    public void EveryHash_SpreadsArraysThatDifferOnlyInAnElementOfTheType(ColumnType type)
    {
        List<ColumnValue> arrays = Samples(type).Select(v => ColumnValue.FromArray(type, [v])).ToList();

        foreach ((string name, Func<ColumnValue, int> hash) in Hashes)
        {
            int distinct = arrays.Select(hash).Distinct().Count();
            Assert.GreaterOrEqual(distinct, Math.Max(2, arrays.Count / 2),
                $"{name} gives {distinct} hashes for {arrays.Count} distinct arrays of {type}.");
        }
    }

    /// <summary>Pairs that are different objects or bit patterns but compare equal.</summary>
    private static IEnumerable<TestCaseData> EqualPairs()
    {
        yield return new TestCaseData(new ColumnValue(ColumnType.Float32, 0.1), new ColumnValue(ColumnType.Float32, (double)0.1f))
            .SetName("Float32 equal at single precision");
        yield return new TestCaseData(new ColumnValue(ColumnType.Float64, 0.0), new ColumnValue(ColumnType.Float64, -0.0))
            .SetName("Float64 zero and negative zero");
        yield return new TestCaseData(
                ColumnValue.FromUuidString("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"),
                ColumnValue.FromUuidString("a0eebc999c0b4ef8bb6d6bb9bd380a11"))
            .SetName("Uuid from two spellings");
        yield return new TestCaseData(ColumnValue.FromNumericString("1.50"), ColumnValue.FromNumericString("1.5"))
            .SetName("Numeric with a trailing zero");
        yield return new TestCaseData(new ColumnValue(Array.Empty<byte>()), new ColumnValue((byte[])null!))
            .SetName("Bytes empty and missing");
        yield return new TestCaseData(
                ColumnValue.FromArray(ColumnType.Float32, [new ColumnValue(ColumnType.Float32, 0.1), ColumnValue.Null]),
                ColumnValue.FromArray(ColumnType.Float32, [new ColumnValue(ColumnType.Float32, (double)0.1f), ColumnValue.Null]))
            .SetName("Array of Float32 with a NULL element");
    }

    [TestCaseSource(nameof(EqualPairs))]
    public void EveryHash_AgreesWithEquality(ColumnValue left, ColumnValue right)
    {
        Assert.AreEqual(0, left.CompareTo(right), "the pair must compare equal");

        foreach ((string name, Func<ColumnValue, int> hash) in Hashes)
            Assert.AreEqual(hash(left), hash(right), $"{name}: equal values must hash alike");
    }
}
