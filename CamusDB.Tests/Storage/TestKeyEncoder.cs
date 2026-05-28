
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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Storage;

[TestFixture]
public class TestKeyEncoder
{
    private static readonly ColumnType[] OrderableTypes =
    {
        ColumnType.Integer64,
        ColumnType.Float64,
        ColumnType.Bool,
        ColumnType.String,
        ColumnType.Id
    };

    /// <summary>
    /// Core invariant: for composites whose columns share a type per position (no NULLs), the ordinal
    /// order of the encoded keys matches CompositeColumnValue.CompareTo.
    /// </summary>
    [Test]
    public void EncodingPreservesOrderAcrossRandomComposites()
    {
        Random random = new(20260527);

        for (int iteration = 0; iteration < 2000; iteration++)
        {
            // Build a random "schema": 1..4 columns, each a fixed orderable type.
            int columns = random.Next(1, 5);
            ColumnType[] schema = new ColumnType[columns];
            for (int c = 0; c < columns; c++)
                schema[c] = OrderableTypes[random.Next(OrderableTypes.Length)];

            // Generate a batch of values conforming to that schema and cross-compare them all.
            List<CompositeColumnValue> values = new();
            for (int n = 0; n < 12; n++)
                values.Add(RandomComposite(random, schema));

            for (int i = 0; i < values.Count; i++)
            {
                for (int j = 0; j < values.Count; j++)
                {
                    CompositeColumnValue a = values[i];
                    CompositeColumnValue b = values[j];

                    int semantic = Math.Sign(a.CompareTo(b));
                    int encoded = Math.Sign(string.CompareOrdinal(KeyEncoder.Encode(a), KeyEncoder.Encode(b)));

                    Assert.AreEqual(
                        semantic,
                        encoded,
                        $"Order mismatch: a={a}, b={b}, schema=[{string.Join(",", schema)}]"
                    );
                }
            }
        }
    }

    [Test]
    public void NegativeAndPositiveIntegersSortCorrectly()
    {
        long[] samples = { long.MinValue, -1000, -1, 0, 1, 1000, long.MaxValue };
        AssertSorted(samples, v => Single(new ColumnValue(ColumnType.Integer64, v)));
    }

    [Test]
    public void NegativeAndPositiveFloatsSortCorrectly()
    {
        double[] samples = { double.MinValue, -1e9, -1.5, 0.0, 1.5, 1e9, double.MaxValue };
        AssertSorted(samples, v => Single(new ColumnValue(ColumnType.Float64, v)));
    }

    [Test]
    public void StringPrefixesSortBeforeLongerStrings()
    {
        string[] samples = { "", "a", "ab", "abc", "b" };
        AssertSorted(samples, v => Single(new ColumnValue(ColumnType.String, v)));
    }

    [Test]
    public void NullSortsBeforeAnyPresentValue()
    {
        ColumnValue nullValue = new(ColumnType.Null, false);
        ColumnValue present = new(ColumnType.Integer64, long.MinValue);

        string encodedNull = KeyEncoder.Encode(Single(nullValue));
        string encodedPresent = KeyEncoder.Encode(Single(present));

        Assert.That(string.CompareOrdinal(encodedNull, encodedPresent), Is.LessThan(0));
    }

    [Test]
    public void TwoNullsEncodeEqually()
    {
        string a = KeyEncoder.Encode(Single(new ColumnValue(ColumnType.Null, false)));
        string b = KeyEncoder.Encode(Single(new ColumnValue(ColumnType.Null, false)));

        Assert.AreEqual(a, b);
    }

    [Test]
    public void StringContainingControlCharsPreservesOrdering()
    {
        // A string containing the terminator code unit (U+0000) must still preserve prefix ordering.
        string plain = "a";
        string withNull = "a" + (char)0x0000;

        string encodedPlain = KeyEncoder.Encode(Single(new ColumnValue(ColumnType.String, plain)));
        string encodedWithNull = KeyEncoder.Encode(Single(new ColumnValue(ColumnType.String, withNull)));

        // Ordinal: "a" < "a\0"; the encoding must agree.
        Assert.That(string.CompareOrdinal(plain, withNull), Is.LessThan(0));
        Assert.That(string.CompareOrdinal(encodedPlain, encodedWithNull), Is.LessThan(0));
    }

    // ---- Decode round-trip tests ------------------------------------------

    [Test]
    public void RoundTripInteger64()
    {
        long[] samples = { long.MinValue, -1000, -1, 0, 1, 1000, long.MaxValue };
        foreach (long v in samples)
        {
            CompositeColumnValue original = Single(new ColumnValue(ColumnType.Integer64, v));
            CompositeColumnValue decoded = KeyEncoder.Decode(KeyEncoder.Encode(original), new[] { ColumnType.Integer64 });
            Assert.AreEqual(ColumnType.Integer64, decoded.Values[0].Type);
            Assert.AreEqual(v, decoded.Values[0].LongValue, $"Round-trip failed for long {v}");
        }
    }

    [Test]
    public void RoundTripFloat64()
    {
        double[] samples = { double.MinValue, -1e9, -1.5, 0.0, 1.5, 1e9, double.MaxValue };
        foreach (double v in samples)
        {
            CompositeColumnValue original = Single(new ColumnValue(ColumnType.Float64, v));
            CompositeColumnValue decoded = KeyEncoder.Decode(KeyEncoder.Encode(original), new[] { ColumnType.Float64 });
            Assert.AreEqual(ColumnType.Float64, decoded.Values[0].Type);
            Assert.AreEqual(v, decoded.Values[0].FloatValue, $"Round-trip failed for double {v}");
        }
    }

    [Test]
    public void RoundTripBool()
    {
        foreach (bool v in new[] { true, false })
        {
            CompositeColumnValue original = Single(new ColumnValue(ColumnType.Bool, v));
            CompositeColumnValue decoded = KeyEncoder.Decode(KeyEncoder.Encode(original), new[] { ColumnType.Bool });
            Assert.AreEqual(ColumnType.Bool, decoded.Values[0].Type);
            Assert.AreEqual(v, decoded.Values[0].BoolValue);
        }
    }

    [Test]
    public void RoundTripString()
    {
        string[] samples = { "", "a", "ab", "abc", "b", "hello world", "a" + (char)0x0000 + "b" };
        foreach (string v in samples)
        {
            CompositeColumnValue original = Single(new ColumnValue(ColumnType.String, v));
            CompositeColumnValue decoded = KeyEncoder.Decode(KeyEncoder.Encode(original), new[] { ColumnType.String });
            Assert.AreEqual(ColumnType.String, decoded.Values[0].Type);
            Assert.AreEqual(v, decoded.Values[0].StrValue, $"Round-trip failed for string {System.Text.RegularExpressions.Regex.Escape(v)}");
        }
    }

    [Test]
    public void RoundTripNull()
    {
        CompositeColumnValue original = Single(new ColumnValue(ColumnType.Null, false));
        CompositeColumnValue decoded = KeyEncoder.Decode(KeyEncoder.Encode(original), new[] { ColumnType.Null });
        Assert.AreEqual(ColumnType.Null, decoded.Values[0].Type);
    }

    [Test]
    public void RoundTripComposite()
    {
        // (Integer64, String, Bool, Float64) multi-column key
        ColumnValue[] cols =
        {
            new ColumnValue(ColumnType.Integer64, -42L),
            new ColumnValue(ColumnType.String, "hello"),
            new ColumnValue(ColumnType.Bool, true),
            new ColumnValue(ColumnType.Float64, -3.14),
        };

        CompositeColumnValue original = new(cols);
        ColumnType[] schema = { ColumnType.Integer64, ColumnType.String, ColumnType.Bool, ColumnType.Float64 };

        CompositeColumnValue decoded = KeyEncoder.Decode(KeyEncoder.Encode(original), schema);

        Assert.AreEqual(-42L, decoded.Values[0].LongValue);
        Assert.AreEqual("hello", decoded.Values[1].StrValue);
        Assert.AreEqual(true, decoded.Values[2].BoolValue);
        Assert.AreEqual(-3.14, decoded.Values[3].FloatValue);
    }

    [Test]
    public void RoundTripRandomComposites()
    {
        Random random = new(20260527);

        for (int iteration = 0; iteration < 2000; iteration++)
        {
            int columns = random.Next(1, 5);
            ColumnType[] schema = new ColumnType[columns];
            for (int c = 0; c < columns; c++)
                schema[c] = OrderableTypes[random.Next(OrderableTypes.Length)];

            CompositeColumnValue original = RandomComposite(random, schema);
            string encoded = KeyEncoder.Encode(original);
            CompositeColumnValue decoded = KeyEncoder.Decode(encoded, schema);

            for (int c = 0; c < columns; c++)
            {
                ColumnValue orig = original.Values[c];
                ColumnValue dec = decoded.Values[c];

                Assert.AreEqual(orig.Type, dec.Type, $"Type mismatch at column {c}, iteration {iteration}");

                switch (orig.Type)
                {
                    case ColumnType.Integer64:
                        Assert.AreEqual(orig.LongValue, dec.LongValue, $"Long mismatch col {c} iter {iteration}");
                        break;
                    case ColumnType.Float64:
                        Assert.AreEqual(orig.FloatValue, dec.FloatValue, $"Float mismatch col {c} iter {iteration}");
                        break;
                    case ColumnType.Bool:
                        Assert.AreEqual(orig.BoolValue, dec.BoolValue, $"Bool mismatch col {c} iter {iteration}");
                        break;
                    case ColumnType.String:
                    case ColumnType.Id:
                        Assert.AreEqual(orig.StrValue, dec.StrValue, $"String mismatch col {c} iter {iteration}");
                        break;
                    case ColumnType.Null:
                        break; // both are null, type check above is sufficient
                }
            }
        }
    }

    private static void AssertSorted<T>(T[] ascendingSamples, Func<T, CompositeColumnValue> toComposite)
    {
        for (int i = 0; i + 1 < ascendingSamples.Length; i++)
        {
            string lower = KeyEncoder.Encode(toComposite(ascendingSamples[i]));
            string higher = KeyEncoder.Encode(toComposite(ascendingSamples[i + 1]));

            Assert.That(
                string.CompareOrdinal(lower, higher),
                Is.LessThan(0),
                $"Expected {ascendingSamples[i]} to encode before {ascendingSamples[i + 1]}"
            );
        }
    }

    private static CompositeColumnValue Single(ColumnValue value) => new(new[] { value });

    private static CompositeColumnValue RandomComposite(Random random, ColumnType[] schema)
    {
        ColumnValue[] values = new ColumnValue[schema.Length];

        for (int i = 0; i < schema.Length; i++)
            values[i] = RandomValue(random, schema[i]);

        return new CompositeColumnValue(values);
    }

    private static ColumnValue RandomValue(Random random, ColumnType type)
    {
        switch (type)
        {
            case ColumnType.Integer64:
                return new ColumnValue(ColumnType.Integer64, RandomLong(random));

            case ColumnType.Float64:
                return new ColumnValue(ColumnType.Float64, RandomDouble(random));

            case ColumnType.Bool:
                return new ColumnValue(ColumnType.Bool, random.Next(2) == 0);

            case ColumnType.String:
                return new ColumnValue(ColumnType.String, RandomString(random));

            case ColumnType.Id:
                return new ColumnValue(ColumnType.Id, RandomString(random));

            default:
                throw new InvalidOperationException("Unsupported type: " + type);
        }
    }

    private static long RandomLong(Random random)
    {
        int bucket = random.Next(5);
        return bucket switch
        {
            0 => long.MinValue,
            1 => long.MaxValue,
            2 => -(long)random.Next(0, 1000),
            3 => random.Next(0, 1000),
            _ => (long)(random.NextDouble() * long.MaxValue) * (random.Next(2) == 0 ? -1 : 1)
        };
    }

    private static double RandomDouble(Random random)
    {
        int bucket = random.Next(5);
        return bucket switch
        {
            0 => double.MinValue,
            1 => double.MaxValue,
            2 => -random.NextDouble() * 1e6,
            3 => random.NextDouble() * 1e6,
            _ => (random.NextDouble() - 0.5) * 10
        };
    }

    private static string RandomString(Random random)
    {
        const string alphabet = "ab AB09";
        int length = random.Next(0, 6);
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = alphabet[random.Next(alphabet.Length)];
        return new string(chars);
    }
}
