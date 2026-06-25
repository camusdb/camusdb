
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
        ColumnType.Float32,
        ColumnType.Bool,
        ColumnType.String,
        ColumnType.Id,
        ColumnType.Date,
        ColumnType.DateTime,
        ColumnType.Bytes,
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

    // Float64 corner cases -- NaN, infinities, subnormals, ±max -- must encode in exactly the order
    // ColumnValue.CompareTo (i.e. double.CompareTo) defines, so a ranged Float64 index never misroutes
    // a pathological value. The expected order is derived from CompareTo itself (asserted as a
    // precondition), not hand-asserted. NB: signed zero is deliberately excluded here because it is the
    // one CompareTo/encoding divergence -- see SignedZeroEncodesDistinctlyDespiteCompareToEquality.
    [Test]
    public void FloatEdgeCasesSortConsistentlyWithCompareTo()
    {
        // All strictly distinct under double.CompareTo: NaN < -Inf < ... < 0.0 < ... < +Inf.
        double[] ascending =
        {
            double.NaN,
            double.NegativeInfinity,
            double.MinValue,
            -1.0,
            -double.Epsilon,   // largest-magnitude negative subnormal
            0.0,
            double.Epsilon,    // smallest positive subnormal
            1.0,
            double.MaxValue,
            double.PositiveInfinity
        };

        for (int i = 0; i + 1 < ascending.Length; i++)
        {
            ColumnValue a = new(ColumnType.Float64, ascending[i]);
            ColumnValue b = new(ColumnType.Float64, ascending[i + 1]);

            Assert.That(a.CompareTo(b), Is.LessThan(0),
                $"Precondition: {ascending[i]} must be < {ascending[i + 1]} under CompareTo");

            string ea = KeyEncoder.Encode(Single(a));
            string eb = KeyEncoder.Encode(Single(b));
            Assert.That(string.CompareOrdinal(ea, eb), Is.LessThan(0),
                $"Encoding order must match CompareTo for {ascending[i]} < {ascending[i + 1]}");
        }
    }

    // KNOWN CAVEAT (pre-existing, independent of routing): double.CompareTo treats -0.0 and +0.0 as
    // EQUAL, but KeyEncoder encodes them to two distinct, adjacent keys (negative-side vs positive-side
    // of the sign boundary). Consequence for a ranged (or hash) Float64 index: a stored -0.0 sorts just
    // before +0.0, so a half-open scan starting at +0.0 (e.g. `x >= 0`) would not see a -0.0 row. This
    // test PINS that divergence so it is visible and intentional rather than a silent surprise; closing
    // it would require normalising -0.0 -> +0.0 in KeyEncoder (a separate encoding change affecting all
    // indexes, not part of C3's routing/lock work).
    [Test]
    public void SignedZeroEncodesDistinctlyDespiteCompareToEquality()
    {
        ColumnValue negZero = new(ColumnType.Float64, -0.0);
        ColumnValue posZero = new(ColumnType.Float64, 0.0);

        Assert.AreEqual(0, negZero.CompareTo(posZero),
            "CompareTo treats -0.0 and +0.0 as equal");

        string encNeg = KeyEncoder.Encode(Single(negZero));
        string encPos = KeyEncoder.Encode(Single(posZero));

        Assert.That(string.CompareOrdinal(encNeg, encPos), Is.LessThan(0),
            "Known caveat: -0.0 encodes strictly before +0.0 even though CompareTo says they are equal");
    }

    [Test]
    public void StringPrefixesSortBeforeLongerStrings()
    {
        string[] samples = { "", "a", "ab", "abc", "b" };
        AssertSorted(samples, v => Single(new ColumnValue(ColumnType.String, v)));
    }

    // Ranged TEXT keys must stay order-preserving across the FULL UTF-16 range, not just ASCII --
    // including a BMP code point above the surrogate block and a supplementary-plane code point.
    // In UTF-16 ordinal (what KeyEncoder, CompareTo, and Kahuna's range layer all use) a
    // supplementary char's lead surrogate (U+D83D) sorts BELOW U+E000+ BMP chars; pinning that
    // ordering makes a future switch to UTF-8-byte comparison anywhere fail loudly.
    [Test]
    public void StringWithHighAndSupplementaryCharsPreservesOrdering()
    {
        // Ascending by UTF-16 code-unit ordinal:
        //   U+4E2D (CJK)  <  U+1F600 (emoji, lead unit U+D83D)  <  U+E000 (PUA)  <  U+FFFF
        string[] samples = { "\u4E2D", "\uD83D\uDE00", "\uE000", "\uFFFF" };

        for (int i = 0; i + 1 < samples.Length; i++)
            Assert.That(string.CompareOrdinal(samples[i], samples[i + 1]), Is.LessThan(0),
                $"Precondition: sample ordinal order at {i}");

        AssertSorted(samples, v => Single(new ColumnValue(ColumnType.String, v)));
    }

    // C3b — the load-bearing invariant for ranged String indexes: an encoded String key is PURE
    // ASCII, so its UTF-8 byte order (how the RocksDB/SQLite persistence backends compare keys)
    // equals its UTF-16-ordinal order (how the in-memory B-tree / range routing / scan merge compare
    // keys) AND equals ColumnValue.CompareTo. Without the hex encoding these diverge for
    // supplementary-plane chars, which would misroute/misorder a key-range-routed String index.
    [Test]
    public void StringKeysAreAsciiAndUtf8OrderMatchesOrdinalAndCompareTo()
    {
        // Spans the divergent zone: BMP CJK, a supplementary emoji, a PUA char, U+FFFF, plus a
        // value containing a literal NUL and some prefixes.
        string[] samples =
        {
            "", "a", "ab", "abc", " ", "a b", "a\u0000b", "\u007F",
            "\u4E2D", "\uD83D\uDE00", "\uE000", "\uFFFF"
        };

        // 1) Every encoded String key is pure ASCII (< U+0080).
        foreach (string s in samples)
        {
            string enc = KeyEncoder.Encode(Single(new ColumnValue(ColumnType.String, s)));
            foreach (char c in enc)
                Assert.That(c, Is.LessThan('\u0080'),
                    $"Encoded String key must be pure ASCII; got U+{(int)c:X4} for input of length {s.Length}");
        }

        // 2) For every pair: UTF-8(encoded) byte order == ordinal(encoded) == CompareTo.
        for (int i = 0; i < samples.Length; i++)
        {
            for (int j = 0; j < samples.Length; j++)
            {
                ColumnValue a = new(ColumnType.String, samples[i]);
                ColumnValue b = new(ColumnType.String, samples[j]);

                string ea = KeyEncoder.Encode(Single(a));
                string eb = KeyEncoder.Encode(Single(b));

                int ordinal = Math.Sign(string.CompareOrdinal(ea, eb));
                int utf8 = Math.Sign(CompareUtf8Bytes(ea, eb));
                int semantic = Math.Sign(a.CompareTo(b));

                Assert.AreEqual(ordinal, utf8,
                    $"UTF-8 byte order must match encoded ordinal order ('{Escape(samples[i])}' vs '{Escape(samples[j])}')");
                Assert.AreEqual(semantic, ordinal,
                    $"Encoded ordinal order must match CompareTo ('{Escape(samples[i])}' vs '{Escape(samples[j])}')");
            }
        }
    }

    private static int CompareUtf8Bytes(string a, string b)
    {
        byte[] ba = Encoding.UTF8.GetBytes(a);
        byte[] bb = Encoding.UTF8.GetBytes(b);
        int n = Math.Min(ba.Length, bb.Length);
        for (int k = 0; k < n; k++)
        {
            int d = ba[k] - bb[k];
            if (d != 0)
                return d;
        }
        return ba.Length - bb.Length;
    }

    private static string Escape(string s) => System.Text.RegularExpressions.Regex.Escape(s);


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

    // ---- new indexable types: order + round-trip + ASCII --------------------

    [Test]
    public void Float32_OrderingMatchesCompareTo()
    {
        Random random = new(20260625);
        for (int iter = 0; iter < 2000; iter++)
        {
            ColumnValue a = new(ColumnType.Float32, (double)RandomFloat(random));
            ColumnValue b = new(ColumnType.Float32, (double)RandomFloat(random));

            int semantic = Math.Sign(a.CompareTo(b));
            int encoded  = Math.Sign(string.CompareOrdinal(
                KeyEncoder.Encode(Single(a)), KeyEncoder.Encode(Single(b))));

            Assert.AreEqual(semantic, encoded, $"Float32 order mismatch: {a} vs {b}");
        }
    }

    [Test]
    public void Float32_RoundTrip()
    {
        float[] samples = { float.MinValue, -1.5f, 0f, 1.5f, float.MaxValue };
        foreach (float v in samples)
        {
            CompositeColumnValue orig = Single(new ColumnValue(ColumnType.Float32, (double)v));
            CompositeColumnValue dec  = KeyEncoder.Decode(KeyEncoder.Encode(orig), [ColumnType.Float32]);
            Assert.That(dec.Values[0].Type, Is.EqualTo(ColumnType.Float32));
            Assert.That((float)dec.Values[0].FloatValue, Is.EqualTo(v), $"Float32 round-trip failed for {v}");
        }
    }

    [Test]
    public void Float32_EncodedIsAscii()
    {
        foreach (float v in new[] { float.MinValue, -1f, 0f, 1f, float.MaxValue })
        {
            string enc = KeyEncoder.Encode(Single(new ColumnValue(ColumnType.Float32, (double)v)));
            foreach (char c in enc)
                Assert.That(c, Is.LessThan(''), $"Non-ASCII char U+{(int)c:X4} for float32 {v}");
        }
    }

    [Test]
    public void Date_OrderingMatchesCompareTo()
    {
        Random random = new(20260625);
        for (int iter = 0; iter < 2000; iter++)
        {
            ColumnValue a = new(ColumnType.Date, RandomDateTicks(random));
            ColumnValue b = new(ColumnType.Date, RandomDateTicks(random));

            int semantic = Math.Sign(a.CompareTo(b));
            int encoded  = Math.Sign(string.CompareOrdinal(
                KeyEncoder.Encode(Single(a)), KeyEncoder.Encode(Single(b))));

            Assert.AreEqual(semantic, encoded, $"Date order mismatch");
        }
    }

    [Test]
    public void Date_RoundTrip()
    {
        long[] samples =
        [
            DateTime.MinValue.Ticks,
            new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks,
            new DateTime(2026, 6, 25, 0, 0, 0, DateTimeKind.Utc).Ticks,
            DateTime.MaxValue.Ticks
        ];
        foreach (long t in samples)
        {
            CompositeColumnValue orig = Single(new ColumnValue(ColumnType.Date, t));
            CompositeColumnValue dec  = KeyEncoder.Decode(KeyEncoder.Encode(orig), [ColumnType.Date]);
            Assert.That(dec.Values[0].Type, Is.EqualTo(ColumnType.Date));
            Assert.That(dec.Values[0].LongValue, Is.EqualTo(t));
        }
    }

    [Test]
    public void DateTime_OrderingMatchesCompareTo()
    {
        Random random = new(20260625);
        for (int iter = 0; iter < 2000; iter++)
        {
            ColumnValue a = new(ColumnType.DateTime, RandomDateTicks(random));
            ColumnValue b = new(ColumnType.DateTime, RandomDateTicks(random));

            int semantic = Math.Sign(a.CompareTo(b));
            int encoded  = Math.Sign(string.CompareOrdinal(
                KeyEncoder.Encode(Single(a)), KeyEncoder.Encode(Single(b))));

            Assert.AreEqual(semantic, encoded);
        }
    }

    [Test]
    public void DateTime_RoundTrip_MinMax()
    {
        foreach (long t in new[] { DateTime.MinValue.Ticks, DateTime.MaxValue.Ticks })
        {
            CompositeColumnValue orig = Single(new ColumnValue(ColumnType.DateTime, t));
            CompositeColumnValue dec  = KeyEncoder.Decode(KeyEncoder.Encode(orig), [ColumnType.DateTime]);
            Assert.That(dec.Values[0].Type, Is.EqualTo(ColumnType.DateTime));
            Assert.That(dec.Values[0].LongValue, Is.EqualTo(t));
        }
    }

    [Test]
    public void Bytes_OrderingMatchesCompareTo()
    {
        Random random = new(20260625);
        for (int iter = 0; iter < 2000; iter++)
        {
            ColumnValue a = new(RandomBytes(random));
            ColumnValue b = new(RandomBytes(random));

            int semantic = Math.Sign(a.CompareTo(b));
            int encoded  = Math.Sign(string.CompareOrdinal(
                KeyEncoder.Encode(Single(a)), KeyEncoder.Encode(Single(b))));

            Assert.AreEqual(semantic, encoded, $"Bytes order mismatch");
        }
    }

    [Test]
    public void Bytes_PrefixSortCorrectly()
    {
        byte[][] ascending = [[], [0x00], [0x00, 0x00], [0x00, 0x01], [0xFF]];
        for (int i = 0; i + 1 < ascending.Length; i++)
        {
            ColumnValue a = new(ascending[i]);
            ColumnValue b = new(ascending[i + 1]);
            string ea = KeyEncoder.Encode(Single(a));
            string eb = KeyEncoder.Encode(Single(b));
            Assert.That(string.CompareOrdinal(ea, eb), Is.LessThan(0),
                $"Bytes prefix order failed at index {i}");
        }
    }

    [Test]
    public void Bytes_RoundTrip()
    {
        byte[][] samples = [[], [0x00], [0xDE, 0xAD, 0xBE, 0xEF], [0xFF, 0x00, 0x01]];
        foreach (byte[] payload in samples)
        {
            CompositeColumnValue orig = Single(new ColumnValue(payload));
            CompositeColumnValue dec  = KeyEncoder.Decode(KeyEncoder.Encode(orig), [ColumnType.Bytes]);
            Assert.That(dec.Values[0].Type, Is.EqualTo(ColumnType.Bytes));
            Assert.That(dec.Values[0].BytesValue, Is.EqualTo(payload));
        }
    }

    [Test]
    public void Bytes_EncodedIsAscii()
    {
        byte[] payload = [0x00, 0x7F, 0x80, 0xFF];
        string enc = KeyEncoder.Encode(Single(new ColumnValue(payload)));
        foreach (char c in enc)
            Assert.That(c, Is.LessThan(''), $"Non-ASCII char U+{(int)c:X4} in bytes encoding");
    }

    [Test]
    public void Array_EncodeThrows()
    {
        ColumnValue arr = ColumnValue.FromArray(ColumnType.Integer64, [new(ColumnType.Integer64, 1L)]);
        Assert.Throws<CamusDB.Core.CamusDBException>(() => KeyEncoder.Encode(Single(arr)));
    }

    [Test]
    public void Composite_MixingNewTypesWithExisting_RoundTrips()
    {
        long dateTicks = new DateTime(2026, 6, 25, 0, 0, 0, DateTimeKind.Utc).Ticks;
        ColumnValue[] cols =
        [
            new(ColumnType.Integer64, -7L),
            new(ColumnType.Float32,   (double)1.5f),
            new(ColumnType.Date,      dateTicks),
            new(new byte[] { 0xAB, 0xCD }),
            new(ColumnType.String,    "hello"),
        ];

        CompositeColumnValue orig = new(cols);
        ColumnType[] schema = [ColumnType.Integer64, ColumnType.Float32, ColumnType.Date, ColumnType.Bytes, ColumnType.String];

        CompositeColumnValue dec = KeyEncoder.Decode(KeyEncoder.Encode(orig), schema);

        Assert.That(dec.Values[0].LongValue,           Is.EqualTo(-7L));
        Assert.That((float)dec.Values[1].FloatValue,   Is.EqualTo(1.5f));
        Assert.That(dec.Values[2].LongValue,           Is.EqualTo(dateTicks));
        Assert.That(dec.Values[3].BytesValue,          Is.EqualTo(new byte[] { 0xAB, 0xCD }));
        Assert.That(dec.Values[4].StrValue,            Is.EqualTo("hello"));
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

            case ColumnType.Float32:
                return new ColumnValue(ColumnType.Float32, (double)RandomFloat(random));

            case ColumnType.Bool:
                return new ColumnValue(ColumnType.Bool, random.Next(2) == 0);

            case ColumnType.String:
                return new ColumnValue(ColumnType.String, RandomString(random));

            case ColumnType.Id:
                return new ColumnValue(ColumnType.Id, RandomString(random));

            case ColumnType.Date:
                return new ColumnValue(ColumnType.Date, RandomDateTicks(random));

            case ColumnType.DateTime:
                return new ColumnValue(ColumnType.DateTime, RandomDateTicks(random));

            case ColumnType.Bytes:
                return new ColumnValue(RandomBytes(random));

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

    private static float RandomFloat(Random random)
    {
        int bucket = random.Next(5);
        return bucket switch
        {
            0 => float.MinValue,
            1 => float.MaxValue,
            2 => -(float)(random.NextDouble() * 1e6),
            3 => (float)(random.NextDouble() * 1e6),
            _ => (float)((random.NextDouble() - 0.5) * 10)
        };
    }

    private static readonly long DateTicksMin = new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
    private static readonly long DateTicksMax = new DateTime(2100, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

    private static long RandomDateTicks(Random random)
    {
        // Uniformly sample ticks in a realistic calendar range, with boundary samples.
        int bucket = random.Next(4);
        return bucket switch
        {
            0 => DateTime.MinValue.Ticks,
            1 => DateTime.MaxValue.Ticks,
            _ => DateTicksMin + (long)(random.NextDouble() * (DateTicksMax - DateTicksMin))
        };
    }

    private static byte[] RandomBytes(Random random)
    {
        int len = random.Next(0, 16);
        byte[] b = new byte[len];
        random.NextBytes(b);
        return b;
    }

    // Tokens deliberately span the full UTF-16 range an index key can contain, not just ASCII —
    // this is what proves ranged TEXT index keys stay order-preserving. Includes: the encoder's
    // own sentinels as CONTENT (U+0000 terminator-lead, U+0001 terminator-tail, U+FFFF escape-tail),
    // the ASCII/Latin boundary, a Latin-1 char, a BMP CJK char (3-byte UTF-8, single UTF-16 unit), and
    // a SUPPLEMENTARY-plane code point as a well-formed surrogate pair (😀 U+1F600 → U+D83D U+DE00).
    // KeyEncoder, ColumnValue.CompareTo, and Kahuna's range layer all compare by UTF-16 code unit
    // (ordinal), so the property test must agree across all of these, including the surrogate range.
    private static readonly string[] StringTokens =
    {
        "a", "b", " ", "A", "B", "0", "9",
        "\u0000",       // FieldTerminatorLead as content -> must be escaped and still ordered
        "\u0001",       // FieldTerminatorTail as content
        "\u007F", "\u0080", // ASCII / Latin-1 boundary
        "\u00E9",       // e-acute (Latin-1 supplement)
        "\u4E2D",       // CJK ideograph (BMP, 3-byte UTF-8, single UTF-16 unit)
        "\uFFFD",       // replacement char
        "\uFFFF",       // EscapeTail sentinel as content (unescaped -> must still order)
        "\uD83D\uDE00"  // U+1F600 emoji (supplementary plane, well-formed surrogate pair)
    };

    private static string RandomString(Random random)
    {
        int tokens = random.Next(0, 6);
        StringBuilder sb = new();
        for (int i = 0; i < tokens; i++)
            sb.Append(StringTokens[random.Next(StringTokens.Length)]);
        return sb.ToString();
    }

    // ---- IndexKeySentinel invariant ------------------------------------------

    // KvTableStore appends IndexKeySentinel (U+FFFF) after the encoded upper bound for non-unique
    // index keys so that every possible rowId suffix ({24 lowercase hex chars, U+0030..U+0066}) is
    // covered by the range lock.  This relies on KeyEncoder never emitting a character >= U+FFFF.
    //
    // This test pins that invariant across every type and a wide sample of values, including the
    // edge cases that already appear in StringTokens (e.g. U+FFFF and U+FFFD as *content*).  If
    // KeyEncoder is ever changed to emit a code unit >= U+FFFF, this test fails first.
    [Test]
    public void AllEncodedCharsBelowIndexKeySentinel()
    {
        const char Sentinel = '￿'; // must match KvTableStore.IndexKeySentinel

        // ---- fixed representative values per type ----
        List<CompositeColumnValue> samples =
        [
            Single(new ColumnValue(ColumnType.Integer64, long.MinValue)),
            Single(new ColumnValue(ColumnType.Integer64, -1L)),
            Single(new ColumnValue(ColumnType.Integer64, 0L)),
            Single(new ColumnValue(ColumnType.Integer64, 1L)),
            Single(new ColumnValue(ColumnType.Integer64, long.MaxValue)),
            Single(new ColumnValue(ColumnType.Float64, double.NegativeInfinity)),
            Single(new ColumnValue(ColumnType.Float64, -1.5)),
            Single(new ColumnValue(ColumnType.Float64, 0.0)),
            Single(new ColumnValue(ColumnType.Float64, 1.5)),
            Single(new ColumnValue(ColumnType.Float64, double.PositiveInfinity)),
            Single(new ColumnValue(ColumnType.Bool, false)),
            Single(new ColumnValue(ColumnType.Bool, true)),
            Single(new ColumnValue(ColumnType.String, "")),
            Single(new ColumnValue(ColumnType.String, "hello")),
            // High-plane content: KeyEncoder encodes these as 4-hex-per-code-unit → pure ASCII output.
            Single(new ColumnValue(ColumnType.String, "�")),   // replacement char as value
            Single(new ColumnValue(ColumnType.String, "￿")),   // the sentinel itself as *value*
            Single(new ColumnValue(ColumnType.String, "😀")), // supplementary plane emoji
            Single(new ColumnValue(ColumnType.Null, false)),
        ];

        // ---- random composites spanning all types ----
        Random rng = new(20260612);
        for (int i = 0; i < 500; i++)
        {
            int cols = rng.Next(1, 5);
            ColumnType[] schema = new ColumnType[cols];
            for (int c = 0; c < cols; c++)
                schema[c] = OrderableTypes[rng.Next(OrderableTypes.Length)];
            samples.Add(RandomComposite(rng, schema));
        }

        foreach (CompositeColumnValue cv in samples)
        {
            string encoded = KeyEncoder.Encode(cv);
            foreach (char ch in encoded)
                Assert.That((int)ch, Is.LessThan((int)Sentinel),
                    $"KeyEncoder emitted U+{(int)ch:X4} which is >= IndexKeySentinel U+FFFF " +
                    $"(encoded output: '{encoded}', input: {cv})");
        }
    }
}
