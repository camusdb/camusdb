/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Formats.Cbor;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models.Jsonb;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit tests for the <c>jsonb</c> codec: text in and text out, the canonical form, the refusals, and
/// the comparison, hash, containment and existence rules on the bytes. Every expected value in the
/// test cases came from PostgreSQL 16.11 for the same input, so these tests pin PostgreSQL behavior,
/// not the behavior of this code.
/// </summary>
[TestFixture]
internal sealed class TestJsonbCodec
{
    private static byte[] Cell(string text) => JsonbEncoder.FromText(text);

    private static ReadOnlySpan<byte> Item(string text) => JsonbDocument.Item(Cell(text));

    private static string Text(string text, bool pretty = false) => JsonbText.ToText(Item(text), pretty);

    [TestCase("null", "null", "null")]
    [TestCase("true", "true", "true")]
    [TestCase("false", "false", "false")]
    [TestCase("0", "0", "0")]
    [TestCase("-0", "0", "0")]
    [TestCase("-0.0", "0.0", "0.0")]
    [TestCase("0.0", "0.0", "0.0")]
    [TestCase("1", "1", "1")]
    [TestCase("-1", "-1", "-1")]
    [TestCase("1.50", "1.50", "1.50")]
    [TestCase("1e3", "1000", "1000")]
    [TestCase("1.5e1", "15", "15")]
    [TestCase("1.50e1", "15.0", "15.0")]
    [TestCase("1e-3", "0.001", "0.001")]
    [TestCase("-1.25E+2", "-125", "-125")]
    [TestCase("123456789012345678901234567890", "123456789012345678901234567890", "123456789012345678901234567890")]
    [TestCase("-123456789012345678901234567890.0012", "-123456789012345678901234567890.0012", "-123456789012345678901234567890.0012")]
    [TestCase("9223372036854775807", "9223372036854775807", "9223372036854775807")]
    [TestCase("9223372036854775808", "9223372036854775808", "9223372036854775808")]
    [TestCase("-9223372036854775808", "-9223372036854775808", "-9223372036854775808")]
    [TestCase("-9223372036854775809", "-9223372036854775809", "-9223372036854775809")]
    [TestCase("0.000000000000000000001", "0.000000000000000000001", "0.000000000000000000001")]
    [TestCase("\"\"", "\"\"", "\"\"")]
    [TestCase("\"abc\"", "\"abc\"", "\"abc\"")]
    [TestCase("\"a\\\"b\\\\c\\/d\"", "\"a\\\"b\\\\c/d\"", "\"a\\\"b\\\\c/d\"")]
    [TestCase("\"\\b\\f\\n\\r\\t\\u0001\\u001f\"", "\"\\b\\f\\n\\r\\t\\u0001\\u001f\"", "\"\\b\\f\\n\\r\\t\\u0001\\u001f\"")]
    [TestCase("\"é中😀\"", "\"é中😀\"", "\"é中😀\"")]
    [TestCase("\"é中😀\"", "\"é中😀\"", "\"é中😀\"")]
    [TestCase("[]", "[]", "[\n]")]
    [TestCase("{}", "{}", "{\n}")]
    [TestCase("[1, 2, 3]", "[1, 2, 3]", "[\n    1,\n    2,\n    3\n]")]
    [TestCase("{\"b\": 1, \"a\": 2, \"aa\": 3, \"c\": {\"z\": [], \"y\": {}}}", "{\"a\": 2, \"b\": 1, \"c\": {\"y\": {}, \"z\": []}, \"aa\": 3}", "{\n    \"a\": 2,\n    \"b\": 1,\n    \"c\": {\n        \"y\": {\n        },\n        \"z\": [\n        ]\n    },\n    \"aa\": 3\n}")]
    [TestCase("{\"a\": 1, \"a\": 2}", "{\"a\": 2}", "{\n    \"a\": 2\n}")]
    [TestCase("{\"a\": {\"x\": 1}, \"a\": [1]}", "{\"a\": [1]}", "{\n    \"a\": [\n        1\n    ]\n}")]
    [TestCase("[[[]]]", "[[[]]]", "[\n    [\n        [\n        ]\n    ]\n]")]
    [TestCase("[{\"k\": null}, [true, false], \"s\", 1.0]", "[{\"k\": null}, [true, false], \"s\", 1.0]", "[\n    {\n        \"k\": null\n    },\n    [\n        true,\n        false\n    ],\n    \"s\",\n    1.0\n]")]
    [TestCase("{\"key with space\": \"v\", \"\": 0, \"ключ\": \"значение\"}", "{\"\": 0, \"ключ\": \"значение\", \"key with space\": \"v\"}", "{\n    \"\": 0,\n    \"ключ\": \"значение\",\n    \"key with space\": \"v\"\n}")]
    [TestCase("  { \"a\" :  [ 1 , 2 ] }  ", "{\"a\": [1, 2]}", "{\n    \"a\": [\n        1,\n        2\n    ]\n}")]
    [TestCase("[1,[2,[3,[4,[5]]]]]", "[1, [2, [3, [4, [5]]]]]", "[\n    1,\n    [\n        2,\n        [\n            3,\n            [\n                4,\n                [\n                    5\n                ]\n            ]\n        ]\n    ]\n]")]
    [TestCase("{\"aa\": 1, \"b\": 2, \"ccc\": 3, \"d\": 4, \"eeee\": 5}", "{\"b\": 2, \"d\": 4, \"aa\": 1, \"ccc\": 3, \"eeee\": 5}", "{\n    \"b\": 2,\n    \"d\": 4,\n    \"aa\": 1,\n    \"ccc\": 3,\n    \"eeee\": 5\n}")]
    public void TextAndPrettyText_MatchPostgreSql(string input, string expectedText, string expectedPretty)
    {
        Assert.AreEqual(expectedText, Text(input));
        Assert.AreEqual(expectedPretty, Text(input, pretty: true));

        // The text form parses back to the same bytes.
        CollectionAssert.AreEqual(Cell(input), Cell(expectedText));
    }

    [Test]
    public void EveryCell_IsCanonicalCbor_ForTheReferenceReader()
    {
        string[] documents =
        [
            "{\"b\": 1, \"a\": [1.50, -9223372036854775809, null, true], \"aa\": {\"y\": \"é\", \"x\": 0.001}}",
            "123456789012345678901234567890.123", "[]", "{}", "\"\"",
        ];

        foreach (string document in documents)
        {
            byte[] cell = Cell(document);
            Assert.AreEqual(JsonbLimits.FormatVersion, cell[0]);

            // The library reader in canonical mode refuses unsorted keys, duplicate keys, indefinite
            // lengths and non-shortest encodings.
            CborReader reader = new(cell.AsMemory(1), CborConformanceMode.Canonical);
            reader.SkipValue();
            Assert.AreEqual(CborReaderState.Finished, reader.PeekState(), document);
        }
    }

    [TestCase("{\"a\":1,\"b\":2}", "{ \"b\" : 2 , \"a\" : 1 }")]
    [TestCase("{\"a\":2}", "{\"a\":1,\"a\":2}")]
    [TestCase("[1,2]", "[ 1 ,\n\t2 ]")]
    [TestCase("1000", "1e3")]
    [TestCase("0", "-0")]
    public void SameDocument_GivesSameBytes(string a, string b)
    {
        CollectionAssert.AreEqual(Cell(a), Cell(b));
    }

    [TestCase("\"\\u0000\"")]
    [TestCase("\"a\\u0000b\"")]
    [TestCase("{\"\\u0000\":1}")]
    [TestCase("\"\\ud800\"")]
    [TestCase("\"\\udc00x\"")]
    [TestCase("{\"\\ud800\":1}")]
    [TestCase("[1,]")]
    [TestCase("{\"a\":1,}")]
    [TestCase("01")]
    [TestCase("NaN")]
    [TestCase(".5")]
    [TestCase("1.")]
    [TestCase("\"a\" \"b\"")]
    [TestCase("")]
    [TestCase(" ")]
    [TestCase("{\"a\"}")]
    [TestCase("[1] // c")]
    [TestCase("1e131072")]
    [TestCase("1e-16384")]
    [TestCase("1.5e-16383")]
    [TestCase("0e-20000")]
    [TestCase("1e2147483648")]
    public void InvalidText_IsRefusedAsPostgreSqlRefusesIt(string input)
    {
        CamusDBException? ex = Assert.Throws<CamusDBException>(() => Cell(input));
        Assert.AreEqual(CamusDBErrorCodes.InvalidJsonText, ex!.Code, input);
    }

    [TestCase("1e131071", 131072)]
    [TestCase("1e-16383", 16385)]
    [TestCase("0e999999", 1)]
    public void NumbersAtThePostgreSqlLimits_AreAccepted(string input, int expectedTextLength)
    {
        Assert.AreEqual(expectedTextLength, Text(input).Length);
    }

    [Test]
    public void LongDigitStrings_AtTheLimits_RoundTrip()
    {
        string smallest = "0." + new string('0', 16382) + "1";
        Assert.AreEqual(smallest, Text(smallest));
        Assert.AreEqual(CamusDBErrorCodes.InvalidJsonText,
            Assert.Throws<CamusDBException>(() => Cell("0." + new string('0', 16383) + "1"))!.Code);

        string widest = "1" + new string('0', 131071) + ".5";
        Assert.AreEqual(widest, Text(widest));
        Assert.AreEqual(CamusDBErrorCodes.InvalidJsonText,
            Assert.Throws<CamusDBException>(() => Cell("1" + new string('0', 131072)))!.Code);
    }

    [Test]
    public void Depth_AtTheLimit_IsAccepted_AndOneMoreIsRefused()
    {
        int max = JsonbLimits.MaxNestingDepth;
        string atLimit = new string('[', max) + new string(']', max);
        Assert.AreEqual(max, JsonbDocument.Depth(Item(atLimit)));
        Assert.AreEqual(atLimit.Length, Text(atLimit).Length);

        string tooDeep = new string('[', max + 1) + new string(']', max + 1);
        Assert.AreEqual(CamusDBErrorCodes.JsonNestingTooDeep, Assert.Throws<CamusDBException>(() => Cell(tooDeep))!.Code);

        string objects = string.Concat(Enumerable.Repeat("{\"a\":", max + 1)) + "1" + new string('}', max + 1);
        Assert.AreEqual(CamusDBErrorCodes.JsonNestingTooDeep, Assert.Throws<CamusDBException>(() => Cell(objects))!.Code);
    }

    [Test]
    public void VeryDeepText_IsRefused_WithoutAStackOverflow()
    {
        string deep = new string('[', 100_000) + new string(']', 100_000);
        Assert.AreEqual(CamusDBErrorCodes.JsonNestingTooDeep, Assert.Throws<CamusDBException>(() => Cell(deep))!.Code);

        // Too deep and also malformed: the depth is what the caller hears about.
        string deepAndBroken = new string('[', 100_000) + "1,";
        Assert.AreEqual(CamusDBErrorCodes.JsonNestingTooDeep, Assert.Throws<CamusDBException>(() => Cell(deepAndBroken))!.Code);
    }

    [TestCase("null", "null", 0)]
    [TestCase("null", "true", -1)]
    [TestCase("null", "false", -1)]
    [TestCase("null", "0", -1)]
    [TestCase("null", "1", -1)]
    [TestCase("null", "1.0", -1)]
    [TestCase("null", "1.00", -1)]
    [TestCase("null", "-1", -1)]
    [TestCase("null", "2", -1)]
    [TestCase("null", "10", -1)]
    [TestCase("null", "1e2", -1)]
    [TestCase("null", "100", -1)]
    [TestCase("null", "\"a\"", -1)]
    [TestCase("null", "\"b\"", -1)]
    [TestCase("null", "\"aa\"", -1)]
    [TestCase("null", "\"\"", -1)]
    [TestCase("null", "[]", 1)]
    [TestCase("null", "[1]", -1)]
    [TestCase("null", "[2]", -1)]
    [TestCase("null", "[1,2]", -1)]
    [TestCase("null", "[[1]]", -1)]
    [TestCase("null", "{}", -1)]
    [TestCase("null", "{\"a\":1}", -1)]
    [TestCase("null", "{\"b\":1}", -1)]
    [TestCase("null", "{\"a\":2}", -1)]
    [TestCase("null", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("null", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("null", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("null", "{\"a\":[1,2]}", -1)]
    [TestCase("null", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("null", "[null]", -1)]
    [TestCase("null", "\"é\"", -1)]
    [TestCase("null", "\"z\"", -1)]
    [TestCase("null", "123456789012345678901234567890", -1)]
    [TestCase("null", "123456789012345678901234567890.0", -1)]
    [TestCase("true", "true", 0)]
    [TestCase("true", "false", 1)]
    [TestCase("true", "0", 1)]
    [TestCase("true", "1", 1)]
    [TestCase("true", "1.0", 1)]
    [TestCase("true", "1.00", 1)]
    [TestCase("true", "-1", 1)]
    [TestCase("true", "2", 1)]
    [TestCase("true", "10", 1)]
    [TestCase("true", "1e2", 1)]
    [TestCase("true", "100", 1)]
    [TestCase("true", "\"a\"", 1)]
    [TestCase("true", "\"b\"", 1)]
    [TestCase("true", "\"aa\"", 1)]
    [TestCase("true", "\"\"", 1)]
    [TestCase("true", "[]", 1)]
    [TestCase("true", "[1]", -1)]
    [TestCase("true", "[2]", -1)]
    [TestCase("true", "[1,2]", -1)]
    [TestCase("true", "[[1]]", -1)]
    [TestCase("true", "{}", -1)]
    [TestCase("true", "{\"a\":1}", -1)]
    [TestCase("true", "{\"b\":1}", -1)]
    [TestCase("true", "{\"a\":2}", -1)]
    [TestCase("true", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("true", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("true", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("true", "{\"a\":[1,2]}", -1)]
    [TestCase("true", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("true", "[null]", -1)]
    [TestCase("true", "\"é\"", 1)]
    [TestCase("true", "\"z\"", 1)]
    [TestCase("true", "123456789012345678901234567890", 1)]
    [TestCase("true", "123456789012345678901234567890.0", 1)]
    [TestCase("false", "false", 0)]
    [TestCase("false", "0", 1)]
    [TestCase("false", "1", 1)]
    [TestCase("false", "1.0", 1)]
    [TestCase("false", "1.00", 1)]
    [TestCase("false", "-1", 1)]
    [TestCase("false", "2", 1)]
    [TestCase("false", "10", 1)]
    [TestCase("false", "1e2", 1)]
    [TestCase("false", "100", 1)]
    [TestCase("false", "\"a\"", 1)]
    [TestCase("false", "\"b\"", 1)]
    [TestCase("false", "\"aa\"", 1)]
    [TestCase("false", "\"\"", 1)]
    [TestCase("false", "[]", 1)]
    [TestCase("false", "[1]", -1)]
    [TestCase("false", "[2]", -1)]
    [TestCase("false", "[1,2]", -1)]
    [TestCase("false", "[[1]]", -1)]
    [TestCase("false", "{}", -1)]
    [TestCase("false", "{\"a\":1}", -1)]
    [TestCase("false", "{\"b\":1}", -1)]
    [TestCase("false", "{\"a\":2}", -1)]
    [TestCase("false", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("false", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("false", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("false", "{\"a\":[1,2]}", -1)]
    [TestCase("false", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("false", "[null]", -1)]
    [TestCase("false", "\"é\"", 1)]
    [TestCase("false", "\"z\"", 1)]
    [TestCase("false", "123456789012345678901234567890", 1)]
    [TestCase("false", "123456789012345678901234567890.0", 1)]
    [TestCase("0", "0", 0)]
    [TestCase("0", "1", -1)]
    [TestCase("0", "1.0", -1)]
    [TestCase("0", "1.00", -1)]
    [TestCase("0", "-1", 1)]
    [TestCase("0", "2", -1)]
    [TestCase("0", "10", -1)]
    [TestCase("0", "1e2", -1)]
    [TestCase("0", "100", -1)]
    [TestCase("0", "\"a\"", 1)]
    [TestCase("0", "\"b\"", 1)]
    [TestCase("0", "\"aa\"", 1)]
    [TestCase("0", "\"\"", 1)]
    [TestCase("0", "[]", 1)]
    [TestCase("0", "[1]", -1)]
    [TestCase("0", "[2]", -1)]
    [TestCase("0", "[1,2]", -1)]
    [TestCase("0", "[[1]]", -1)]
    [TestCase("0", "{}", -1)]
    [TestCase("0", "{\"a\":1}", -1)]
    [TestCase("0", "{\"b\":1}", -1)]
    [TestCase("0", "{\"a\":2}", -1)]
    [TestCase("0", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("0", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("0", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("0", "{\"a\":[1,2]}", -1)]
    [TestCase("0", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("0", "[null]", -1)]
    [TestCase("0", "\"é\"", 1)]
    [TestCase("0", "\"z\"", 1)]
    [TestCase("0", "123456789012345678901234567890", -1)]
    [TestCase("0", "123456789012345678901234567890.0", -1)]
    [TestCase("1", "1", 0)]
    [TestCase("1", "1.0", 0)]
    [TestCase("1", "1.00", 0)]
    [TestCase("1", "-1", 1)]
    [TestCase("1", "2", -1)]
    [TestCase("1", "10", -1)]
    [TestCase("1", "1e2", -1)]
    [TestCase("1", "100", -1)]
    [TestCase("1", "\"a\"", 1)]
    [TestCase("1", "\"b\"", 1)]
    [TestCase("1", "\"aa\"", 1)]
    [TestCase("1", "\"\"", 1)]
    [TestCase("1", "[]", 1)]
    [TestCase("1", "[1]", -1)]
    [TestCase("1", "[2]", -1)]
    [TestCase("1", "[1,2]", -1)]
    [TestCase("1", "[[1]]", -1)]
    [TestCase("1", "{}", -1)]
    [TestCase("1", "{\"a\":1}", -1)]
    [TestCase("1", "{\"b\":1}", -1)]
    [TestCase("1", "{\"a\":2}", -1)]
    [TestCase("1", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("1", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("1", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("1", "{\"a\":[1,2]}", -1)]
    [TestCase("1", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("1", "[null]", -1)]
    [TestCase("1", "\"é\"", 1)]
    [TestCase("1", "\"z\"", 1)]
    [TestCase("1", "123456789012345678901234567890", -1)]
    [TestCase("1", "123456789012345678901234567890.0", -1)]
    [TestCase("1.0", "1.0", 0)]
    [TestCase("1.0", "1.00", 0)]
    [TestCase("1.0", "-1", 1)]
    [TestCase("1.0", "2", -1)]
    [TestCase("1.0", "10", -1)]
    [TestCase("1.0", "1e2", -1)]
    [TestCase("1.0", "100", -1)]
    [TestCase("1.0", "\"a\"", 1)]
    [TestCase("1.0", "\"b\"", 1)]
    [TestCase("1.0", "\"aa\"", 1)]
    [TestCase("1.0", "\"\"", 1)]
    [TestCase("1.0", "[]", 1)]
    [TestCase("1.0", "[1]", -1)]
    [TestCase("1.0", "[2]", -1)]
    [TestCase("1.0", "[1,2]", -1)]
    [TestCase("1.0", "[[1]]", -1)]
    [TestCase("1.0", "{}", -1)]
    [TestCase("1.0", "{\"a\":1}", -1)]
    [TestCase("1.0", "{\"b\":1}", -1)]
    [TestCase("1.0", "{\"a\":2}", -1)]
    [TestCase("1.0", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("1.0", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("1.0", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("1.0", "{\"a\":[1,2]}", -1)]
    [TestCase("1.0", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("1.0", "[null]", -1)]
    [TestCase("1.0", "\"é\"", 1)]
    [TestCase("1.0", "\"z\"", 1)]
    [TestCase("1.0", "123456789012345678901234567890", -1)]
    [TestCase("1.0", "123456789012345678901234567890.0", -1)]
    [TestCase("1.00", "1.00", 0)]
    [TestCase("1.00", "-1", 1)]
    [TestCase("1.00", "2", -1)]
    [TestCase("1.00", "10", -1)]
    [TestCase("1.00", "1e2", -1)]
    [TestCase("1.00", "100", -1)]
    [TestCase("1.00", "\"a\"", 1)]
    [TestCase("1.00", "\"b\"", 1)]
    [TestCase("1.00", "\"aa\"", 1)]
    [TestCase("1.00", "\"\"", 1)]
    [TestCase("1.00", "[]", 1)]
    [TestCase("1.00", "[1]", -1)]
    [TestCase("1.00", "[2]", -1)]
    [TestCase("1.00", "[1,2]", -1)]
    [TestCase("1.00", "[[1]]", -1)]
    [TestCase("1.00", "{}", -1)]
    [TestCase("1.00", "{\"a\":1}", -1)]
    [TestCase("1.00", "{\"b\":1}", -1)]
    [TestCase("1.00", "{\"a\":2}", -1)]
    [TestCase("1.00", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("1.00", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("1.00", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("1.00", "{\"a\":[1,2]}", -1)]
    [TestCase("1.00", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("1.00", "[null]", -1)]
    [TestCase("1.00", "\"é\"", 1)]
    [TestCase("1.00", "\"z\"", 1)]
    [TestCase("1.00", "123456789012345678901234567890", -1)]
    [TestCase("1.00", "123456789012345678901234567890.0", -1)]
    [TestCase("-1", "-1", 0)]
    [TestCase("-1", "2", -1)]
    [TestCase("-1", "10", -1)]
    [TestCase("-1", "1e2", -1)]
    [TestCase("-1", "100", -1)]
    [TestCase("-1", "\"a\"", 1)]
    [TestCase("-1", "\"b\"", 1)]
    [TestCase("-1", "\"aa\"", 1)]
    [TestCase("-1", "\"\"", 1)]
    [TestCase("-1", "[]", 1)]
    [TestCase("-1", "[1]", -1)]
    [TestCase("-1", "[2]", -1)]
    [TestCase("-1", "[1,2]", -1)]
    [TestCase("-1", "[[1]]", -1)]
    [TestCase("-1", "{}", -1)]
    [TestCase("-1", "{\"a\":1}", -1)]
    [TestCase("-1", "{\"b\":1}", -1)]
    [TestCase("-1", "{\"a\":2}", -1)]
    [TestCase("-1", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("-1", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("-1", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("-1", "{\"a\":[1,2]}", -1)]
    [TestCase("-1", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("-1", "[null]", -1)]
    [TestCase("-1", "\"é\"", 1)]
    [TestCase("-1", "\"z\"", 1)]
    [TestCase("-1", "123456789012345678901234567890", -1)]
    [TestCase("-1", "123456789012345678901234567890.0", -1)]
    [TestCase("2", "2", 0)]
    [TestCase("2", "10", -1)]
    [TestCase("2", "1e2", -1)]
    [TestCase("2", "100", -1)]
    [TestCase("2", "\"a\"", 1)]
    [TestCase("2", "\"b\"", 1)]
    [TestCase("2", "\"aa\"", 1)]
    [TestCase("2", "\"\"", 1)]
    [TestCase("2", "[]", 1)]
    [TestCase("2", "[1]", -1)]
    [TestCase("2", "[2]", -1)]
    [TestCase("2", "[1,2]", -1)]
    [TestCase("2", "[[1]]", -1)]
    [TestCase("2", "{}", -1)]
    [TestCase("2", "{\"a\":1}", -1)]
    [TestCase("2", "{\"b\":1}", -1)]
    [TestCase("2", "{\"a\":2}", -1)]
    [TestCase("2", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("2", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("2", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("2", "{\"a\":[1,2]}", -1)]
    [TestCase("2", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("2", "[null]", -1)]
    [TestCase("2", "\"é\"", 1)]
    [TestCase("2", "\"z\"", 1)]
    [TestCase("2", "123456789012345678901234567890", -1)]
    [TestCase("2", "123456789012345678901234567890.0", -1)]
    [TestCase("10", "10", 0)]
    [TestCase("10", "1e2", -1)]
    [TestCase("10", "100", -1)]
    [TestCase("10", "\"a\"", 1)]
    [TestCase("10", "\"b\"", 1)]
    [TestCase("10", "\"aa\"", 1)]
    [TestCase("10", "\"\"", 1)]
    [TestCase("10", "[]", 1)]
    [TestCase("10", "[1]", -1)]
    [TestCase("10", "[2]", -1)]
    [TestCase("10", "[1,2]", -1)]
    [TestCase("10", "[[1]]", -1)]
    [TestCase("10", "{}", -1)]
    [TestCase("10", "{\"a\":1}", -1)]
    [TestCase("10", "{\"b\":1}", -1)]
    [TestCase("10", "{\"a\":2}", -1)]
    [TestCase("10", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("10", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("10", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("10", "{\"a\":[1,2]}", -1)]
    [TestCase("10", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("10", "[null]", -1)]
    [TestCase("10", "\"é\"", 1)]
    [TestCase("10", "\"z\"", 1)]
    [TestCase("10", "123456789012345678901234567890", -1)]
    [TestCase("10", "123456789012345678901234567890.0", -1)]
    [TestCase("1e2", "1e2", 0)]
    [TestCase("1e2", "100", 0)]
    [TestCase("1e2", "\"a\"", 1)]
    [TestCase("1e2", "\"b\"", 1)]
    [TestCase("1e2", "\"aa\"", 1)]
    [TestCase("1e2", "\"\"", 1)]
    [TestCase("1e2", "[]", 1)]
    [TestCase("1e2", "[1]", -1)]
    [TestCase("1e2", "[2]", -1)]
    [TestCase("1e2", "[1,2]", -1)]
    [TestCase("1e2", "[[1]]", -1)]
    [TestCase("1e2", "{}", -1)]
    [TestCase("1e2", "{\"a\":1}", -1)]
    [TestCase("1e2", "{\"b\":1}", -1)]
    [TestCase("1e2", "{\"a\":2}", -1)]
    [TestCase("1e2", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("1e2", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("1e2", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("1e2", "{\"a\":[1,2]}", -1)]
    [TestCase("1e2", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("1e2", "[null]", -1)]
    [TestCase("1e2", "\"é\"", 1)]
    [TestCase("1e2", "\"z\"", 1)]
    [TestCase("1e2", "123456789012345678901234567890", -1)]
    [TestCase("1e2", "123456789012345678901234567890.0", -1)]
    [TestCase("100", "100", 0)]
    [TestCase("100", "\"a\"", 1)]
    [TestCase("100", "\"b\"", 1)]
    [TestCase("100", "\"aa\"", 1)]
    [TestCase("100", "\"\"", 1)]
    [TestCase("100", "[]", 1)]
    [TestCase("100", "[1]", -1)]
    [TestCase("100", "[2]", -1)]
    [TestCase("100", "[1,2]", -1)]
    [TestCase("100", "[[1]]", -1)]
    [TestCase("100", "{}", -1)]
    [TestCase("100", "{\"a\":1}", -1)]
    [TestCase("100", "{\"b\":1}", -1)]
    [TestCase("100", "{\"a\":2}", -1)]
    [TestCase("100", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("100", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("100", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("100", "{\"a\":[1,2]}", -1)]
    [TestCase("100", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("100", "[null]", -1)]
    [TestCase("100", "\"é\"", 1)]
    [TestCase("100", "\"z\"", 1)]
    [TestCase("100", "123456789012345678901234567890", -1)]
    [TestCase("100", "123456789012345678901234567890.0", -1)]
    [TestCase("\"a\"", "\"a\"", 0)]
    [TestCase("\"a\"", "\"b\"", -1)]
    [TestCase("\"a\"", "\"aa\"", -1)]
    [TestCase("\"a\"", "\"\"", 1)]
    [TestCase("\"a\"", "[]", 1)]
    [TestCase("\"a\"", "[1]", -1)]
    [TestCase("\"a\"", "[2]", -1)]
    [TestCase("\"a\"", "[1,2]", -1)]
    [TestCase("\"a\"", "[[1]]", -1)]
    [TestCase("\"a\"", "{}", -1)]
    [TestCase("\"a\"", "{\"a\":1}", -1)]
    [TestCase("\"a\"", "{\"b\":1}", -1)]
    [TestCase("\"a\"", "{\"a\":2}", -1)]
    [TestCase("\"a\"", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("\"a\"", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("\"a\"", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("\"a\"", "{\"a\":[1,2]}", -1)]
    [TestCase("\"a\"", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("\"a\"", "[null]", -1)]
    [TestCase("\"a\"", "\"é\"", -1)]
    [TestCase("\"a\"", "\"z\"", -1)]
    [TestCase("\"a\"", "123456789012345678901234567890", -1)]
    [TestCase("\"a\"", "123456789012345678901234567890.0", -1)]
    [TestCase("\"b\"", "\"b\"", 0)]
    [TestCase("\"b\"", "\"aa\"", 1)]
    [TestCase("\"b\"", "\"\"", 1)]
    [TestCase("\"b\"", "[]", 1)]
    [TestCase("\"b\"", "[1]", -1)]
    [TestCase("\"b\"", "[2]", -1)]
    [TestCase("\"b\"", "[1,2]", -1)]
    [TestCase("\"b\"", "[[1]]", -1)]
    [TestCase("\"b\"", "{}", -1)]
    [TestCase("\"b\"", "{\"a\":1}", -1)]
    [TestCase("\"b\"", "{\"b\":1}", -1)]
    [TestCase("\"b\"", "{\"a\":2}", -1)]
    [TestCase("\"b\"", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("\"b\"", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("\"b\"", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("\"b\"", "{\"a\":[1,2]}", -1)]
    [TestCase("\"b\"", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("\"b\"", "[null]", -1)]
    [TestCase("\"b\"", "\"é\"", -1)]
    [TestCase("\"b\"", "\"z\"", -1)]
    [TestCase("\"b\"", "123456789012345678901234567890", -1)]
    [TestCase("\"b\"", "123456789012345678901234567890.0", -1)]
    [TestCase("\"aa\"", "\"aa\"", 0)]
    [TestCase("\"aa\"", "\"\"", 1)]
    [TestCase("\"aa\"", "[]", 1)]
    [TestCase("\"aa\"", "[1]", -1)]
    [TestCase("\"aa\"", "[2]", -1)]
    [TestCase("\"aa\"", "[1,2]", -1)]
    [TestCase("\"aa\"", "[[1]]", -1)]
    [TestCase("\"aa\"", "{}", -1)]
    [TestCase("\"aa\"", "{\"a\":1}", -1)]
    [TestCase("\"aa\"", "{\"b\":1}", -1)]
    [TestCase("\"aa\"", "{\"a\":2}", -1)]
    [TestCase("\"aa\"", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("\"aa\"", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("\"aa\"", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("\"aa\"", "{\"a\":[1,2]}", -1)]
    [TestCase("\"aa\"", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("\"aa\"", "[null]", -1)]
    [TestCase("\"aa\"", "\"é\"", -1)]
    [TestCase("\"aa\"", "\"z\"", -1)]
    [TestCase("\"aa\"", "123456789012345678901234567890", -1)]
    [TestCase("\"aa\"", "123456789012345678901234567890.0", -1)]
    [TestCase("\"\"", "\"\"", 0)]
    [TestCase("\"\"", "[]", 1)]
    [TestCase("\"\"", "[1]", -1)]
    [TestCase("\"\"", "[2]", -1)]
    [TestCase("\"\"", "[1,2]", -1)]
    [TestCase("\"\"", "[[1]]", -1)]
    [TestCase("\"\"", "{}", -1)]
    [TestCase("\"\"", "{\"a\":1}", -1)]
    [TestCase("\"\"", "{\"b\":1}", -1)]
    [TestCase("\"\"", "{\"a\":2}", -1)]
    [TestCase("\"\"", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("\"\"", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("\"\"", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("\"\"", "{\"a\":[1,2]}", -1)]
    [TestCase("\"\"", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("\"\"", "[null]", -1)]
    [TestCase("\"\"", "\"é\"", -1)]
    [TestCase("\"\"", "\"z\"", -1)]
    [TestCase("\"\"", "123456789012345678901234567890", -1)]
    [TestCase("\"\"", "123456789012345678901234567890.0", -1)]
    [TestCase("[]", "[]", 0)]
    [TestCase("[]", "[1]", -1)]
    [TestCase("[]", "[2]", -1)]
    [TestCase("[]", "[1,2]", -1)]
    [TestCase("[]", "[[1]]", -1)]
    [TestCase("[]", "{}", -1)]
    [TestCase("[]", "{\"a\":1}", -1)]
    [TestCase("[]", "{\"b\":1}", -1)]
    [TestCase("[]", "{\"a\":2}", -1)]
    [TestCase("[]", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("[]", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("[]", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("[]", "{\"a\":[1,2]}", -1)]
    [TestCase("[]", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("[]", "[null]", -1)]
    [TestCase("[]", "\"é\"", -1)]
    [TestCase("[]", "\"z\"", -1)]
    [TestCase("[]", "123456789012345678901234567890", -1)]
    [TestCase("[]", "123456789012345678901234567890.0", -1)]
    [TestCase("[1]", "[1]", 0)]
    [TestCase("[1]", "[2]", -1)]
    [TestCase("[1]", "[1,2]", -1)]
    [TestCase("[1]", "[[1]]", -1)]
    [TestCase("[1]", "{}", -1)]
    [TestCase("[1]", "{\"a\":1}", -1)]
    [TestCase("[1]", "{\"b\":1}", -1)]
    [TestCase("[1]", "{\"a\":2}", -1)]
    [TestCase("[1]", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("[1]", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("[1]", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("[1]", "{\"a\":[1,2]}", -1)]
    [TestCase("[1]", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("[1]", "[null]", 1)]
    [TestCase("[1]", "\"é\"", 1)]
    [TestCase("[1]", "\"z\"", 1)]
    [TestCase("[1]", "123456789012345678901234567890", 1)]
    [TestCase("[1]", "123456789012345678901234567890.0", 1)]
    [TestCase("[2]", "[2]", 0)]
    [TestCase("[2]", "[1,2]", -1)]
    [TestCase("[2]", "[[1]]", -1)]
    [TestCase("[2]", "{}", -1)]
    [TestCase("[2]", "{\"a\":1}", -1)]
    [TestCase("[2]", "{\"b\":1}", -1)]
    [TestCase("[2]", "{\"a\":2}", -1)]
    [TestCase("[2]", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("[2]", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("[2]", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("[2]", "{\"a\":[1,2]}", -1)]
    [TestCase("[2]", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("[2]", "[null]", 1)]
    [TestCase("[2]", "\"é\"", 1)]
    [TestCase("[2]", "\"z\"", 1)]
    [TestCase("[2]", "123456789012345678901234567890", 1)]
    [TestCase("[2]", "123456789012345678901234567890.0", 1)]
    [TestCase("[1,2]", "[1,2]", 0)]
    [TestCase("[1,2]", "[[1]]", 1)]
    [TestCase("[1,2]", "{}", -1)]
    [TestCase("[1,2]", "{\"a\":1}", -1)]
    [TestCase("[1,2]", "{\"b\":1}", -1)]
    [TestCase("[1,2]", "{\"a\":2}", -1)]
    [TestCase("[1,2]", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("[1,2]", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("[1,2]", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("[1,2]", "{\"a\":[1,2]}", -1)]
    [TestCase("[1,2]", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("[1,2]", "[null]", 1)]
    [TestCase("[1,2]", "\"é\"", 1)]
    [TestCase("[1,2]", "\"z\"", 1)]
    [TestCase("[1,2]", "123456789012345678901234567890", 1)]
    [TestCase("[1,2]", "123456789012345678901234567890.0", 1)]
    [TestCase("[[1]]", "[[1]]", 0)]
    [TestCase("[[1]]", "{}", -1)]
    [TestCase("[[1]]", "{\"a\":1}", -1)]
    [TestCase("[[1]]", "{\"b\":1}", -1)]
    [TestCase("[[1]]", "{\"a\":2}", -1)]
    [TestCase("[[1]]", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("[[1]]", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("[[1]]", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("[[1]]", "{\"a\":[1,2]}", -1)]
    [TestCase("[[1]]", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("[[1]]", "[null]", 1)]
    [TestCase("[[1]]", "\"é\"", 1)]
    [TestCase("[[1]]", "\"z\"", 1)]
    [TestCase("[[1]]", "123456789012345678901234567890", 1)]
    [TestCase("[[1]]", "123456789012345678901234567890.0", 1)]
    [TestCase("{}", "{}", 0)]
    [TestCase("{}", "{\"a\":1}", -1)]
    [TestCase("{}", "{\"b\":1}", -1)]
    [TestCase("{}", "{\"a\":2}", -1)]
    [TestCase("{}", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("{}", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("{}", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("{}", "{\"a\":[1,2]}", -1)]
    [TestCase("{}", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("{}", "[null]", 1)]
    [TestCase("{}", "\"é\"", 1)]
    [TestCase("{}", "\"z\"", 1)]
    [TestCase("{}", "123456789012345678901234567890", 1)]
    [TestCase("{}", "123456789012345678901234567890.0", 1)]
    [TestCase("{\"a\":1}", "{\"a\":1}", 0)]
    [TestCase("{\"a\":1}", "{\"b\":1}", -1)]
    [TestCase("{\"a\":1}", "{\"a\":2}", -1)]
    [TestCase("{\"a\":1}", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("{\"a\":1}", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("{\"a\":1}", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("{\"a\":1}", "{\"a\":[1,2]}", -1)]
    [TestCase("{\"a\":1}", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("{\"a\":1}", "[null]", 1)]
    [TestCase("{\"a\":1}", "\"é\"", 1)]
    [TestCase("{\"a\":1}", "\"z\"", 1)]
    [TestCase("{\"a\":1}", "123456789012345678901234567890", 1)]
    [TestCase("{\"a\":1}", "123456789012345678901234567890.0", 1)]
    [TestCase("{\"b\":1}", "{\"b\":1}", 0)]
    [TestCase("{\"b\":1}", "{\"a\":2}", 1)]
    [TestCase("{\"b\":1}", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("{\"b\":1}", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("{\"b\":1}", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("{\"b\":1}", "{\"a\":[1,2]}", 1)]
    [TestCase("{\"b\":1}", "{\"a\":{\"b\":1}}", 1)]
    [TestCase("{\"b\":1}", "[null]", 1)]
    [TestCase("{\"b\":1}", "\"é\"", 1)]
    [TestCase("{\"b\":1}", "\"z\"", 1)]
    [TestCase("{\"b\":1}", "123456789012345678901234567890", 1)]
    [TestCase("{\"b\":1}", "123456789012345678901234567890.0", 1)]
    [TestCase("{\"a\":2}", "{\"a\":2}", 0)]
    [TestCase("{\"a\":2}", "{\"a\":1,\"b\":2}", -1)]
    [TestCase("{\"a\":2}", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("{\"a\":2}", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("{\"a\":2}", "{\"a\":[1,2]}", -1)]
    [TestCase("{\"a\":2}", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("{\"a\":2}", "[null]", 1)]
    [TestCase("{\"a\":2}", "\"é\"", 1)]
    [TestCase("{\"a\":2}", "\"z\"", 1)]
    [TestCase("{\"a\":2}", "123456789012345678901234567890", 1)]
    [TestCase("{\"a\":2}", "123456789012345678901234567890.0", 1)]
    [TestCase("{\"a\":1,\"b\":2}", "{\"a\":1,\"b\":2}", 0)]
    [TestCase("{\"a\":1,\"b\":2}", "{\"aa\":1,\"c\":1}", -1)]
    [TestCase("{\"a\":1,\"b\":2}", "{\"b\":1,\"d\":1}", -1)]
    [TestCase("{\"a\":1,\"b\":2}", "{\"a\":[1,2]}", 1)]
    [TestCase("{\"a\":1,\"b\":2}", "{\"a\":{\"b\":1}}", 1)]
    [TestCase("{\"a\":1,\"b\":2}", "[null]", 1)]
    [TestCase("{\"a\":1,\"b\":2}", "\"é\"", 1)]
    [TestCase("{\"a\":1,\"b\":2}", "\"z\"", 1)]
    [TestCase("{\"a\":1,\"b\":2}", "123456789012345678901234567890", 1)]
    [TestCase("{\"a\":1,\"b\":2}", "123456789012345678901234567890.0", 1)]
    [TestCase("{\"aa\":1,\"c\":1}", "{\"aa\":1,\"c\":1}", 0)]
    [TestCase("{\"aa\":1,\"c\":1}", "{\"b\":1,\"d\":1}", 1)]
    [TestCase("{\"aa\":1,\"c\":1}", "{\"a\":[1,2]}", 1)]
    [TestCase("{\"aa\":1,\"c\":1}", "{\"a\":{\"b\":1}}", 1)]
    [TestCase("{\"aa\":1,\"c\":1}", "[null]", 1)]
    [TestCase("{\"aa\":1,\"c\":1}", "\"é\"", 1)]
    [TestCase("{\"aa\":1,\"c\":1}", "\"z\"", 1)]
    [TestCase("{\"aa\":1,\"c\":1}", "123456789012345678901234567890", 1)]
    [TestCase("{\"aa\":1,\"c\":1}", "123456789012345678901234567890.0", 1)]
    [TestCase("{\"b\":1,\"d\":1}", "{\"b\":1,\"d\":1}", 0)]
    [TestCase("{\"b\":1,\"d\":1}", "{\"a\":[1,2]}", 1)]
    [TestCase("{\"b\":1,\"d\":1}", "{\"a\":{\"b\":1}}", 1)]
    [TestCase("{\"b\":1,\"d\":1}", "[null]", 1)]
    [TestCase("{\"b\":1,\"d\":1}", "\"é\"", 1)]
    [TestCase("{\"b\":1,\"d\":1}", "\"z\"", 1)]
    [TestCase("{\"b\":1,\"d\":1}", "123456789012345678901234567890", 1)]
    [TestCase("{\"b\":1,\"d\":1}", "123456789012345678901234567890.0", 1)]
    [TestCase("{\"a\":[1,2]}", "{\"a\":[1,2]}", 0)]
    [TestCase("{\"a\":[1,2]}", "{\"a\":{\"b\":1}}", -1)]
    [TestCase("{\"a\":[1,2]}", "[null]", 1)]
    [TestCase("{\"a\":[1,2]}", "\"é\"", 1)]
    [TestCase("{\"a\":[1,2]}", "\"z\"", 1)]
    [TestCase("{\"a\":[1,2]}", "123456789012345678901234567890", 1)]
    [TestCase("{\"a\":[1,2]}", "123456789012345678901234567890.0", 1)]
    [TestCase("{\"a\":{\"b\":1}}", "{\"a\":{\"b\":1}}", 0)]
    [TestCase("{\"a\":{\"b\":1}}", "[null]", 1)]
    [TestCase("{\"a\":{\"b\":1}}", "\"é\"", 1)]
    [TestCase("{\"a\":{\"b\":1}}", "\"z\"", 1)]
    [TestCase("{\"a\":{\"b\":1}}", "123456789012345678901234567890", 1)]
    [TestCase("{\"a\":{\"b\":1}}", "123456789012345678901234567890.0", 1)]
    [TestCase("[null]", "[null]", 0)]
    [TestCase("[null]", "\"é\"", 1)]
    [TestCase("[null]", "\"z\"", 1)]
    [TestCase("[null]", "123456789012345678901234567890", 1)]
    [TestCase("[null]", "123456789012345678901234567890.0", 1)]
    [TestCase("\"é\"", "\"é\"", 0)]
    [TestCase("\"é\"", "\"z\"", 1)]
    [TestCase("\"é\"", "123456789012345678901234567890", -1)]
    [TestCase("\"é\"", "123456789012345678901234567890.0", -1)]
    [TestCase("\"z\"", "\"z\"", 0)]
    [TestCase("\"z\"", "123456789012345678901234567890", -1)]
    [TestCase("\"z\"", "123456789012345678901234567890.0", -1)]
    [TestCase("123456789012345678901234567890", "123456789012345678901234567890", 0)]
    [TestCase("123456789012345678901234567890", "123456789012345678901234567890.0", 0)]
    [TestCase("123456789012345678901234567890.0", "123456789012345678901234567890.0", 0)]
    public void Compare_MatchesPostgreSqlOrder(string a, string b, int expectedSign)
    {
        Assert.AreEqual(expectedSign, Math.Sign(JsonbDocument.Compare(Item(a), Item(b))), $"{a} vs {b}");
        Assert.AreEqual(-expectedSign, Math.Sign(JsonbDocument.Compare(Item(b), Item(a))), $"{b} vs {a}");
        Assert.AreEqual(expectedSign == 0, JsonbDocument.ValueEquals(Item(a), Item(b)), $"{a} = {b}");

        if (expectedSign == 0)
            Assert.AreEqual(Hash(a), Hash(b), $"equal values {a} and {b} must hash the same");
    }

    private static int Hash(string text)
    {
        HashCode hash = new();
        JsonbDocument.AddHash(ref hash, Item(text));
        return hash.ToHashCode();
    }

    [Test]
    public void Hash_SpreadsDistinctValues()
    {
        HashSet<int> hashes = [];
        for (int i = 0; i < 1000; i++)
            hashes.Add(Hash($"{{\"k\": {i}, \"s\": \"v{i % 7}\", \"f\": {i}.5}}"));
        Assert.Greater(hashes.Count, 990);
    }

    [TestCase("{\"a\":1,\"b\":2}", "{\"a\":1}", true)]
    [TestCase("{\"a\":1}", "{\"a\":1,\"b\":2}", false)]
    [TestCase("{\"a\":1.0}", "{\"a\":1}", true)]
    [TestCase("{\"a\":[1,2,3]}", "{\"a\":[3,1]}", true)]
    [TestCase("{\"a\":[1,2,3]}", "{\"a\":[1,1]}", true)]
    [TestCase("{\"a\":[1]}", "{\"a\":1}", false)]
    [TestCase("{\"a\":1}", "{\"a\":[1]}", false)]
    [TestCase("[1,2,[1,3]]", "[1,3]", false)]
    [TestCase("[1,2,[1,3]]", "[[1,3]]", true)]
    [TestCase("[\"foo\",\"bar\"]", "\"foo\"", true)]
    [TestCase("\"foo\"", "[\"foo\"]", false)]
    [TestCase("\"foo\"", "\"foo\"", true)]
    [TestCase("1", "1.0", true)]
    [TestCase("[]", "[]", true)]
    [TestCase("{}", "{}", true)]
    [TestCase("{\"a\":1}", "{}", true)]
    [TestCase("[1]", "[]", true)]
    [TestCase("{\"a\":{\"b\":1,\"c\":2}}", "{\"a\":{\"b\":1}}", true)]
    [TestCase("{\"a\":{\"b\":1}}", "{\"a\":{\"b\":1,\"c\":2}}", false)]
    [TestCase("[{\"a\":1},{\"b\":2}]", "[{\"a\":1,\"b\":2}]", false)]
    [TestCase("[{\"a\":1,\"b\":2}]", "[{\"a\":1}]", true)]
    [TestCase("[{\"a\":1,\"b\":2}]", "[{\"b\":2},{\"a\":1}]", true)]
    [TestCase("{\"a\":null}", "{\"a\":null}", true)]
    [TestCase("[null]", "null", true)]
    [TestCase("[[1,2]]", "[[1],[2]]", true)]
    [TestCase("[1]", "{\"a\":1}", false)]
    [TestCase("{\"a\":1}", "[1]", false)]
    [TestCase("\"a\"", "\"b\"", false)]
    [TestCase("[1,[2]]", "[2]", false)]
    [TestCase("{\"tags\":[\"vip\",\"beta\"],\"n\":{\"x\":[1,{\"y\":true}]}}", "{\"n\":{\"x\":[{\"y\":true}]}}", true)]
    public void Contains_MatchesPostgreSql(string container, string contained, bool expected)
    {
        Assert.AreEqual(expected, JsonbDocument.Contains(Item(container), Item(contained)), $"{container} @> {contained}");
    }

    [TestCase("{\"a\":1,\"b\":2}", "a", true)]
    [TestCase("{\"a\":1}", "c", false)]
    [TestCase("[\"a\",\"b\"]", "b", true)]
    [TestCase("[\"a\",[\"b\"]]", "b", false)]
    [TestCase("\"a\"", "a", true)]
    [TestCase("\"a\"", "b", false)]
    [TestCase("1", "1", false)]
    [TestCase("[1]", "1", false)]
    [TestCase("{\"a\":{\"b\":1}}", "b", false)]
    [TestCase("{\"\":1}", "", true)]
    [TestCase("{\"é\":1}", "é", true)]
    public void Exists_MatchesPostgreSql(string document, string key, bool expected)
    {
        Assert.AreEqual(expected, JsonbDocument.Exists(Item(document), Encoding.UTF8.GetBytes(key)), $"{document} ? {key}");
    }

    [TestCase("[1, \"a\"]", "array", 2)]
    [TestCase("{\"a\": 1}", "object", -1)]
    [TestCase("\"s\"", "string", -1)]
    [TestCase("1.5", "number", -1)]
    [TestCase("true", "boolean", -1)]
    [TestCase("null", "null", -1)]
    public void Kind_AndArrayLength(string document, string expectedType, int expectedLength)
    {
        Assert.AreEqual(expectedType, JsonbDocument.TypeName(JsonbDocument.Kind(Item(document))));
        Assert.AreEqual(expectedLength, JsonbDocument.ArrayLength(Item(document)));
    }

    [Test]
    public void ToSqlText_UnquotesStrings_AndGivesNullForJsonNull()
    {
        Assert.AreEqual("a\"b", JsonbDocument.ToSqlText(Item("\"a\\\"b\"")));
        Assert.IsNull(JsonbDocument.ToSqlText(Item("null")));
        Assert.AreEqual("[1, 2]", JsonbDocument.ToSqlText(Item("[1,2]")));
    }

    [Test]
    public void RandomDocuments_RoundTripAgainstSystemTextJson()
    {
        Random random = new(20261030);
        for (int i = 0; i < 2000; i++)
        {
            string text = RandomDocument(random, 0)?.ToJsonString() ?? "null";
            string ours = Text(text);

            // The structure survives: the text we print parses to an equal document, and printing
            // it again changes nothing.
            Assert.IsTrue(JsonNode.DeepEquals(Normalize(JsonNode.Parse(text)), Normalize(JsonNode.Parse(ours))), text);
            Assert.AreEqual(ours, Text(ours));
        }
    }

    [Test]
    public void DamagedBytes_FailWithADomainError_NeverACrash()
    {
        Random random = new(7);
        byte[] original = Cell("{\"a\": [1, 2.5, \"x\", {\"b\": null}], \"cc\": 123456789012345678901234567890, \"d\": true}");

        for (int i = 0; i < 20000; i++)
        {
            byte[] damaged = (byte[])original.Clone();
            int flips = 1 + random.Next(3);
            for (int f = 0; f < flips; f++)
                damaged[1 + random.Next(damaged.Length - 1)] = (byte)random.Next(256);
            if (random.Next(4) == 0)
                Array.Resize(ref damaged, 1 + random.Next(damaged.Length - 1));

            try
            {
                ReadOnlySpan<byte> item = JsonbDocument.Item(damaged);
                JsonbText.ToText(item);
                JsonbDocument.Compare(item, item);
                HashCode hash = new();
                JsonbDocument.AddHash(ref hash, item);
                JsonbDocument.Contains(item, item);
            }
            catch (CamusDBException ex)
            {
                Assert.AreEqual(CamusDBErrorCodes.JsonbValueCorrupt, ex.Code);
            }
        }
    }

    [Test]
    public void UnknownFormatVersion_IsRefused()
    {
        byte[] cell = Cell("[1]");
        cell[0] = 2;
        Assert.AreEqual(CamusDBErrorCodes.JsonbValueCorrupt, Assert.Throws<CamusDBException>(() => JsonbDocument.Item(cell).ToArray())!.Code);
    }

    private static JsonNode? RandomDocument(Random random, int depth)
    {
        int choice = random.Next(depth > 4 ? 6 : 8);
        switch (choice)
        {
            case 0: return null;
            case 1: return JsonValue.Create(random.Next(2) == 0);
            case 2: return JsonValue.Create(random.NextInt64(long.MinValue, long.MaxValue));
            case 3: return JsonValue.Create(Math.Round(random.NextDouble() * 1000, random.Next(6)));
            case 4: return JsonValue.Create(RandomString(random));
            case 5: return JsonValue.Create(random.Next(-5, 5));
            case 6:
            {
                JsonArray array = [];
                int count = random.Next(5);
                for (int i = 0; i < count; i++)
                    array.Add(RandomDocument(random, depth + 1));
                return array;
            }
            default:
            {
                JsonObject obj = [];
                int count = random.Next(5);
                for (int i = 0; i < count; i++)
                    obj[RandomString(random)] = RandomDocument(random, depth + 1);
                return obj;
            }
        }
    }

    private static string RandomString(Random random)
    {
        const string alphabet = "abcAB \"\\/\n\té中😀\u0001";
        StringBuilder builder = new();
        int length = random.Next(6);
        while (builder.Length < length)
        {
            int index = random.Next(alphabet.Length);
            if (char.IsHighSurrogate(alphabet[index]))
                builder.Append(alphabet, index, 2);
            else if (!char.IsLowSurrogate(alphabet[index]))
                builder.Append(alphabet[index]);
        }
        return builder.ToString();
    }

    /// <summary>Numbers as decimals, so 1.0 and 1 compare equal in <see cref="JsonNode.DeepEquals"/>.</summary>
    private static JsonNode? Normalize(JsonNode? node) => node switch
    {
        JsonObject obj => new JsonObject(obj.Select(p => KeyValuePair.Create(p.Key, Normalize(p.Value)))),
        JsonArray array => new JsonArray(array.Select(Normalize).ToArray()),
        JsonValue value when value.GetValueKind() == System.Text.Json.JsonValueKind.Number =>
            JsonValue.Create(decimal.Parse(value.ToJsonString(), System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture)),
        _ => node?.DeepClone(),
    };
}
