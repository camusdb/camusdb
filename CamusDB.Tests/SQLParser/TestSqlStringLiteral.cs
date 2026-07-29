
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// Covers the literal codec directly, because the guarantee it carries — every value survives
/// <c>Quote</c> → <c>Decode</c> unchanged — is what the schema renderers and the dump tool rely on,
/// and it cannot be established by a handful of end-to-end DDL cases.
/// </summary>
[TestFixture]
internal sealed class TestSqlStringLiteral
{
    [TestCase("")]
    [TestCase("plain")]
    [TestCase("it's")]
    [TestCase("say \"hi\"")]
    [TestCase("both ' and \" together")]
    [TestCase("a\\b")]
    [TestCase("trailing\\")]
    [TestCase("\\\\")]
    [TestCase("a\\'b")]
    [TestCase("a\\\"b")]
    [TestCase("a\\'b\\\"c")]
    [TestCase("line1\nline2")]
    [TestCase("tab\there")]
    [TestCase("nul\0inside")]
    [TestCase("\a\b\f\v\r")]
    [TestCase("unicode: é中😀")]
    [TestCase("'); DROP TABLE t; --")]
    [TestCase("(\\d{4})-(\\d{2})")]
    public void QuoteAndDecodeAreInverses(string value)
    {
        Assert.AreEqual(value, SqlStringLiteral.Decode(SqlStringLiteral.Quote(value)));
    }

    /// <summary>
    /// Ordinary text — backslashes included — must come out in the plain form, unescaped. This is the
    /// property that keeps a regex or a Windows path readable in <c>SHOW CREATE TABLE</c> output, and
    /// keeps it identical to what the user typed.
    /// </summary>
    [TestCase("plain", "'plain'")]
    [TestCase("C:\\Users", "'C:\\Users'")]
    [TestCase("(\\d+)", "'(\\d+)'")]
    [TestCase("trailing\\", "'trailing\\'")]
    [TestCase("it's", "'it''s'")]
    [TestCase("say \"hi\"", "'say \"hi\"'")]
    public void QuotePrefersThePlainForm(string value, string expected)
    {
        Assert.AreEqual(expected, SqlStringLiteral.Quote(value));
    }

    /// <summary>
    /// Only a control character — which the lexer's plain string body excludes — forces the escape
    /// form. Emitting one raw would produce a token that does not lex at all.
    /// </summary>
    [TestCase("line1\nline2", "E'line1\\nline2'")]
    [TestCase("a\tb", "E'a\\tb'")]
    [TestCase("nul\0here", "E'nul\\0here'")]
    public void QuoteFallsBackToTheEscapeFormForControlCharacters(string value, string expected)
    {
        Assert.AreEqual(expected, SqlStringLiteral.Quote(value));
    }

    /// <summary>
    /// The emitted literal must be a single well-formed token that does not close early. The walk
    /// mirrors the lexer: a backslash is inert in a plain literal and consumes the next character in
    /// an escape literal.
    /// </summary>
    [TestCase("trailing\\")]
    [TestCase("a\\'b")]
    [TestCase("it's")]
    [TestCase("line1\nline2")]
    [TestCase("both ' and \" together")]
    public void QuoteEmitsASingleWellFormedToken(string value)
    {
        string literal = SqlStringLiteral.Quote(value);
        bool escapeForm = literal.StartsWith("E'");
        int start = escapeForm ? 2 : 1;

        Assert.That(literal, Does.EndWith("'"));

        for (int i = start; i < literal.Length - 1; i++)
        {
            if (escapeForm && literal[i] == '\\')
            {
                i++;
                continue;
            }

            if (literal[i] != '\'')
                continue;

            // A quote inside the body is legal only as a doubled pair.
            Assert.That(i + 1, Is.LessThan(literal.Length - 1), $"literal closes early at {i}: {literal}");
            Assert.AreEqual('\'', literal[i + 1], $"unpaired quote at {i}: {literal}");
            i++;
        }
    }

    /// <summary>
    /// Exhaustive sweep over the character range where the rules actually branch (controls, quotes,
    /// backslash, ASCII), each embedded in a context that would expose an off-by-one.
    /// </summary>
    [Test]
    public void EveryLowCharacterRoundTrips()
    {
        for (int c = 0; c < 0x100; c++)
        {
            string value = "a" + (char)c + "b";
            Assert.AreEqual(value, SqlStringLiteral.Decode(SqlStringLiteral.Quote(value)), $"U+{c:X4} in the middle");

            string trailing = "a" + (char)c;
            Assert.AreEqual(trailing, SqlStringLiteral.Decode(SqlStringLiteral.Quote(trailing)), $"U+{c:X4} at the end");
        }
    }

    // ── plain form: no escape processing ────────────────────────────────────

    [TestCase("'(\\d+)'", "(\\d+)")]
    [TestCase("'C:\\Users'", "C:\\Users")]
    [TestCase("'a\\nb'", "a\\nb")]
    [TestCase("'trailing\\'", "trailing\\")]
    [TestCase("'a''b'", "a'b")]
    [TestCase("'say \"hi\"'", "say \"hi\"")]
    [TestCase("\"a\"\"b\"", "a\"b")]
    [TestCase("\"a'b\"", "a'b")]
    [TestCase("''", "")]
    public void PlainFormTakesBackslashesLiterally(string literal, string expected)
    {
        Assert.AreEqual(expected, SqlStringLiteral.Decode(literal));
    }

    // ── escape form ─────────────────────────────────────────────────────────

    [TestCase("E'a\\nb'", "a\nb")]
    [TestCase("e'a\\nb'", "a\nb")]
    [TestCase("E'a\\tb'", "a\tb")]
    [TestCase("E'a\\\\b'", "a\\b")]
    [TestCase("E'a\\'b'", "a'b")]
    [TestCase("E'a\\\"b'", "a\"b")]
    [TestCase("E'a\\x41b'", "aAb")]
    [TestCase("E'a\\u0041b'", "aAb")]
    [TestCase("E'a\\U00000041b'", "aAb")]
    [TestCase("E'a\\101b'", "aAb")]
    [TestCase("E'a\\0b'", "a\0b")]
    [TestCase("E'a''b'", "a'b")]
    [TestCase("E\"a\\nb\"", "a\nb")]
    public void EscapeFormDecodesEscapes(string literal, string expected)
    {
        Assert.AreEqual(expected, SqlStringLiteral.Decode(literal));
    }

    /// <summary>
    /// An unrecognized escape yields the character itself, as in PostgreSQL. The plain form is what
    /// carries a literal backslash, so there is nothing to gain by failing here.
    /// </summary>
    [TestCase("E'\\d+'", "d+")]
    [TestCase("E'\\q'", "q")]
    public void EscapeFormTakesAnUnknownEscapeAsTheCharacterItself(string literal, string expected)
    {
        Assert.AreEqual(expected, SqlStringLiteral.Decode(literal));
    }

    /// <summary>
    /// Octal wins over the named <c>\0</c> escape, matching the lexer's longest-match rule — otherwise
    /// <c>\101</c> would decode as NUL followed by "101".
    /// </summary>
    [Test]
    public void OctalEscapeBeatsTheNamedNulEscape()
    {
        Assert.AreEqual("A", SqlStringLiteral.Decode("E'\\101'"));
        Assert.AreEqual("\0", SqlStringLiteral.Decode("E'\\0'"));
        Assert.AreEqual("\0" + "99", SqlStringLiteral.Decode("E'\\099'"));
    }

    // ── rejection ───────────────────────────────────────────────────────────

    /// <summary>
    /// Eight-digit escapes must be range-checked without wrapping. A signed 32-bit accumulator turns
    /// anything above 0x7FFFFFFF negative, which slips past the range guard and gets cast to an
    /// unrelated code unit — accepting invalid input and silently changing the value.
    /// </summary>
    [TestCase("E'\\U00110000'", TestName = "JustAboveUnicodeMax")]
    [TestCase("E'\\U7FFFFFFF'", TestName = "IntMax")]
    [TestCase("E'\\U80000000'", TestName = "IntMaxPlusOne_WouldWrapNegative")]
    [TestCase("E'\\UFFFFFFFF'", TestName = "AllOnes_WouldWrapToMinusOne")]
    public void RejectsOutOfRangeUnicodeEscape(string literal)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => SqlStringLiteral.Decode(literal))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    /// <summary>The largest legal code point must still decode, so the range check is not off by one.</summary>
    [Test]
    public void AcceptsTheHighestLegalCodePoint()
    {
        Assert.AreEqual(char.ConvertFromUtf32(0x10FFFF), SqlStringLiteral.Decode("E'\\U0010FFFF'"));
    }

    [TestCase("E'a\\x4'", TestName = "TruncatedHexEscape")]
    [TestCase("E'a\\xZZ'", TestName = "NonHexDigits")]
    [TestCase("E'a\\u041'", TestName = "TruncatedUnicodeEscape")]
    [TestCase("E'\\ud800'", TestName = "LoneHighSurrogate")]
    [TestCase("E'\\udc00'", TestName = "LoneLowSurrogate")]
    public void RejectsUndecodableEscapeLiteral(string literal)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => SqlStringLiteral.Decode(literal))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    /// <summary>A surrogate pair written as two escapes is legal — only an unpaired one is not.</summary>
    [Test]
    public void AcceptsAWrittenSurrogatePair()
    {
        Assert.AreEqual("😀", SqlStringLiteral.Decode("E'\\ud83d\\ude00'"));
    }

    /// <summary>Bare (unquoted) text is handed back unchanged; callers pass synthesized AST text.</summary>
    [Test]
    public void PassesThroughUnquotedText()
    {
        Assert.AreEqual("bare", SqlStringLiteral.Decode("bare"));
    }
}
