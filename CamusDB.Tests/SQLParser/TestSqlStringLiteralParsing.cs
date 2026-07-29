
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
/// Drives literal handling through the <b>generated lexer</b> rather than calling the codec directly.
///
/// <para>This is the gap the codec's own unit tests cannot close: they prove <c>Decode(Quote(v)) == v</c>
/// for a hand-written notion of a token, but not that the real scanner accepts exactly what
/// <c>Quote</c> emits, nor where it chooses to end a token. A drift between the two would surface as
/// unparseable <c>SHOW CREATE TABLE</c> output, which is the failure this whole area exists to
/// prevent.</para>
/// </summary>
[TestFixture]
internal sealed class TestSqlStringLiteralParsing
{
    /// <summary>
    /// Parses <c>SELECT &lt;literal&gt;</c> and returns the decoded value of the projected literal,
    /// so the assertion covers scan → parse → decode rather than decode alone.
    /// </summary>
    private static string ParseLiteral(string literal)
    {
        NodeAst ast = SQLParserProcessor.Parse($"SELECT {literal}");
        NodeAst node = FindString(ast) ?? throw new AssertionException($"no string node parsed from {literal}");
        return SqlStringLiteral.Decode(node.yytext ?? "");
    }

    private static NodeAst? FindString(NodeAst? node)
    {
        if (node is null)
            return null;

        if (node.nodeType == NodeType.String)
            return node;

        return FindString(node.leftAst)
            ?? FindString(node.rightAst)
            ?? FindString(node.extendedOne)
            ?? FindString(node.extendedTwo)
            ?? FindString(node.extendedThree);
    }

    /// <summary>
    /// The central guarantee: whatever <c>Quote</c> emits must lex as a single literal that decodes
    /// back to the original value. Run through the parser, so a token the scanner would split or
    /// reject fails here even though the codec round-trip passes.
    /// </summary>
    [TestCase("plain")]
    [TestCase("it's")]
    [TestCase("say \"hi\"")]
    [TestCase("both ' and \" together")]
    [TestCase("a\\b")]
    [TestCase("trailing\\")]
    [TestCase("a\\'b")]
    [TestCase("a\\'b\\\"c")]
    [TestCase("(\\d{4})-(\\d{2})")]
    [TestCase("C:\\Users\\data")]
    [TestCase("line1\nline2")]
    [TestCase("tab\there")]
    [TestCase("nul\0inside")]
    [TestCase("\a\b\f\v\r")]
    [TestCase("unicode: é中😀")]
    [TestCase("'); DROP TABLE t; --")]
    public void QuotedValueSurvivesTheRealLexer(string value)
    {
        Assert.AreEqual(value, ParseLiteral(SqlStringLiteral.Quote(value)));
    }

    /// <summary>
    /// A plain literal does no escape processing, so a regex or a Windows path written the obvious way
    /// reaches the engine unchanged. This is the compatibility guarantee the two-form design exists to
    /// provide; it must hold through the scanner, not just the decoder.
    /// </summary>
    [TestCase("'(\\d+)'", "(\\d+)")]
    [TestCase("'C:\\Users'", "C:\\Users")]
    [TestCase("'a\\nb'", "a\\nb")]
    [TestCase("'it''s'", "it's")]
    [TestCase("'say \"hi\"'", "say \"hi\"")]
    [TestCase("\"a'b\"", "a'b")]
    public void PlainLiteralIsNotEscapeProcessed(string literal, string expected)
    {
        Assert.AreEqual(expected, ParseLiteral(literal));
    }

    [TestCase("E'a\\nb'", "a\nb")]
    [TestCase("e'a\\tb'", "a\tb")]
    [TestCase("E'a\\\\b'", "a\\b")]
    [TestCase("E'a\\'b'", "a'b")]
    [TestCase("E'\\x41'", "A")]
    [TestCase("E'\\u0041'", "A")]
    [TestCase("E'\\d+'", "d+")]
    public void EscapeLiteralIsEscapeProcessed(string literal, string expected)
    {
        Assert.AreEqual(expected, ParseLiteral(literal));
    }

    /// <summary>
    /// A plain literal ends at the first unpaired delimiter even when a backslash precedes it — the
    /// one behavior change of the two-form model. `'a\'` is therefore a complete literal worth `a\`,
    /// which is exactly what makes a trailing backslash representable at all.
    /// </summary>
    [Test]
    public void PlainLiteralEndsAtAQuoteFollowingABackslash()
    {
        Assert.AreEqual("a\\", ParseLiteral("'a\\'"));
    }

    /// <summary>
    /// The <c>E</c> prefix binds only when a delimiter follows immediately. With a space it is an
    /// ordinary identifier, so a column named <c>e</c> keeps working — the same split PostgreSQL makes.
    /// </summary>
    [Test]
    public void IdentifierEIsOnlyAPrefixWhenAdjacentToTheDelimiter()
    {
        Assert.AreEqual("x", ParseLiteral("E'x'"));

        // `e` followed by whitespace is an identifier; the literal after it is plain, so the
        // backslash stays literal.
        NodeAst ast = SQLParserProcessor.Parse("SELECT e FROM t WHERE e = '\\d'");
        Assert.AreEqual("\\d", SqlStringLiteral.Decode(FindString(ast)!.yytext ?? ""));
    }

    /// <summary>
    /// Out-of-range and malformed escapes must be rejected when they arrive through the parser, not
    /// only through a direct <c>Decode</c> call — an eight-digit escape that overflowed a signed
    /// accumulator would otherwise be accepted here and silently change the value.
    /// </summary>
    [TestCase("E'\\U00110000'", TestName = "Parse_JustAboveUnicodeMax")]
    [TestCase("E'\\U80000000'", TestName = "Parse_WouldWrapNegative")]
    [TestCase("E'\\UFFFFFFFF'", TestName = "Parse_AllOnes")]
    [TestCase("E'\\x4'", TestName = "Parse_TruncatedHex")]
    [TestCase("E'\\ud800'", TestName = "Parse_LoneSurrogate")]
    public void RejectsMalformedEscapeThroughTheParser(string literal)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => ParseLiteral(literal))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    /// <summary>
    /// A user-typed astral character (emoji, any code point above U+FFFF) must lex in the plain form.
    /// The codec can route its own output through <c>E'\uHHHH'</c> escapes, but that does nothing for
    /// SQL a client actually writes, which is the case that matters here.
    /// </summary>
    [Test]
    public void PlainLiteralCarriesAstralCharacters()
    {
        Assert.AreEqual("\U0001F600", ParseLiteral("'\U0001F600'"));
    }

    // ── bytes literals ──────────────────────────────────────────────────────

    private static NodeAst? FindBytes(NodeAst? node)
    {
        if (node is null)
            return null;

        if (node.nodeType == NodeType.BytesLiteral)
            return node;

        return FindBytes(node.leftAst)
            ?? FindBytes(node.rightAst)
            ?? FindBytes(node.extendedOne)
            ?? FindBytes(node.extendedTwo)
            ?? FindBytes(node.extendedThree);
    }

    private static byte[] ParseBytes(string literal)
    {
        NodeAst ast = SQLParserProcessor.Parse($"SELECT {literal}");
        NodeAst node = FindBytes(ast) ?? throw new AssertionException($"no bytes node parsed from {literal}");
        return SqlStringLiteral.DecodeBytes(node.yytext ?? "");
    }

    /// <summary>
    /// The bytes literal must lex and decode as its own node type. A bare <c>0x…</c> cannot serve
    /// this purpose — it is already an integer literal — so the type of a bytes value is only
    /// recoverable from the source when written in this form.
    /// </summary>
    [Test]
    public void BytesLiteralParses()
    {
        CollectionAssert.AreEqual(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }, ParseBytes("X'DEADBEEF'"));
        CollectionAssert.AreEqual(new byte[] { 0xDE, 0xAD }, ParseBytes("x'dead'"));
        CollectionAssert.AreEqual(new byte[] { 0x4D, 0x5A }, ParseBytes("X'4d5A'"));
        CollectionAssert.AreEqual(System.Array.Empty<byte>(), ParseBytes("X''"));
    }

    /// <summary>
    /// An odd digit count is refused rather than padded — either padding guess silently produces
    /// bytes the author did not write.
    /// </summary>
    [Test]
    public void BytesLiteralWithOddDigitCountIsRejected()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => ParseBytes("X'ABC'"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    /// <summary>
    /// `0x…` keeps its existing meaning as an integer literal. Retyping it would silently change
    /// every hex integer in existing SQL, which is why the bytes literal got its own syntax.
    /// </summary>
    [Test]
    public void BareHexRemainsAnIntegerLiteral()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT 0xFF");
        Assert.IsNull(FindBytes(ast), "0xFF must not become a bytes literal");
    }

    /// <summary>An identifier named `x` still works when it is not glued to a quote.</summary>
    [Test]
    public void IdentifierXIsOnlyAPrefixWhenAdjacentToTheDelimiter()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT x FROM t WHERE x = 'v'");
        Assert.IsNull(FindBytes(ast), "a bare identifier x must not lex as a bytes literal");
    }
}
