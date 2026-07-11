
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Tasks;
using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// Tests for SQL comment support: line comments (<c>-- …</c>) and block comments (<c>/* … */</c>).
/// Both forms must be silently discarded by the lexer so that commented SQL parses identically
/// to the same SQL without comments.
/// </summary>
[TestFixture]
public class TestSQLComments
{
    // ── Line comments (--) ────────────────────────────────────────────────────

    [Test]
    public void LineComment_TrailingOnStatement_IsIgnored()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT id FROM t -- trailing note");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
    }

    [Test]
    public void LineComment_TrailingOnSelectFrom_IsIgnored()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT id FROM users -- end of line");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual("id", ast.leftAst!.yytext);
    }

    [Test]
    public void LineComment_FullLineBeforeStatement_IsIgnored()
    {
        NodeAst ast = SQLParserProcessor.Parse("-- header\nSELECT id FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual("id", ast.leftAst!.yytext);
    }

    [Test]
    public void LineComment_OnlyCommentAtEof_DoesNotCrash()
    {
        // A stand-alone comment with no SQL is not a valid statement, but it
        // must not produce a confusing lexer crash — the parser sees an empty
        // input and raises a normal syntax error (not an unexpected token).
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("-- just a comment"));
    }

    [Test]
    public void LineComment_MiddleOfMultilineStatement_IsIgnored()
    {
        const string sql = "SELECT id\n-- pick the right table\nFROM users";
        NodeAst ast = SQLParserProcessor.Parse(sql);
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual("id", ast.leftAst!.yytext);
    }

    [Test]
    public void LineComment_InsideSingleQuotedString_IsNotAComment()
    {
        // '-- not a comment' must lex as one TSTRING token, not stop at '--'.
        NodeAst ast = SQLParserProcessor.Parse("SELECT 'a -- b' FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        // The string literal survives intact as a quoted value node.
        Assert.IsNotNull(ast.leftAst);
    }

    [Test]
    public void LineComment_InsideDoubleQuotedString_IsNotAComment()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT \"x -- y\" FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.leftAst);
    }

    [Test]
    public void LineComment_DashDashFive_IsCommentNotSubtraction()
    {
        // SQL standard: '--5' after a value is a trailing comment, not '- (-5)'.
        // SELECT 10 FROM t --5  =>  SELECT 10 FROM t  (the '--5' is a comment to end of line).
        NodeAst ast = SQLParserProcessor.Parse("SELECT 10 FROM t --5");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        // Only '10' should survive; the '--5' comment is gone.
        Assert.AreEqual("10", ast.leftAst!.yytext);
    }

    [Test]
    public void LineComment_SpacedMinusMinus_IsSubtractionOfNegative()
    {
        // '10 - -5' (space before second minus) is subtraction of a negative number.
        NodeAst ast = SQLParserProcessor.Parse("SELECT 10 - -5 FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        // The expression node represents a subtraction.
        Assert.IsNotNull(ast.leftAst);
    }

    [Test]
    public void LineComment_NegativeLiteralUnaffected()
    {
        // '-5' must still lex as one number token.
        NodeAst ast = SQLParserProcessor.Parse("SELECT -5 FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual("-5", ast.leftAst!.yytext);
    }

    // ── Block comments (/* … */) ──────────────────────────────────────────────

    [Test]
    public void BlockComment_InlineMiddleOfStatement_IsIgnored()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT /* inline */ 1 FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
    }

    [Test]
    public void BlockComment_BetweenTokensNoSpace_IsIgnored()
    {
        // Deliberately no spaces: SELECT/* x */1 must still produce two tokens.
        NodeAst ast = SQLParserProcessor.Parse("SELECT/* x */id FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual("id", ast.leftAst!.yytext);
    }

    [Test]
    public void BlockComment_MultilineComment_IsIgnored()
    {
        const string sql = "SELECT\n/* this\n   spans\n   lines */\nid FROM t";
        NodeAst ast = SQLParserProcessor.Parse(sql);
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual("id", ast.leftAst!.yytext);
    }

    [Test]
    public void BlockComment_MultilineComment_LineNumberAfterIsCorrect()
    {
        // A syntax error *after* a multi-line block comment must report a line
        // number that accounts for the newlines inside the comment.
        const string sql = "SELECT\n/*\nspan\n*/\nBADSQL";
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => SQLParserProcessor.Parse(sql))!;
        // The error should reference line 6 (the 'BADSQL' line per GPLEX line counter), not line 1.
        Assert.That(ex.Message, Does.Contain("line 6"), ex.Message);
    }

    [Test]
    public void BlockComment_NonNesting_FirstCloseWins()
    {
        // Non-nesting: /* a */ SELECT id FROM t /* b */ => SELECT id FROM t (two separate comments).
        NodeAst ast = SQLParserProcessor.Parse("/* a */ SELECT id FROM t /* b */");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
    }

    [Test]
    public void BlockComment_InsideSingleQuotedString_IsNotAComment()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT '/* not a comment */' FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.leftAst);
    }

    [Test]
    public void BlockComment_InsideDoubleQuotedString_IsNotAComment()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT \"/* not a comment */\" FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.leftAst);
    }

    [Test]
    public void BlockComment_Unterminated_RaisesCleanError()
    {
        // An unclosed /* must produce a clear error, not a TDIV/TMULT cascade.
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => SQLParserProcessor.Parse("SELECT 1 /* oops"))!;
        Assert.That(ex.Message.ToLowerInvariant(), Does.Contain("unterminated").Or.Contain("block comment"),
            $"Expected error about unterminated block comment, got: {ex.Message}");
    }

    [Test]
    public void BlockComment_DivisionOperator_Unaffected()
    {
        // 'a / b' must still lex as TDIV, not be confused with a block comment open.
        NodeAst ast = SQLParserProcessor.Parse("SELECT a / b FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.leftAst);
    }

    [Test]
    public void BlockComment_MultiplicationOperator_Unaffected()
    {
        // 'a * b' must still lex as TMULT.
        NodeAst ast = SQLParserProcessor.Parse("SELECT a * b FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.leftAst);
    }

    [Test]
    public void BlockComment_SelectStar_Unaffected()
    {
        // 'SELECT *' must still work — the lone '*' is TMULT, not a comment fragment.
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.leftAst);
    }

    // ── Combined / interaction coverage (Task 3) ──────────────────────────────

    [Test]
    public void BothCommentForms_TogetherInOneStatement_AllIgnored()
    {
        const string sql = "-- line\nSELECT /* mid */ id FROM t /* tail */ -- eol";
        NodeAst ast = SQLParserProcessor.Parse(sql);
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual("id", ast.leftAst!.yytext);
    }

    [Test]
    public void LineCommentInsideBlockComment_IsInert()
    {
        // A '--' inside '/* … */' is part of the block comment body, not a new comment.
        NodeAst ast = SQLParserProcessor.Parse("SELECT /* -- this is comment body */ id FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual("id", ast.leftAst!.yytext);
    }

    [Test]
    public void BlockCommentOpenInsideLineComment_IsInert()
    {
        // A '/*' that appears after '--' is inside the line comment, not a new block comment.
        NodeAst ast = SQLParserProcessor.Parse("SELECT id -- /* ignored\nFROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual("id", ast.leftAst!.yytext);
    }

    [Test]
    public async Task CacheRoundTrip_CommentedAndUncommentedProduceEqualAsts_AndAreDistinctEntries()
    {
        await using SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60);

        const string plain    = "SELECT id FROM users";
        const string commented = "SELECT id -- note\nFROM users";

        NodeAst astPlain    = SQLParserProcessor.Parse(plain,    cache);
        NodeAst astCommented = SQLParserProcessor.Parse(commented, cache);

        // Structurally equal: both are SELECT nodes with the same shape.
        Assert.AreEqual(astPlain.nodeType, astCommented.nodeType);
        Assert.AreEqual(astPlain.leftAst!.yytext, astCommented.leftAst!.yytext);

        // Distinct cache entries: commented and plain SQL are different keys.
        Assert.That(astCommented, Is.Not.SameAs(astPlain),
            "Commented SQL must occupy its own cache entry, not alias the plain-SQL entry");

        // A second call with the SAME commented SQL returns the cached reference.
        NodeAst astCommented2 = SQLParserProcessor.Parse(commented, cache);
        Assert.That(astCommented2, Is.SameAs(astCommented),
            "Second parse of identical commented SQL must return the cached NodeAst reference");
    }
}
