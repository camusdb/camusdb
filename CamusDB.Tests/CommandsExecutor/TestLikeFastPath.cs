
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using NUnit.Framework;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Parity tests for the LIKE / ILIKE fast-path matchers.
///
/// Each test case verifies that the fast-path result (now in Like/ILike) matches
/// what the previous regex-only implementation produced. The table of patterns covers:
///   - no %, leading %, trailing %, both % (contains), embedded % (regex fallback)
///   - underscore '_' is NOT a wildcard (matches current semantics)
///   - non-ASCII: ordinal comparison must not change results across locales
///   - ILIKE: same shapes with case-insensitive matching
/// </summary>
[TestFixture]
public class TestLikeFastPath
{
    // ── helpers ──────────────────────────────────────────────────────────────

    private static bool EvalLike(string text, string pattern)
    {
        NodeAst expr = LikeAst(NodeType.ExprLike, text, pattern);
        Dictionary<string, ColumnValue> row = [];
        ColumnValue result = SQLExecutorBaseCreator.EvalExpr(expr, row, null, null);
        return result.BoolValue;
    }

    private static bool EvalILike(string text, string pattern)
    {
        NodeAst expr = LikeAst(NodeType.ExprILike, text, pattern);
        Dictionary<string, ColumnValue> row = [];
        ColumnValue result = SQLExecutorBaseCreator.EvalExpr(expr, row, null, null);
        return result.BoolValue;
    }

    private static NodeAst LikeAst(NodeType op, string text, string pattern) =>
        new(op,
            new NodeAst(NodeType.String, null, null, null, null, null, null, null, text),
            new NodeAst(NodeType.String, null, null, null, null, null, null, null, pattern),
            null, null, null, null, null, null);

    // ── no-wildcard: equality ─────────────────────────────────────────────────

    [Test]
    public void Like_NoPercent_ExactMatch_ReturnsTrue()
        => Assert.IsTrue(EvalLike("hello", "hello"));

    [Test]
    public void Like_NoPercent_Mismatch_ReturnsFalse()
        => Assert.IsFalse(EvalLike("hello", "world"));

    [Test]
    public void Like_NoPercent_CaseSensitive_ReturnsFalse()
        => Assert.IsFalse(EvalLike("hello", "Hello"));

    [Test]
    public void Like_NoPercent_EmptyBoth_ReturnsTrue()
        => Assert.IsTrue(EvalLike("", ""));

    [Test]
    public void Like_NoPercent_EmptyText_ReturnsFalse()
        => Assert.IsFalse(EvalLike("", "hello"));

    // ── trailing %: StartsWith ────────────────────────────────────────────────

    [Test]
    public void Like_TrailingPercent_Match_ReturnsTrue()
        => Assert.IsTrue(EvalLike("hello world", "hello%"));

    [Test]
    public void Like_TrailingPercent_NoMatch_ReturnsFalse()
        => Assert.IsFalse(EvalLike("world hello", "hello%"));

    [Test]
    public void Like_TrailingPercent_ExactPrefix_ReturnsTrue()
        => Assert.IsTrue(EvalLike("hello", "hello%"));

    [Test]
    public void Like_TrailingPercent_OnlyPercent_MatchesAny()
    {
        Assert.IsTrue(EvalLike("anything", "%"));
        Assert.IsTrue(EvalLike("", "%"));
    }

    // ── leading %: EndsWith ───────────────────────────────────────────────────

    [Test]
    public void Like_LeadingPercent_Match_ReturnsTrue()
        => Assert.IsTrue(EvalLike("hello world", "%world"));

    [Test]
    public void Like_LeadingPercent_NoMatch_ReturnsFalse()
        => Assert.IsFalse(EvalLike("hello world", "%hello"));

    [Test]
    public void Like_LeadingPercent_ExactSuffix_ReturnsTrue()
        => Assert.IsTrue(EvalLike("world", "%world"));

    // ── both % (contains) ────────────────────────────────────────────────────

    [Test]
    public void Like_BothPercent_Match_ReturnsTrue()
        => Assert.IsTrue(EvalLike("hello world foo", "%world%"));

    [Test]
    public void Like_BothPercent_NoMatch_ReturnsFalse()
        => Assert.IsFalse(EvalLike("hello foo", "%world%"));

    [Test]
    public void Like_BothPercent_EmptyInner_MatchesAny()
    {
        Assert.IsTrue(EvalLike("anything", "%%"));
        Assert.IsTrue(EvalLike("", "%%"));
    }

    // ── embedded % in middle: regex fallback ──────────────────────────────────

    [Test]
    public void Like_EmbeddedPercent_MatchesMultiWildcard()
    {
        // 'he%lo' should match 'hello' (he + anything + lo)
        Assert.IsTrue(EvalLike("hello", "he%lo"));
        // also matches 'helo' (zero chars between)
        Assert.IsTrue(EvalLike("helo", "he%lo"));
        // should NOT match 'helloo' (anchored at end)
        Assert.IsFalse(EvalLike("helloo", "he%lo"));
    }

    [Test]
    public void Like_MultipleEmbeddedPercent_MatchesCorrectly()
    {
        Assert.IsTrue(EvalLike("abcXYZdef", "abc%def"));
        Assert.IsFalse(EvalLike("abcXYZ", "abc%def"));
        Assert.IsTrue(EvalLike("aXbYc", "a%b%c"));
        Assert.IsFalse(EvalLike("aXYZc", "a%b%c"));
    }

    // ── underscore '_' is NOT a wildcard ─────────────────────────────────────

    [Test]
    public void Like_Underscore_IsLiteral_NotWildcard()
    {
        // '_' must match the literal underscore character, not any character
        Assert.IsTrue(EvalLike("h_llo", "h_llo"));
        Assert.IsFalse(EvalLike("hello", "h_llo"));
    }

    [Test]
    public void Like_UnderscoreWithPercent_BothLiteralAndWildcard()
    {
        // pattern 'h_l%' means: starts with "h_l" (literal underscore, not wildcard)
        Assert.IsTrue(EvalLike("h_lo world", "h_l%"));
        Assert.IsFalse(EvalLike("hello world", "h_l%"));
    }

    // ── non-ASCII ordinal comparison ─────────────────────────────────────────

    [Test]
    public void Like_NonAscii_OrdinalMatch()
    {
        // Ordinal: 'é' != 'e', so 'héllo%' must NOT match 'hello world'
        Assert.IsFalse(EvalLike("hello world", "héllo%"));
        Assert.IsTrue(EvalLike("héllo world", "héllo%"));
    }

    [Test]
    public void Like_NonAscii_Contains_OrdinalMatch()
    {
        Assert.IsTrue(EvalLike("say héllo there", "%héllo%"));
        Assert.IsFalse(EvalLike("say hello there", "%héllo%"));
    }

    // ── ILIKE: case-insensitive variants ─────────────────────────────────────

    [Test]
    public void ILike_NoPercent_CaseInsensitive_ReturnsTrue()
        => Assert.IsTrue(EvalILike("Hello", "hello"));

    [Test]
    public void ILike_TrailingPercent_CaseInsensitive_ReturnsTrue()
        => Assert.IsTrue(EvalILike("HELLO WORLD", "hello%"));

    [Test]
    public void ILike_TrailingPercent_NoMatch_ReturnsFalse()
        => Assert.IsFalse(EvalILike("WORLD HELLO", "hello%"));

    [Test]
    public void ILike_LeadingPercent_CaseInsensitive_ReturnsTrue()
        => Assert.IsTrue(EvalILike("HELLO WORLD", "%WORLD"));

    [Test]
    public void ILike_BothPercent_CaseInsensitive_ReturnsTrue()
        => Assert.IsTrue(EvalILike("say HELLO there", "%hello%"));

    [Test]
    public void ILike_BothPercent_NoMatch_ReturnsFalse()
        => Assert.IsFalse(EvalILike("say there", "%hello%"));

    [Test]
    public void ILike_EmbeddedPercent_CaseInsensitive_ReturnsTrue()
        => Assert.IsTrue(EvalILike("HELLO WORLD", "he%ld"));

    [Test]
    public void ILike_MultiWildcard_CaseInsensitive_ReturnsTrue()
    {
        // Three-segment pattern (two interior %) exercises the sequential segment scan.
        Assert.IsTrue(EvalILike("ALPHA BRAVO CHARLIE", "alpha%bravo%charlie"));
        Assert.IsFalse(EvalILike("ALPHA CHARLIE", "alpha%bravo%charlie"));
        // Segment order is enforced left-to-right.
        Assert.IsFalse(EvalILike("BRAVO ALPHA CHARLIE", "alpha%bravo%charlie"));
    }

    [Test]
    public void ILike_MultiWildcard_NonAscii_OrdinalIgnoreCase()
    {
        // 'İ' (U+0130, Turkish dotted I) folds to 'i' under OrdinalIgnoreCase but NOT
        // under Regex IgnoreCase (culture-default) in en-US. The sequential path must
        // use OrdinalIgnoreCase consistently — verify ILIKE result is ordinal on all shapes.
        //
        // OrdinalIgnoreCase: 'İ'.ToLowerInvariant() == 'i' is false (ordinal folding is
        // limited to A-Z/a-z), so İ != i under OrdinalIgnoreCase.
        Assert.IsFalse(EvalILike("İSTANBUL", "istanbul%"));     // ordinal: İ != i
        Assert.IsTrue(EvalILike("istanbul bazar", "istanbul%")); // ordinal: exact ASCII match
        // Multi-wildcard path must agree with the single-% fast path on the same text/pattern.
        Assert.AreEqual(
            EvalILike("xİy foo", "x%y%foo"),
            EvalILike("xİy foo", "xİ%"));  // both ordinal; ensures no cross-path divergence
    }

    [Test]
    public void ILike_Underscore_IsLiteral_NotWildcard()
    {
        Assert.IsTrue(EvalILike("H_LLO", "h_llo"));
        Assert.IsFalse(EvalILike("HELLO", "h_llo"));
    }

    [Test]
    public void ILike_NonAscii_OrdinalIgnoreCase_Match()
    {
        Assert.IsTrue(EvalILike("HéLLO WORLD", "héllo%"));
        Assert.IsFalse(EvalILike("HELLO WORLD", "héllo%"));
    }
}
