
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Structural comparison for parsed SQL expression trees (GROUP BY matching and aggregate dedup).
/// Two nodes are equivalent when they have the same <see cref="NodeType"/>, the same
/// <c>yytext</c>, and recursively equivalent children including all extended fields
/// (<c>extendedOne</c> through <c>extendedSix</c>). This is the single authoritative definition
/// of "structurally equal" for SQL AST nodes — both GROUP-BY matching and aggregate-accumulator
/// deduplication rely on it so the two cannot drift apart.
/// </summary>
internal static class QueryAstComparer
{
    public static bool AreEquivalent(NodeAst? left, NodeAst? right)
    {
        if (left is null && right is null)
            return true;

        if (left is null || right is null)
            return false;

        if (left.nodeType != right.nodeType)
            return false;

        if (!string.Equals(left.yytext, right.yytext, StringComparison.Ordinal))
            return false;

        return AreEquivalent(left.leftAst, right.leftAst)
            && AreEquivalent(left.rightAst, right.rightAst)
            && AreEquivalent(left.extendedOne, right.extendedOne)
            && AreEquivalent(left.extendedTwo, right.extendedTwo)
            && AreEquivalent(left.extendedThree, right.extendedThree)
            && AreEquivalent(left.extendedFour, right.extendedFour)
            && AreEquivalent(left.extendedFive, right.extendedFive)
            && AreEquivalent(left.extendedSix, right.extendedSix);
    }
}
