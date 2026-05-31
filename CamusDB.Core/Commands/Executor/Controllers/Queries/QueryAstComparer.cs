
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Structural comparison for parsed SQL expression trees (GROUP BY matching).
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
            && AreEquivalent(left.rightAst, right.rightAst);
    }
}
