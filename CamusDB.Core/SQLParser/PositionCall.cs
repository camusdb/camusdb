/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// The SQL-standard <c>POSITION(needle IN haystack)</c> form, as a call to the scalar function
/// <c>strpos(haystack, needle)</c>. The operands change places: the standard form names the needle
/// first, and <c>strpos</c> (PostgreSQL) names the haystack first.
///
/// <para><b>Why a plain call node.</b> As for <see cref="ExtractCall"/>, every expression walker already
/// handles <see cref="NodeType.ExprFuncCall"/>, so the call needs no new node type. The renderers print
/// the node as <c>strpos(haystack, needle)</c>, which parses back to the same node.</para>
///
/// <para><b>Why POSITION is not a keyword.</b> The grammar matches the name as an identifier, so a table
/// or column named <c>position</c> still parses. <see cref="Build"/> therefore receives any name that is
/// followed by <c>(expr IN operand)</c> and must reject every name other than <c>position</c>.</para>
/// </summary>
internal static class PositionCall
{
    public const string FunctionName = "position";

    public const string TargetFunctionName = "strpos";

    /// <summary>
    /// Builds the call node <c>strpos(haystack, needle)</c> for <c>name(needle IN haystack)</c>.
    /// </summary>
    public static NodeAst Build(NodeAst name, NodeAst needle, NodeAst haystack)
    {
        if (!string.Equals(name.yytext, FunctionName, StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.SqlSyntaxError,
                $"IN is not valid in the arguments of '{name.yytext}'. Only POSITION(needle IN haystack) takes this form");

        NodeAst target = new(NodeType.Identifier, null, null, null, null, null, null, null, TargetFunctionName);
        NodeAst arguments = new(NodeType.ExprArgumentList, haystack, needle, null, null, null, null, null, null);

        return new NodeAst(NodeType.ExprFuncCall, target, arguments, null, null, null, null, null, null);
    }
}
