
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Normalizes user-supplied SQL identifiers to lower-case so table and column
/// lookups are case-insensitive regardless of how the identifier was written.
/// </summary>
internal static class IdentifierNormalizer
{
    public static void Normalize(NodeAst? node)
    {
        if (node is null)
            return;

        if (node.nodeType == NodeType.Identifier && node.yytext is not null)
            node.yytext = node.yytext.ToLowerInvariant();

        Normalize(node.leftAst);
        Normalize(node.rightAst);
        Normalize(node.extendedOne);
        Normalize(node.extendedTwo);
        Normalize(node.extendedThree);
        Normalize(node.extendedFour);
        Normalize(node.extendedFive);
    }
}
