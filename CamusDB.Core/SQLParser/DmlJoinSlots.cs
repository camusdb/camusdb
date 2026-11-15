/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Reads the two parts of an <c>UPDATE</c> or <c>DELETE</c> node that only the join forms have: the
/// relation list (<c>UPDATE … FROM</c>, <c>DELETE … USING</c>) and the alias of the target table.
///
/// <para>The grammar puts both in the slots after RETURNING, so every consumer that predates the join
/// forms reads the node as before. A statement that has either part runs through
/// <c>JoinedDmlExecutor</c>; a statement with neither runs through the plain locate scan.</para>
/// </summary>
internal static class DmlJoinSlots
{
    /// <summary>The FROM list of an UPDATE or the USING list of a DELETE, or null when it has none.</summary>
    public static NodeAst? RelationList(NodeAst ast) => ast.nodeType switch
    {
        NodeType.Update => ast.extendedFour,
        NodeType.Delete => ast.extendedThree,
        _ => null,
    };

    /// <summary>The alias of the target table of an UPDATE or a DELETE, or null when it has none.</summary>
    public static NodeAst? TargetAlias(NodeAst ast) => ast.nodeType switch
    {
        NodeType.Update => ast.extendedFive,
        NodeType.Delete => ast.extendedFour,
        _ => null,
    };

    /// <summary>True when the statement has a relation list or a target alias.</summary>
    public static bool IsJoined(NodeAst ast) => RelationList(ast) is not null || TargetAlias(ast) is not null;
}
