
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

public static class NodeTypeExtensions
{
    public static bool IsBinary(this NodeType nodeType)
    {
        return nodeType switch
        {
            NodeType.ExprEquals or
            NodeType.ExprNotEquals or
            NodeType.ExprGreaterThan or
            NodeType.ExprGreaterEqualsThan or
            NodeType.ExprLessThan or
            NodeType.ExprLessEqualsThan or
            NodeType.ExprAdd or
            NodeType.ExprSub or
            NodeType.ExprMult or
            NodeType.ExprDiv or
            NodeType.ExprOr or
            NodeType.ExprAnd => true,
            _ => false,
        };
    }
}
