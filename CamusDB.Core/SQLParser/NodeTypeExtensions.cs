
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
        switch (nodeType)
        {
            case NodeType.ExprEquals:
            case NodeType.ExprNotEquals:
            case NodeType.ExprGreaterThan:
            case NodeType.ExprGreaterEqualsThan:
            case NodeType.ExprLessThan:
            case NodeType.ExprLessEqualsThan:
            case NodeType.ExprAdd:
            case NodeType.ExprSub:
            case NodeType.ExprMult:
            case NodeType.ExprOr:
            case NodeType.ExprAnd:
                return true;
        }

        return false;
    }
}
