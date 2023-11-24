
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

public enum NodeType
{
    Number = 1,
    String = 2,
    Identifier = 3,
    IdentifierList = 4,
    ExprEquals = 5,
    ExprNotEquals = 6,
    ExprLessThan = 7,
    ExprGreaterThan = 8,
    ExprOr = 9,
    ExprAnd = 10,
    ExprAllFields = 19,
    Select = 20,
}
