
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

public enum NodeType
{
    Number,
    String,
    Bool,
    Null,
    Identifier,
    IdentifierList,
    ExprList,
    ExprEquals,
    ExprNotEquals,
    ExprLessThan,
    ExprGreaterThan,
    ExprGreaterEqualsThan,
    ExprLessEqualsThan,
    ExprOr,
    ExprAnd,
    ExprAllFields,
    ExprFuncCall,
    Select,
    Update,
    UpdateList,
    UpdateItem,
    Delete,
    Insert
}
