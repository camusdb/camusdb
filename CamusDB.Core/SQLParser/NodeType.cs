
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
    Placeholder,
    ExprList,
    ExprEquals,
    ExprNotEquals,
    ExprLessThan,
    ExprGreaterThan,
    ExprGreaterEqualsThan,
    ExprLessEqualsThan,
    ExprOr,
    ExprAnd,
    ExprAdd,
    ExprSub,
    ExprMult,
    ExprAllFields,
    ExprFuncCall,
    ExprArgumentList,
    Select,
    Update,
    UpdateList,
    UpdateItem,
    Delete,
    Insert,
    CreateTable,
    CreateTableItem,
    CreateTableItemList,
    CreateTableConstraintList,
    CreateTableConstraintItem,
    ConstraintNull,
    ConstraintNotNull,
    ConstraintPrimaryKey,
    ConstraintUnique,
    DropTable,
    AlterTableAddColumn,
    AlterTableDropColumn,
    TypeObjectId,
    TypeString,
    TypeInteger64,
    TypeFloat64
}
