
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
    IdentifierWithOpts,
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
    ExprAlias,
    Select,
    Update,
    UpdateList,
    UpdateItem,
    Delete,
    Insert,
    SortAsc,
    SortDesc,
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
    TypeFloat64,
    ShowColumns,
    ShowTables,
    ShowCreateTable,
    ShowDatabase
}
