
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

public enum NodeType
{
    Integer,
    Float,
    String,
    Bool,
    Null,
    ObjectIdLiteral,
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
    ExprBetween,
    ExprOr,
    ExprAnd,
    ExprNot,
    ExprAdd,
    ExprSub,
    ExprMult,
    ExprDiv,
    ExprAllFields,
    ExprFuncCall,
    ExprCast,
    ExprArgumentList,
    ExprAlias,
    ExprLike,
    ExprILike,
    ExprDefault,
    ExprIsNull,
    ExprIsNotNull,
    ExprScalarSubquery,
    ExprInSubquery,
    ExprInMembership,
    ExprNotInSubquery,
    ExprNotInMembership,
    ExprExistsCorrelated,
    ExprExistsSubquery,
    Select,
    GroupBy,
    Having,
    TableReference,
    DerivedTableReference,
    CommaJoin,
    CommaJoinTableList,
    Join,
    Update,
    UpdateList,
    UpdateItem,
    Delete,
    Insert,
    InsertBatchList,
    Begin,
    Commit,
    Rollback,
    SetTransaction,
    SortAsc,
    SortDesc,
    CreateTable,
    CreateTableIfNotExists,
    CreateTableItem,
    CreateTableItemList,
    CreateTableFieldConstraintList,
    CreateTableConstraintList,
    CreateTableConstraintPrimaryKey,
    CreateTableConstraintMultiIndex,
    CreateTableConstraintUniqueIndex,
    ConstraintNull,
    ConstraintNotNull,
    ConstraintPrimaryKey,
    ConstraintUnique,
    ConstraintDefault,
    /// <summary>
    /// Column-level <c>CHECK (condition)</c> constraint written inline on a column definition.
    /// <c>leftAst</c> = the condition expression. Desugared into a table-level
    /// <see cref="CreateTableConstraintCheck"/> at schema-build time.
    /// </summary>
    ConstraintCheck,
    /// <summary>
    /// Table-level <c>CHECK (condition)</c> constraint, optionally named via
    /// <c>CONSTRAINT name CHECK (condition)</c>. <c>leftAst</c> = condition; <c>yytext</c> = name
    /// (null when auto-naming is required).
    /// </summary>
    CreateTableConstraintCheck,
    /// <summary>
    /// <c>ALTER TABLE t ADD CONSTRAINT name CHECK (condition)</c>.
    /// <c>leftAst</c> = table name node; <c>rightAst</c> = condition; <c>yytext</c> = constraint name.
    /// </summary>
    AlterTableAddConstraintCheck,
    /// <summary>
    /// <c>ALTER TABLE t DROP CONSTRAINT name</c>.
    /// <c>leftAst</c> = table name node; <c>yytext</c> = constraint name.
    /// </summary>
    AlterTableDropConstraint,
    /// <summary>
    /// <c>ALTER TABLE t ALTER [COLUMN] c SET NOT NULL</c>.
    /// <c>leftAst</c> = table name node; <c>rightAst</c> = column name node.
    /// </summary>
    AlterTableSetNotNull,
    /// <summary>
    /// <c>ALTER TABLE t ALTER [COLUMN] c DROP NOT NULL</c>.
    /// <c>leftAst</c> = table name node; <c>rightAst</c> = column name node.
    /// </summary>
    AlterTableDropNotNull,
    /// <summary>
    /// Column-level <c>CONSTRAINT name NOT NULL</c> written inline on a column definition.
    /// <c>yytext</c> = the user-supplied constraint name.
    /// </summary>
    ConstraintNotNullNamed,
    DropTable,
    DropTableIfExists,
    AlterTableAddColumn,
    AlterTableDropColumn,
    AlterTableAddIndex,
    AlterTableAddIndexIfNotExists,
    AlterTableAddUniqueIndex,
    AlterTableAddUniqueIndexIfNotExists,
    AlterTableDropIndex,
    AlterTableAddPrimaryKey,
    AlterTableDropPrimaryKey,
    AlterTableRenameTo,
    AlterTableRenameColumn,
    AlterTableRenameIndex,
    IndexIdentifierList,
    IndexIdentifierAsc,
    IndexIdentifierDesc,
    TypeObjectId,
    TypeString,
    TypeStringSized,
    TypeInteger64,
    TypeFloat64,
    TypeBool,
    TypeFloat32,
    TypeBytes,
    TypeDate,
    TypeDateTime,
    TypeUuid,
    TypeArray,
    ShowColumns,
    ShowTables,
    ShowCreateTable,
    ShowDatabase,
    ShowDatabases,
    ShowBranches,
    ShowAncestors,
    CreateDatabase,
    CreateDatabaseIfNotExists,
    CreateDatabaseBranch,
    CreateDatabaseBranchIfNotExists,
    DropDatabase,
    DropDatabaseIfExists,
    RenameDatabase,
    ShowIndexes,
    Explain,
    ExplainLogical,
    ExplainPhysical,
    ExplainAnalyze,
    AnalyzeTable,
    /// <summary>
    /// Cache hint extracted from a <c>{cache=name}</c> table-reference hint.
    /// Layout: <c>yytext</c> = cache name; <c>leftAst</c> = option list (ExprList of
    /// String/Integer option nodes) or single option node; null when no options.
    /// Option nodes: NodeType.String with yytext="strict"; NodeType.Integer with yytext=ttl_ms.
    /// </summary>
    CacheHint,
    /// <summary>
    /// <c>EVICT CACHE 'name'</c> — drop all entries in the named cache family for the current
    /// database. <c>yytext</c> holds the quoted cache name; the executor strips the surrounding
    /// quotes before calling <see cref="IQueryResultCache.InvalidateCacheName"/>.
    /// </summary>
    EvictCache,
    /// <summary>
    /// <c>EVICT CACHE ALL</c> — drop every result-cache entry for the current database.
    /// Calls <see cref="IQueryResultCache.InvalidateDatabase"/> with the database id.
    /// </summary>
    EvictCacheAll,
}
