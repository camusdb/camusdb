
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

    /// <summary>
    /// A <c>CASE … WHEN … THEN … [ELSE …] END</c> conditional expression. <c>leftAst</c> is the
    /// simple-CASE operand (compared for equality against each WHEN value) or <see langword="null"/>
    /// for a searched CASE; <c>rightAst</c> is the WHEN/THEN chain (a left-recursive
    /// <see cref="ExprCaseWhenList"/> of <see cref="ExprCaseWhen"/> clauses); <c>extendedOne</c> is
    /// the ELSE result, or <see langword="null"/> when omitted (no match then yields typed NULL).
    /// </summary>
    ExprCase,

    /// <summary>
    /// One <c>WHEN c THEN r</c> clause of an <see cref="ExprCase"/>: <c>leftAst</c> is the condition
    /// (searched CASE) or the comparison value (simple CASE); <c>rightAst</c> is the result expression.
    /// </summary>
    ExprCaseWhen,

    /// <summary>
    /// Left-recursive chain of <see cref="ExprCaseWhen"/> clauses that preserves WHEN order:
    /// <c>leftAst</c> is the chain of earlier clauses (another <see cref="ExprCaseWhenList"/> or the
    /// first <see cref="ExprCaseWhen"/>), <c>rightAst</c> is the clause appended at this position.
    /// The evaluator flattens it top-to-bottom so the first matching WHEN wins.
    /// </summary>
    ExprCaseWhenList,

    ExprAlias,
    ExprLike,
    ExprILike,
    ExprRegexMatch,
    ExprRegexMatchCi,
    ExprRegexNotMatch,
    ExprRegexNotMatchCi,
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
    /// <summary>
    /// <c>SET TRANSACTION LOCKING { PESSIMISTIC | OPTIMISTIC }</c>. Selects the Kahuna
    /// coordinator locking strategy for the current transaction. <c>yytext</c> carries the
    /// resolved enum name ("Pessimistic" or "Optimistic"). Must be issued before any data
    /// statement in the transaction, exactly like <see cref="SetTransaction"/>.
    /// </summary>
    SetTransactionLocking,
    SortAsc,
    SortDesc,
    CreateTable,
    CreateTableIfNotExists,
    /// <summary>Recover a dropped table's data under a new name by re-linking to its orphan id (<c>CREATE TABLE x RELINK TO '&lt;id&gt;'</c>).</summary>
    CreateTableRelink,
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
    /// <summary>List tables dropped-but-recoverable in the current database (<c>SHOW ORPHAN TABLES</c>).</summary>
    ShowOrphanTables,
    /// <summary>List root databases dropped-but-recoverable (<c>SHOW ORPHAN DATABASES</c>); resolved from the registry with no open database.</summary>
    ShowOrphanDatabases,
    CreateDatabase,
    CreateDatabaseIfNotExists,
    CreateDatabaseBranch,
    CreateDatabaseBranchIfNotExists,
    /// <summary>Recover a dropped root database's data under a new name by re-linking to its orphan id (<c>CREATE DATABASE x RELINK TO '&lt;id&gt;'</c>).</summary>
    CreateDatabaseRelink,
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
