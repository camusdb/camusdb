
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

    /// <summary>
    /// An <c>X'4D5A'</c> hex-string literal, carrying its source token in <c>yytext</c>. Distinct
    /// from <see cref="String"/> because the type is recoverable from the literal itself: a bytes
    /// value written as a string would depend on String→Bytes coercion at the destination, which
    /// only works where a target column type is known.
    /// </summary>
    BytesLiteral,

    /// <summary>
    /// An <c>ARRAY[…]</c> literal. <c>leftAst</c> is the element list — an
    /// <see cref="ExprList"/> tree for two or more elements, the element node itself for exactly one,
    /// and <c>null</c> for the empty <c>ARRAY[]</c>, whose element type can only come from the target
    /// column.
    /// </summary>
    ArrayLiteral,
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

    /// <summary>
    /// <c>x IS TRUE</c>. Unlike <c>x = TRUE</c> this never yields unknown: a NULL operand makes it
    /// FALSE. The two forms still select the same rows in a WHERE clause, because unknown and false
    /// are both non-matching there.
    /// </summary>
    ExprIsTrue,

    /// <summary>
    /// <c>x IS NOT TRUE</c> — the exact negation of <see cref="ExprIsTrue"/>, so it matches FALSE
    /// <em>and NULL</em>. It is therefore NOT equivalent to <c>x = FALSE</c> unless the operand is
    /// known to be non-nullable.
    /// </summary>
    ExprIsNotTrue,

    /// <summary><c>x IS FALSE</c>. NULL operand yields FALSE rather than unknown.</summary>
    ExprIsFalse,

    /// <summary>
    /// <c>x IS NOT FALSE</c> — matches TRUE <em>and NULL</em>, so it is not equivalent to
    /// <c>x = TRUE</c> unless the operand is known to be non-nullable.
    /// </summary>
    ExprIsNotFalse,
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
    /// <c>ALTER TABLE t SET (key = bool, ...)</c> — table storage-parameter settings.
    /// <c>leftAst</c> = table name node; <c>rightAst</c> = a chain of <see cref="UpdateList"/> /
    /// <see cref="UpdateItem"/> nodes, each item's <c>leftAst</c> the setting key and <c>rightAst</c>
    /// a <see cref="Bool"/> leaf. Currently the only recognized key is
    /// <c>sql_stats_automatic_collection_enabled</c> (per-table auto-analyze opt-out).
    /// </summary>
    AlterTableSetSetting,
    /// <summary>
    /// Column-level <c>CONSTRAINT name NOT NULL</c> written inline on a column definition.
    /// <c>yytext</c> = the user-supplied constraint name.
    /// </summary>
    ConstraintNotNullNamed,
    /// <summary>
    /// Column-level <c>COMMENT '&lt;text&gt;'</c> written inline on a column definition in
    /// <c>CREATE TABLE</c> or <c>ALTER TABLE … ADD COLUMN</c>. <c>leftAst</c> = a
    /// <see cref="String"/> leaf whose <c>yytext</c> still carries the surrounding single quotes;
    /// the ticket creator strips them and un-doubles embedded quotes. There is no inline form for
    /// removing a comment — removal is <c>COMMENT ON … IS NULL</c>.
    /// </summary>
    ConstraintComment,
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
    /// <summary>
    /// <c>COMMENT ON TABLE t IS '&lt;text&gt;' | NULL</c>. <c>leftAst</c> = table name node;
    /// <c>rightAst</c> = a <see cref="String"/> leaf carrying the still-quoted literal, or
    /// <b>null</b> for <c>IS NULL</c>. That null-vs-empty-literal distinction is the whole
    /// encoding: <c>IS NULL</c> removes the comment, <c>IS ''</c> stores an empty string.
    /// </summary>
    CommentOnTable,
    /// <summary>
    /// <c>COMMENT ON COLUMN t.c IS '&lt;text&gt;' | NULL</c>. <c>leftAst</c> = a single
    /// <see cref="Identifier"/> whose <c>yytext</c> is the dotted <c>table.column</c> text (the
    /// grammar folds qualified names into one node); the ticket creator splits it and rejects the
    /// unqualified form. <c>rightAst</c> follows the same null-vs-literal encoding as
    /// <see cref="CommentOnTable"/>.
    /// </summary>
    CommentOnColumn,
    /// <summary>
    /// <c>COMMENT ON INDEX t.i IS '&lt;text&gt;' | NULL</c>. Indexes are per-table in CamusDB — there
    /// is no global index namespace to resolve a bare name against — so the table-qualified form is
    /// required. Node layout matches <see cref="CommentOnColumn"/>.
    /// </summary>
    CommentOnIndex,
    /// <summary>
    /// <c>COMMENT ON DATABASE d IS '&lt;text&gt;' | NULL</c>. <c>leftAst</c> = database name node;
    /// <c>rightAst</c> follows the null-vs-literal encoding of <see cref="CommentOnTable"/>. Handled
    /// before any database is opened — the comment lives on the cross-database registry entry, not in
    /// the per-database schema log.
    /// </summary>
    CommentOnDatabase,

    /// <summary>
    /// <c>CREATE USER u [IDENTIFIED [WITH plugin] BY secret]</c>. <c>leftAst</c> = user name node;
    /// <c>rightAst</c> = the password value (<see cref="String"/> or <see cref="Placeholder"/>), or
    /// null when no password clause was given (user cannot authenticate); <c>extendedOne</c> = the
    /// plugin identifier node, or null when <c>IDENTIFIED BY</c> defaulted it. Server-level: dispatched
    /// before any database is opened.
    /// </summary>
    CreateUser,

    /// <summary><c>CREATE USER IF NOT EXISTS …</c>. Node layout matches <see cref="CreateUser"/>; a
    /// pre-existing user is a no-op instead of an error.</summary>
    CreateUserIfNotExists,

    /// <summary>
    /// <c>ALTER USER u IDENTIFIED [WITH plugin] BY secret</c> — rotate the password. Node layout
    /// matches <see cref="CreateUser"/> except <c>rightAst</c> is always present (a password clause is
    /// required).
    /// </summary>
    AlterUser,

    /// <summary><c>DROP USER u</c>. <c>leftAst</c> = user name node. Removes the user and all its
    /// grants in one catalog transaction.</summary>
    DropUser,

    /// <summary><c>DROP USER IF EXISTS u</c>. Node layout matches <see cref="DropUser"/>; an unknown
    /// user is a no-op instead of an error.</summary>
    DropUserIfExists,

    /// <summary>
    /// <c>GRANT priv_list ON object TO user</c>. <c>leftAst</c> = a privilege-list chain
    /// (<see cref="GrantPrivilegeList"/> / <see cref="GrantPrivilege"/>); <c>rightAst</c> = user name
    /// node; <c>extendedOne</c> = the object identifier node (database name, or dotted <c>db.table</c>),
    /// null for global; <c>yytext</c> = the scope kind marker (<c>"global"</c> / <c>"database"</c> /
    /// <c>"table"</c>). Server-level.
    /// </summary>
    Grant,

    /// <summary><c>REVOKE priv_list ON object FROM user</c>. Node layout matches <see cref="Grant"/>.</summary>
    Revoke,

    /// <summary>Leaf carrying one privilege token in <c>yytext</c> (e.g. <c>"select"</c>,
    /// <c>"create table"</c>, <c>"all"</c>). Built by the privilege grammar rule.</summary>
    GrantPrivilege,

    /// <summary>Left-recursive cons node joining a privilege list: <c>leftAst</c> = the list so far,
    /// <c>rightAst</c> = the next <see cref="GrantPrivilege"/>.</summary>
    GrantPrivilegeList,

    /// <summary>
    /// <c>SHOW GRANTS [FOR user]</c>. <c>leftAst</c> = the target user name node, or null for the
    /// current authenticated principal. Server-level query — dispatched before any database is opened.
    /// </summary>
    ShowGrants,

    /// <summary>
    /// <c>SHOW ENGINE STATS [LIKE 'pattern']</c>. <c>leftAst</c> = the LIKE pattern node, or null for
    /// no filter. Reports the local process's embedded Kommander/Kahuna metrics, so it is dispatched
    /// before any database is opened and never forwards to the leader.
    /// </summary>
    ShowEngineStats,
}
