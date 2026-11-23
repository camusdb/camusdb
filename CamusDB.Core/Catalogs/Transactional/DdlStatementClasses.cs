
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.Catalogs.Transactional;

/// <summary>
/// How a schema statement behaves inside an explicit transaction when transactional DDL is on.
/// </summary>
internal enum DdlStatementClass
{
    /// <summary>
    /// Changes metadata only. Inside a transaction it becomes part of the transaction's change set: the
    /// statements after it see the change, <c>COMMIT</c> publishes it and <c>ROLLBACK</c> drops it.
    /// </summary>
    MetadataOnly,

    /// <summary>
    /// Reads every row of its table (an index build, a column fill, a constraint validation). Inside a
    /// transaction it runs on a table the transaction created, over the transaction's own rows. On any
    /// other table it runs the staged form: the online steps on the shared table, owned by the
    /// transaction, which commits or removes them with the transaction.
    /// </summary>
    Staged,

    /// <summary>
    /// Commits work that a later <c>ROLLBACK</c> cannot undo. Refused inside an explicit transaction
    /// with <see cref="CamusDBErrorCodes.StatementNotAllowedInTransaction"/>.
    /// </summary>
    Irrevocable,

    /// <summary>
    /// A schema statement whose transactional form is not built yet. Refused inside an explicit
    /// transaction when transactional DDL is on, so it never commits apart from the transaction while
    /// the client expects it to roll back with it.
    /// </summary>
    NotYetTransactional
}

/// <summary>
/// The class of every schema statement the DDL dispatcher runs against an open database. A test fails
/// when a node type that <see cref="StatementScope.IsSchemaDdl"/> accepts, or that the dispatcher
/// handles, has no class here. Statements about the server or a database as a whole (CREATE DATABASE,
/// users, grants, cluster settings) are dispatched before a database is opened and keep their
/// behavior; they are not listed.
/// </summary>
internal static class DdlStatementClasses
{
    /// <summary>
    /// The class of <paramref name="nodeType"/>, or null when it is not a schema statement on an open
    /// database.
    /// </summary>
    internal static DdlStatementClass? ClassOf(NodeType nodeType) => nodeType switch
    {
        NodeType.CreateTable or NodeType.CreateTableIfNotExists or
        NodeType.DropTable or NodeType.DropTableIfExists or
        NodeType.AlterTableDropColumn or
        NodeType.AlterTableRenameTo or NodeType.AlterTableRenameColumn or NodeType.AlterTableRenameIndex or
        NodeType.AlterTableDropIndex or NodeType.AlterTableDropIndexIfExists or NodeType.AlterTableDropPrimaryKey or
        NodeType.AlterTableSetSetting or NodeType.AlterTableResetSetting or
        NodeType.CommentOnTable or NodeType.CommentOnColumn or NodeType.CommentOnIndex or
        NodeType.CreateView or NodeType.CreateOrReplaceView or
        NodeType.DropView or NodeType.DropViewIfExists or
        NodeType.AlterViewRenameTo
            => DdlStatementClass.MetadataOnly,

        NodeType.AlterTableAddColumn or
        NodeType.AlterTableAddIndex or NodeType.AlterTableAddIndexIfNotExists or
        NodeType.AlterTableAddUniqueIndex or NodeType.AlterTableAddUniqueIndexIfNotExists or
        NodeType.AlterTableAddPrimaryKey or
        NodeType.AlterTableAddConstraintCheck or NodeType.AlterTableAddConstraintForeignKey or
        NodeType.AlterTableSetNotNull
            => DdlStatementClass.Staged,

        NodeType.TruncateTable or
        NodeType.AlterTableRewriteStorage or
        NodeType.RefreshMaterializedView or
        NodeType.CreateMaterializedView or NodeType.CreateMaterializedViewIfNotExists
            => DdlStatementClass.Irrevocable,

        NodeType.CreateTableRelink or
        NodeType.CreateTableAsSelect or NodeType.CreateTableAsSelectIfNotExists or
        NodeType.AlterTableDropConstraint or
        NodeType.AlterTableDropNotNull or
        NodeType.AlterTableSetColumnStorage or
        NodeType.AlterViewOwnerTo or
        NodeType.CommentOnView or NodeType.CommentOnMaterializedView or NodeType.CommentOnSequence or
        NodeType.DropMaterializedView or NodeType.DropMaterializedViewIfExists or
        NodeType.AlterMaterializedViewRenameTo or
        NodeType.CreateSequence or NodeType.CreateSequenceIfNotExists or
        NodeType.DropSequence or NodeType.DropSequenceIfExists or
        NodeType.AlterSequence or NodeType.AlterSequenceRenameTo or
        NodeType.CreateTrigger or NodeType.DropTrigger or
        NodeType.CreateExtension or NodeType.AlterExtensionUpdate or NodeType.DropExtension or
        NodeType.CreateFunction or NodeType.CreateOrReplaceFunction or NodeType.DropFunction or NodeType.AlterFunctionOwnerTo
            => DdlStatementClass.NotYetTransactional,

        _ => null
    };
}
