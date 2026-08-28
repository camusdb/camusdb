/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Classifies a parsed statement by the context it needs, for the transport layers that must decide
/// whether to resolve a database and open a transaction before handing the statement to
/// <c>CommandExecutor</c>.
///
/// <para>This exists because that decision used to be an inline <c>nodeType is … or …</c> list
/// copy-pasted into each entry point (REST DDL, REST non-query, gRPC DDL, and the ticket validator).
/// Adding a statement meant remembering every copy, and forgetting one failed in a particularly bad
/// way: the transport opened a transaction for a statement that returns no descriptor, then passed
/// that null descriptor to <c>CommitAsync</c> — surfacing a <c>NullReferenceException</c> to the
/// caller <em>after</em> the mutation had already committed. One list, one place.</para>
/// </summary>
public static class StatementScope
{
    /// <summary>
    /// True for statements <c>CommandExecutor</c> dispatches <b>before</b> opening a database: they
    /// name their target inside the SQL, touch only the cross-database registry, and return no
    /// <c>DatabaseDescriptor</c>.
    ///
    /// <para>A transport must not begin a transaction for these, and must not pass the returned
    /// (default, descriptor-less) result to a commit.</para>
    /// </summary>
    public static bool IsDatabaseScopedMutation(NodeType nodeType) => nodeType is
        NodeType.CreateDatabase or NodeType.CreateDatabaseIfNotExists or
        NodeType.CreateDatabaseBranch or NodeType.CreateDatabaseBranchIfNotExists or
        NodeType.CreateDatabaseRelink or
        NodeType.DropDatabase or NodeType.DropDatabaseIfExists or
        NodeType.RenameDatabase or
        NodeType.CommentOnDatabase or
        // Users and grants are server-level: they name their target in the SQL, touch only the shared
        // _system/auth keyspace, and return no DatabaseDescriptor — so no transport opens a database
        // or a transaction for them.
        NodeType.CreateUser or NodeType.CreateUserIfNotExists or
        NodeType.AlterUser or
        NodeType.DropUser or NodeType.DropUserIfExists or
        NodeType.Grant or NodeType.Revoke or
        // Cluster settings are server-level: they name their key in the SQL, touch only the shared
        // _system/settings keyspace (via the replicated settings log), and return no descriptor.
        NodeType.SetClusterSetting or NodeType.ResetClusterSetting;

    /// <summary>
    /// True for schema DDL: statements that change what objects a database contains, run inside their
    /// own DDL transaction, and return no rows.
    ///
    /// <para>Its purpose is to let the <b>non-query</b> entry point accept them. A client routes every
    /// non-SELECT statement to whichever endpoint it uses for those, so DDL arrives there as readily
    /// as at the DDL endpoint — and a statement that works through one and reports "unknown statement"
    /// through the other is, to that client, indistinguishable from one the server does not support.
    /// The non-query path recognizes them here and hands them to the same implementation rather than
    /// carrying a second copy of it.</para>
    ///
    /// <para>Deliberately excluded: <c>CREATE TABLE … AS SELECT</c> and the <c>COMMENT ON</c> family,
    /// which the non-query path already handles itself — the first because it writes rows and returns
    /// the table it created, the second because it is reachable without a DDL transaction. Also
    /// excluded is <c>REFRESH MATERIALIZED VIEW</c>, which is not DDL at all: it replaces a relation's
    /// contents and reports a row count.</para>
    /// </summary>
    public static bool IsSchemaDdl(NodeType nodeType) => nodeType is
        NodeType.CreateTable or NodeType.CreateTableIfNotExists or NodeType.CreateTableRelink or
        NodeType.DropTable or NodeType.DropTableIfExists or
        NodeType.TruncateTable or
        NodeType.AlterTableAddColumn or NodeType.AlterTableDropColumn or
        NodeType.AlterTableAddIndex or NodeType.AlterTableAddIndexIfNotExists or
        NodeType.AlterTableAddUniqueIndex or NodeType.AlterTableAddUniqueIndexIfNotExists or
        NodeType.AlterTableDropIndex or
        NodeType.AlterTableAddPrimaryKey or NodeType.AlterTableDropPrimaryKey or
        NodeType.AlterTableRenameTo or NodeType.AlterTableRenameColumn or NodeType.AlterTableRenameIndex or
        NodeType.AlterTableAddConstraintCheck or NodeType.AlterTableDropConstraint or
        NodeType.AlterTableSetNotNull or NodeType.AlterTableDropNotNull or
        NodeType.AlterTableSetSetting or NodeType.AlterTableResetSetting or
        NodeType.CreateView or NodeType.CreateOrReplaceView or
        NodeType.DropView or NodeType.DropViewIfExists or
        NodeType.AlterViewRenameTo or NodeType.AlterViewOwnerTo or
        NodeType.CreateMaterializedView or NodeType.CreateMaterializedViewIfNotExists or
        NodeType.DropMaterializedView or NodeType.DropMaterializedViewIfExists or
        NodeType.AlterMaterializedViewRenameTo;

    /// <summary>
    /// True for statements that are valid without a context database — every database-scoped
    /// mutation above, plus the server-level introspection statements, which resolve their own
    /// target (or none at all) rather than reading the current database's schema.
    /// </summary>
    public static bool AllowsEmptyContextDatabase(NodeType nodeType) =>
        IsDatabaseScopedMutation(nodeType) ||
        nodeType is
            NodeType.ShowDatabases or
            NodeType.ShowBranches or
            NodeType.ShowAncestors or
            NodeType.ShowOrphanDatabases or
            NodeType.ShowGrants or
            // Per-process metrics; there is no database to read them from.
            NodeType.ShowEngineStats or
            // Per-process configuration; likewise not read from any database.
            NodeType.ShowVariables or
            // This process's own slow query log; not read from any database either.
            NodeType.ShowSlowQueries or
            // The cluster-wide settings overlay; not read from any database either.
            NodeType.ShowClusterSettings;
}
