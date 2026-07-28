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
        NodeType.Grant or NodeType.Revoke;

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
            NodeType.ShowGrants;
}
