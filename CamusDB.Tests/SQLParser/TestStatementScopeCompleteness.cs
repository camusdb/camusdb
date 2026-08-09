/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// Guards <see cref="StatementScope.IsSchemaDdl"/> against the one way it fails: someone adds a DDL
/// statement, wires it into the DDL dispatcher, and does not think about this list.
///
/// <para>That is not hypothetical. <c>ALTER VIEW … OWNER TO</c> was added to the DDL dispatcher after
/// the list was written and was never added here, so it stayed the single view statement that
/// answered "Unknown non-query AST stmt" on the endpoint most clients send administrative statements
/// to. A hand-maintained list cannot catch that — it has the same blind spot as the thing it is
/// checking — so this walks the <see cref="NodeType"/> enum instead and requires every
/// DDL-shaped name to be either classified as schema DDL or <b>explicitly</b> excluded below.</para>
///
/// <para>Adding a statement therefore forces a decision. Getting it wrong fails here rather than in
/// production, and the exclusion list makes each "no" a written one rather than an oversight.</para>
/// </summary>
[TestFixture]
public sealed class TestStatementScopeCompleteness
{
    /// <summary>
    /// DDL-shaped node types that are deliberately <b>not</b> schema DDL. Every entry needs a reason,
    /// because an entry added without one is indistinguishable from the bug this fixture exists to
    /// catch.
    /// </summary>
    private static readonly Dictionary<NodeType, string> DeliberatelyExcluded = new()
    {
        // Database- and server-scoped: dispatched before any database is opened, and covered by
        // StatementScope.IsDatabaseScopedMutation instead.
        [NodeType.CreateDatabase] = "database-scoped",
        [NodeType.CreateDatabaseIfNotExists] = "database-scoped",
        [NodeType.CreateDatabaseBranch] = "database-scoped",
        [NodeType.CreateDatabaseBranchIfNotExists] = "database-scoped",
        [NodeType.CreateDatabaseRelink] = "database-scoped",
        [NodeType.DropDatabase] = "database-scoped",
        [NodeType.DropDatabaseIfExists] = "database-scoped",
        [NodeType.CreateUser] = "server-scoped (auth keyspace)",
        [NodeType.CreateUserIfNotExists] = "server-scoped (auth keyspace)",
        [NodeType.AlterUser] = "server-scoped (auth keyspace)",
        [NodeType.DropUser] = "server-scoped (auth keyspace)",
        [NodeType.DropUserIfExists] = "server-scoped (auth keyspace)",

        // Handled by the non-query path in its own right: it writes rows and returns the table it
        // created, so it cannot be forwarded as a no-descriptor DDL statement.
        [NodeType.CreateTableAsSelect] = "writes rows; has its own non-query arm",
        [NodeType.CreateTableAsSelectIfNotExists] = "writes rows; has its own non-query arm",

        // Not statements at all: these are the AST fragments a CREATE TABLE body is built from — the
        // column list, the constraint lists, and each inline constraint kind. They never reach a
        // dispatcher on their own, so classifying them would be meaningless.
        [NodeType.CreateTableItem] = "AST fragment, not a statement",
        [NodeType.CreateTableItemList] = "AST fragment, not a statement",
        [NodeType.CreateTableFieldConstraintList] = "AST fragment, not a statement",
        [NodeType.CreateTableConstraintList] = "AST fragment, not a statement",
        [NodeType.CreateTableConstraintPrimaryKey] = "AST fragment, not a statement",
        [NodeType.CreateTableConstraintMultiIndex] = "AST fragment, not a statement",
        [NodeType.CreateTableConstraintUniqueIndex] = "AST fragment, not a statement",
        [NodeType.CreateTableConstraintCheck] = "AST fragment, not a statement",
    };

    /// <summary>
    /// Names that look like DDL. Deliberately broad — a false positive costs one line in the
    /// exclusion table above, while a false negative is the gap this fixture is for.
    /// </summary>
    private static bool LooksLikeDdl(NodeType nodeType)
    {
        string name = nodeType.ToString();

        // Renaming a database is database-scoped and the COMMENT family is reachable without a DDL
        // transaction, but neither is named Create/Drop/Alter, so neither reaches here.
        return name.StartsWith("Create", StringComparison.Ordinal)
               || name.StartsWith("Drop", StringComparison.Ordinal)
               || name.StartsWith("Alter", StringComparison.Ordinal);
    }

    [Test]
    public void EveryDdlStatementIsEitherSchemaDdlOrExplicitlyExcluded()
    {
        List<string> unclassified = [];

        foreach (NodeType nodeType in Enum.GetValues<NodeType>())
        {
            if (!LooksLikeDdl(nodeType))
                continue;

            if (StatementScope.IsSchemaDdl(nodeType) || DeliberatelyExcluded.ContainsKey(nodeType))
                continue;

            unclassified.Add(nodeType.ToString());
        }

        Assert.IsEmpty(
            unclassified,
            "These DDL statements are neither classified as schema DDL nor explicitly excluded, so the " +
            "non-query endpoint will reject them as unknown while the DDL endpoint accepts them: " +
            string.Join(", ", unclassified) +
            ". Add them to StatementScope.IsSchemaDdl, or to this fixture's exclusion table with a reason.");
    }

    /// <summary>
    /// The two classifications must stay disjoint: a statement dispatched before any database is open
    /// cannot also be one that runs inside a DDL transaction on an open database, and claiming both
    /// would make the transport's routing decision depend on which check it happened to ask first.
    /// </summary>
    [Test]
    public void SchemaDdlAndDatabaseScopedMutationsDoNotOverlap()
    {
        List<string> overlapping = [.. Enum.GetValues<NodeType>()
            .Where(n => StatementScope.IsSchemaDdl(n) && StatementScope.IsDatabaseScopedMutation(n))
            .Select(n => n.ToString())];

        Assert.IsEmpty(overlapping, "Classified as both schema DDL and a database-scoped mutation: " +
            string.Join(", ", overlapping));
    }
}
