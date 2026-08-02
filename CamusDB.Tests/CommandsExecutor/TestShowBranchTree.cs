
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for SHOW BRANCHES FROM and SHOW ANCESTORS FROM through ExecuteSQLQuery.
/// Both statements are server-level (no database context required) and read from the registry.
/// </summary>
internal sealed class TestShowBranchTree : BaseTest
{
    private static string NewName() => "db_" + Guid.NewGuid().ToString("n");

    // Helper: create a database and track it.
    private async Task<(string name, DatabaseDescriptor descriptor)> NewRoot(CommandExecutor executor)
    {
        string name = NewName();
        DatabaseDescriptor descriptor = await executor.CreateDatabase(new CreateDatabaseTicket(name, ifNotExists: false));
        TrackDatabase(name, executor);
        return (name, descriptor);
    }

    // Helper: branch from an existing database.
    private async Task<(string name, DatabaseDescriptor descriptor)> NewBranch(
        CommandExecutor executor, string parentName)
    {
        string name = NewName();
        DatabaseDescriptor descriptor = await executor.CreateDatabase(
            new CreateDatabaseTicket(name, ifNotExists: false, branchFrom: parentName));
        TrackDatabase(name, executor);
        return (name, descriptor);
    }

    // Helper: run a server-level SHOW query (no database context needed).
    private static async Task<List<QueryResultRow>> ShowQuery(CommandExecutor executor, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null));
        return await cursor.ToListAsync();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SHOW ANCESTORS
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A root database (no branch ancestry) returns an empty result set.
    /// </summary>
    [Test]
    public async Task ShowAncestors_RootDatabase_ReturnsZeroRows()
    {
        CommandExecutor executor = CreateCommandExecutor();
        (string rootName, _) = await NewRoot(executor);

        List<QueryResultRow> rows = await ShowQuery(executor, $"SHOW ANCESTORS FROM {rootName}");

        Assert.AreEqual(0, rows.Count, "root has no ancestors — result set must be empty");
    }

    /// <summary>
    /// A direct child returns exactly one ancestor row with depth=1, correct id, and non-empty
    /// fork_timestamp. Negative-proof: swap the depth assertion and confirm the test fails.
    /// </summary>
    [Test]
    public async Task ShowAncestors_DirectChild_ReturnsOneRowDepth1()
    {
        CommandExecutor executor = CreateCommandExecutor();
        (string rootName, DatabaseDescriptor root) = await NewRoot(executor);
        (string childName, _) = await NewBranch(executor, rootName);

        List<QueryResultRow> rows = await ShowQuery(executor, $"SHOW ANCESTORS FROM {childName}");

        Assert.AreEqual(1, rows.Count, "direct child has exactly one ancestor");

        QueryResultRow row = rows[0];
        Assert.AreEqual(rootName, row.Row["database"].StrValue,
            "ancestor database name must equal the parent");
        Assert.AreEqual(root.Id, row.Row["id"].StrValue,
            "ancestor id must equal the parent's stable id");
        Assert.AreEqual(1L, row.Row["depth"].LongValue,
            "depth must be 1 for the immediate parent");
        Assert.IsFalse(string.IsNullOrEmpty(row.Row["fork_timestamp"].StrValue),
            "fork_timestamp must be non-empty");

        // Negative-proof: the depth is exactly 1, not 2.
        Assert.AreNotEqual(2L, row.Row["depth"].LongValue,
            "depth must not be 2 for a direct parent");
    }

    /// <summary>
    /// A grandchild returns two ancestors: depth 1 = parent, depth 2 = grandparent.
    /// Fork timestamps differ between levels.
    /// </summary>
    [Test]
    public async Task ShowAncestors_Grandchild_ReturnsTwoRowsCorrectDepths()
    {
        CommandExecutor executor = CreateCommandExecutor();
        (string rootName, DatabaseDescriptor root) = await NewRoot(executor);
        (string childName, DatabaseDescriptor child) = await NewBranch(executor, rootName);
        (string grandchildName, _) = await NewBranch(executor, childName);

        List<QueryResultRow> rows = await ShowQuery(executor, $"SHOW ANCESTORS FROM {grandchildName}");

        Assert.AreEqual(2, rows.Count, "grandchild must have exactly 2 ancestors");

        // Nearest parent first (depth 1).
        Assert.AreEqual(childName, rows[0].Row["database"].StrValue);
        Assert.AreEqual(child.Id, rows[0].Row["id"].StrValue);
        Assert.AreEqual(1L, rows[0].Row["depth"].LongValue);

        // Grandparent (depth 2).
        Assert.AreEqual(rootName, rows[1].Row["database"].StrValue);
        Assert.AreEqual(root.Id, rows[1].Row["id"].StrValue);
        Assert.AreEqual(2L, rows[1].Row["depth"].LongValue);

        // Negative-proof: if depth were computed incorrectly (e.g. always 1), the second row would fail.
        Assert.AreNotEqual(1L, rows[1].Row["depth"].LongValue,
            "grandparent must have depth 2, not 1");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SHOW BRANCHES
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A leaf database (no descendants) returns an empty result set.
    /// </summary>
    [Test]
    public async Task ShowBranches_LeafDatabase_ReturnsZeroRows()
    {
        CommandExecutor executor = CreateCommandExecutor();
        (string rootName, _) = await NewRoot(executor);
        (string childName, _) = await NewBranch(executor, rootName);

        List<QueryResultRow> rows = await ShowQuery(executor, $"SHOW BRANCHES FROM {childName}");

        Assert.AreEqual(0, rows.Count, "leaf has no descendants — result set must be empty");
    }

    /// <summary>
    /// A root with one direct child returns exactly one row with depth=1, correct parent name,
    /// and non-empty fork_timestamp.
    /// </summary>
    [Test]
    public async Task ShowBranches_Root_ReturnsSingleChild()
    {
        CommandExecutor executor = CreateCommandExecutor();
        (string rootName, _) = await NewRoot(executor);
        (string childName, DatabaseDescriptor child) = await NewBranch(executor, rootName);

        List<QueryResultRow> rows = await ShowQuery(executor, $"SHOW BRANCHES FROM {rootName}");

        Assert.AreEqual(1, rows.Count, "root with one child must return exactly one row");

        QueryResultRow row = rows[0];
        Assert.AreEqual(childName, row.Row["database"].StrValue);
        Assert.AreEqual(child.Id, row.Row["id"].StrValue);
        Assert.AreEqual(1L, row.Row["depth"].LongValue);
        Assert.AreEqual(rootName, row.Row["parent"].StrValue,
            "parent column must name the direct parent");
        Assert.IsFalse(string.IsNullOrEmpty(row.Row["fork_timestamp"].StrValue));
    }

    /// <summary>
    /// A three-level tree: root → child → grandchild. SHOW BRANCHES FROM root returns both
    /// descendants. SHOW BRANCHES FROM a mid-tree node returns only its own subtree (not the root).
    /// Rows are ordered depth-ascending then name-ascending.
    /// </summary>
    [Test]
    public async Task ShowBranches_ThreeLevelTree_CorrectDepthsAndScope()
    {
        CommandExecutor executor = CreateCommandExecutor();
        (string rootName, _) = await NewRoot(executor);
        (string childName, _) = await NewBranch(executor, rootName);
        (string grandchildName, _) = await NewBranch(executor, childName);

        // From root: 2 descendants.
        List<QueryResultRow> fromRoot = await ShowQuery(executor, $"SHOW BRANCHES FROM {rootName}");
        Assert.AreEqual(2, fromRoot.Count, "root must see both child and grandchild");

        // Depth ordering: child first (depth 1), grandchild second (depth 2).
        Assert.AreEqual(childName, fromRoot[0].Row["database"].StrValue);
        Assert.AreEqual(1L, fromRoot[0].Row["depth"].LongValue);
        Assert.AreEqual(grandchildName, fromRoot[1].Row["database"].StrValue);
        Assert.AreEqual(2L, fromRoot[1].Row["depth"].LongValue);

        // Parent column at each depth.
        Assert.AreEqual(rootName, fromRoot[0].Row["parent"].StrValue);
        Assert.AreEqual(childName, fromRoot[1].Row["parent"].StrValue);

        // From mid-tree (child): only the grandchild is visible (child not included).
        List<QueryResultRow> fromChild = await ShowQuery(executor, $"SHOW BRANCHES FROM {childName}");
        Assert.AreEqual(1, fromChild.Count, "mid-tree node sees only its own subtree");
        Assert.AreEqual(grandchildName, fromChild[0].Row["database"].StrValue);
        Assert.AreEqual(1L, fromChild[0].Row["depth"].LongValue,
            "grandchild is at depth 1 relative to the mid-tree node");

        // Negative-proof: if the ancestry filter were wrong (e.g. always included the root
        // among the branches), fromChild would incorrectly contain 2 rows.
        Assert.AreNotEqual(2, fromChild.Count,
            "mid-tree scope must not include the root or unrelated siblings");
    }

    /// <summary>
    /// Multiple children of the same parent all appear in SHOW BRANCHES, ordered by name.
    /// </summary>
    [Test]
    public async Task ShowBranches_MultipleChildren_AllReturnedOrderedByName()
    {
        CommandExecutor executor = CreateCommandExecutor();
        (string rootName, _) = await NewRoot(executor);
        (string b1, _) = await NewBranch(executor, rootName);
        (string b2, _) = await NewBranch(executor, rootName);
        (string b3, _) = await NewBranch(executor, rootName);

        List<QueryResultRow> rows = await ShowQuery(executor, $"SHOW BRANCHES FROM {rootName}");

        Assert.AreEqual(3, rows.Count, "root with 3 children must return 3 rows");

        // All rows must be at depth 1.
        Assert.IsTrue(rows.All(r => r.Row["depth"].LongValue == 1L));

        // Names must be in ascending order.
        List<string> returnedNames = rows.Select(r => r.Row["database"].StrValue!).ToList();
        List<string> expected = new[] { b1, b2, b3 }.OrderBy(n => n, StringComparer.Ordinal).ToList();
        CollectionAssert.AreEqual(expected, returnedNames,
            "sibling branches must be ordered by database name ascending");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Error paths
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// SHOW BRANCHES FROM with an unknown name throws a database-not-found error.
    /// </summary>
    [Test]
    public void ShowBranches_UnknownDatabase_Throws()
    {
        Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            CommandExecutor executor = CreateCommandExecutor();
            await ShowQuery(executor, "SHOW BRANCHES FROM db_does_not_exist_xyzzy");
        });
    }

    /// <summary>
    /// SHOW ANCESTORS FROM with an unknown name throws a database-not-found error.
    /// </summary>
    [Test]
    public void ShowAncestors_UnknownDatabase_Throws()
    {
        Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            CommandExecutor executor = CreateCommandExecutor();
            await ShowQuery(executor, "SHOW ANCESTORS FROM db_does_not_exist_xyzzy");
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cross-node visibility
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A descendant registered through a *different* registry instance (same shared node,
    /// independent cache — as if created on another cluster node) must still appear in
    /// SHOW BRANCHES, because the statement enumerates the persistent registry via
    /// ScanAllEntriesAsync rather than the querying node's local cache. The precondition asserts
    /// the branch is genuinely absent from that cache, so a cache-only implementation would miss it.
    /// </summary>
    [Test]
    public async Task ShowBranches_DescendantRegisteredCrossNode_IsVisible()
    {
        CommandExecutor executor = CreateCommandExecutor();
        (string rootName, DatabaseDescriptor root) = await NewRoot(executor);

        // Register a branch of root through a second registry instance ("remote node"): its write
        // lands in the shared KV but not in this executor's registry cache.
        await using DatabaseRegistry remoteRegistry = await DatabaseRegistry.OpenAsync(TestNode!, Options);
        string branchName = NewName();
        string branchId = await remoteRegistry.AllocateIdAsync();
        List<DatabaseBranchAncestor> ancestors =
        [
            new DatabaseBranchAncestor { DatabaseId = root.Id, ForkTimestamp = new HLCTimestamp(1, 0, 0) }
        ];
        await remoteRegistry.RegisterAsync(branchName, branchId, ancestors);

        // Precondition: the querying executor's own registry cache does not know this branch.
        Assert.IsNull(sharedRegistry!.Get(branchName),
            "cross-node branch must be absent from this node's registry cache");

        // SHOW BRANCHES reads the persistent registry, so it must still see the remote branch.
        List<QueryResultRow> rows = await ShowQuery(executor, $"SHOW BRANCHES FROM {rootName}");

        Assert.AreEqual(1, rows.Count, "cross-node descendant must be visible via the persistent scan");
        Assert.AreEqual(branchName, rows[0].Row["database"].StrValue);
        Assert.AreEqual(branchId, rows[0].Row["id"].StrValue);
        Assert.AreEqual(1L, rows[0].Row["depth"].LongValue);
        Assert.AreEqual(rootName, rows[0].Row["parent"].StrValue,
            "parent name must resolve for a cross-node branch");
    }
}
