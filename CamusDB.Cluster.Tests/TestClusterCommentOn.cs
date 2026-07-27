/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Tests.Cluster;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Cluster coverage for <c>COMMENT ON</c>: the replicated schema-log path.
///
/// <para>These exist because the standalone path proves nothing about replication — comments ride a
/// <c>SchemaOp.SetComment</c> delta, so what matters here is that every node applies it, that the
/// database schema version advances, that a re-delivered entry is a harmless no-op, and that a
/// follower-issued statement reaches the schema leader through the DDL forwarder.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestClusterCommentOn
{
    private static async Task<string> CreateRobotsTableAsync(InProcessSchemaCluster cluster)
    {
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns:
                [
                    new ColumnInfo("id",   ColumnType.Id),
                    new ColumnInfo("name", ColumnType.String, notNull: true),
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)]),
                    new ConstraintInfo(ConstraintType.IndexMulti, "name_idx",
                        [new ColumnIndexInfo("name", OrderType.Ascending)]),
                ],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        return db;
    }

    [Test]
    public async Task CommentOnTableColumnAndIndex_ConvergesOnAllNodes()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = await CreateRobotsTableAsync(cluster);

        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.Comment(new CommentTicket(CommentTarget.Table, db, "robots", null, "All robots"));
            await leader.Executor.Comment(new CommentTicket(CommentTarget.Column, db, "robots", "name", "Robot name"));
            await leader.Executor.Comment(new CommentTicket(CommentTarget.Index, db, "robots", "name_idx", "Lookup by name"));
            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(20));

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableSchema table = node.Database!.Schema.Tables["robots"];

            Assert.AreEqual("All robots", table.Comment, $"table comment on node {node.Index}");
            Assert.AreEqual("Robot name", table.Columns!.Single(c => c.Name == "name").Comment,
                $"column comment on node {node.Index}");
            Assert.AreEqual("Lookup by name", table.Indexes!.Single(i => i.Name == "name_idx").Comment,
                $"index comment on node {node.Index}");
        }
    }

    [Test]
    public async Task CommentAdvancesSchemaVersionButNotTableVersion()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = await CreateRobotsTableAsync(cluster);

        long schemaVersionBefore = 0;
        int tableVersionBefore = 0;
        long schemaVersionAfter = 0;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            schemaVersionBefore = leader.Database!.Schema.SchemaVersion;
            tableVersionBefore = leader.Database!.Schema.Tables["robots"].Version;

            await leader.Executor.Comment(new CommentTicket(CommentTarget.Table, db, "robots", null, "documented"));

            schemaVersionAfter = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, schemaVersionAfter, timeout: TimeSpan.FromSeconds(20));

        Assert.AreEqual(schemaVersionBefore + 1, schemaVersionAfter,
            "a comment change must advance the database schema version");

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            // Comments do not affect row encoding, so the column-layout version must not move —
            // bumping it would force every stored row to be re-decoded against a new layout.
            Assert.AreEqual(tableVersionBefore, node.Database!.Schema.Tables["robots"].Version,
                $"TableSchema.Version must not change on node {node.Index}");
        }
    }

    [Test]
    public async Task RemovingAndReAddingACommentConverges()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = await CreateRobotsTableAsync(cluster);

        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.Comment(new CommentTicket(CommentTarget.Column, db, "robots", "name", "first"));
            // null removes; "" is a present-but-empty comment. Both must replicate distinctly.
            await leader.Executor.Comment(new CommentTicket(CommentTarget.Column, db, "robots", "name", null));
            await leader.Executor.Comment(new CommentTicket(CommentTarget.Column, db, "robots", "name", ""));
            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(20));

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableColumnSchema column = node.Database!.Schema.Tables["robots"].Columns!.Single(c => c.Name == "name");
            Assert.AreEqual("", column.Comment,
                $"node {node.Index} must hold an empty comment, not null — IS '' differs from IS NULL");
        }
    }

    /// <summary>
    /// Two separate statements with the same text. This is <b>not</b> a replay test — each call
    /// proposes its own delta at its own version — it only asserts that re-stating a comment is not
    /// rejected. Genuine redelivery of one entry is covered by
    /// <c>TestCommentOnHardening.ReapplyingTheSameSetCommentDeltaIsANoOp</c>, which re-applies a
    /// single <c>SchemaChangeLogEntry</c>.
    /// </summary>
    [Test]
    public async Task RestatingTheSameCommentIsAccepted()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = await CreateRobotsTableAsync(cluster);

        long version = 0;
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            // Two distinct deltas carrying identical text — the apply must overwrite rather than
            // reject a value that is already set.
            await leader.Executor.Comment(new CommentTicket(CommentTarget.Table, db, "robots", null, "same"));
            await leader.Executor.Comment(new CommentTicket(CommentTarget.Table, db, "robots", null, "same"));
            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(20));

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.AreEqual("same", node.Database!.Schema.Tables["robots"].Comment, $"node {node.Index}");
    }

    // ── Database comments across nodes ──────────────────────────────────────

    /// <summary>
    /// A database created on one node must be commentable from another, whose registry cache
    /// predates the create.
    ///
    /// <para>Consulting only the local <c>byName</c> under a process-local semaphore reports
    /// <c>DatabaseDoesntExist</c> for a database the authoritative registry plainly contains — the
    /// semaphore says nothing about what another node has done. Reconciling against the shared
    /// generation first is what makes this resolve, since the remote create advanced it.</para>
    /// </summary>
    [Test]
    public async Task DatabaseCommentSeesADatabaseCreatedOnAnotherNode()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);

        string db = "db" + Guid.NewGuid().ToString("n");
        await cluster.Nodes[0].Executor.CreateDatabase(new CreateDatabaseTicket(db, ifNotExists: false));

        // A different node, with its own registry instance and its own cache.
        await cluster.Nodes[1].Executor.Comment(
            new CommentTicket(CommentTarget.Database, db, null, null, "written from another node"));

        // Visible from a third node, which must also reconcile rather than trust its stale cache.
        DatabaseRegistryEntry? seen = await cluster.Nodes[2].Executor.ResolveRegistryEntryForTestingAsync(db);
        Assert.AreEqual("written from another node", seen?.Comment);
    }

    /// <summary>
    /// Commenting a database that another node has dropped must fail — and, critically, must not
    /// bring the dropped name back.
    ///
    /// <para>An unconditional <c>Set</c> on a name this node merely had cached rewrites the
    /// <c>db:{name}</c> key another node just deleted, resurrecting a database that no longer exists.
    /// The reconcile evicts the stale name before the read, and the write is a compare-and-set on the
    /// database id as a second line of defence.</para>
    /// </summary>
    [Test]
    public async Task DatabaseCommentDoesNotResurrectADatabaseDroppedOnAnotherNode()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);

        string db = "db" + Guid.NewGuid().ToString("n");
        await cluster.Nodes[0].Executor.CreateDatabase(new CreateDatabaseTicket(db, ifNotExists: false));

        // Warm node 1's cache so it genuinely believes the database exists.
        Assert.IsNotNull(await cluster.Nodes[1].Executor.ResolveRegistryEntryForTestingAsync(db));

        await cluster.Nodes[0].Executor.DropDatabase(new DropDatabaseTicket(db, ifExists: false, force: true));

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await cluster.Nodes[1].Executor.Comment(
                new CommentTicket(CommentTarget.Database, db, null, null, "should not apply")))!;

        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex.Code);

        // The name must stay gone on every node — including the one that tried to write it.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.IsNull(await node.Executor.ResolveRegistryEntryForTestingAsync(db),
                $"node {node.Index} resurrected the dropped database");
    }

    [Test]
    public async Task CommentIssuedOnAFollowerIsForwardedToTheLeader()
    {
        // The in-process leader forwarder stands in for HttpSchemaDdlForwarder; without it a
        // follower rejects the statement instead of relaying it.
        await using InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, wireLeaderForwarder: true);
        string db = await CreateRobotsTableAsync(cluster);

        InProcessSchemaCluster.Node leaderNode = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leaderNode.Index);

        await follower.Executor.Comment(new CommentTicket(CommentTarget.Table, db, "robots", null, "set from a follower"));

        long version = (await cluster.WaitForSchemaLeaderNodeAsync(db)).Database!.Schema.SchemaVersion;
        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(20));

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.AreEqual("set from a follower", node.Database!.Schema.Tables["robots"].Comment,
                $"node {node.Index} must observe the forwarded comment");
    }
}
