/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging.Abstractions;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Tests.Cluster;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end cluster tests for the cluster ADD COLUMN path (ExecuteClusterAddColumnAsync).
///
/// These tests exercise the full command-executor path — not just the coordinator directly —
/// so they cover the semaphore wrap, the backfill firing before Public, existing rows
/// receiving the default, and schema convergence on all nodes.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestClusterAddColumn
{
    // ── Helpers ──────────────────────────────────────────────────────────────

    private static async Task InsertRowsAsync(InProcessSchemaCluster cluster, string db, int count)
    {
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            for (int i = 0; i < count; i++)
            {
                KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.Insert(new InsertTicket(
                    txnState: tx,
                    databaseName: db,
                    tableName: "robots",
                    values:
                    [
                        new Dictionary<string, ColumnValue>
                        {
                            ["id"]   = new(ColumnType.Id, $"0000000000000000000{i + 1:D5}"),
                            ["name"] = new(ColumnType.String, $"robot-{i}"),
                        }
                    ]
                ));
                await leader.Database!.Transactions.CommitAsync(tx);
            }
        });
    }

    private static async Task<List<IReadOnlyDictionary<string, ColumnValue>>> QueryAllRowsAsync(
        InProcessSchemaCluster.Node node,
        string db
    )
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.Query(
            new QueryTicket(
                txnState: tx,
                databaseName: db,
                tableName: "robots",
                index: null,
                projection: null,
                filters: null,
                where: null,
                orderBy: null,
                limit: null,
                offset: null,
                parameters: null
            )
        );

        List<IReadOnlyDictionary<string, ColumnValue>> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row.Row);

        await node.Database!.Transactions.CommitAsync(tx);
        return rows;
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Golden-path test: <c>executor.AlterTable(AddColumn)</c> on a table with existing
    /// rows.  Verifies that:
    /// <list type="bullet">
    ///   <item>All three nodes converge to <c>Public</c> after the coordinator completes.</item>
    ///   <item>Existing rows on every node return the new column with its default value.</item>
    /// </list>
    /// </summary>
    [Test]
    public async Task AddColumn_ViaExecutorAlterTable_AllNodesConvergeAndRowsCarryDefault()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Create table and insert pre-existing rows.
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
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        await InsertRowsAsync(cluster, db, 3);

        // ADD COLUMN with a default via the full executor path.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.AlterTable(new AlterTableTicket(
                databaseName: db,
                tableName: "robots",
                column: new ColumnInfo("score", ColumnType.Integer64, notNull: false,
                    defaultValue: new ColumnValue(ColumnType.Integer64, 42L)),
                operation: AlterTableOperation.AddColumn
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });

        // All nodes must converge to the final version.
        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(20));

        // On every node: column must be Public and existing rows must carry the default.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableColumnSchema? col = node.Database?.Schema.Tables["robots"].Columns?
                .FirstOrDefault(c => c.Name == "score");

            Assert.IsNotNull(col, $"Column 'score' must exist on node {node.Index}");
            Assert.AreEqual(SchemaElementState.Public, col!.State,
                $"Column 'score' must be Public on node {node.Index}");

            List<IReadOnlyDictionary<string, ColumnValue>> rows = await QueryAllRowsAsync(node, db);
            Assert.AreEqual(3, rows.Count,
                $"All 3 pre-existing rows must be readable on node {node.Index}");

            foreach (IReadOnlyDictionary<string, ColumnValue> row in rows)
            {
                Assert.IsTrue(row.ContainsKey("score"),
                    $"Row on node {node.Index} must contain the new 'score' column");
                Assert.AreEqual(ColumnType.Integer64, row["score"].Type,
                    $"'score' on node {node.Index} must be Integer64, not Null — backfill failed");
                Assert.AreEqual(42L, row["score"].LongValue,
                    $"'score' on node {node.Index} must equal the default 42");
            }
        }
    }

    /// <summary>
    /// Crash-in-window test: simulates a leader crash between the <c>WriteOnly</c> Raft
    /// commit and the backfill transaction commit.  Without the fix the new leader's
    /// <c>ResumeJobsAsync</c> drives <c>WriteOnly → Public</c> without backfilling, leaving
    /// pre-existing rows with <c>TypeNull</c> for the new column.
    ///
    /// <para>
    /// The crash is simulated by manually driving the column to <c>WriteOnly</c> and
    /// persisting a coordinator job — deliberately skipping the backfill — then transferring
    /// leadership.  The new leader's <c>OnBecameLeader</c> fires <c>ResumeJobsAsync</c>,
    /// which calls <c>BackfillAsync</c> because <c>current == WriteOnly &amp;&amp; nextState == Public</c>,
    /// then advances to <c>Public</c>.
    /// </para>
    /// </summary>
    [Test]
    public async Task LeaderChangeMidAdd_ResumedCoordinatorBackfillsDefaultOnExistingRows()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Create table and insert pre-existing rows.
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
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            ));
            version = leader.Database!.Schema.SchemaVersion;
        });
        await cluster.WaitForSchemaConvergenceAsync(db, version);

        await InsertRowsAsync(cluster, db, 3);

        // Step 1: drive the column to WriteOnly WITHOUT backfilling
        // (simulates a crash between the WriteOnly ack and the backfill commit).
        int originalLeaderIndex = -1;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            originalLeaderIndex = leader.Index;
            CatalogsManager catalogs = new(NullLogger<ICamusDB>.Instance);

            await catalogs.ReplicateAddColumnInStateAsync(
                leader.Database!, "robots",
                new ColumnInfo("score", ColumnType.Integer64, notNull: false,
                    defaultValue: new ColumnValue(ColumnType.Integer64, 42L)),
                SchemaElementState.DeleteOnly
            );

            await catalogs.ReplicateElementStateAsync(
                leader.Database!, "robots", "score", SchemaElementState.WriteOnly
            );

            // Persist the coordinator job so the resume path can reconstruct column info.
            await catalogs.PersistCoordinatorJobAsync(leader.Database!, new PersistedCoordinatorJob
            {
                TableName   = "robots",
                ElementName = "score",
                TargetState = SchemaElementState.Public,
                ColumnType  = ColumnType.Integer64,
                ColumnNotNull = false,
                ColumnDefault = new ColumnValue(ColumnType.Integer64, 42L),
            });

            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(10));

        // Confirm all nodes are at WriteOnly and no backfill has run yet.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableColumnSchema? col = node.Database?.Schema.Tables["robots"].Columns?
                .FirstOrDefault(c => c.Name == "score");
            Assert.IsNotNull(col, $"Column must exist at WriteOnly on node {node.Index}");
            Assert.AreEqual(SchemaElementState.WriteOnly, col!.State,
                $"Column must be WriteOnly on node {node.Index} before leader change");
        }

        // Step 2: transfer leadership to a different node.
        // The new leader's OnBecameLeader callback fires ResumeJobsAsync, which reads
        // the persisted job, calls BackfillAsync (current=WriteOnly → nextState=Public),
        // and commits the backfill before advancing the schema to Public.
        InProcessSchemaCluster.Node newLeader =
            cluster.Nodes.First(n => n.Index != originalLeaderIndex);
        await cluster.TransferSchemaLeadershipAsync(db, newLeader, timeout: TimeSpan.FromSeconds(15));

        // Step 3: wait for all nodes to converge one step beyond WriteOnly (→ Public).
        await cluster.WaitForSchemaConvergenceAsync(db, version + 1, timeout: TimeSpan.FromSeconds(20));

        // Step 4: assert the column is Public and pre-existing rows carry the default.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableColumnSchema? col = node.Database?.Schema.Tables["robots"].Columns?
                .FirstOrDefault(c => c.Name == "score");
            Assert.IsNotNull(col, $"Column 'score' must exist on node {node.Index}");
            Assert.AreEqual(SchemaElementState.Public, col!.State,
                $"Column 'score' must be Public on node {node.Index} after resumed add");

            List<IReadOnlyDictionary<string, ColumnValue>> rows = await QueryAllRowsAsync(node, db);
            Assert.AreEqual(3, rows.Count,
                $"All 3 pre-existing rows must be readable on node {node.Index}");

            foreach (IReadOnlyDictionary<string, ColumnValue> row in rows)
            {
                Assert.IsTrue(row.ContainsKey("score"),
                    $"Row on node {node.Index} must contain 'score' after resumed add");
                Assert.AreEqual(ColumnType.Integer64, row["score"].Type,
                    $"'score' on node {node.Index} must be Integer64 — BackfillAsync must fire on resume");
                Assert.AreEqual(42L, row["score"].LongValue,
                    $"'score' on node {node.Index} must equal the default 42 after resumed add");
            }
        }
    }
}
