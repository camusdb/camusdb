
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

using Nito.AsyncEx;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Cluster coverage for automatic background ANALYZE. The dangerous case the single-node tests
/// cannot reach: a table created and mutated on one node must be discovered and analyzed by the
/// node that leads the registry partition (the auto-analyze owner), even though that owner may never
/// have opened the table. This exercises the cluster-visible discovery path (registry scan +
/// per-object meta scan + persisted-staleness load) rather than a node-local open-object list.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster. Concurrent clusters contend for ports and skew
// each other's Raft election timing, which shows up as spurious leadership churn.
[NonParallelizable]
public sealed class TestAutoAnalyzeCluster
{

    // The fixed key whose partition leadership gates auto-analyze ownership.
    private const string RegistryBucket = "_system/dbregistry";

    /// <summary>
    /// Auto-analyze triggered by staleness alone: no fraction threshold, and only a few stale rows
    /// needed, so a sweep over a small table decides to analyze it.
    /// </summary>
    private static CamusDBOptions EagerAutoAnalyze => CamusDBOptions.Default with
    {
        AutoAnalyzeEnabled = true,
        AutoAnalyzeFractionStaleRows = 0.0,
        AutoAnalyzeMinStaleRows = 5,
    };



    private static async Task<InProcessSchemaCluster.Node?> FindRegistryOwnerAsync(InProcessSchemaCluster cluster)
    {
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            if (await node.Kahuna.AmILeaderForKeyAsync(RegistryBucket, CancellationToken.None).ConfigureAwait(false))
                return node;
        }
        return null;
    }

    private static async Task InsertRobotsAsync(InProcessSchemaCluster cluster, string db, int count)
    {
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            for (int i = 0; i < count; i++)
            {
                KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx, database: db,
                    sql: $"INSERT INTO robots (id, name, year) VALUES (gen_id(), 'r{i}', {2000 + i})",
                    parameters: null));
                await leader.Database.Transactions.CommitAsync(tx);
            }
        });
    }

    private static async Task CreateRobotsTableAsync(InProcessSchemaCluster cluster, string db)
    {
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "robots",
            columns:
            [
                new ColumnInfo("id",   ColumnType.Id),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)]),
                new ConstraintInfo(ConstraintType.IndexMulti, "year_idx", [new ColumnIndexInfo("year", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);
    }

    [Test]
    public async Task Cluster_StaleTableDiscoveredAndAnalyzedByRegistryLeader()
    {
        // Auto-analyze triggered by staleness alone, so a handful of stale rows is enough.
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 1, options: EagerAutoAnalyze);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Create the table on the schema leader (id PK, name, indexed year).
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "robots",
            columns:
            [
                new ColumnInfo("id",   ColumnType.Id),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)]),
                new ConstraintInfo(ConstraintType.IndexMulti, "year_idx", [new ColumnIndexInfo("year", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        // Mutate the table through the schema leader, then flush its stats so the registry-leader can
        // read the cluster-wide mutation count from KV (the persisted-staleness discovery path).
        InProcessSchemaCluster.Node writer = await cluster.WaitForSchemaLeaderNodeAsync(db);
        for (int i = 0; i < 10; i++)
        {
            KvTransaction tx = await writer.Database!.Transactions.BeginAsync();
            await writer.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: tx, database: db,
                sql: $"INSERT INTO robots (id, name, year) VALUES (gen_id(), 'r{i}', {2000 + i})",
                parameters: null));
            await writer.Database.Transactions.CommitAsync(tx);
        }

        TableDescriptor writerTable = await writer.Database!.TableDescriptors["robots"];
        // Force the stats entry loaded before flushing: an unloaded entry's flush is a no-op, which
        // would leave the mutation count unpersisted and the table undiscoverable by the leader.
        await writer.Executor.Statistics.LoadByIdAsync(writer.Database, writerTable.Id);
        await writer.Executor.Statistics.FlushAsync(writer.Database, writerTable);

        // Run a sweep on every node. Only the registry-partition leader does work; the rest return 0.
        // The owner discovers the stale table via authoritative metadata even if it never opened it.
        int analyzed = 0;
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            analyzed += await node.Executor.RunAutoAnalyzeForTestsAsync();

        Assert.GreaterOrEqual(analyzed, 1,
            "The registry-leader must discover and analyze the stale table via cluster metadata");

        // ANALYZE published column NDV (DML tracking never sets NDV) — confirm it reached KV by
        // reloading the persisted stats on the writer node after evicting its cache.
        writer.Executor.Statistics.EvictForTesting(writer.Database, writerTable);
        await writer.Executor.Statistics.LoadByIdAsync(writer.Database, writerTable.Id);

        Assert.IsNotNull(writer.Executor.Statistics.GetColumnNdv(writer.Database, writerTable, "year"),
            "Background ANALYZE must have published column NDV to KV, visible cluster-wide after reload");
    }

    /// <summary>
    /// Exactly-once under failover: if the auto-analyze owner loses its registry-partition leadership
    /// while a scan is in flight, that scan must abort and publish nothing — either proactively via the
    /// mid-scan ownership check, or reactively if a paged snapshot read reroutes through the leadership
    /// change and errors. Both outcomes are correct because publish is gated on a complete scan and the
    /// under-fence ownership check; a former leader can never write the new generation.
    /// </summary>
    [Test]
    public async Task Cluster_LeadershipLostMidScanAbortsWithoutPublishing()
    {
        // Slow scan that spans the step-down, re-checking ownership often enough to notice it.
        CamusDBOptions slowScan = CamusDBOptions.Default with
        {
            AutoAnalyzeEnabled = true,
            AutoAnalyzeMaxRowsPerSecond = 50,
            AutoAnalyzeOwnershipCheckRows = 25,
        };

        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 1, options: slowScan);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);
        await CreateRobotsTableAsync(cluster, db);
        await InsertRobotsAsync(cluster, db, 300); // 300 rows @ 50/s ≈ 6s scan

        InProcessSchemaCluster.Node? owner = await FindRegistryOwnerAsync(cluster);
        Assert.NotNull(owner, "A registry-partition leader (auto-analyze owner) must exist");

        // Start a slow analyze on the current owner, wired with the real leadership ownership check.
        Task analyze = owner!.Executor.RunBackgroundAnalyzeWithOwnershipForTestsAsync(db, "robots", CancellationToken.None);

        // Let it get well into the scan, then gracefully revoke the owner's registry leadership. The
        // node stays online (no transport isolation), so its snapshot reads keep working and the abort
        // comes from the ownership check flipping to false.
        await Task.Delay(1000);
        await owner.Executor.StepDownAutoAnalyzeLeadershipForTestsAsync();

        Exception? thrown = null;
        try { await analyze; }
        catch (Exception ex) { thrown = ex; }

        Assert.NotNull(thrown, "A former owner must abort its analyze on leadership loss, not publish");

        // Nothing was published: ANALYZE alone sets NDV (DML tracking never does), so its absence after
        // a reload proves the aborted analyze wrote no generation.
        TableDescriptor ownerTable = await owner.Database!.TableDescriptors["robots"];
        owner.Executor.Statistics.EvictForTesting(owner.Database, ownerTable);
        await owner.Executor.Statistics.LoadByIdAsync(owner.Database, ownerTable.Id);

        Assert.IsNull(owner.Executor.Statistics.GetColumnNdv(owner.Database, ownerTable, "year"),
            "A former owner that lost its lease mid-scan must publish nothing");
    }

    /// <summary>
    /// The per-table opt-out replicates cluster-wide (SchemaOp.SetTableSettings): after
    /// <c>SET (sql_stats_automatic_collection_enabled = false)</c> on the schema leader, every node's
    /// schema shows it and the registry leader's sweep skips the table even though it is stale.
    /// </summary>
    [Test]
    public async Task Cluster_DisabledTableIsNotAutoAnalyzed()
    {
        // Auto-analyze triggered by staleness alone, so a handful of stale rows is enough.
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 1, options: EagerAutoAnalyze);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);
        await CreateRobotsTableAsync(cluster, db);
        await InsertRobotsAsync(cluster, db, 10);

        // Opt the table out on the schema leader; the delta replicates (schema version 1 → 2).
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: db,
            sql: "ALTER TABLE robots SET (sql_stats_automatic_collection_enabled = false)", parameters: null)));
        await cluster.WaitForSchemaConvergenceAsync(db, version: 2);

        // Every node's in-memory schema reflects the opt-out.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            Assert.IsTrue(node.Database!.Schema.Tables.TryGetValue("robots", out TableSchema? schema));
            Assert.IsFalse(schema!.AutoStatsCollectionEnabled,
                $"Node {node.Index} must see the replicated opt-out");
        }

        // Sweep on every node: the disabled table must not be analyzed (no NDV published).
        int analyzed = 0;
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            analyzed += await node.Executor.RunAutoAnalyzeForTestsAsync();

        Assert.AreEqual(0, analyzed, "A disabled table must not be auto-analyzed cluster-wide");

        InProcessSchemaCluster.Node writer = await cluster.WaitForSchemaLeaderNodeAsync(db);
        TableDescriptor writerTable = await writer.Database!.TableDescriptors["robots"];
        writer.Executor.Statistics.EvictForTesting(writer.Database, writerTable);
        await writer.Executor.Statistics.LoadByIdAsync(writer.Database, writerTable.Id);
        Assert.IsNull(writer.Executor.Statistics.GetColumnNdv(writer.Database, writerTable, "year"),
            "No statistics may be published for a disabled table");
    }
}
