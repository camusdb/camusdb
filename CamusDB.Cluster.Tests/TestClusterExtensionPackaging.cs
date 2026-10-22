/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// The extension packaging layer in cluster mode: <c>CREATE EXTENSION</c> and
/// <c>ALTER EXTENSION ... UPDATE</c> replicate through the schema log so every node binds the same
/// build at the same schema version, a trigger keeps firing across the update with no re-create, and
/// the proposer refuses an install or an update while a live node does not load the target build.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterExtensionPackaging
{
    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static string FixturePath(string fixture) =>
        Path.Combine(TestContext.CurrentContext.TestDirectory, "extension-fixtures", fixture, fixture + ".dll");

    private static async Task DdlOnLeaderAsync(InProcessSchemaCluster cluster, string db, string sql)
    {
        long version = 0;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null))
                .WaitAsync(TimeSpan.FromSeconds(60));
            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(30));
    }

    private static async Task<string> ScalarAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await node.Database.Transactions.CommitAsync(tx);
            return rows.Single().Row.Values.Single().StrValue!;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task InsertAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
        await node.Database.Transactions.CommitAsync(tx);
    }

    [Test]
    public async Task RollingUpgrade_MovesEveryNodeToTheNewBuild_AndTriggersKeepFiring()
    {
        // Step 1 of the rolling upgrade is already done: every node loads both versions.
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 1, loggerFactory: sharedLoggerFactory, logger: logger,
            options: CamusDBOptions.Default with { ExtensionAssemblies = [FixturePath("Fixture.Good"), FixturePath("Fixture.GoodV2")] });

        try
        {
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            await DdlOnLeaderAsync(cluster, db, "CREATE EXTENSION good VERSION '1.0.0'");
            await DdlOnLeaderAsync(cluster, db, "CREATE TABLE t (id INT64 PRIMARY KEY, build STRING)");
            await DdlOnLeaderAsync(cluster, db, "CREATE TRIGGER stamp ON t BEFORE INSERT EXTERNAL NAME 'CamusDB.Fixtures.Good.BuildStampTrigger'");

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                Assert.AreEqual("1.0.0", await ScalarAsync(node, db, "SELECT fx_good_version()"), $"node {node.Index}");

            InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
            InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leader.Index);

            // Extension DDL on a follower is refused with the leader's name.
            CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(() => follower.Executor.ExecuteDDLSQL(
                new ExecuteSQLTicket(txnState: null!, database: db, sql: "ALTER EXTENSION good UPDATE", parameters: null)))!;
            StringAssert.Contains("schema leader", refused.Message);

            await InsertAsync(follower, db, "INSERT INTO t (id) VALUES (1)");
            Assert.AreEqual("1.0.0", await ScalarAsync(follower, db, "SELECT build FROM t WHERE id = 1"));

            // Step 2: one statement moves every node at the same schema version.
            await DdlOnLeaderAsync(cluster, db, "ALTER EXTENSION good UPDATE TO '2.0.0'");

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.AreEqual("2.0.0", node.Database!.Schema.Extensions["good"].Version, $"node {node.Index}");
                Assert.AreEqual("2.0.0", await ScalarAsync(node, db, "SELECT fx_good_version()"), $"node {node.Index}");
            }

            // The same trigger record now fires the new build on a node that did not run the update.
            await InsertAsync(follower, db, "INSERT INTO t (id) VALUES (2)");
            Assert.AreEqual("2.0.0", await ScalarAsync(follower, db, "SELECT build FROM t WHERE id = 2"));
            Assert.AreEqual("stamp", follower.Database!.Schema.Tables["t"].Triggers!.Single().Name);

            // A node that reopens the database reads the updated record back from the checkpoint.
            await cluster.ReopenDatabaseAsync(follower.Index, db);
            Assert.AreEqual("2.0.0", await ScalarAsync(cluster.Nodes[follower.Index], db, "SELECT fx_good_version()"));
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task InstallAndUpdate_AreRefusedWhileALiveNodeLacksTheBuild()
    {
        // Node 2 loads version 1.0.0 only; nodes 0 and 1 load both versions.
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 1, loggerFactory: sharedLoggerFactory, logger: logger,
            options: CamusDBOptions.Default with { ExtensionAssemblies = [FixturePath("Fixture.Good")] },
            configureNodeOptions: (index, options) => index == 2
                ? options
                : options with { ExtensionAssemblies = [FixturePath("Fixture.Good"), FixturePath("Fixture.GoodV2")] });

        try
        {
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            InProcessSchemaCluster.Node lacking = cluster.Nodes[2];
            string lackingEndpoint = lacking.Kahuna.Raft.GetLocalEndpoint();

            // Installing the version node 2 does not load is refused, and names node 2.
            CamusDBException install = await RefusedOnLeaderAsync(cluster, db, "CREATE EXTENSION good VERSION '2.0.0'");
            Assert.AreEqual(CamusDBErrorCodes.ExtensionClusterMismatch, install.Code);
            StringAssert.Contains(lackingEndpoint, install.Message);

            // The version every node loads installs.
            await DdlOnLeaderAsync(cluster, db, "CREATE EXTENSION good VERSION '1.0.0'");

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                Assert.AreEqual("1.0.0", await ScalarAsync(node, db, "SELECT fx_good_version()"), $"node {node.Index}");

            // The update to the version node 2 does not load is refused, and nothing changed.
            CamusDBException update = await RefusedOnLeaderAsync(cluster, db, "ALTER EXTENSION good UPDATE TO '2.0.0'");
            Assert.AreEqual(CamusDBErrorCodes.ExtensionClusterMismatch, update.Code);
            StringAssert.Contains(lackingEndpoint, update.Message);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                Assert.AreEqual("1.0.0", node.Database!.Schema.Extensions["good"].Version, $"node {node.Index}");
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    private static async Task<CamusDBException> RefusedOnLeaderAsync(InProcessSchemaCluster cluster, string db, string sql)
    {
        CamusDBException? error = null;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            try
            {
                await leader.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null))
                    .WaitAsync(TimeSpan.FromSeconds(60));
            }
            catch (CamusDBException ex)
            {
                error = ex;
            }
        });

        Assert.IsNotNull(error, sql + " must be refused");
        return error!;
    }
}
