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
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Inline SQL functions in cluster mode: <c>CREATE FUNCTION</c> replicates the source text through
/// the schema log, every node compiles the function itself at its first call, a replace and a drop
/// reach every node at the same schema version, and a node that does not accept
/// <c>CREATE FUNCTION</c> still runs a function that exists.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterInlineFunctions
{
    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    private const string Slugify =
        "CREATE FUNCTION slugify(s STRING) RETURNS STRING LANGUAGE csharp AS $$ return s.Trim().ToLowerInvariant().Replace(' ', '-'); $$";

    private static async Task DdlOnLeaderAsync(InProcessSchemaCluster cluster, string db, string sql)
    {
        long version = 0;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            // A view checks its body inside the statement's transaction, as every DDL endpoint gives it one.
            KvTransaction tx = await leader.Database!.Transactions.BeginAsync();

            try
            {
                await leader.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null)).WaitAsync(TimeSpan.FromSeconds(60));
                await leader.Database.Transactions.CommitAsync(tx);
            }
            finally
            {
                await leader.Database.Transactions.RollbackIfNotCompletedAsync(tx);
            }

            version = leader.Database.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(30));
    }

    private static async Task<List<QueryResultRow>> QueryAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await node.Database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<string> ScalarAsync(InProcessSchemaCluster.Node node, string db, string sql) =>
        (await QueryAsync(node, db, sql)).Single().Row.Values.Single().StrValue!;

    private static async Task<string> StateAsync(InProcessSchemaCluster.Node node, string db, string function) =>
        (await QueryAsync(node, db, "SHOW FUNCTIONS")).Single(row => row.Row["name"].StrValue == function).Row["state"].StrValue!;

    [Test]
    public async Task CreateReplaceAndDrop_ReachEveryNode_AndEachNodeCompilesForItself()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 1, loggerFactory: sharedLoggerFactory, logger: logger,
            options: CamusDBOptions.Default with { InlineFunctionsEnabled = true });

        try
        {
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
            InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leader.Index);

            // Function DDL on a follower is refused with the leader's name.
            CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(() => follower.Executor.ExecuteDDLSQL(
                new ExecuteSQLTicket(txnState: null!, database: db, sql: Slugify, parameters: null)))!;
            StringAssert.Contains("schema leader", refused.Message);

            await DdlOnLeaderAsync(cluster, db, Slugify);

            // The log carried the source. No follower has compiled anything yet.
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                InlineFunctionDefinition record = node.Database!.Schema.Functions["slugify"];
                StringAssert.Contains("ToLowerInvariant", record.Source, $"node {node.Index}");
                Assert.AreEqual(64, record.SourceSha256.Length);

                if (node.Index != leader.Index)
                    Assert.AreEqual("not compiled", await StateAsync(node, db, "slugify"), $"node {node.Index}");
            }

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.AreEqual("a-b", await ScalarAsync(node, db, "SELECT slugify(' A B ')"), $"node {node.Index}");
                Assert.AreEqual("compiled", await StateAsync(node, db, "slugify"), $"node {node.Index} compiled at its first call");
            }

            // One statement moves every node to the new body at the same schema version.
            await DdlOnLeaderAsync(cluster, db,
                "CREATE OR REPLACE FUNCTION slugify(s STRING) RETURNS STRING LANGUAGE csharp AS $$ return s.Trim().ToUpperInvariant().Replace(' ', '_'); $$");

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.AreEqual(1, node.Database!.Schema.Functions["slugify"].Generation, $"node {node.Index}");
                Assert.AreEqual("A_B", await ScalarAsync(node, db, "SELECT slugify(' A B ')"), $"node {node.Index}");
            }

            // A node that reopens the database reads the record back from the checkpoint.
            await cluster.ReopenDatabaseAsync(follower.Index, db);
            Assert.AreEqual("A_B", await ScalarAsync(cluster.Nodes[follower.Index], db, "SELECT slugify(' A B ')"));

            // A view on one node calls the function on all of them.
            await DdlOnLeaderAsync(cluster, db, "CREATE TABLE posts (id INT64 PRIMARY KEY, title STRING)");
            await DdlOnLeaderAsync(cluster, db, "CREATE VIEW slugs AS SELECT id, slugify(title) AS slug FROM posts");

            // The drop is refused while the view calls the function, and then reaches every node.
            CamusDBException blocked = Assert.ThrowsAsync<CamusDBException>(() => DdlOnLeaderAsync(cluster, db, "DROP FUNCTION slugify"))!;
            Assert.AreEqual(CamusDBErrorCodes.FunctionHasDependents, blocked.Code);

            await DdlOnLeaderAsync(cluster, db, "DROP VIEW slugs");
            await DdlOnLeaderAsync(cluster, db, "DROP FUNCTION slugify");

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.IsFalse(node.Database!.Schema.Functions.ContainsKey("slugify"), $"node {node.Index}");

                CamusDBException gone = Assert.ThrowsAsync<CamusDBException>(() => ScalarAsync(node, db, "SELECT slugify('x')"))!;
                StringAssert.Contains("Function not found", gone.Message, $"node {node.Index}");
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task ANodeWithTheSettingOff_AppliesTheDelta_AndRunsTheFunction()
    {
        // Node 2 does not accept CREATE FUNCTION; nodes 0 and 1 do.
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 1, loggerFactory: sharedLoggerFactory, logger: logger,
            options: CamusDBOptions.Default with { InlineFunctionsEnabled = true },
            configureNodeOptions: (index, options) => index == 2 ? options with { InlineFunctionsEnabled = false } : options);

        try
        {
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            InProcessSchemaCluster.Node off = cluster.Nodes[2];

            if ((await cluster.WaitForSchemaLeaderNodeAsync(db)).Index == off.Index)
                await cluster.TransferSchemaLeadershipAsync(db, cluster.Nodes[0], timeout: TimeSpan.FromSeconds(30));

            await DdlOnLeaderAsync(cluster, db, Slugify);

            Assert.AreEqual("a-b", await ScalarAsync(off, db, "SELECT slugify('A B')"), "a function that exists runs on a node with the setting off");

            // As the schema leader, that node refuses to create one.
            await cluster.TransferSchemaLeadershipAsync(db, off, timeout: TimeSpan.FromSeconds(30));

            CamusDBException disabled = Assert.ThrowsAsync<CamusDBException>(() => off.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                txnState: null!, database: db, sql: "CREATE FUNCTION more(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ return n; $$", parameters: null)))!;

            Assert.AreEqual(CamusDBErrorCodes.InlineFunctionsDisabled, disabled.Code);
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
