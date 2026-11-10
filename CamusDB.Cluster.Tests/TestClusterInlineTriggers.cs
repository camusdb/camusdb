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
/// Inline C# triggers in cluster mode: <c>CREATE TRIGGER ... LANGUAGE csharp</c> replicates the body
/// through the schema log, each node compiles it at its own first fire and fires it for the statements
/// it runs, a node with <c>inline_triggers_enabled</c> off still fires it, a reopen keeps it, and
/// <c>DROP TRIGGER</c> reaches every node.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterInlineTriggers
{
    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    private const string Slugify =
        "CREATE FUNCTION slugify(s STRING) RETURNS STRING LANGUAGE csharp AS $$ return s.Trim().ToLowerInvariant().Replace(' ', '-'); $$";

    private const string SlugTrigger =
        "CREATE TRIGGER slug ON posts BEFORE INSERT LANGUAGE csharp AS $$ if (NEW!.title is { } t) NEW.slug = slugify(t); $$";

    private const string AuditTrigger =
        "CREATE TRIGGER audit ON posts AFTER INSERT LANGUAGE csharp AS $$ " +
        "await Sql.Set(\"id\", NEW!.id).Set(\"slug\", NEW.slug).ExecuteAsync(\"INSERT INTO audit_log (post_id, slug) VALUES (@id, @slug)\"); $$";

    private static async Task DdlOnLeaderAsync(InProcessSchemaCluster cluster, string db, string sql)
    {
        long version = 0;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
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

    private static async Task DmlAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();

        try
        {
            await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await node.Database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
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

    [Test]
    public async Task CreateFireAndDrop_OnEveryNode_EachNodeCompilesForItself_AndANodeWithTheSettingOffStillFires()
    {
        // Node 2 does not accept CREATE TRIGGER ... LANGUAGE csharp; nodes 0 and 1 do.
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 1, loggerFactory: sharedLoggerFactory, logger: logger,
            options: CamusDBOptions.Default with { InlineFunctionsEnabled = true, InlineTriggersEnabled = true },
            configureNodeOptions: (index, options) => index == 2 ? options with { InlineTriggersEnabled = false } : options);

        try
        {
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            InProcessSchemaCluster.Node off = cluster.Nodes[2];

            if ((await cluster.WaitForSchemaLeaderNodeAsync(db)).Index == off.Index)
                await cluster.TransferSchemaLeadershipAsync(db, cluster.Nodes[0], timeout: TimeSpan.FromSeconds(30));

            await DdlOnLeaderAsync(cluster, db, "CREATE TABLE posts (id INT64 PRIMARY KEY, title STRING, slug STRING)");
            await DdlOnLeaderAsync(cluster, db, "CREATE TABLE audit_log (id OID PRIMARY KEY DEFAULT (gen_id()), post_id INT64, slug STRING)");
            await DdlOnLeaderAsync(cluster, db, Slugify);
            await DdlOnLeaderAsync(cluster, db, SlugTrigger);
            await DdlOnLeaderAsync(cluster, db, AuditTrigger);

            // The log carried the body; no node compiled it for a fire yet.
            Dictionary<int, long> compiled = [];

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<TriggerDefinition> triggers = node.Database!.Schema.Tables["posts"].Triggers!;
                CollectionAssert.AreEqual(new[] { "slug", "audit" }, triggers.Select(t => t.Name).ToArray(), $"node {node.Index}");
                Assert.IsTrue(triggers.All(t => t.IsInline && t.SourceSha256.Length == 64), $"node {node.Index}");
                compiled[node.Index] = node.Executor.InlineFunctions.Compilations;
            }

            // Each node fires the triggers of the statements it runs, with a unit it compiled itself.
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                await DmlAsync(node, db, $"INSERT INTO posts (id, title) VALUES ({node.Index + 1}, 'Node {node.Index}')");
                Assert.Greater(node.Executor.InlineFunctions.Compilations, compiled[node.Index], $"node {node.Index} compiled at its first fire");
            }

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<QueryResultRow> audit = await QueryAsync(node, db, "SELECT post_id, slug FROM audit_log");
                CollectionAssert.AreEquivalent(
                    new[] { "1:node-0", "2:node-1", "3:node-2" },
                    audit.Select(r => r.Row["post_id"].LongValue + ":" + r.Row["slug"].StrValue).ToArray(),
                    $"node {node.Index} reads the rows every node's triggers wrote");
            }

            // A reopen reads the trigger back from the checkpoint, and it fires there again.
            await cluster.ReopenDatabaseAsync(off.Index, db);
            await DmlAsync(cluster.Nodes[off.Index], db, "INSERT INTO posts (id, title) VALUES (10, 'After Reopen')");
            Assert.AreEqual("after-reopen", (await QueryAsync(cluster.Nodes[0], db, "SELECT slug FROM posts WHERE id = 10")).Single().Row["slug"].StrValue);

            // The drop reaches every node.
            await DdlOnLeaderAsync(cluster, db, "DROP TRIGGER slug ON posts");
            await DdlOnLeaderAsync(cluster, db, "DROP TRIGGER audit ON posts");

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                Assert.IsNull(node.Database!.Schema.Tables["posts"].Triggers, $"node {node.Index}");
                await DmlAsync(node, db, $"INSERT INTO posts (id, title) VALUES ({node.Index + 20}, 'X Y')");
            }

            Assert.AreEqual(4, (await QueryAsync(cluster.Nodes[0], db, "SELECT post_id FROM audit_log")).Count, "no trigger fired after the drop");
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
