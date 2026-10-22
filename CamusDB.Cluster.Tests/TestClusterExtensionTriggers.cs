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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Row triggers in cluster mode: CREATE TRIGGER replicates through the schema log to every node, a
/// write on a follower fires the trigger exactly once per row (on the node that runs the statement,
/// never again on the nodes that replicate the rows), and trigger DDL on a follower is refused with
/// the schema leader's name, the same as view and sequence DDL.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterExtensionTriggers
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
    public async Task Trigger_ReplicatesAndFiresOncePerRow_OnTheNodeThatWrites()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 1, loggerFactory: sharedLoggerFactory, logger: logger,
            options: CamusDBOptions.Default with { ExtensionAssemblies = [FixturePath("Fixture.Good")] });

        try
        {
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            // The table name is unique per run: the counting trigger keeps a static count per name.
            string table = "t" + Guid.NewGuid().ToString("N")[..8];

            await DdlOnLeaderAsync(cluster, db, "CREATE EXTENSION good");
            await DdlOnLeaderAsync(cluster, db, $"CREATE TABLE {table} (id OID PRIMARY KEY DEFAULT (gen_id()), name STRING)");
            await DdlOnLeaderAsync(cluster, db,
                $"CREATE TRIGGER counter ON {table} AFTER INSERT EXTERNAL NAME 'CamusDB.Fixtures.Good.CountingTrigger'");

            // Every node applied the record.
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                Assert.AreEqual("counter", node.Database!.Schema.Tables[table].Triggers!.Single().Name, $"node {node.Index}");

            InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
            InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leader.Index);

            // Trigger DDL on a follower is refused with the leader's name.
            CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(() => follower.Executor.ExecuteDDLSQL(
                new ExecuteSQLTicket(txnState: null!, database: db,
                    sql: $"DROP TRIGGER counter ON {table}", parameters: null)))!;
            StringAssert.Contains("schema leader", refused.Message);

            // Four rows written through a follower.
            KvTransaction tx = await follower.Database!.Transactions.BeginAsync();
            await follower.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db,
                $"INSERT INTO {table} (name) VALUES ('a'), ('b'), ('c'), ('d')", null));
            await follower.Database.Transactions.CommitAsync(tx);

            // Each node loads the extension into its own load context, so each node has its own count:
            // four on the node that ran the statement, and none on the nodes that only replicate the
            // rows. A short pause lets any late firing elsewhere show up before the check.
            await Task.Delay(500);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<QueryResultRow> count = await QueryAsync(node, db, $"SELECT fx_trigger_count('{table}') AS n");
                long expected = node.Index == follower.Index ? 4 : 0;
                Assert.AreEqual(expected, count.Single().Row["n"].LongValue, $"node {node.Index}: once per row, on the writing node only");
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
