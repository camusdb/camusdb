/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// A write-driving scan (the source of <c>INSERT … SELECT</c>, the locate scan of <c>UPDATE</c> and
/// <c>DELETE</c>) across nodes. With key-range sharding the scan takes an exclusive range lock at every
/// isolation level, and the write intents that lock places live on the partition leader of the range,
/// which is usually not the node that runs the statement. The scan must carry its transaction's identity
/// to that leader, or every page after the first waits on intents only its own transaction can resolve.
/// Each statement reads more than one 512-row page of freshly written rows, and runs on a node that did
/// not write them.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterWriteDrivingScanOwnLocks
{
    private const int Rows = 1_500;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static Task DdlAsync(InProcessSchemaCluster.Node node, string db, string sql) =>
        node.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null))
            .WaitAsync(TimeSpan.FromSeconds(60));

    private static async Task<int> DmlAsync(InProcessSchemaCluster.Node node, string db, string sql, CamusIsolationLevel? isolation = null)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync(isolation);
        try
        {
            ExecuteNonSQLResult result = await node.Executor.ExecuteNonSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));
            await node.Database.Transactions.CommitAsync(tx);
            return result.ModifiedRows;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<long> CountAsync(InProcessSchemaCluster.Node node, string db, string table)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: db, sql: $"SELECT COUNT(*) AS c FROM {table}", parameters: null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            return rows[0].Row["c"].LongValue;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [TestCase(CamusIsolationLevel.ReadCommitted)]
    [TestCase(CamusIsolationLevel.Serializable)]
    public async Task WriteDrivingScans_OverManyPages_CompleteFromAnotherNode(CamusIsolationLevel isolation)
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: 2,
                loggerFactory: sharedLoggerFactory, logger: logger,
                options: CamusDBOptions.Default with { KeyRangeShardingEnabled = true });

        try
        {
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            long version = 0;
            await cluster.RunOnSchemaLeaderAsync(db, async leader =>
            {
                await DdlAsync(leader, db, "CREATE TABLE src (id INT64 PRIMARY KEY, x INT64)");
                await DdlAsync(leader, db, "CREATE TABLE dst (id INT64 PRIMARY KEY, x INT64)");
                version = leader.Database!.Schema.SchemaVersion;
            });

            await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(30));

            StringBuilder insert = new("INSERT INTO src (id, x) VALUES ");
            for (int id = 1; id <= Rows; id++)
            {
                if (id > 1)
                    insert.Append(", ");

                insert.Append('(').Append(id).Append(", ").Append(id).Append(')');
            }

            await DmlAsync(cluster.Nodes[0], db, insert.ToString());

            InProcessSchemaCluster.Node runner = cluster.Nodes[1];

            Assert.AreEqual(Rows, await DmlAsync(runner, db, "INSERT INTO dst (id, x) SELECT id, x FROM src", isolation),
                "INSERT … SELECT copies every row");
            Assert.AreEqual(Rows, await DmlAsync(runner, db, "UPDATE src SET x = 0 WHERE x > 0", isolation),
                "a full-scan UPDATE updates every row");
            Assert.AreEqual(Rows, await DmlAsync(runner, db, "DELETE FROM src WHERE x = 0", isolation),
                "a full-scan DELETE deletes every row");

            Assert.AreEqual(0, await CountAsync(cluster.Nodes[2], db, "src"));
            Assert.AreEqual(Rows, await CountAsync(cluster.Nodes[2], db, "dst"));
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
