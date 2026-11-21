/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>SHOW PARTITIONS</c> on a standalone node, driven through <c>ExecuteSQLQuery</c> with no database
/// and no transaction, the way every transport sends a server-level statement.
///
/// <para>A standalone node hosts and leads every partition, so the leader columns are fixed: the local
/// endpoint, as <c>EmbeddedKahuna.BuildSpan</c> reports for <c>SHOW RANGES</c>. The cluster behavior
/// (leaders that differ by node, partitions a node does not host, a leadership transfer) is in
/// <c>TestClusterShowPartitions</c>.</para>
///
/// <para>Serial: boots an embedded Kahuna node per test.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestShowPartitions : BaseTest
{
    /// <summary>More than one data partition, so the order and the per-partition values are tested.</summary>
    private const int DataPartitions = 3;

    protected override int NodeInitialPartitions => DataPartitions;

    private static readonly string[] ExpectedColumns =
    [
        "node", "partition_id", "kind", "state", "routing", "hash_start", "hash_end", "generation",
        "leader", "leader_source", "leader_is_local", "hosted_locally", "term", "commit_index",
        "replication_factor", "voters", "learners", "removing", "awaiting_placement",
        "log_ops_per_sec", "wal_queue_depth", "commit_wait_ms", "max_follower_lag", "stalled_followers",
    ];

    private static async Task<List<QueryResultRow>> ShowPartitionsAsync(
        CommandExecutor executor, string database = "", QuerySchemaHolder? schema = null)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: database, sql: "SHOW PARTITIONS", parameters: null),
            schemaOut: schema);

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        return rows;
    }

    private static long Long(QueryResultRow row, string column) => row.Row[column].LongValue;

    private static bool IsNull(QueryResultRow row, string column) => row.Row[column].Type == ColumnType.Null;

    [Test]
    public async Task ListsTheSystemPartitionFirst_ThenEveryDataPartitionInOrder()
    {
        CommandExecutor executor = CreateCommandExecutor();

        List<QueryResultRow> rows = await ShowPartitionsAsync(executor);

        CollectionAssert.AreEqual(
            Enumerable.Range(0, DataPartitions + 1).Select(i => (long)i).ToArray(),
            rows.Select(r => Long(r, "partition_id")).ToArray(),
            "one row for partition 0, then one for each data partition, in ascending id order");

        QueryResultRow system = rows[0];
        Assert.AreEqual("system", system.Row["kind"].StrValue);
        Assert.AreEqual("active", system.Row["state"].StrValue);

        // The system partition is not an entry of the hash-routing map.
        Assert.IsTrue(IsNull(system, "routing"));
        Assert.IsTrue(IsNull(system, "hash_start"));
        Assert.IsTrue(IsNull(system, "hash_end"));
        Assert.IsTrue(IsNull(system, "generation"));
        Assert.AreEqual(0, Long(system, "replication_factor"), "every roster voter hosts the system partition");

        long previousEnd = long.MinValue;

        foreach (QueryResultRow data in rows.Skip(1))
        {
            Assert.AreEqual("data", data.Row["kind"].StrValue);
            Assert.AreEqual("active", data.Row["state"].StrValue);
            Assert.AreEqual("hash_range", data.Row["routing"].StrValue);
            Assert.IsFalse(IsNull(data, "generation"));

            long start = Long(data, "hash_start");
            long end = Long(data, "hash_end");
            Assert.LessOrEqual(start, end, $"partition {Long(data, "partition_id")} has an inverted hash range");
            Assert.Greater(start, previousEnd, "the hash ranges of the data partitions must not overlap");
            previousEnd = end;

            Assert.IsFalse(data.Row["awaiting_placement"].BoolValue);
        }
    }

    /// <summary>
    /// A single node leads and hosts every partition. It reports its own endpoint as the leader, and
    /// does not depend on a leader hint, which a single node can lack.
    /// </summary>
    [Test]
    public async Task StandaloneNodeLeadsAndHostsEveryPartition()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string localEndpoint = TestNode!.Raft.GetLocalEndpoint();

        List<QueryResultRow> rows = await ShowPartitionsAsync(executor);
        Assert.AreEqual(DataPartitions + 1, rows.Count);

        foreach (QueryResultRow row in rows)
        {
            long id = Long(row, "partition_id");

            Assert.AreEqual(localEndpoint, row.Row["node"].StrValue, $"partition {id}: node label");
            Assert.AreEqual(localEndpoint, row.Row["leader"].StrValue, $"partition {id}: leader");
            Assert.AreEqual("local", row.Row["leader_source"].StrValue, $"partition {id}: leader source");
            Assert.IsTrue(row.Row["leader_is_local"].BoolValue, $"partition {id}: leader_is_local");
            Assert.IsTrue(row.Row["hosted_locally"].BoolValue, $"partition {id}: hosted_locally");

            // No replica set: a single node is full replication over a roster of one.
            Assert.AreEqual("", row.Row["voters"].StrValue, $"partition {id}: voters");
            Assert.AreEqual("", row.Row["learners"].StrValue, $"partition {id}: learners");
            Assert.AreEqual("", row.Row["removing"].StrValue, $"partition {id}: removing");

            // The leader has no follower, so no follower ever reported: the lag is unknown, and no
            // follower is stalled.
            Assert.IsTrue(IsNull(row, "max_follower_lag"), $"partition {id}: max_follower_lag");
            Assert.AreEqual(0, Long(row, "stalled_followers"), $"partition {id}: stalled_followers");

            // Load signals are numbers on the leader, never NULL.
            Assert.IsFalse(IsNull(row, "log_ops_per_sec"), $"partition {id}: log_ops_per_sec");
            Assert.IsFalse(IsNull(row, "wal_queue_depth"), $"partition {id}: wal_queue_depth");
            Assert.IsFalse(IsNull(row, "commit_wait_ms"), $"partition {id}: commit_wait_ms");
        }

        // Every data partition is hosted here, so it has a term.
        foreach (QueryResultRow data in rows.Skip(1))
            Assert.GreaterOrEqual(Long(data, "term"), 0, $"partition {Long(data, "partition_id")}: term");
    }

    /// <summary>
    /// The commit index is a live read. Writes through the engine commit Raft entries, so the commit
    /// index of the partitions they reach is a number afterwards, and no partition goes backwards.
    /// </summary>
    [Test]
    public async Task CommitIndexIsLiveAndDoesNotGoBackwards()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Dictionary<long, long?> before = (await ShowPartitionsAsync(executor))
            .ToDictionary(r => Long(r, "partition_id"), r => IsNull(r, "commit_index") ? (long?)null : Long(r, "commit_index"));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: db, sql: "CREATE TABLE robots (id OID PRIMARY KEY, name STRING)", parameters: null));

        for (int i = 0; i < 10; i++)
        {
            KvTransaction txn = await database.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txn, db, $"INSERT INTO robots (id, name) VALUES (gen_id(), 'r{i}')", null));
            await database.Transactions.CommitAsync(txn);
        }

        List<QueryResultRow> after = await ShowPartitionsAsync(executor);

        Assert.IsTrue(
            after.Skip(1).Any(r => !IsNull(r, "commit_index")),
            "the writes committed Raft entries on at least one data partition");

        foreach (QueryResultRow row in after)
        {
            long id = Long(row, "partition_id");
            if (before[id] is long earlier)
                Assert.GreaterOrEqual(Long(row, "commit_index"), earlier, $"partition {id}: commit index went backwards");
        }
    }

    /// <summary>
    /// The declared schema is the contract a client decodes positionally: every row carries exactly
    /// the declared columns, and each cell has its column's type or is NULL.
    /// </summary>
    [Test]
    public async Task EveryRowCarriesExactlyTheDeclaredColumns()
    {
        CommandExecutor executor = CreateCommandExecutor();
        QuerySchemaHolder schema = new();

        List<QueryResultRow> rows = await ShowPartitionsAsync(executor, schema: schema);

        Assert.IsNotNull(schema.Schema);
        CollectionAssert.AreEqual(ExpectedColumns, schema.Schema!.Select(c => c.Name).ToArray());

        foreach (QueryResultRow row in rows)
        {
            CollectionAssert.AreEquivalent(ExpectedColumns, row.Row.Keys.ToArray());

            foreach (DerivedColumnSchema column in schema.Schema!)
            {
                ColumnType actual = row.Row[column.Name].Type;
                Assert.IsTrue(
                    actual == column.Type || actual == ColumnType.Null,
                    $"partition {Long(row, "partition_id")}: column {column.Name} is {actual}, declared {column.Type}");
            }
        }
    }

    /// <summary>
    /// The statement is server-level. It opens no database, so a context database that does not exist
    /// plays no part.
    /// </summary>
    [Test]
    public async Task NeedsNoDatabaseContext()
    {
        CommandExecutor executor = CreateCommandExecutor();

        Assert.AreEqual(DataPartitions + 1, (await ShowPartitionsAsync(executor, database: "")).Count);
        Assert.AreEqual(DataPartitions + 1, (await ShowPartitionsAsync(executor, database: "nosuchdatabase")).Count);
    }

    /// <summary>Two runs read the same committed map, so they list the same partitions.</summary>
    [Test]
    public async Task RepeatedRunsListTheSamePartitions()
    {
        CommandExecutor executor = CreateCommandExecutor();

        long[] first = (await ShowPartitionsAsync(executor)).Select(r => Long(r, "partition_id")).ToArray();
        long[] second = (await ShowPartitionsAsync(executor)).Select(r => Long(r, "partition_id")).ToArray();

        CollectionAssert.AreEqual(first, second);
    }
}
