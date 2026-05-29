
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using Kahuna;
using Kommander;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Verifies that CamusDB operations work correctly when the embedded Kahuna node
/// runs with InitialPartitions = 3. Uses the in-process fake Raft transport
/// (EmbeddedRaftCommunication) — no real gRPC needed.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestMultiPartitionRouting
{
    private const int Partitions = 3;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    /// <summary>
    /// Single shared node for the whole test class. Using one instance avoids background
    /// actor tasks from a disposed node racing with the next test's actors on the shared
    /// .NET thread pool.
    /// </summary>
    private EmbeddedKahuna? sharedNode;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        sharedNode = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            NodeName = $"test-cluster-{Guid.NewGuid():N}",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = Partitions
        });

        await sharedNode.StartAsync(CancellationToken.None);

        HashSet<int> seenPartitions = new();
        int i = 0;
        while (seenPartitions.Count < Partitions && i < 100)
        {
            string key = $"{i++}/warmup";
            int p = sharedNode.Raft.GetPartitionKey(key);
            if (!seenPartitions.Contains(p))
            {
                TestContext.Out.WriteLine($"Waiting for leader of partition {p} using key {key}");
                await sharedNode.WaitForLeaderAsync(key, CancellationToken.None);
                seenPartitions.Add(p);
            }
        }

        if (seenPartitions.Count < Partitions)
            throw new Exception($"Failed to find all {Partitions} partitions after 100 attempts");

        await sharedNode.FlushAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (sharedNode is not null)
            await sharedNode.DisposeAsync();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// <summary>
    /// Creates a CommandExecutor that uses the shared cluster node.
    /// Each test passes its own unique database name so there is no KV-level interference.
    /// </summary>
    private (string dbname, CommandExecutor executor) CreateExecutor()
    {
        string dbname = Guid.NewGuid().ToString("n");
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        CommandExecutor executor = new(validator, catalogsManager, logger,
            loggerFactory: sharedLoggerFactory, clusterNode: sharedNode!);
        return (dbname, executor);
    }

    private static async Task CleanupAsync(string dbname, CommandExecutor executor)
    {
        try { await executor.CloseDatabase(new CloseDatabaseTicket(dbname)); } catch { }
        try
        {
            string dataPath = Path.Combine(CamusConfig.DataDirectory, dbname);
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
        }
        catch { }
    }

    // -----------------------------------------------------------------------
    // Test 1 — Partition routing invariant
    // -----------------------------------------------------------------------

    /// <summary>
    /// All row write keys for the same table must route to the same Raft partition.
    /// GetPartitionKey uses InversePrefixedStaticHash(key, '/') which strips the last
    /// path segment: "{tableId}/r/{rowId}" → hashes "{tableId}/r" regardless of rowId.
    /// This ensures all rows land on the same partition so full-table scans work.
    /// </summary>
    [Test]
    public void RowKeys_AllRouteToSamePartition()
    {
        EmbeddedKahuna node = sharedNode!;
        string tableId = ObjectIdGenerator.Generate().ToString();

        ObjectIdValue[] rowIds =
        [
            new ObjectIdValue(1, 1, 1),
            new ObjectIdValue(2, 2, 2),
            new ObjectIdValue(3, 3, 3)
        ];

        // All row write keys hash via InversePrefixedStaticHash → SimpleHash("{tableId}:r").
        string firstKey = $"{tableId}:r/{rowIds[0]}";
        int expectedPartition = node.Raft.GetPartitionKey(firstKey);

        int bucketPartition = node.Raft.GetPartitionKey($"{tableId}:r");
        Assert.AreEqual(expectedPartition, bucketPartition, "Row bucket prefix must route to the same partition as row keys");

        Assert.GreaterOrEqual(expectedPartition, 1);
        Assert.LessOrEqual(expectedPartition, Partitions);

        foreach (ObjectIdValue rowId in rowIds.Skip(1))
        {
            string key = $"{tableId}:r/{rowId}";
            int keyPartition = node.Raft.GetPartitionKey(key);
            Assert.AreEqual(expectedPartition, keyPartition,
                $"Row key '{key}' routes to partition {keyPartition} but expected {expectedPartition} " +
                "— all rows must share one partition");
        }
    }

    /// <summary>
    /// All index write keys for the same index must route to the same Raft partition.
    /// "{tableId}/i/{indexId}/{encodedKey}" → hashes "{tableId}/i/{indexId}" regardless of
    /// the encoded key suffix, keeping all entries of one index co-located.
    /// </summary>
    [Test]
    public void IndexKeys_AllRouteToSamePartition()
    {
        EmbeddedKahuna node = sharedNode!;
        string tableId = ObjectIdGenerator.Generate().ToString();
        string indexId = ObjectIdGenerator.Generate().ToString();

        string[] indexKeys =
        [
            $"{tableId}:i:{indexId}/aaaa",
            $"{tableId}:i:{indexId}/zzzz",
            $"{tableId}:i:{indexId}/0000",
            $"{tableId}:i:{indexId}/ffffeeeebbbb000000000000"
        ];

        int expectedPartition = node.Raft.GetPartitionKey(indexKeys[0]);

        int bucketPartition = node.Raft.GetPartitionKey($"{tableId}:i:{indexId}");
        Assert.AreEqual(expectedPartition, bucketPartition, "Bucket prefix must route to the same partition as index keys");

        Assert.GreaterOrEqual(expectedPartition, 1);
        Assert.LessOrEqual(expectedPartition, Partitions);

        foreach (string key in indexKeys.Skip(1))
        {
            int keyPartition = node.Raft.GetPartitionKey(key);
            Assert.AreEqual(expectedPartition, keyPartition,
                $"Index key '{key}' routes to partition {keyPartition} " +
                $"but expected {expectedPartition} — all index entries must share one partition");
        }
    }

    // -----------------------------------------------------------------------
    // Test 2 — Insert + full-table scan
    // -----------------------------------------------------------------------

    /// <summary>
    /// Insert 10 rows into a table, then do a full-table scan and verify all 10 come back.
    /// This exercises the row-write path and the LocateAndGetByBucket scan path across
    /// a 3-partition node.
    /// </summary>
    [Test]
    public async Task TenRowInsert_FullTableScan_ReturnsAllRows()
    {
        (string dbname, CommandExecutor executor) = CreateExecutor();

        try
        {
            CreateDatabaseTicket dbTicket = new(name: dbname, ifNotExists: false);
            DatabaseDescriptor database = await executor.CreateDatabase(dbTicket);

            KvTransaction txnState = await database.Transactions.BeginAsync();

            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname,
                tableName: "robots",
                columns:
                [
                    new ColumnInfo("id",   ColumnType.Id),
                    new ColumnInfo("name", ColumnType.String, notNull: true),
                    new ColumnInfo("year", ColumnType.Integer64)
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            ));

            await database.Transactions.CommitAsync(txnState);

            // Insert 10 rows.
            txnState = await database.Transactions.BeginAsync();

            for (int i = 0; i < 10; i++)
            {
                await executor.Insert(new InsertTicket(
                    txnState: txnState,
                    databaseName: dbname,
                    tableName: "robots",
                    values:
                    [
                        new Dictionary<string, ColumnValue>
                        {
                            { "id",   new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                            { "name", new ColumnValue(ColumnType.String, $"robot-{i}") },
                            { "year", new ColumnValue(ColumnType.Integer64, 2020 + i) }
                        }
                    ]
                ));
            }

            await database.Transactions.CommitAsync(txnState);

            // Full-table scan.
            txnState = await database.Transactions.BeginAsync();

            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(new QueryTicket(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                index: null,
                projection: null,
                where: null,
                filters: null,
                orderBy: null,
                limit: null,
                offset: null,
                parameters: null
            ));

            List<QueryResultRow> rows = await cursor.ToListAsync();

            Assert.AreEqual(10, rows.Count, "Full-table scan must return all 10 inserted rows");
        }
        finally
        {
            await CleanupAsync(dbname, executor);
        }
    }

    // -----------------------------------------------------------------------
    // Test 3 — Cross-partition 2PC
    // -----------------------------------------------------------------------

    /// <summary>
    /// Two tables with different tableIds may hash to different partitions.
    /// A single transaction that inserts into both exercises the 2PC path.
    /// Both tables must contain their rows after commit.
    /// </summary>
    [Test]
    public async Task TwoTableInsert_SingleTransaction_BothTablesHaveRows()
    {
        (string dbname, CommandExecutor executor) = CreateExecutor();

        try
        {
            CreateDatabaseTicket dbTicket = new(name: dbname, ifNotExists: false);
            DatabaseDescriptor database = await executor.CreateDatabase(dbTicket);

            // Create two tables.
            foreach (string tableName in new[] { "table_a", "table_b" })
            {
                KvTransaction createTxn = await database.Transactions.BeginAsync();

                await executor.CreateTable(new CreateTableTicket(
                    databaseName: dbname,
                    tableName: tableName,
                    columns:
                    [
                        new ColumnInfo("id",  ColumnType.Id),
                        new ColumnInfo("val", ColumnType.Integer64)
                    ],
                    constraints:
                    [
                        new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                            [new ColumnIndexInfo("id", OrderType.Ascending)])
                    ],
                    ifNotExists: false
                ));

                await database.Transactions.CommitAsync(createTxn);
            }

            // Insert into both tables in ONE transaction.
            KvTransaction txn = await database.Transactions.BeginAsync();

            for (int i = 0; i < 5; i++)
            {
                foreach (string tableName in new[] { "table_a", "table_b" })
                {
                    await executor.Insert(new InsertTicket(
                        txnState: txn,
                        databaseName: dbname,
                        tableName: tableName,
                        values:
                        [
                            new Dictionary<string, ColumnValue>
                            {
                                { "id",  new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                                { "val", new ColumnValue(ColumnType.Integer64, (long)i) }
                            }
                        ]
                    ));
                }
            }

            await database.Transactions.CommitAsync(txn);

            // Verify both tables have 5 rows each.
            foreach (string tableName in new[] { "table_a", "table_b" })
            {
                KvTransaction queryTxn = await database.Transactions.BeginAsync();

                (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(new QueryTicket(
                    txnState: queryTxn,
                    databaseName: dbname,
                    tableName: tableName,
                    index: null,
                    projection: null,
                    where: null,
                    filters: null,
                    orderBy: null,
                    limit: null,
                    offset: null,
                    parameters: null
                ));

                List<QueryResultRow> rows = await cursor.ToListAsync();

                Assert.AreEqual(5, rows.Count,
                    $"Table '{tableName}' must have 5 rows after cross-partition 2PC commit");
            }
        }
        finally
        {
            await CleanupAsync(dbname, executor);
        }
    }

    // -----------------------------------------------------------------------
    // Test 4 — Schema persistence across close/reopen (cluster shared node)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Closing and reopening a database must reload schema from KV on the shared node.
    /// </summary>
    [Test]
    public async Task MetaSchema_PersistsAfterCloseAndReopen()
    {
        (string dbname, CommandExecutor executor) = CreateExecutor();

        try
        {
            DatabaseDescriptor database = await executor.CreateDatabase(
                new CreateDatabaseTicket(dbname, ifNotExists: false));

            KvTransaction txn = await database.Transactions.BeginAsync();
            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname,
                tableName: "robots",
                columns:
                [
                    new ColumnInfo("id", ColumnType.Id),
                    new ColumnInfo("name", ColumnType.String, notNull: true)
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            ));
            await database.Transactions.CommitAsync(txn);

            Assert.IsTrue(database.Schema.Tables.ContainsKey("robots"));

            await executor.CloseDatabase(new CloseDatabaseTicket(dbname));

            DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
            Assert.IsTrue(reopened.Schema.Tables.ContainsKey("robots"),
                "Schema must survive close/reopen on the shared cluster node");
            Assert.AreEqual(2, reopened.Schema.Tables["robots"].Columns!.Count,
                "Table must retain id and name columns after reopen");
        }
        finally
        {
            await CleanupAsync(dbname, executor);
        }
    }

    // -----------------------------------------------------------------------
    // Test 5 — Regression: mirror TestRowUpdater / TestRowMultiInsertor / TestExecuteSqlSelect
    // -----------------------------------------------------------------------

    /// <summary>
    /// Mirrors <c>TestRowUpdater.TestUpdateMany</c> — bulk update + filtered full-table scan.
    /// </summary>
    [Test]
    public async Task Regression_RowUpdater_UpdateMany_FullScanComplete()
    {
        (string dbname, CommandExecutor executor) = CreateExecutor();

        try
        {
            DatabaseDescriptor database = await SetupRobotsTableWith25Rows(dbname, executor);

            KvTransaction txn = await database.Transactions.BeginAsync();

            UpdateResult updateResult = await executor.Update(new UpdateTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                plainValues: new() { { "name", new ColumnValue(ColumnType.String, "updated value") } },
                exprValues: null,
                where: null,
                filters: [new QueryFilter("year", ">", new ColumnValue(ColumnType.Integer64, 2010))],
                parameters: null
            ));
            Assert.AreEqual(14, updateResult.UpdatedRows);

            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(new QueryTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                index: null,
                projection: null,
                where: null,
                filters: [new QueryFilter("year", ">", new ColumnValue(ColumnType.Integer64, 2010))],
                orderBy: null,
                limit: null,
                offset: null,
                parameters: null
            ));

            List<QueryResultRow> rows = await cursor.ToListAsync();
            Assert.AreEqual(14, rows.Count);
            foreach (QueryResultRow row in rows)
                Assert.AreEqual("updated value", row.Row["name"].StrValue);
        }
        finally
        {
            await CleanupAsync(dbname, executor);
        }
    }

    /// <summary>
    /// Mirrors <c>TestRowMultiInsertor.TestCheckSuccessfulMultiInsertWithQueryIndex</c>.
    /// </summary>
    [Test]
    public async Task Regression_RowMultiInsertor_IndexScan_ReturnsAllRows()
    {
        (string dbname, CommandExecutor executor) = CreateExecutor();

        try
        {
            DatabaseDescriptor database = await executor.CreateDatabase(
                new CreateDatabaseTicket(dbname, ifNotExists: false));

            KvTransaction setup = await database.Transactions.BeginAsync();
            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname,
                tableName: "user_robots",
                columns:
                [
                    new ColumnInfo("id", ColumnType.Id),
                    new ColumnInfo("robots_id", ColumnType.Id, notNull: true),
                    new ColumnInfo("amount", ColumnType.Integer64)
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)]),
                    new ConstraintInfo(ConstraintType.IndexMulti, "robots_id_idx",
                        [new ColumnIndexInfo("robots_id", OrderType.Ascending)])
                ],
                ifNotExists: false
            ));
            await database.Transactions.CommitAsync(setup);

            KvTransaction insertTxn = await database.Transactions.BeginAsync();
            for (int i = 0; i < 10; i++)
            {
                await executor.Insert(new InsertTicket(
                    txnState: insertTxn,
                    databaseName: dbname,
                    tableName: "user_robots",
                    values:
                    [
                        new Dictionary<string, ColumnValue>
                        {
                            { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                            { "robots_id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                            { "amount", new ColumnValue(ColumnType.Integer64, i * 1000L) }
                        }
                    ]
                ));
            }
            await database.Transactions.CommitAsync(insertTxn);

            // --- full-table scan returns 10 ---
            KvTransaction scanTxn = await database.Transactions.BeginAsync();
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> scanCursor) = await executor.Query(new QueryTicket(
                txnState: scanTxn,
                databaseName: dbname,
                tableName: "user_robots",
                index: null,
                projection: null,
                filters: null,
                where: null,
                orderBy: null,
                limit: null,
                offset: null,
                parameters: null
            ));
            List<QueryResultRow> scanRows = await scanCursor.ToListAsync();
            Assert.AreEqual(10, scanRows.Count, "Full-table scan must return 10 rows");

            KvTransaction queryTxn = await database.Transactions.BeginAsync();
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(new QueryTicket(
                txnState: queryTxn,
                databaseName: dbname,
                tableName: "user_robots",
                index: "robots_id_idx",
                projection: null,
                where: null,
                filters: null,
                orderBy: null,
                limit: null,
                offset: null,
                parameters: null
            ));

            List<QueryResultRow> rows = await cursor.ToListAsync();
            Assert.AreEqual(10, rows.Count);
            List<long> amounts = rows.Select(r => r.Row["amount"].LongValue).OrderBy(x => x).ToList();
            for (int i = 0; i < 10; i++)
                Assert.AreEqual(i * 1000L, amounts[i]);
        }
        finally
        {
            await CleanupAsync(dbname, executor);
        }
    }

    /// <summary>
    /// Mirrors <c>TestExecuteSqlSelect.TestExecuteSelectOrderBy</c> — SQL full-table scan.
    /// </summary>
    [Test]
    public async Task Regression_ExecuteSqlSelect_OrderBy_ReturnsAllRows()
    {
        (string dbname, CommandExecutor executor) = CreateExecutor();

        try
        {
            DatabaseDescriptor database = await SetupRobotsTableWith25Rows(dbname, executor);

            KvTransaction txn = await database.Transactions.BeginAsync();
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(
                    txnState: txn,
                    database: dbname,
                    sql: "SELECT * FROM robots ORDER BY year",
                    parameters: null
                ));

            List<QueryResultRow> rows = await cursor.ToListAsync();
            Assert.AreEqual(25, rows.Count);
            Assert.AreEqual(2000L, rows[0].Row["year"].LongValue);
            Assert.AreEqual(2001L, rows[1].Row["year"].LongValue);
            Assert.AreEqual(2024L, rows[24].Row["year"].LongValue);
        }
        finally
        {
            await CleanupAsync(dbname, executor);
        }
    }

    /// <summary>
    /// Creates the robots table and inserts 25 rows (same shape as TestRowUpdater.SetupBasicTable).
    /// </summary>
    private static async Task<DatabaseDescriptor> SetupRobotsTableWith25Rows(
        string dbname, CommandExecutor executor)
    {
        DatabaseDescriptor database = await executor.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64),
                new ColumnInfo("enabled", ColumnType.Bool)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        ));

        for (int i = 0; i < 25; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values:
                [
                    new Dictionary<string, ColumnValue>
                    {
                        { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new ColumnValue(ColumnType.String, $"some name {i}") },
                        { "year", new ColumnValue(ColumnType.Integer64, 2000L + i) },
                        { "enabled", new ColumnValue(ColumnType.Bool, false) }
                    }
                ]
            ));
        }

        await database.Transactions.CommitAsync(txn);
        return database;
    }
}
