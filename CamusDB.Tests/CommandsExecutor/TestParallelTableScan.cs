/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Full primary-row scans with <see cref="CamusDBOptions.MaxQueryParallelism"/> &gt; 1 must be
/// observably identical to the sequential scan: same rows, same order (chunks are consumed in
/// dispatch order), same filtering/limit/aggregate results. The row count is chosen to span
/// several decode chunks so the pipeline actually runs multi-chunk. A second engine configured
/// with parallelism 1 over the same database provides the sequential comparison — the setting
/// is fixed per engine at construction, so comparing configurations requires two engines.
/// </summary>
[NonParallelizable]
public sealed class TestParallelTableScan : BaseTest
{
    private const int RowCount = 300;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { MaxQueryParallelism = 4 };

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "readings",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("num", ColumnType.Integer64, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        KvTransaction txn = await database.Transactions.BeginAsync();

        for (int i = 0; i < RowCount; i++)
        {
            InsertTicket ticket = new(
                txnState: txn,
                databaseName: dbname,
                tableName: "readings",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, $"n{i:D4}") },
                        { "num", new(ColumnType.Integer64, (long)i) },
                    }
                });
            await executor.Insert(ticket);
        }

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> RunSql(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txn,
            database: dbname,
            sql: sql,
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();

        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    /// <summary>
    /// Runs the same SQL on the fixture's parallel engine and on an independent sequential
    /// engine opened over the same database, and asserts identical row ids in identical order.
    /// </summary>
    private async Task AssertMatchesSequential(
        string dbname,
        DatabaseDescriptor parallelDb,
        CommandExecutor parallelExecutor,
        string sql)
    {
        CommandExecutor sequentialExecutor = CreateCommandExecutor(Options with { MaxQueryParallelism = 1 });
        DatabaseDescriptor sequentialDb = await sequentialExecutor.OpenDatabase(dbname);
        TrackDatabase(dbname, sequentialExecutor);

        List<QueryResultRow> parallel = await RunSql(parallelDb, parallelExecutor, dbname, sql);
        List<QueryResultRow> sequential = await RunSql(sequentialDb, sequentialExecutor, dbname, sql);

        Assert.AreEqual(sequential.Count, parallel.Count, $"Row count differs for: {sql}");

        for (int i = 0; i < sequential.Count; i++)
        {
            Assert.AreEqual(sequential[i].RowId.ToString(), parallel[i].RowId.ToString(),
                $"Row id at position {i} differs for: {sql}");
        }
    }

    [Test]
    public async Task ParallelScan_SelectAll_ReturnsAllRowsInSequentialOrder()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> rows = await RunSql(database, executor, dbname, "SELECT id, name, num FROM readings");

        Assert.AreEqual(RowCount, rows.Count, "Parallel scan must return every row exactly once");

        // The pipeline consumes chunks in dispatch order, so rows arrive in ascending row-id
        // (ordinal hex) order — the sequential scan's order.
        for (int i = 1; i < rows.Count; i++)
        {
            Assert.Less(
                string.CompareOrdinal(rows[i - 1].RowId.ToString(), rows[i].RowId.ToString()), 0,
                $"Row ids must be strictly ascending; position {i} is out of order");
        }

        HashSet<long> nums = rows.Select(r => r.Row["num"].LongValue).ToHashSet();
        Assert.AreEqual(RowCount, nums.Count, "Every inserted row must appear exactly once");
    }

    [Test]
    public async Task ParallelScan_WithResidualFilter_MatchesSequential()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        string sql = "SELECT id, name, num FROM readings WHERE num >= 100 AND num < 250";
        List<QueryResultRow> rows = await RunSql(database, executor, dbname, sql);

        Assert.AreEqual(150, rows.Count, "Residual filter must keep exactly the matching rows");
        Assert.IsTrue(rows.All(r => r.Row["num"].LongValue is >= 100 and < 250));

        await AssertMatchesSequential(dbname, database, executor, sql);
    }

    [Test]
    public async Task ParallelScan_WithLimit_MatchesSequential()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        // LIMIT below one chunk and LIMIT spanning several chunks both cut at the same rows
        // the sequential scan would return, because chunk order is scan order.
        await AssertMatchesSequential(dbname, database, executor,
            "SELECT id, name FROM readings LIMIT 10");
        await AssertMatchesSequential(dbname, database, executor,
            "SELECT id, name FROM readings LIMIT 170");
    }

    /// <summary>
    /// Manual wall-clock comparison of parallel vs sequential scan over a larger table.
    /// Timing assertions are inherently flaky on shared runners, so this reports numbers
    /// instead of asserting a speedup; it is excluded from normal runs.
    /// </summary>
    [Test]
    [Explicit("Benchmark-style timing comparison; run manually to measure the parallel scan")]
    public async Task ParallelScan_Benchmark_ReportTimings()
    {
        const int benchRows = 20_000;

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "bench",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("num", ColumnType.Integer64, notNull: true),
                new("payload", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        string payload = new('x', 200);
        KvTransaction insertTxn = await database.Transactions.BeginAsync();
        for (int i = 0; i < benchRows; i++)
        {
            if (i > 0 && i % 5000 == 0)
            {
                await database.Transactions.CommitAsync(insertTxn);
                insertTxn = await database.Transactions.BeginAsync();
            }

            InsertTicket ticket = new(
                txnState: insertTxn,
                databaseName: dbname,
                tableName: "bench",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, $"name-{i:D6}") },
                        { "num", new(ColumnType.Integer64, (long)i) },
                        { "payload", new(ColumnType.String, payload) },
                    }
                });
            await executor.Insert(ticket);
        }
        await database.Transactions.CommitAsync(insertTxn);

        CommandExecutor sequentialExecutor = CreateCommandExecutor(Options with { MaxQueryParallelism = 1 });
        DatabaseDescriptor sequentialDb = await sequentialExecutor.OpenDatabase(dbname);
        TrackDatabase(dbname, sequentialExecutor);

        const string sql = "SELECT id, name, num FROM bench WHERE num >= 3000 AND num < 9000";

        // Warm both engines (decode plans, plan cache, Kahuna pages), then measure.
        await RunSql(sequentialDb, sequentialExecutor, dbname, sql);
        await RunSql(database, executor, dbname, sql);

        System.Diagnostics.Stopwatch sw = System.Diagnostics.Stopwatch.StartNew();
        List<QueryResultRow> seqRows = await RunSql(sequentialDb, sequentialExecutor, dbname, sql);
        sw.Stop();
        long sequentialMs = sw.ElapsedMilliseconds;

        sw.Restart();
        List<QueryResultRow> parRows = await RunSql(database, executor, dbname, sql);
        sw.Stop();
        long parallelMs = sw.ElapsedMilliseconds;

        Assert.AreEqual(seqRows.Count, parRows.Count, "Both engines must return the same rows");

        TestContext.Out.WriteLine(
            $"Scan of {benchRows} rows (filtered to {parRows.Count}): sequential={sequentialMs}ms parallel(x4)={parallelMs}ms");
    }

    [Test]
    public async Task ParallelScan_OrderByAndAggregate_MatchSequential()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        await AssertMatchesSequential(dbname, database, executor,
            "SELECT id, name, num FROM readings ORDER BY num DESC");

        List<QueryResultRow> count = await RunSql(database, executor, dbname,
            "SELECT COUNT(*) AS c FROM readings");
        Assert.AreEqual(1, count.Count);
        Assert.AreEqual(RowCount, count[0].Row["c"].LongValue);

        List<QueryResultRow> sum = await RunSql(database, executor, dbname,
            "SELECT SUM(num) AS s FROM readings");
        Assert.AreEqual(1, sum.Count);
        Assert.AreEqual((long)RowCount * (RowCount - 1) / 2, sum[0].Row["s"].LongValue);
    }
}
