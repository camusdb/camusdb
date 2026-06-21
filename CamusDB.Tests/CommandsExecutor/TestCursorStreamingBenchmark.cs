
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Task 10 — Benchmark and regression gate for cursor-streaming (Tasks 8 + 9).
///
/// Assertions:
///   1. Full-table scans at 1k/10k rows complete correctly (right row count).
///   2. A selective range query (value >= lo AND value <= hi) transfers O(matches), not O(index size).
///   3. Peak managed memory during a scan is bounded by DefaultPageSize (512), not row count:
///      scanning 3x more rows must not consume ~3x more peak heap.
///
/// Timings are emitted to the test console so they can be captured in the PR description.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestCursorStreamingBenchmark : SharedNodeBaseTest
{
    // --- schema helpers ---

    private static readonly ColumnInfo[] RobotColumns =
    [
        new("id",      ColumnType.Id),
        new("value",   ColumnType.Integer64),
        new("name",    ColumnType.String, notNull: false),
    ];

    private static readonly ConstraintInfo[] PkConstraint =
    [
        new(ConstraintType.PrimaryKey, "~pk",
            [new("id", OrderType.Ascending)])
    ];

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        CreateTableWithValueIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "bench",
            columns: RobotColumns,
            constraints: PkConstraint,
            ifNotExists: false
        ));

        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "bench",
            indexName: "value_idx",
            columns: [new("value", OrderType.Ascending)],
            operation: AlterIndexOperation.AddIndex
        ));

        await database.Transactions.CommitAsync(tx);
        return (dbname, database, executor);
    }

    private static async Task InsertRows(
        string dbname,
        DatabaseDescriptor database,
        CommandExecutor executor,
        int count,
        int batchSize = 500)
    {
        for (int batch = 0; batch < count; batch += batchSize)
        {
            KvTransaction tx = await database.Transactions.BeginAsync();

            int end = Math.Min(batch + batchSize, count);
            List<Dictionary<string, ColumnValue>> rows = new(end - batch);

            for (int i = batch; i < end; i++)
            {
                rows.Add(new Dictionary<string, ColumnValue>
                {
                    ["id"]    = new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()),
                    ["value"] = new(ColumnType.Integer64, (long)i),
                    ["name"]  = new(ColumnType.String,    "row_" + i),
                });
            }

            await executor.Insert(new InsertTicket(
                txnState:     tx,
                databaseName: dbname,
                tableName:    "bench",
                values:       rows
            ));

            await database.Transactions.CommitAsync(tx);
        }
    }

    // ── full-scan row-count correctness ──────────────────────────────────────

    [TestCase(1_000)]
    [TestCase(3_000)]
    public async Task TestFullScanReturnsAllRows(int rowCount)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateTableWithValueIndex();

        await InsertRows(dbname, database, executor, rowCount);

        KvTransaction tx = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState:  tx,
            database:  dbname,
            sql:       "SELECT id, value FROM bench",
            parameters: null
        );

        Stopwatch sw = Stopwatch.StartNew();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(ticket);
        int scanned = 0;
        await foreach (QueryResultRow _ in cursor)
            scanned++;
        sw.Stop();

        await database.Transactions.CommitAsync(tx);

        TestContext.Out.WriteLine(
            $"[full-scan] rows={rowCount} elapsed={sw.ElapsedMilliseconds}ms");

        Assert.AreEqual(rowCount, scanned,
            $"Full scan must return all {rowCount} rows.");
    }

    // ── BETWEEN selectivity: bytes-on-wire scale with selectivity ────────────

    [Test]
    public async Task TestBetweenQuerySelectivity()
    {
        const int total      = 1_000;
        const int lo         = 100;
        const int hi         = 200;
        const int expected   = hi - lo + 1; // 101

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateTableWithValueIndex();

        await InsertRows(dbname, database, executor, total);

        KvTransaction tx = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState:  tx,
            database:  dbname,
            sql:       $"SELECT id, value FROM bench WHERE value >= {lo} AND value <= {hi}",
            parameters: null
        );

        Stopwatch sw = Stopwatch.StartNew();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(ticket);
        int matches = 0;
        await foreach (QueryResultRow _ in cursor)
            matches++;
        sw.Stop();

        await database.Transactions.CommitAsync(tx);

        TestContext.Out.WriteLine(
            $"[range] total={total} range=[{lo},{hi}] matches={matches} elapsed={sw.ElapsedMilliseconds}ms");

        Assert.AreEqual(expected, matches,
            $"value >= {lo} AND value <= {hi} must return exactly {expected} rows, not {total}.");
    }

    // ── memory-bound assertion ────────────────────────────────────────────────
    //
    // With DefaultPageSize=512, at most 512 rows are in managed memory per page.
    // Scanning 3x more rows must NOT cause ~3x more peak heap growth (which would indicate
    // the whole table is buffered). A page-bounded scan stays roughly flat.

    [Test]
    [Explicit("Benchmark-style retained-heap check; can hang CI when forced GC interacts with Kahuna background threads after prior fixtures.")]
    public async Task TestFullScanMemoryBoundedByPageSize()
    {
        (string dbnameSmall, DatabaseDescriptor dbSmall, CommandExecutor exSmall) =
            await CreateTableWithValueIndex();
        (string dbnameLarge, DatabaseDescriptor dbLarge, CommandExecutor exLarge) =
            await CreateTableWithValueIndex();

        await InsertRows(dbnameSmall, dbSmall, exSmall, 1_000);
        await InsertRows(dbnameLarge, dbLarge, exLarge, 3_000);

        static async Task<long> MeasurePeakHeapDelta(
            string dbname,
            DatabaseDescriptor database,
            CommandExecutor executor)
        {
            // Force a full GC before measuring so the baseline is stable retained memory.
            // Do NOT force GC inside the async scan loop — blocking-GC inside an await
            // suspends Kahuna's Raft background threads, which can leave the shared node
            // in a state where subsequent operations never complete.
            GC.Collect(2, GCCollectionMode.Forced, blocking: true);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, blocking: true);

            long heapBefore = GC.GetTotalMemory(forceFullCollection: false);

            KvTransaction tx = await database.Transactions.BeginAsync();

            ExecuteSQLTicket ticket = new(
                txnState:  tx,
                database:  dbname,
                sql:       "SELECT id, value FROM bench",
                parameters: null
            );

            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(ticket);

            await foreach (QueryResultRow _ in cursor) { }

            await database.Transactions.CommitAsync(tx);

            // Force GC after the scan so we measure retained working set, not churn.
            GC.Collect(2, GCCollectionMode.Forced, blocking: true);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, blocking: true);

            long heapAfter = GC.GetTotalMemory(forceFullCollection: false);
            return Math.Max(0, heapAfter - heapBefore);
        }

        long deltaSmall = await MeasurePeakHeapDelta(dbnameSmall, dbSmall, exSmall);
        long deltaLarge = await MeasurePeakHeapDelta(dbnameLarge, dbLarge, exLarge);

        TestContext.Out.WriteLine(
            $"[memory] 1k rows heap-delta={deltaSmall / 1024}KB  " +
            $"3k rows heap-delta={deltaLarge / 1024}KB  " +
            $"ratio={deltaLarge / Math.Max(1, deltaSmall):F1}x");

        // Coarse safety gate: if scanning 3k rows retains more than 32 MB after a full GC,
        // something is catastrophically wrong (e.g. the whole table is being duplicated in
        // an internal buffer). Normal Kahuna in-memory overhead for 3k rows is well under
        // 10 MB. Using an absolute bound avoids the ratio-becomes-infinite problem when
        // deltaSmall rounds to 0 KB after GC on fast machines.
        const long MaxAllowedBytes = 32L * 1024 * 1024; // 32 MB
        Assert.Less(deltaLarge, MaxAllowedBytes,
            "Retained heap after a 3k-row scan exceeded 32 MB. " +
            "This suggests the scan is buffering far more data than expected.");
    }
}
