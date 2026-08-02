
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
/// Parity matrix for the covering (index-only) and batch-fetch non-covering index scan paths.
/// Each test compares a covered query against a forced-non-covered baseline (SELECT * or adding
/// a non-indexed column to the projection) to verify both paths return identical results.
///
/// Every "covered" test asserts rows_read = 0 from EXPLAIN ANALYZE, which proves the index-only
/// path ran. If the cost model falls back to a full table scan the assertion fails immediately
/// rather than the test passing silently on the wrong path.
///
/// All predicates are deliberately selective (≤ 10% of table rows) so the cost model chooses
/// the secondary index over a full table scan.
///
/// Dimensions covered:
///   - unique vs non-unique secondary indexes
///   - NULL values in the indexed column
///   - read-committed intent visibility and pinned snapshot isolation
///   - UPDATE and DELETE visibility (covering scan reflects mutations committed before the scan)
///   - batch fetch correctness (multi-page paging via a temporarily small IndexScanFetchBatchSize)
/// </summary>
[NonParallelizable]
public class TestQueryScannerCoveringParity : BaseTest
{
    // ── Fixture helpers ───────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupWithUniqueNameIndex(int rowCount = 100)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",      ColumnType.Id),
                new("name",    ColumnType.String, notNull: true),
                new("year",    ColumnType.Integer64),
                new("enabled", ColumnType.Bool),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));

        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddUniqueIndex
        ));

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 1; i <= rowCount; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",      new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name",    new(ColumnType.String, "robot" + i) },
                        { "year",    new(ColumnType.Integer64, (long)i) },
                        { "enabled", new(ColumnType.Bool, i % 2 == 0) },
                    }
                }
            ));
        }
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupWithYearIndex(int rowCount = 100, bool includeNullRows = false, CamusDBOptions? options = null)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options ?? Options);

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",      ColumnType.Id),
                new("name",    ColumnType.String, notNull: true),
                new("year",    ColumnType.Integer64),
                new("enabled", ColumnType.Bool),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));

        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "robots",
            indexName: "year_idx",
            columns: new ColumnIndexInfo[] { new("year", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        ));

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 1; i <= rowCount; i++)
        {
            Dictionary<string, ColumnValue> values = new()
            {
                { "id",      new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "name",    new(ColumnType.String, "robot" + i) },
                { "enabled", new(ColumnType.Bool, i % 2 == 0) },
            };

            // When includeNullRows is true, every 10th row has a NULL year.
            values["year"] = includeNullRows && i % 10 == 0
                ? ColumnValue.Null
                : new(ColumnType.Integer64, (long)i);

            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new() { values }
            ));
        }
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> RunSql(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql,
        CamusIsolationLevel? isolationLevel = null)
    {
        KvTransaction txn = await database.Transactions.BeginAsync(isolationLevel);
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    private static async Task RunNonQuery(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// Returns the rows_read value from the first index scan node in EXPLAIN ANALYZE output,
    /// or null if no index-lookup / index-range-scan node appears (which means the cost model
    /// chose a full table scan and the covering path was never invoked).
    /// </summary>
    private static async Task<long?> ExplainAnalyzeScanRowsRead(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname,
            sql: "EXPLAIN (ANALYZE) " + sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);

        foreach (QueryResultRow r in rows)
        {
            if (r.Row.TryGetValue("node", out ColumnValue? node)
                && node.StrValue is "index-lookup" or "index-range-scan"
                && r.Row.TryGetValue("rows_read", out ColumnValue? rr)
                && rr.Type == ColumnType.Integer64)
                return rr.LongValue;
        }
        return null;
    }

    // ── Parity: unique index ──────────────────────────────────────────────────

    [Test]
    public async Task UniqueIndex_CoveredLookup_MatchesNonCoveredBaseline()
    {
        // name_idx is unique. SELECT name WHERE name = 'robot42' projects only the indexed column
        // (covered). SELECT * WHERE name = 'robot42' fetches the full row (non-covered). Both must
        // match. Equality on the unique index → 1 row out of 100 (~1% selectivity), guaranteeing
        // the cost model chooses the index. rows_read = 0 confirms the covering path ran.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupWithUniqueNameIndex();

        List<QueryResultRow> baseline = await RunSql(executor, database, dbname,
            "SELECT * FROM robots WHERE name = 'robot42'");
        List<QueryResultRow> covered = await RunSql(executor, database, dbname,
            "SELECT name FROM robots WHERE name = 'robot42'");

        Assert.AreEqual(1, baseline.Count, "exactly one row with name = 'robot42'");
        Assert.AreEqual(baseline.Count, covered.Count,
            "covered and non-covered must return the same row count");
        Assert.AreEqual(
            baseline[0].Row["name"].StrValue,
            covered[0].Row["name"].StrValue,
            "covered scan must return the same name value as the non-covered baseline");

        long? rowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT name FROM robots WHERE name = 'robot42'");
        Assert.IsNotNull(rowsRead, "cost model must have chosen an index scan (not a table scan)");
        Assert.AreEqual(0L, rowsRead!.Value, "covering path must read zero primary rows");
    }

    [Test]
    public async Task UniqueIndex_NullRows_NotReturnedByCoveredScan()
    {
        // Unique indexes omit entries for NULL-valued rows (standard SQL semantics — NULLs are
        // distinct and exempt from the unique constraint). A covering scan on a unique index
        // must not surface rows whose indexed column is NULL.
        //
        // Fixture: 100 named rows + 20 NULL-name rows = 120 total rows. The 20 NULL-name rows
        // have no entry in name_idx so they cannot be returned by an index scan.
        // Predicate: WHERE name > 'robot97' → 'robot98', 'robot99' (2 rows, ~1.7% of 120)
        // forces the cost model to use the index. rows_read = 0 confirms covering path ran.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String),   // nullable — allows NULL index omission
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));

        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddUniqueIndex
        ));

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 1; i <= 100; i++)
            await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname,
                tableName: "robots", values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "robot" + i) },
                        { "year", new(ColumnType.Integer64, (long)i) },
                    }
                }));
        for (int i = 101; i <= 120; i++)
            await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname,
                tableName: "robots", values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", ColumnValue.Null },   // NULL — no unique-index entry
                        { "year", new(ColumnType.Integer64, (long)i) },
                    }
                }));
        await database.Transactions.CommitAsync(txn);

        // 'robot98' and 'robot99' sort above 'robot97' in lexicographic order; 'robot100'
        // does not ('1' < '9' in lex). So exactly 2 rows match.
        List<QueryResultRow> covered = await RunSql(executor, database, dbname,
            "SELECT name FROM robots WHERE name > 'robot97'");

        Assert.AreEqual(2, covered.Count,
            "only the 2 named rows above 'robot97' should appear; the 20 NULL-name rows have no index entry");
        Assert.IsTrue(covered.All(r => r.Row["name"].Type != ColumnType.Null),
            "no row in the covered result should carry a NULL name");

        long? rowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT name FROM robots WHERE name > 'robot97'");
        Assert.IsNotNull(rowsRead, "cost model must have chosen an index scan (not a table scan)");
        Assert.AreEqual(0L, rowsRead!.Value, "covering path must read zero primary rows");
    }

    // ── Parity: non-unique index, NULL-valued rows ────────────────────────────

    [Test]
    public async Task NonUniqueIndex_NullRows_CoveredMatchesNonCovered()
    {
        // Non-unique (Multi) indexes store entries for NULL-valued rows. A covering scan and a
        // non-covering scan must both omit null-year rows when a non-null predicate is applied,
        // because NULL > 90 evaluates to false in CamusDB's expression evaluator.
        //
        // Fixture: 100 rows, every 10th has NULL year (i=10,20,...,100).
        // Predicate: WHERE year > 90 → rows 91–99 are non-null; row 90 and row 100 are null.
        // That is 9 rows out of 100 (~9% selectivity) — forces the cost model to use the index.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupWithYearIndex(rowCount: 100, includeNullRows: true);

        List<QueryResultRow> baseline = await RunSql(executor, database, dbname,
            "SELECT * FROM robots WHERE year > 90");
        List<QueryResultRow> covered = await RunSql(executor, database, dbname,
            "SELECT year FROM robots WHERE year > 90");

        // Rows 91–99 have non-null year; rows 90 and 100 have null year and are excluded.
        Assert.AreEqual(9, baseline.Count,
            "baseline must return the 9 non-null rows with year > 90");
        Assert.AreEqual(baseline.Count, covered.Count,
            "covered and non-covered must return the same row count with null rows present");

        HashSet<long> baselineYears = baseline.Select(r => r.Row["year"].LongValue).ToHashSet();
        HashSet<long> coveredYears  = covered.Select(r => r.Row["year"].LongValue).ToHashSet();
        Assert.That(coveredYears, Is.EquivalentTo(baselineYears),
            "covered scan must return the same year values as the non-covered baseline");

        long? rowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT year FROM robots WHERE year > 90");
        Assert.IsNotNull(rowsRead, "cost model must have chosen an index scan (not a table scan)");
        Assert.AreEqual(0L, rowsRead!.Value, "covering path must read zero primary rows");
    }

    // ── Parity: read-committed visibility ─────────────────────────────────────

    [Test]
    public async Task CoveredScan_DoesNotSeeUncommittedRow()
    {
        // A read-committed covering scan must not expose a foreign uncommitted intent. Kahuna's
        // paged range scan may wait for that intent's outcome to preserve its page snapshot, so
        // the scan and rollback must run concurrently rather than waiting on each other.
        //
        // year = 999 is far outside 1–50, so 0 or 1 row out of 51 → highly selective.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupWithYearIndex(rowCount: 50);

        string rowId = ObjectIdGenerator.Generate().ToString();

        // Begin txn1 but do not commit — insert year=999, start the covering scan, then resolve
        // the foreign intent as aborted. The scan must return the previously committed view.
        KvTransaction txn1 = await database.Transactions.BeginAsync();
        List<QueryResultRow> uncommittedView;
        try
        {
            await executor.Insert(new InsertTicket(
                txnState: txn1,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",      new(ColumnType.Id, rowId) },
                        { "name",    new(ColumnType.String, "future-robot") },
                        { "year",    new(ColumnType.Integer64, 999L) },
                        { "enabled", new(ColumnType.Bool, true) },
                    }
                }
            ));

            Task<List<QueryResultRow>> scanTask = RunSql(executor, database, dbname,
                "SELECT year FROM robots WHERE year = 999",
                CamusIsolationLevel.ReadCommitted);
            await database.Transactions.RollbackIfNotCompletedAsync(txn1);
            uncommittedView = await scanTask;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(txn1);
        }

        // Commit the same logical row in a fresh transaction, then re-scan.
        KvTransaction committedTxn = await database.Transactions.BeginAsync();
        try
        {
            await executor.Insert(new InsertTicket(
                txnState: committedTxn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",      new(ColumnType.Id, rowId) },
                        { "name",    new(ColumnType.String, "future-robot") },
                        { "year",    new(ColumnType.Integer64, 999L) },
                        { "enabled", new(ColumnType.Bool, true) },
                    }
                }
            ));
            await database.Transactions.CommitAsync(committedTxn);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(committedTxn);
        }

        List<QueryResultRow> afterCommit = await RunSql(executor, database, dbname,
            "SELECT year FROM robots WHERE year = 999",
            CamusIsolationLevel.ReadCommitted);

        Assert.AreEqual(0, uncommittedView.Count,
            "read-committed covering scan must not see an uncommitted row");
        Assert.AreEqual(1, afterCommit.Count,
            "covering scan must see the row after its transaction commits");
        Assert.AreEqual(999L, afterCommit[0].Row["year"].LongValue,
            "covered scan must synthesize the correct year value from the index key after commit");

        // Confirm the afterCommit scan ran via the index-only path, not a table scan.
        long? rowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT year FROM robots WHERE year = 999");
        Assert.IsNotNull(rowsRead, "cost model must have chosen an index scan (not a table scan)");
        Assert.AreEqual(0L, rowsRead!.Value, "covering path must read zero primary rows");
    }

    [Test]
    public async Task CoveredScan_PinnedSnapshot_DoesNotSeeIndexEntryCommittedAfterStart()
    {
        // Real snapshot-isolation guard for the covering path.
        //
        // BeginAsync(Serializable, ReadOnly) mints a server HLC timestamp T_snap and stores it as
        // ReadTimestamp immediately — the snapshot is pinned at begin time, before any read occurs.
        // The index scan in the covering path issues reads as-of T_snap; an index entry committed
        // after T_snap must not appear.
        //
        // Structure:
        //   1. Pin snapshot (T_snap) — no reads yet.
        //   2. Commit a write (year=999) in a separate txn after T_snap.
        //   3. A fresh autocommit txn confirms the write is durable and visible (1 row found).
        //   4. Re-scan in the SAME snapshotTxn — ReadTimestamp is still T_snap → 0 rows.
        //
        // This directly tests that ScanIndex forwards ReadTimestamp to Kahuna correctly.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupWithYearIndex(rowCount: 50);

        // Step 1: pin snapshot — no range locks acquired yet.
        KvTransaction snapshotTxn = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        try
        {
            // Step 2: commit a new row after T_snap. year=999 is 1/51 (~2%) — forces index.
            KvTransaction writeTxn = await database.Transactions.BeginAsync();
            await executor.Insert(new InsertTicket(
                txnState: writeTxn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",      new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name",    new(ColumnType.String, "snapshot-robot") },
                        { "year",    new(ColumnType.Integer64, 999L) },
                        { "enabled", new(ColumnType.Bool, true) },
                    }
                }
            ));
            await database.Transactions.CommitAsync(writeTxn);

            // Step 3: fresh txn confirms year=999 is durable and visible to new transactions.
            List<QueryResultRow> freshRows = await RunSql(executor, database, dbname,
                "SELECT year FROM robots WHERE year = 999");
            Assert.AreEqual(1, freshRows.Count,
                "pre-condition: year=999 must be visible to a fresh transaction after commit");

            // Step 4: covering scan in the PINNED snapshot — must NOT see year=999.
            ExecuteSQLTicket snapTicket = new(txnState: snapshotTxn, database: dbname,
                sql: "SELECT year FROM robots WHERE year = 999", parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> snapCursor) = await executor.ExecuteSQLQuery(snapTicket);
            List<QueryResultRow> snapRows = await snapCursor.ToListAsync();

            Assert.AreEqual(0, snapRows.Count,
                "covering scan in a pinned Serializable RO txn must not see an index entry " +
                "committed after T_snap — ScanIndex must forward ReadTimestamp to Kahuna");

            // Verify the path is covered (no primary-row fetch) using a fresh EXPLAIN scan.
            long? rowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
                "SELECT year FROM robots WHERE year = 999");
            Assert.IsNotNull(rowsRead, "cost model must have chosen an index scan (not a table scan)");
            Assert.AreEqual(0L, rowsRead!.Value, "covering path must read zero primary rows");
        }
        finally
        {
            await database.Transactions.CommitAsync(snapshotTxn);
        }
    }

    [Test]
    public async Task NonCoveredBatchFetch_PinnedSnapshot_DoesNotSeePrimaryRowUpdatedAfterStart()
    {
        // Real snapshot-isolation guard for the non-covering batch-fetch path (GetRowsBatch).
        //
        // Unlike the covered test above, the index entry for year=49 EXISTS both before and after
        // the snapshot. The update changes only 'enabled' (a non-indexed column), so the index
        // entry is untouched. GetRowsBatch is called with the row's ID and must read the primary
        // row as-of ReadTimestamp = T_snap — returning the pre-update value.
        //
        // Structure:
        //   1. Pin snapshot (T_snap). At T_snap: row year=49 has enabled=false (49 is odd).
        //   2. UPDATE SET enabled=true WHERE year=49 in a separate txn. Commit.
        //   3. Fresh txn confirms enabled=true (update visible to new transactions).
        //   4. Re-scan SELECT enabled WHERE year=49 in the SAME snapshotTxn.
        //      The index entry for year=49 is present at T_snap; GetRowsBatch fetches the
        //      primary row as-of T_snap → must return enabled=false.
        //
        // This directly tests that GetRowsBatch forwards ReadTimestamp to Kahuna correctly.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupWithYearIndex(rowCount: 50);

        // Step 1: pin snapshot.
        KvTransaction snapshotTxn = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        try
        {
            // Step 2: update enabled for year=49 after T_snap. The index key is unchanged.
            await RunNonQuery(executor, database, dbname,
                "UPDATE robots SET enabled = true WHERE year = 49");

            // Step 3: fresh txn confirms the update is visible.
            List<QueryResultRow> freshRows = await RunSql(executor, database, dbname,
                "SELECT enabled FROM robots WHERE year = 49");
            Assert.AreEqual(1, freshRows.Count, "pre-condition: year=49 row must exist");
            Assert.IsTrue(freshRows[0].Row["enabled"].BoolValue,
                "pre-condition: fresh txn must see enabled=true after the update");

            // Step 4: non-covered scan in the PINNED snapshot.
            // 'enabled' is not in year_idx → IndexOnly=false → GetRowsBatch is called.
            // GetRowsBatch must read the primary row as-of T_snap → enabled=false.
            ExecuteSQLTicket snapTicket = new(txnState: snapshotTxn, database: dbname,
                sql: "SELECT enabled FROM robots WHERE year = 49", parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> snapCursor) = await executor.ExecuteSQLQuery(snapTicket);
            List<QueryResultRow> snapRows = await snapCursor.ToListAsync();

            Assert.AreEqual(1, snapRows.Count,
                "the index entry for year=49 exists at T_snap so the scan must return 1 row");
            Assert.IsFalse(snapRows[0].Row["enabled"].BoolValue,
                "GetRowsBatch must read the primary row as-of T_snap (enabled=false), " +
                "not the version committed after T_snap (enabled=true)");

            // Verify the path is non-covered: rows_read > 0 (GetRowsBatch fetched the primary row).
            long? rowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
                "SELECT enabled FROM robots WHERE year = 49");
            Assert.IsNotNull(rowsRead, "cost model must have chosen an index scan (not a table scan)");
            Assert.Greater(rowsRead!.Value, 0L,
                "non-covering path must fetch primary rows (GetRowsBatch called, rows_read > 0)");
        }
        finally
        {
            await database.Transactions.CommitAsync(snapshotTxn);
        }
    }

    // ── Parity: UPDATE visibility ─────────────────────────────────────────────

    [Test]
    public async Task CoveredScan_ReflectsUpdatedIndexedColumn()
    {
        // When a row's indexed column (year) is updated, the old index entry is removed and a
        // new one is written in the same transaction. A subsequent covering scan must see the
        // new value and must not return the row for the old value.
        //
        // year = 42 and year = 200 are each 1 row out of 100 (~1% selectivity) — index forced.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupWithYearIndex(rowCount: 100);

        // Pre-condition: confirm the row exists via the covered path.
        List<QueryResultRow> before = await RunSql(executor, database, dbname,
            "SELECT year FROM robots WHERE year = 42");
        Assert.AreEqual(1, before.Count, "pre-condition: row with year=42 must exist");

        long? beforeRowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT year FROM robots WHERE year = 42");
        Assert.IsNotNull(beforeRowsRead, "cost model must have chosen an index scan before update");
        Assert.AreEqual(0L, beforeRowsRead!.Value, "pre-condition covered scan must read zero primary rows");

        await RunNonQuery(executor, database, dbname,
            "UPDATE robots SET year = 200 WHERE year = 42");

        List<QueryResultRow> oldKey = await RunSql(executor, database, dbname,
            "SELECT year FROM robots WHERE year = 42");
        List<QueryResultRow> newKey = await RunSql(executor, database, dbname,
            "SELECT year FROM robots WHERE year = 200");
        List<QueryResultRow> baselineNewKey = await RunSql(executor, database, dbname,
            "SELECT * FROM robots WHERE year = 200");

        Assert.AreEqual(0, oldKey.Count,
            "covering scan on the old year value must return 0 rows after update");
        Assert.AreEqual(1, newKey.Count,
            "covering scan on the new year value must return the updated row");
        Assert.AreEqual(baselineNewKey.Count, newKey.Count,
            "covered and non-covered must agree on the updated row count");
        Assert.AreEqual(200L, newKey[0].Row["year"].LongValue,
            "covered scan must return the new year value");

        long? afterRowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT year FROM robots WHERE year = 200");
        Assert.IsNotNull(afterRowsRead, "cost model must have chosen an index scan after update");
        Assert.AreEqual(0L, afterRowsRead!.Value, "post-update covered scan must read zero primary rows");
    }

    // ── Parity: DELETE visibility ─────────────────────────────────────────────

    [Test]
    public async Task CoveredScan_DeletedRowNotReturned()
    {
        // When a row is deleted, its index entry is also removed. A covering scan must not
        // return the deleted row, matching the non-covering baseline.
        //
        // year = 42 is 1 row out of 100 (~1% selectivity) — index forced.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupWithYearIndex(rowCount: 100);

        // Pre-condition: confirm covered path is used before the delete, and check the value.
        List<QueryResultRow> before = await RunSql(executor, database, dbname,
            "SELECT year FROM robots WHERE year = 42");
        Assert.AreEqual(1, before.Count, "pre-condition: row with year=42 must exist");
        Assert.AreEqual(42L, before[0].Row["year"].LongValue,
            "covered scan must synthesize year=42 from the index key before delete");

        long? beforeRowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT year FROM robots WHERE year = 42");
        Assert.IsNotNull(beforeRowsRead, "cost model must have chosen an index scan before delete");
        Assert.AreEqual(0L, beforeRowsRead!.Value, "pre-condition covered scan must read zero primary rows");

        await RunNonQuery(executor, database, dbname,
            "DELETE FROM robots WHERE year = 42");

        List<QueryResultRow> coveredAfter  = await RunSql(executor, database, dbname,
            "SELECT year FROM robots WHERE year = 42");
        List<QueryResultRow> baselineAfter = await RunSql(executor, database, dbname,
            "SELECT * FROM robots WHERE year = 42");

        Assert.AreEqual(0, coveredAfter.Count,
            "covering scan must not return a deleted row");
        Assert.AreEqual(0, baselineAfter.Count,
            "non-covering scan must also return 0 rows after delete");
    }

    // ── Parity: batch fetch (multi-page with temporarily small batch size) ────

    [Test]
    public async Task CoveredScan_IdenticalYearSetAcrossMultipleMatchingRows()
    {
        // A covered scan over several matching rows must return the same value set as SELECT *.
        // The covering path (ScanIndex loop → SynthesizeCoveredValues) has no internal paging
        // and is not affected by IndexScanFetchBatchSize — that knob only applies to the
        // non-covering batch-fetch path. This test exercises the covered path with 5 matching
        // rows (years 96–100, ~5% selectivity) to verify value correctness over a range result.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupWithYearIndex(rowCount: 100);

        List<QueryResultRow> baseline = await RunSql(executor, database, dbname,
            "SELECT * FROM robots WHERE year > 95");
        List<QueryResultRow> covered = await RunSql(executor, database, dbname,
            "SELECT year FROM robots WHERE year > 95");

        Assert.AreEqual(5, baseline.Count, "years 96..100 → 5 rows");
        Assert.AreEqual(baseline.Count, covered.Count,
            "covered scan must return the same row count as SELECT *");

        HashSet<long> baselineYears = baseline.Select(r => r.Row["year"].LongValue).ToHashSet();
        HashSet<long> coveredYears  = covered.Select(r => r.Row["year"].LongValue).ToHashSet();
        Assert.That(coveredYears, Is.EquivalentTo(baselineYears),
            "covered scan must synthesize the correct year values for every matching index entry");

        long? rowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT year FROM robots WHERE year > 95");
        Assert.IsNotNull(rowsRead, "cost model must have chosen an index scan (not a table scan)");
        Assert.AreEqual(0L, rowsRead!.Value,
            "covered scan must read zero primary rows regardless of how many index entries matched");
    }

    [Test]
    public async Task NonCoveredBatchFetch_IdenticalRowCountToSelectStar()
    {
        // The non-covering batch-fetch path (GetRowsBatch) must return the same rows as
        // SELECT * when a non-indexed column ('enabled') is projected.
        //
        // With IndexScanFetchBatchSize = 2 and 9 matching rows (years 92–100), the scan pages
        // as 2+2+2+2+1. Predicate: WHERE year > 91 on 100 rows (~9% selectivity) — index forced.
        // 'enabled' is not in year_idx, so IndexOnly=false and the batch-fetch non-covering path runs.
        // rows_read > 0 confirms primary rows were fetched (not the covering path).
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupWithYearIndex(rowCount: 100, options: Options with { IndexScanFetchBatchSize = 2 });

        List<QueryResultRow> baseline = await RunSql(executor, database, dbname,
            "SELECT * FROM robots WHERE year > 91");
        List<QueryResultRow> batchFetched = await RunSql(executor, database, dbname,
            "SELECT enabled FROM robots WHERE year > 91");

        Assert.AreEqual(9, baseline.Count, "years 92..100 → 9 rows");
        Assert.AreEqual(baseline.Count, batchFetched.Count,
            "batch-fetched non-covered result must have the same row count as SELECT *");
        Assert.IsTrue(batchFetched.All(r => r.Row.ContainsKey("enabled")),
            "every batch-fetched row must contain the projected 'enabled' column");

        long? rowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT enabled FROM robots WHERE year > 91");
        Assert.IsNotNull(rowsRead, "cost model must have chosen an index scan (not a table scan)");
        Assert.Greater(rowsRead!.Value, 0L,
            "non-covered batch scan must fetch primary rows (rows_read > 0)");
    }

    [Test]
    public async Task NonCoveredBatchFetch_NullRowsHandledCorrectly()
    {
        // Non-unique indexes store entries for NULL-valued rows. When the batch-fetch
        // non-covering path decodes rows, a row with a NULL-year index entry must be
        // decoded correctly from the primary row and then excluded by the WHERE predicate.
        //
        // Fixture: 100 rows; rows 10, 20, ..., 100 have NULL year. Row 100 is NULL.
        // Predicate: WHERE year > 95 on this fixture → rows 96, 97, 98, 99 are non-null
        // (row 100 has NULL year and does not satisfy year > 95). That is 4 rows (~4%).
        // The cost model picks the index; 4 rows with batch size 2 → 2 full pages.
        // 'enabled' is not in year_idx → non-covering batch-fetch path.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupWithYearIndex(rowCount: 100, includeNullRows: true,
                                     options: Options with { IndexScanFetchBatchSize = 2 });

        List<QueryResultRow> baseline = await RunSql(executor, database, dbname,
            "SELECT * FROM robots WHERE year > 95");
        List<QueryResultRow> batchFetched = await RunSql(executor, database, dbname,
            "SELECT enabled FROM robots WHERE year > 95");

        // Rows 96–99 are non-null; row 100 has NULL year and is excluded.
        Assert.AreEqual(4, baseline.Count,
            "baseline must return only the 4 non-null rows with year > 95");
        Assert.AreEqual(baseline.Count, batchFetched.Count,
            "batch-fetched path must exclude null-year rows identically to SELECT *");

        long? rowsRead = await ExplainAnalyzeScanRowsRead(executor, database, dbname,
            "SELECT enabled FROM robots WHERE year > 95");
        Assert.IsNotNull(rowsRead, "cost model must have chosen an index scan (not a table scan)");
        Assert.Greater(rowsRead!.Value, 0L,
            "non-covered batch scan must fetch primary rows (rows_read > 0)");
    }
}
