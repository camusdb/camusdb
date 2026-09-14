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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies spillable DELETE and UPDATE row buffers.
///
/// <para>
/// DELETE and UPDATE first collect all matching rows into a <see cref="SpillableRowList"/>
/// before applying any mutations (Halloween problem). When the matched set exceeds the
/// spill threshold, rows are written to a temporary file on disk instead of accumulating
/// in the process heap.
/// </para>
///
/// <para>
/// Every test verifies one of: (a) result equivalence between the spill and in-memory paths,
/// (b) spill-file cleanup after the operation completes or errors, or (c) flag-off baseline
/// correctness (no spill files created when <see cref="CamusDBOptions.SpillEnabled"/> is
/// <c>false</c>).
/// </para>
///
/// <para>
/// The row-id-only tests additionally lock the buffered representation: DELETE and plain-values
/// UPDATE buffer row-id-only records (the mutation phase re-reads each row under its lock), so a
/// match on a wide column must not retain the scanned values — in memory or in a spill file. The
/// observable is <c>StatisticsManager.DmlLocateBufferMaxColumnsSeen</c>, measured when the
/// mutation phase drains the buffer.
/// </para>
/// </summary>
[TestFixture]
// Serial: SpillFileManager's instance lock is process-wide, so two fixtures holding it at once
// would write spill files into each other's directory.
[NonParallelizable]
public sealed class TestDeleteUpdateSpill : SharedNodeBaseTest
{
    private string _dataDir = null!;

    [SetUp]
    public void SetUpSpill()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_du_spill_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDownSpill()
    {
        SpillFileManager.ReleaseInstanceLock();

        try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    // ── Fixture helpers ───────────────────────────────────────────────────────

    private sealed record Fixture(
        string DbName,
        DatabaseDescriptor Database,
        CommandExecutor Executor,
        List<string> Ids);

    /// <summary>
    /// <summary>Spill disabled — the in-memory buffering path.</summary>
    private CamusDBOptions SpillOff => Options with { SpillEnabled = false, DataDirectory = _dataDir };

    /// <summary>
    /// Spill forced after <paramref name="thresholdRows"/> rows, so the chunked mutation path runs on
    /// inputs small enough for a unit test. Independent of any other configuration in play.
    /// </summary>
    private CamusDBOptions SpillOn(int thresholdRows) =>
        Options with { SpillEnabled = true, ForceSpillThresholdRows = thresholdRows, DataDirectory = _dataDir };

    /// <summary>
    /// Creates a <c>things</c> table (id, name, value) with <paramref name="count"/> rows.
    /// </summary>
    private async Task<Fixture> SetupThingsTable(CamusDBOptions options, int count = 20)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "things",
            columns:
            [
                new("id",    ColumnType.Id),
                new("name",  ColumnType.String, notNull: true),
                new("value", ColumnType.Integer64),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        List<string> ids = new(count);
        List<Dictionary<string, ColumnValue>> rows = new(count);

        for (int i = 0; i < count; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            ids.Add(id);
            rows.Add(new()
            {
                { "id",    new(ColumnType.Id,        id) },
                { "name",  new(ColumnType.String,    "Thing" + i) },
                { "value", new(ColumnType.Integer64, (long)i) },
            });
        }

        await executor.Insert(new InsertTicket(txn, dbname, "things", values: rows));
        await database.Transactions.CommitAsync(txn);

        return new Fixture(dbname, database, executor, ids);
    }

    private static async Task<List<QueryResultRow>> RunQuery(Fixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await f.Database.Transactions.CommitAsync(txn);
        return rows;
    }

    private static async Task RunNonQuery(Fixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null);
        await f.Executor.ExecuteNonSQLQuery(ticket);
        await f.Database.Transactions.CommitAsync(txn);
    }

    private static string[] SpillFiles(string dataDir)
    {
        string spillRoot = Path.Combine(dataDir, "tmp", "spill");
        if (!Directory.Exists(spillRoot))
            return [];
        return Directory.GetFiles(spillRoot, "*.spill", SearchOption.AllDirectories);
    }

    // ── DELETE spill tests ────────────────────────────────────────────────────

    /// <summary>
    /// Verifies that a DELETE with spill enabled removes the same rows as the in-memory
    /// path. The forced threshold causes the 20-row match set to overflow to disk, then
    /// <c>DeleteRowsAndIndexesFromDisk</c> streams them back for mutation.
    /// </summary>
    [Test]
    public async Task DeleteSpill_MatchesInMemoryPath()
    {
        Fixture fRef = await SetupThingsTable(SpillOff, 20);
        await RunNonQuery(fRef, "DELETE FROM things WHERE value >= 5 AND value < 15");
        List<QueryResultRow> refRemaining = await RunQuery(fRef, "SELECT name FROM things ORDER BY name");

        Fixture fSpill = await SetupThingsTable(SpillOn(3), 20);
        await RunNonQuery(fSpill, "DELETE FROM things WHERE value >= 5 AND value < 15");
        List<QueryResultRow> spillRemaining = await RunQuery(fSpill, "SELECT name FROM things ORDER BY name");

        Assert.AreEqual(refRemaining.Count, spillRemaining.Count,
            "DELETE with spill must remove the same number of rows as the in-memory path.");

        List<string> refNames   = refRemaining.Select(r => r.Row["name"].StrValue ?? "").OrderBy(s => s).ToList();
        List<string> spillNames = spillRemaining.Select(r => r.Row["name"].StrValue ?? "").OrderBy(s => s).ToList();
        CollectionAssert.AreEqual(refNames, spillNames,
            "Remaining rows after DELETE must be identical between spill and in-memory paths.");
    }

    /// <summary>
    /// Verifies that a DELETE ALL (no WHERE clause) with a forced spill threshold deletes
    /// every row and leaves the table empty — same as the flag-off path.
    /// </summary>
    [Test]
    public async Task DeleteSpill_DeleteAll_LeavesTableEmpty()
    {
        Fixture f = await SetupThingsTable(SpillOn(4), 15);
        await RunNonQuery(f, "DELETE FROM things WHERE value >= 0");
        List<QueryResultRow> remaining = await RunQuery(f, "SELECT name FROM things");

        Assert.That(remaining.Count, Is.EqualTo(0),
            "DELETE matching every row must leave the table empty even when spill is active.");
    }

    /// <summary>
    /// Verifies that spill files created during DELETE are deleted after the operation
    /// completes successfully.
    /// </summary>
    [Test]
    public async Task DeleteSpill_NoSpillFilesRemainAfterCompletion()
    {
        Fixture f = await SetupThingsTable(SpillOn(3), 20);
        await RunNonQuery(f, "DELETE FROM things WHERE value < 10");

        Assert.IsEmpty(SpillFiles(_dataDir),
            "DELETE spill files must be deleted after the operation completes.");
    }

    /// <summary>
    /// Verifies that with SpillEnabled=false, DELETE produces no spill files while
    /// still removing the correct rows.
    /// </summary>
    [Test]
    public async Task DeleteSpill_FlagOff_NoSpillFiles()
    {
        Fixture f = await SetupThingsTable(SpillOff, 20);
        await RunNonQuery(f, "DELETE FROM things WHERE value >= 10");
        List<QueryResultRow> remaining = await RunQuery(f, "SELECT name FROM things");

        Assert.That(remaining.Count, Is.EqualTo(10),
            "DELETE without spill must remove exactly the matching rows.");
        Assert.IsEmpty(SpillFiles(_dataDir),
            "Flag-off DELETE must not create any spill files.");
    }

    // ── DELETE batch chunking discriminator ───────────────────────────────────

    /// <summary>
    /// Verifies that the DELETE mutation phase applies <see cref="KvTableStore.DeleteRowsBatch"/>
    /// in bounded chunks rather than one O(matched) batch. The discriminator is
    /// <c>StatisticsManager.DeleteBatchMaxChunkSeen</c>: its value must never exceed the
    /// configured chunk size (which equals <see cref="CamusDBOptions.SpillEffectiveThreshold"/>).
    ///
    /// <para>Negative proof: reverting to the single-batch code path sets
    /// <c>DeleteBatchMaxChunkSeen = 20</c> (all matched rows), which exceeds the forced
    /// chunk size of 3 and trips the assertion.</para>
    /// </summary>
    [Test]
    public async Task DeleteBatch_ChunkedMutation_MaxChunkDoesNotExceedThreshold()
    {
        const int rowCount  = 20;
        const int chunkSize = 3;   // ForceSpillThresholdRows drives both the row-buffer and the chunk size

        Fixture f = await SetupThingsTable(SpillOn(chunkSize), rowCount);
        f.Executor.Statistics.DeleteBatchMaxChunkSeen = 0;

        // Delete all rows so the chunking path is exercised across multiple batches.
        await RunNonQuery(f, "DELETE FROM things WHERE value >= 0");

        List<QueryResultRow> remaining = await RunQuery(f, "SELECT name FROM things");
        Assert.That(remaining.Count, Is.EqualTo(0),
            "All rows must be deleted regardless of how many batches are used.");

        Assert.That(f.Executor.Statistics.DeleteBatchMaxChunkSeen, Is.GreaterThan(0),
            "At least one DeleteRowsBatch call must have been tracked.");

        Assert.That(f.Executor.Statistics.DeleteBatchMaxChunkSeen, Is.LessThanOrEqualTo(chunkSize),
            $"No single DeleteRowsBatch call must exceed chunkSize={chunkSize}. " +
            "A higher value proves the unbounded single-batch code path was taken.");
    }

    // ── UPDATE spill tests ────────────────────────────────────────────────────

    /// <summary>
    /// Verifies that an UPDATE with spill enabled produces the same mutations as the
    /// in-memory path. The forced threshold causes the match set to overflow to disk.
    /// </summary>
    [Test]
    public async Task UpdateSpill_MatchesInMemoryPath()
    {
        Fixture fRef = await SetupThingsTable(SpillOff, 20);
        await RunNonQuery(fRef, "UPDATE things SET name = 'updated' WHERE value < 10");
        List<QueryResultRow> refRows = await RunQuery(fRef, "SELECT name FROM things WHERE name = 'updated' ORDER BY name");

        Fixture fSpill = await SetupThingsTable(SpillOn(3), 20);
        await RunNonQuery(fSpill, "UPDATE things SET name = 'updated' WHERE value < 10");
        List<QueryResultRow> spillRows = await RunQuery(fSpill, "SELECT name FROM things WHERE name = 'updated' ORDER BY name");

        Assert.AreEqual(refRows.Count, spillRows.Count,
            "UPDATE with spill must modify the same number of rows as the in-memory path.");
    }

    /// <summary>
    /// Verifies that an UPDATE ALL (no WHERE) with spill changes every row and leaves
    /// the correct state — same as the flag-off path.
    /// </summary>
    [Test]
    public async Task UpdateSpill_UpdateAll_ChangesEveryRow()
    {
        Fixture f = await SetupThingsTable(SpillOn(4), 15);
        await RunNonQuery(f, "UPDATE things SET name = 'bulk' WHERE value >= 0");
        List<QueryResultRow> rows = await RunQuery(f, "SELECT name FROM things WHERE name = 'bulk'");

        Assert.That(rows.Count, Is.EqualTo(15),
            "UPDATE matching every row must change all rows even when spill is active.");
    }

    /// <summary>
    /// Verifies that spill files created during UPDATE are deleted after the operation
    /// completes successfully.
    /// </summary>
    [Test]
    public async Task UpdateSpill_NoSpillFilesRemainAfterCompletion()
    {
        Fixture f = await SetupThingsTable(SpillOn(3), 20);
        await RunNonQuery(f, "UPDATE things SET value = 999 WHERE value < 10");

        Assert.IsEmpty(SpillFiles(_dataDir),
            "UPDATE spill files must be deleted after the operation completes.");
    }

    /// <summary>
    /// Verifies that with SpillEnabled=false, UPDATE produces no spill files while
    /// still modifying the correct rows.
    /// </summary>
    [Test]
    public async Task UpdateSpill_FlagOff_NoSpillFiles()
    {
        Fixture f = await SetupThingsTable(SpillOff, 20);
        await RunNonQuery(f, "UPDATE things SET name = 'patched' WHERE value >= 10");
        List<QueryResultRow> patched = await RunQuery(f, "SELECT name FROM things WHERE name = 'patched'");

        Assert.That(patched.Count, Is.EqualTo(10),
            "UPDATE without spill must update exactly the matching rows.");
        Assert.IsEmpty(SpillFiles(_dataDir),
            "Flag-off UPDATE must not create any spill files.");
    }

    // ── Row-id-only locate buffer (retention) tests ───────────────────────────

    private const int WidePayloadLength = 4096;

    private static string WidePayload(char marker) => new(marker, WidePayloadLength);

    /// <summary>
    /// Creates a <c>wide</c> table whose predicate column carries a ~4 KB string, plus a unique
    /// index (<c>u_code</c> on <c>code</c>) and a non-unique index (<c>m_name</c> on <c>name</c>).
    /// Even rows get payload 'A…', odd rows 'B…'; <c>name</c> groups rows as <c>grp0</c>/<c>grp1</c>
    /// on the same parity, so a payload predicate and a name lookup select the same rows.
    /// </summary>
    private async Task<Fixture> SetupWideTable(CamusDBOptions options, int count = 20)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "wide",
            columns:
            [
                new("id",      ColumnType.Id),
                new("name",    ColumnType.String, notNull: true),
                new("code",    ColumnType.Integer64),
                new("payload", ColumnType.String),
                new("value",   ColumnType.Integer64),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey,  "~pk",    [new("id",   OrderType.Ascending)]),
                new(ConstraintType.IndexUnique, "u_code", [new("code", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti,  "m_name", [new("name", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        List<string> ids = new(count);
        List<Dictionary<string, ColumnValue>> rows = new(count);

        for (int i = 0; i < count; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            ids.Add(id);
            rows.Add(new()
            {
                { "id",      new(ColumnType.Id,        id) },
                { "name",    new(ColumnType.String,    "grp" + (i % 2)) },
                { "code",    new(ColumnType.Integer64, (long)i) },
                { "payload", new(ColumnType.String,    WidePayload(i % 2 == 0 ? 'A' : 'B')) },
                { "value",   new(ColumnType.Integer64, (long)i) },
            });
        }

        await executor.Insert(new InsertTicket(txn, dbname, "wide", values: rows));
        await database.Transactions.CommitAsync(txn);

        return new Fixture(dbname, database, executor, ids);
    }

    private static async Task<List<QueryResultRow>> RunQueryParams(
        Fixture f, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: parameters);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await f.Database.Transactions.CommitAsync(txn);
        return rows;
    }

    private static async Task RunNonQueryParams(
        Fixture f, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: parameters);
        await f.Executor.ExecuteNonSQLQuery(ticket);
        await f.Database.Transactions.CommitAsync(txn);
    }

    private static Dictionary<string, ColumnValue> PayloadParam(char marker) =>
        new() { { "@p", new(ColumnType.String, WidePayload(marker)) } };

    /// <summary>
    /// Locks the row-id-only DELETE buffer: a DELETE whose predicate reads a ~4 KB column must
    /// drain zero-column records from its locate buffer — the scanned values (and, for a
    /// borrowed-backed scan row, its full KV bytes) must not be retained past the locate scan.
    /// Also asserts row parity, unique-index cleanup (a deleted unique value is insertable
    /// again), non-unique-index parity, and no leftover spill file.
    /// </summary>
    [TestCase(false)]
    [TestCase(true)]
    public async Task DeleteWidePredicate_LocateBufferKeepsRowIdsOnly(bool spillOn)
    {
        Fixture f = await SetupWideTable(spillOn ? SpillOn(3) : SpillOff, 20);
        f.Executor.Statistics.DmlLocateBufferMaxColumnsSeen = 0;

        await RunNonQueryParams(f, "DELETE FROM wide WHERE payload = @p", PayloadParam('A'));

        Assert.That(f.Executor.Statistics.DmlLocateBufferMaxColumnsSeen, Is.EqualTo(0),
            "The DELETE locate buffer must drain row-id-only records; a positive column count " +
            "proves scanned values were retained past the locate scan.");

        List<QueryResultRow> remaining = await RunQuery(f, "SELECT name, value FROM wide ORDER BY value");
        Assert.That(remaining.Count, Is.EqualTo(10), "Exactly the payload-A rows must be deleted.");
        Assert.IsTrue(remaining.All(r => r.Row["value"].LongValue % 2 == 1),
            "Only odd (payload-B) rows must remain.");

        // Non-unique index parity: every deleted row was in grp0, every survivor in grp1.
        Assert.IsEmpty(await RunQuery(f, "SELECT id FROM wide WHERE name = 'grp0'"),
            "The non-unique index must hold no entries for deleted rows.");
        Assert.That((await RunQuery(f, "SELECT id FROM wide WHERE name = 'grp1'")).Count, Is.EqualTo(10));

        // Unique index parity: a deleted unique value is gone and insertable again.
        Assert.IsEmpty(await RunQuery(f, "SELECT id FROM wide WHERE code = 2"),
            "The unique index must hold no entry for a deleted row.");

        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        await f.Executor.Insert(new InsertTicket(txn, f.DbName, "wide", values:
        [
            new()
            {
                { "id",      new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                { "name",    new(ColumnType.String,    "reborn") },
                { "code",    new(ColumnType.Integer64, 0L) },
                { "payload", new(ColumnType.String,    "reborn") },
                { "value",   new(ColumnType.Integer64, 999L) },
            }
        ]));
        await f.Database.Transactions.CommitAsync(txn);

        Assert.That((await RunQuery(f, "SELECT id FROM wide WHERE code = 0")).Count, Is.EqualTo(1),
            "Re-inserting a deleted unique value must succeed — its old index entry must be gone.");

        Assert.IsEmpty(SpillFiles(_dataDir), "No spill file may remain after the DELETE.");
    }

    /// <summary>
    /// Locks the row-id-only plain-values UPDATE buffer: a plain-values update never reads the
    /// located row's values (the write phase re-reads every row under its lock and the new cells
    /// come from the ticket), so its locate buffer must drain zero-column records even when the
    /// predicate reads a ~4 KB column. Also asserts row parity and index maintenance on the
    /// updated (non-unique-indexed) column.
    /// </summary>
    [TestCase(false)]
    [TestCase(true)]
    public async Task UpdatePlainValuesWidePredicate_LocateBufferKeepsRowIdsOnly(bool spillOn)
    {
        Fixture f = await SetupWideTable(spillOn ? SpillOn(3) : SpillOff, 20);
        f.Executor.Statistics.DmlLocateBufferMaxColumnsSeen = 0;

        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        UpdateResult result = await f.Executor.Update(new UpdateTicket(
            txnState:     txn,
            databaseName: f.DbName,
            tableName:    "wide",
            plainValues:  new() { { "name", new(ColumnType.String, "changed") } },
            exprValues:   null,
            where:        null,
            filters:      new() { new("payload", "=", new(ColumnType.String, WidePayload('A'))) },
            parameters:   null));
        await f.Database.Transactions.CommitAsync(txn);

        Assert.That(result.UpdatedRows, Is.EqualTo(10), "Exactly the payload-A rows must be updated.");
        Assert.That(f.Executor.Statistics.DmlLocateBufferMaxColumnsSeen, Is.EqualTo(0),
            "The plain-values UPDATE locate buffer must drain row-id-only records; a positive " +
            "column count proves scanned values were retained past the locate scan.");

        // Index maintenance on the id-only path: the old entries came from the locked re-read,
        // not from the buffer, so the non-unique index must have moved every updated row.
        Assert.IsEmpty(await RunQuery(f, "SELECT id FROM wide WHERE name = 'grp0'"));
        Assert.That((await RunQuery(f, "SELECT id FROM wide WHERE name = 'changed'")).Count, Is.EqualTo(10));
        Assert.That((await RunQuery(f, "SELECT id FROM wide WHERE name = 'grp1'")).Count, Is.EqualTo(10));

        Assert.IsEmpty(SpillFiles(_dataDir), "No spill file may remain after the UPDATE.");
    }

    /// <summary>
    /// Negative control for the drain-time retention counter: an expression-SET UPDATE must keep
    /// its scanned locate columns (they feed the SET evaluation), so the counter observes a
    /// positive column count. This proves the counter actually measures the drained records —
    /// the zero asserted by the row-id-only tests is a real zero, not a dead counter.
    /// </summary>
    [Test]
    public async Task UpdateExprValues_LocateBufferRetainsScannedColumns()
    {
        Fixture f = await SetupWideTable(SpillOff, 10);
        f.Executor.Statistics.DmlLocateBufferMaxColumnsSeen = 0;

        await RunNonQueryParams(f, "UPDATE wide SET value = value + 100 WHERE payload = @p", PayloadParam('A'));

        Assert.That(f.Executor.Statistics.DmlLocateBufferMaxColumnsSeen, Is.GreaterThan(0),
            "An expression-SET UPDATE buffers its locate columns; a zero here means the " +
            "retention counter is not observing the drained records.");

        List<QueryResultRow> bumped = await RunQuery(f, "SELECT value FROM wide WHERE value >= 100");
        Assert.That(bumped.Count, Is.EqualTo(5), "The expression SET must still update the matched rows.");
    }

    /// <summary>
    /// LIMIT rides inside the locate scan (the ticket's limit is applied before a row reaches the
    /// buffer), so a limited DELETE on the row-id-only path removes exactly the limit count.
    /// </summary>
    [Test]
    public async Task DeleteWithLimit_RowIdOnlyBuffer_DeletesExactlyLimit()
    {
        Fixture f = await SetupWideTable(SpillOn(3), 20);
        f.Executor.Statistics.DmlLocateBufferMaxColumnsSeen = 0;

        await RunNonQueryParams(f, "DELETE FROM wide WHERE payload = @p LIMIT 4", PayloadParam('A'));

        Assert.That(f.Executor.Statistics.DmlLocateBufferMaxColumnsSeen, Is.EqualTo(0));

        List<QueryResultRow> remainingA = await RunQueryParams(
            f, "SELECT id FROM wide WHERE payload = @p", PayloadParam('A'));
        Assert.That(remainingA.Count, Is.EqualTo(6), "LIMIT 4 must delete exactly 4 of the 10 matches.");
        Assert.That((await RunQuery(f, "SELECT id FROM wide")).Count, Is.EqualTo(16));
    }

    /// <summary>
    /// An empty match set on the row-id-only path deletes nothing and buffers nothing.
    /// </summary>
    [Test]
    public async Task DeleteEmptyMatchSet_RowIdOnlyBuffer_DeletesNothing()
    {
        Fixture f = await SetupWideTable(SpillOff, 8);
        f.Executor.Statistics.DmlLocateBufferMaxColumnsSeen = 0;

        await RunNonQuery(f, "DELETE FROM wide WHERE value = -1");

        Assert.That(f.Executor.Statistics.DmlLocateBufferMaxColumnsSeen, Is.EqualTo(0));
        Assert.That((await RunQuery(f, "SELECT id FROM wide")).Count, Is.EqualTo(8),
            "A DELETE that matches nothing must delete nothing.");
    }

    /// <summary>
    /// Rolls back a multi-chunk DELETE (forced spill threshold 3 over 20 rows, so several
    /// mutation chunks ran) and verifies the transaction restores every chunk: all rows are
    /// back and both the unique and the non-unique index answer for them again.
    /// </summary>
    [Test]
    public async Task DeleteMultiChunk_Rollback_RestoresEveryChunkAndIndexes()
    {
        Fixture f = await SetupWideTable(SpillOn(3), 20);

        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        await f.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: txn, database: f.DbName, sql: "DELETE FROM wide WHERE value >= 0", parameters: null));

        // The same transaction sees its own delete before the rollback.
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: f.DbName, sql: "SELECT id FROM wide", parameters: null));
        Assert.IsEmpty(await cursor.ToListAsync(), "Inside the transaction every row is deleted.");

        await f.Database.Transactions.RollbackAsync(txn);

        Assert.That((await RunQuery(f, "SELECT id FROM wide")).Count, Is.EqualTo(20),
            "Rollback must restore every deleted chunk.");
        Assert.That((await RunQuery(f, "SELECT id FROM wide WHERE code = 5")).Count, Is.EqualTo(1),
            "Rollback must restore unique-index entries from every chunk.");
        Assert.That((await RunQuery(f, "SELECT id FROM wide WHERE name = 'grp0'")).Count, Is.EqualTo(10),
            "Rollback must restore non-unique-index entries from every chunk.");
        Assert.IsEmpty(SpillFiles(_dataDir), "No spill file may remain after a rolled-back DELETE.");
    }

    /// <summary>
    /// Rows written under an older column layout still delete correctly on the row-id-only path:
    /// the locate scan and the mutation-phase re-read both decode the pre-ALTER rows through
    /// schema history, and the buffer itself carries no values that could go stale.
    /// </summary>
    [Test]
    public async Task DeleteOldSchemaLayoutRows_RowIdOnlyBuffer()
    {
        Fixture f = await SetupWideTable(SpillOn(3), 10);

        KvTransaction ddlTxn = await f.Database.Transactions.BeginAsync();
        await f.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: ddlTxn, database: f.DbName,
            sql: "ALTER TABLE wide ADD COLUMN extra INT64 DEFAULT (7)", parameters: null));
        await f.Database.Transactions.CommitAsync(ddlTxn);

        f.Executor.Statistics.DmlLocateBufferMaxColumnsSeen = 0;

        await RunNonQueryParams(f, "DELETE FROM wide WHERE payload = @p", PayloadParam('A'));

        Assert.That(f.Executor.Statistics.DmlLocateBufferMaxColumnsSeen, Is.EqualTo(0));

        List<QueryResultRow> remaining = await RunQuery(f, "SELECT value, extra FROM wide ORDER BY value");
        Assert.That(remaining.Count, Is.EqualTo(5), "Exactly the payload-A rows (old layout) must be deleted.");
        Assert.IsTrue(remaining.All(r => r.Row["extra"].LongValue == 7),
            "Surviving old-layout rows must still decode the added column's default.");
    }

    /// <summary>
    /// Verifies spill-on vs spill-off result equivalence for UPDATE with a numeric range
    /// filter: the set of (id, value) pairs visible after the update must be identical
    /// whether the row buffer spilled to disk or stayed in memory.
    /// </summary>
    [Test]
    public async Task UpdateSpill_FlagOnVsOff_IdenticalResults()
    {
        const string update = "UPDATE things SET value = value + 100 WHERE value >= 5 AND value < 15";
        const string query  = "SELECT name, value FROM things ORDER BY name";

        Fixture fOff = await SetupThingsTable(SpillOff, 20);
        await RunNonQuery(fOff, update);
        List<QueryResultRow> offRows = await RunQuery(fOff, query);

        Fixture fOn = await SetupThingsTable(SpillOn(3), 20);
        await RunNonQuery(fOn, update);
        List<QueryResultRow> onRows = await RunQuery(fOn, query);

        Assert.AreEqual(offRows.Count, onRows.Count,
            "UPDATE flag-on vs flag-off row counts must be equal.");

        List<(string name, long value)> offKv = offRows
            .Select(r => (r.Row["name"].StrValue ?? "", r.Row["value"].LongValue))
            .OrderBy(p => p.Item1)
            .ToList();

        List<(string name, long value)> onKv = onRows
            .Select(r => (r.Row["name"].StrValue ?? "", r.Row["value"].LongValue))
            .OrderBy(p => p.Item1)
            .ToList();

        CollectionAssert.AreEqual(offKv, onKv,
            "UPDATE spill-on and spill-off post-update (name, value) pairs must be identical.");
    }
}
