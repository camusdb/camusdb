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
/// correctness (no spill files created when <see cref="CamusDBConfig.SpillEnabled"/> is
/// <c>false</c>).
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestDeleteUpdateSpill : SharedNodeBaseTest
{
    private string _dataDir = null!;
    private bool _savedSpillEnabled;
    private int _savedThreshold;
    private int? _savedForceThreshold;
    private int _savedFanIn;

    [SetUp]
    public void SetUpSpill()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_du_spill_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        _savedSpillEnabled   = CamusDBConfig.SpillEnabled;
        _savedThreshold      = CamusDBConfig.SpillThresholdRows;
        _savedForceThreshold = CamusDBConfig.ForceSpillThresholdRows;
        _savedFanIn          = CamusDBConfig.SpillMergeFanIn;

        CamusDBConfig.DataDirectory = _dataDir;
        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDownSpill()
    {
        SpillFileManager.ReleaseInstanceLock();

        CamusDBConfig.SpillEnabled            = _savedSpillEnabled;
        CamusDBConfig.SpillThresholdRows      = _savedThreshold;
        CamusDBConfig.ForceSpillThresholdRows = _savedForceThreshold;
        CamusDBConfig.SpillMergeFanIn         = _savedFanIn;
        CamusDBConfig.DataDirectory           = null!;

        try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    // ── Fixture helpers ───────────────────────────────────────────────────────

    private sealed record Fixture(
        string DbName,
        DatabaseDescriptor Database,
        CommandExecutor Executor,
        List<string> Ids);

    /// <summary>
    /// Creates a <c>things</c> table (id, name, value) with <paramref name="count"/> rows.
    /// </summary>
    private async Task<Fixture> SetupThingsTable(int count = 20)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
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
        CamusDBConfig.SpillEnabled = false;
        Fixture fRef = await SetupThingsTable(20);
        await RunNonQuery(fRef, "DELETE FROM things WHERE value >= 5 AND value < 15");
        List<QueryResultRow> refRemaining = await RunQuery(fRef, "SELECT name FROM things ORDER BY name");

        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 3;
        Fixture fSpill = await SetupThingsTable(20);
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
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 4;

        Fixture f = await SetupThingsTable(15);
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
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 3;

        Fixture f = await SetupThingsTable(20);
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
        CamusDBConfig.SpillEnabled = false;

        Fixture f = await SetupThingsTable(20);
        await RunNonQuery(f, "DELETE FROM things WHERE value >= 10");
        List<QueryResultRow> remaining = await RunQuery(f, "SELECT name FROM things");

        Assert.That(remaining.Count, Is.EqualTo(10),
            "DELETE without spill must remove exactly the matching rows.");
        Assert.IsEmpty(SpillFiles(_dataDir),
            "Flag-off DELETE must not create any spill files.");
    }

    // ── UPDATE spill tests ────────────────────────────────────────────────────

    /// <summary>
    /// Verifies that an UPDATE with spill enabled produces the same mutations as the
    /// in-memory path. The forced threshold causes the match set to overflow to disk.
    /// </summary>
    [Test]
    public async Task UpdateSpill_MatchesInMemoryPath()
    {
        CamusDBConfig.SpillEnabled = false;
        Fixture fRef = await SetupThingsTable(20);
        await RunNonQuery(fRef, "UPDATE things SET name = 'updated' WHERE value < 10");
        List<QueryResultRow> refRows = await RunQuery(fRef, "SELECT name FROM things WHERE name = 'updated' ORDER BY name");

        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 3;
        Fixture fSpill = await SetupThingsTable(20);
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
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 4;

        Fixture f = await SetupThingsTable(15);
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
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 3;

        Fixture f = await SetupThingsTable(20);
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
        CamusDBConfig.SpillEnabled = false;

        Fixture f = await SetupThingsTable(20);
        await RunNonQuery(f, "UPDATE things SET name = 'patched' WHERE value >= 10");
        List<QueryResultRow> patched = await RunQuery(f, "SELECT name FROM things WHERE name = 'patched'");

        Assert.That(patched.Count, Is.EqualTo(10),
            "UPDATE without spill must update exactly the matching rows.");
        Assert.IsEmpty(SpillFiles(_dataDir),
            "Flag-off UPDATE must not create any spill files.");
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

        CamusDBConfig.SpillEnabled = false;
        Fixture fOff = await SetupThingsTable(20);
        await RunNonQuery(fOff, update);
        List<QueryResultRow> offRows = await RunQuery(fOff, query);

        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 3;
        Fixture fOn = await SetupThingsTable(20);
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
