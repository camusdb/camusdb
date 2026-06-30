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
/// Verifies the spill-aware DISTINCT path in <see cref="QueryDistincter"/>.
///
/// <para>
/// The spill path is activated by <c>SpillEnabled = true</c> with a small
/// <c>ForceSpillThresholdRows</c>. It sorts all rows by the projected columns using the
/// external merge sort then deduplicates adjacent equal rows with the O(1)-memory streaming
/// dedup. Correctness is asserted by comparing sorted results against the flag-off hash-set
/// path.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestQueryDistincterSpill : SharedNodeBaseTest
{
    private string _dataDir = null!;
    private bool _savedSpillEnabled;
    private int _savedThreshold;
    private int? _savedForceThreshold;
    private int _savedFanIn;

    [SetUp]
    public void SetUpSpill()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_dist_spill_" + System.Guid.NewGuid().ToString("N"));
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

    private sealed record DistFixture(
        string DbName,
        DatabaseDescriptor Database,
        CommandExecutor Executor);

    /// <summary>
    /// Creates a <c>people</c> table with <c>city</c> (string) and <c>score</c> (int64).
    /// Inserts <paramref name="cities"/> distinct city values, each repeated
    /// <paramref name="dupsPerCity"/> times, with varying scores so row content is not
    /// entirely identical (only <c>city</c> is duplicated by value).
    /// </summary>
    private async Task<DistFixture> SetupPeople(int cities = 5, int dupsPerCity = 4)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "people",
            columns:
            [
                new("id",    ColumnType.Id),
                new("city",  ColumnType.String, notNull: true),
                new("score", ColumnType.Integer64),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        List<Dictionary<string, ColumnValue>> rows = new();
        for (int c = 0; c < cities; c++)
        {
            string city = "City" + c;
            for (int d = 0; d < dupsPerCity; d++)
            {
                rows.Add(new()
                {
                    { "id",    new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                    { "city",  new(ColumnType.String,    city) },
                    { "score", new(ColumnType.Integer64, (long)(d + 1)) },
                });
            }
        }

        await executor.Insert(new InsertTicket(txn, dbname, "people", values: rows));
        await database.Transactions.CommitAsync(txn);

        return new DistFixture(dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> Run(DistFixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    private static List<string> SortedCities(List<QueryResultRow> rows) =>
        rows.Select(r => r.Row.TryGetValue("city", out var v) ? v.StrValue ?? "" : "")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task DistinctSpill_SingleColumn_MatchesInMemoryPath()
    {
        const string sql = "SELECT DISTINCT city FROM people";

        CamusDBConfig.SpillEnabled = false;
        DistFixture fRef = await SetupPeople();
        List<QueryResultRow> reference = await Run(fRef, sql);

        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 5;
        CamusDBConfig.SpillMergeFanIn         = 4;
        DistFixture fSpill = await SetupPeople();
        List<QueryResultRow> spillResult = await Run(fSpill, sql);

        Assert.AreEqual(reference.Count, spillResult.Count,
            "Spill DISTINCT must return the same number of distinct values as the hash-set path.");
        CollectionAssert.AreEqual(SortedCities(reference), SortedCities(spillResult),
            "The set of distinct cities must be identical between the spill and in-memory paths.");
    }

    [Test]
    public async Task DistinctSpill_FlagOnVsOff_IdenticalResults()
    {
        const string sql = "SELECT DISTINCT city FROM people";

        CamusDBConfig.SpillEnabled = false;
        DistFixture fOff = await SetupPeople(cities: 8, dupsPerCity: 5);
        List<QueryResultRow> offRows = await Run(fOff, sql);

        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 6;
        CamusDBConfig.SpillMergeFanIn         = 4;
        DistFixture fOn = await SetupPeople(cities: 8, dupsPerCity: 5);
        List<QueryResultRow> onRows = await Run(fOn, sql);

        CollectionAssert.AreEqual(SortedCities(offRows), SortedCities(onRows),
            "Spill-on and spill-off DISTINCT must produce identical sorted city lists.");
    }

    [Test]
    public async Task DistinctSpill_NoSpillFilesRemainAfterCompletion()
    {
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 3;
        CamusDBConfig.SpillMergeFanIn         = 4;

        DistFixture f = await SetupPeople(cities: 4, dupsPerCity: 3);
        List<QueryResultRow> rows = await Run(f, "SELECT DISTINCT city FROM people");

        Assert.That(rows.Count, Is.GreaterThan(0));

        string spillRoot = Path.Combine(_dataDir, "tmp", "spill");
        if (Directory.Exists(spillRoot))
        {
            string[] remaining = Directory.GetFiles(spillRoot, "*.spill", SearchOption.AllDirectories);
            Assert.IsEmpty(remaining,
                "All spill files must be deleted after the DISTINCT query completes.");
        }
    }

    [Test]
    public async Task DistinctSpill_FlagOff_UsesHashSetPath()
    {
        // When SpillEnabled=false, the hash-set path returns the correct result and must not
        // create any *.spill partition files.
        CamusDBConfig.SpillEnabled = false;

        DistFixture f = await SetupPeople(cities: 3, dupsPerCity: 4);
        List<QueryResultRow> rows = await Run(f, "SELECT DISTINCT city FROM people");

        Assert.That(rows.Count, Is.EqualTo(3));

        string spillRoot = Path.Combine(_dataDir, "tmp", "spill");
        if (Directory.Exists(spillRoot))
        {
            string[] spills = Directory.GetFiles(spillRoot, "*.spill", SearchOption.AllDirectories);
            Assert.IsEmpty(spills, "Flag-off path must not create any *.spill files.");
        }
    }

    [Test]
    public async Task DistinctSpill_NullValues_TreatedAsEqual()
    {
        // Two rows with a NULL city must deduplicate to one output row.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "locs",
            columns:
            [
                new("id",   ColumnType.Id),
                new("city", ColumnType.String),
                new("val",  ColumnType.Integer64),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.Insert(new InsertTicket(txn, dbname, "locs",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "city", new(ColumnType.String, "London") }, { "val", new(ColumnType.Integer64, 1L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "city", new(ColumnType.Null,   0) },         { "val", new(ColumnType.Integer64, 2L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "city", new(ColumnType.Null,   0) },         { "val", new(ColumnType.Integer64, 3L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "city", new(ColumnType.String, "Paris") },   { "val", new(ColumnType.Integer64, 4L) } },
            ]));

        await database.Transactions.CommitAsync(txn);

        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 2; // spill after 2 rows
        CamusDBConfig.SpillMergeFanIn         = 4;

        KvTransaction runTxn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: runTxn, database: dbname,
            sql: "SELECT DISTINCT city FROM locs", parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();

        // London, NULL (deduplicated), Paris → 3 distinct values
        Assert.That(rows.Count, Is.EqualTo(3),
            "Two NULL city values must be deduplicated to a single row in the spill DISTINCT path.");
    }

    [Test]
    public async Task DistinctSpill_MultiColumn_DistinguishesBySecondColumn()
    {
        // Multi-column DISTINCT: rows sharing the first column but differing in the second
        // must NOT be deduplicated. A comparer/streaming-dedup that only looked at the first
        // column would collapse (London,1) and (London,2) into one row — this test catches that.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "visits",
            columns:
            [
                new("id",    ColumnType.Id),
                new("city",  ColumnType.String, notNull: true),
                new("score", ColumnType.Integer64),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        // Distinct (city,score) tuples = {(London,1),(London,2),(Paris,1),(Paris,2)} = 4, from 7 rows.
        (string City, long Score)[] tuples =
        [
            ("London", 1), ("London", 1),               // duplicate tuple → 1
            ("London", 2),                              // same city, different score → kept
            ("Paris", 1), ("Paris", 1), ("Paris", 1),   // duplicate tuple → 1
            ("Paris", 2),                               // same city, different score → kept
        ];

        await executor.Insert(new InsertTicket(txn, dbname, "visits",
            values: tuples.Select(t => new Dictionary<string, ColumnValue>
            {
                { "id",    new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                { "city",  new(ColumnType.String,    t.City) },
                { "score", new(ColumnType.Integer64, t.Score) },
            }).ToList()));
        await database.Transactions.CommitAsync(txn);

        const string sql = "SELECT DISTINCT city, score FROM visits";

        CamusDBConfig.SpillEnabled = false;
        KvTransaction offTxn = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> offCursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(offTxn, dbname, sql, null));
        List<QueryResultRow> offRows = await offCursor.ToListAsync();

        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.ForceSpillThresholdRows = 2; // force the external sort to spill
        CamusDBConfig.SpillMergeFanIn         = 4;
        KvTransaction onTxn = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> onCursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(onTxn, dbname, sql, null));
        List<QueryResultRow> onRows = await onCursor.ToListAsync();

        Assert.That(offRows.Count, Is.EqualTo(4), "flag-off: 4 distinct (city,score) tuples expected");
        Assert.That(onRows.Count, Is.EqualTo(4),
            "spill DISTINCT must distinguish tuples by the second column, not collapse on the first");

        // Sorted (city,score) projections must match between paths.
        static List<(string, long)> Pairs(List<QueryResultRow> rows) =>
            rows.Select(r => (
                    r.Row.TryGetValue("city", out var c) ? c.StrValue ?? "" : "",
                    r.Row.TryGetValue("score", out var s) ? s.LongValue : -1L))
                .OrderBy(p => p.Item1, StringComparer.Ordinal).ThenBy(p => p.Item2)
                .ToList();

        CollectionAssert.AreEqual(Pairs(offRows), Pairs(onRows),
            "spill and in-memory multi-column DISTINCT must produce identical tuples");
    }
}
