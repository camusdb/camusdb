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
///
/// <para>
/// Each case states the configuration it needs as options passed to the engine it runs against, so
/// nothing here mutates process-wide state. [NonParallelizable] nonetheless stays, for a different
/// reason: <see cref="SpillFileManager.AcquireInstanceLock"/> holds a single process-wide
/// <c>FileStream</c>, so two fixtures acquiring it concurrently would clobber one another. That lock,
/// not configuration, is what keeps this fixture serialized.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestQueryDistincterSpill : SharedNodeBaseTest
{
    private string _dataDir = null!;

    [SetUp]
    public void SetUpSpill()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_dist_spill_" + System.Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDownSpill()
    {
        SpillFileManager.ReleaseInstanceLock();

        try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    /// <summary>Spill disabled — the in-memory hash-set path.</summary>
    private CamusDBOptions SpillOff => Options with { SpillEnabled = false, DataDirectory = _dataDir };

    /// <summary>
    /// Spill forced after <paramref name="thresholdRows"/> rows, so the external path runs on inputs
    /// small enough for a unit test. Each call is an independent configuration — nothing here is
    /// process-wide, so two of these can be in play at once.
    /// </summary>
    private CamusDBOptions SpillOn(int thresholdRows, int fanIn = 4) =>
        Options with
        {
            SpillEnabled = true,
            ForceSpillThresholdRows = thresholdRows,
            SpillMergeFanIn = fanIn,
            DataDirectory = _dataDir,
        };

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
    private async Task<DistFixture> SetupPeople(CamusDBOptions options, int cities = 5, int dupsPerCity = 4)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);
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

        DistFixture fRef = await SetupPeople(SpillOff);
        List<QueryResultRow> reference = await Run(fRef, sql);

        DistFixture fSpill = await SetupPeople(SpillOn(5, 4));
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

        DistFixture fOff = await SetupPeople(SpillOff, cities: 8, dupsPerCity: 5);
        List<QueryResultRow> offRows = await Run(fOff, sql);

        DistFixture fOn = await SetupPeople(SpillOn(6, 4), cities: 8, dupsPerCity: 5);
        List<QueryResultRow> onRows = await Run(fOn, sql);

        CollectionAssert.AreEqual(SortedCities(offRows), SortedCities(onRows),
            "Spill-on and spill-off DISTINCT must produce identical sorted city lists.");
    }

    [Test]
    public async Task DistinctSpill_NoSpillFilesRemainAfterCompletion()
    {
        DistFixture f = await SetupPeople(SpillOn(3, 4), cities: 4, dupsPerCity: 3);
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
        DistFixture f = await SetupPeople(SpillOff, cities: 3, dupsPerCity: 4);
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
        // Two rows with a NULL city must deduplicate to one output row. The engine spills after 2 rows,
        // which is the path under test.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(SpillOn(2));
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
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(SpillOff);
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

        KvTransaction offTxn = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> offCursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(offTxn, dbname, sql, null));
        List<QueryResultRow> offRows = await offCursor.ToListAsync();

        // The same data through a second engine that spills after 2 rows — one engine cannot answer
        // for both configurations, since it fixes them when it is built.
        CommandExecutor spillingExecutor = CreateCommandExecutor(SpillOn(2));

        KvTransaction onTxn = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> onCursor) =
            await spillingExecutor.ExecuteSQLQuery(new ExecuteSQLTicket(onTxn, dbname, sql, null));
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
