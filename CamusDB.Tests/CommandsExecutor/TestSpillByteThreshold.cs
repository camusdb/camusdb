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
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The per-operator byte cap (<see cref="CamusDBOptions.SpillThresholdBytes"/>). Every case keeps
/// the row cap far out of reach, so a spill can come from the byte cap only, and every case that
/// spills compares its result with the same query on a spill-disabled engine. Each table row holds
/// two 4096-character strings, about 16 KiB of heap, and the cap is 32 KiB, so an operator spills
/// after a few rows when the cap works and never when it does not.
/// </summary>
[TestFixture]
// Serial: SpillFileManager's instance lock is process-wide, so two fixtures holding it at once
// would write spill files into each other's directory.
[NonParallelizable]
public sealed class TestSpillByteThreshold : SharedNodeBaseTest
{
    private const int RowCount = 40;

    private const int GroupCount = 10;

    private const int WideChars = 4096;

    private const long ByteCap = 32 * 1024;

    private string _dataDir = null!;

    [SetUp]
    public void SetUpSpill()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_spill_bytes_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDownSpill()
    {
        SpillFileManager.ReleaseInstanceLock();

        try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    /// <summary>Spill on, the row cap out of reach, the byte cap at <see cref="ByteCap"/>.</summary>
    private CamusDBOptions SpillOnBytes => Options with
    {
        SpillEnabled = true,
        ForceSpillThresholdRows = null,
        SpillThresholdRows = 500_000,
        SpillThresholdBytes = ByteCap,
        SpillMergeFanIn = 4,
        DataDirectory = _dataDir,
    };

    /// <summary>Spill on with no byte cap: only the (unreachable) row cap applies.</summary>
    private CamusDBOptions SpillOnNoByteCap => SpillOnBytes with { SpillThresholdBytes = 0 };

    /// <summary>Spill off: the byte cap is set but must be ignored.</summary>
    private CamusDBOptions SpillOff => SpillOnBytes with { SpillEnabled = false };

    private sealed record Fixture(string DbName, DatabaseDescriptor Database, CommandExecutor Executor);

    private static string Wide(string prefix) => prefix + new string('x', WideChars - prefix.Length);

    /// <summary>
    /// <c>docs</c>: <see cref="RowCount"/> rows, <c>k</c> a permutation of 0..39, <c>grp</c> one of
    /// <see cref="GroupCount"/> wide strings (4 rows each), <c>payload</c> a wide string per row.
    /// </summary>
    private async Task<Fixture> Setup(CamusDBOptions options)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase(options);

        KvTransaction ddl = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db.Name,
            "CREATE TABLE docs (id OID NOT NULL, k int64, grp string, payload string, PRIMARY KEY (id))", null));

        foreach (int i in Enumerable.Range(0, RowCount).OrderBy(x => (x * 7) % RowCount))
        {
            KvTransaction tx = await db.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name,
                "INSERT INTO docs (id, k, grp, payload) VALUES (@id, @k, @grp, @payload)",
                new()
                {
                    { "@id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "@k", new(ColumnType.Integer64, (long)i) },
                    { "@grp", new(ColumnType.String, Wide("g" + (i % GroupCount))) },
                    { "@payload", new(ColumnType.String, Wide("p" + i)) },
                }));
            await db.Transactions.CommitAsync(tx);
        }

        return new Fixture(dbname, db, executor);
    }

    private static async Task<List<QueryResultRow>> Run(Fixture f, string sql, StatementProbe? probe = null)
    {
        KvTransaction tx = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: f.DbName, sql: sql, parameters: null, probe: probe);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    private static async Task<(List<QueryResultRow> Rows, bool Spilled)> RunProbed(Fixture f, string sql)
    {
        StatementProbe probe = new();
        List<QueryResultRow> rows = await Run(f, sql, probe);
        return (rows, probe.Spilled);
    }

    private static async Task<List<string>> PlanNodes(Fixture f, string sql) =>
        (await Run(f, "EXPLAIN " + sql)).Select(r => r.Row["node"].StrValue!).ToList();

    /// <summary>The cells of <paramref name="column"/> as plain text: an integer's digits or a string's value.</summary>
    private static List<string> Column(List<QueryResultRow> rows, string column) =>
        rows.Select(r => r.Row[column] is { Type: ColumnType.Integer64 } n ? n.LongValue.ToString() : r.Row[column].StrValue ?? "").ToList();

    // ── Sort ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Sort_OfWideRows_SpillsOnTheByteCapAlone()
    {
        const string sql = "SELECT k, payload FROM docs ORDER BY k";

        Fixture off = await Setup(SpillOff);
        Fixture on = await Setup(SpillOnBytes);

        (List<QueryResultRow> offRows, bool offSpilled) = await RunProbed(off, sql);
        (List<QueryResultRow> onRows, bool onSpilled) = await RunProbed(on, sql);

        Assert.IsFalse(offSpilled, "with spill disabled the byte cap must not apply");
        Assert.IsTrue(onSpilled, "40 rows of 16 KiB pass a 32 KiB cap long before the 500,000-row cap");
        Assert.AreEqual(Enumerable.Range(0, RowCount).Select(i => i.ToString()), Column(onRows, "k"));
        CollectionAssert.AreEqual(Column(offRows, "payload"), Column(onRows, "payload"));
    }

    [Test]
    public async Task Sort_WithNoByteCap_DoesNotSpill()
    {
        Fixture f = await Setup(SpillOnNoByteCap);

        (List<QueryResultRow> rows, bool spilled) = await RunProbed(f, "SELECT k, payload FROM docs ORDER BY k");

        Assert.AreEqual(RowCount, rows.Count);
        Assert.IsFalse(spilled, "a byte cap of zero removes the byte cap");
    }

    // ── Top-k ────────────────────────────────────────────────────────────────

    [Test]
    public async Task TopK_OfWideRows_HandsOverToTheSpillingSelection()
    {
        const string sql = "SELECT k, payload FROM docs ORDER BY k DESC LIMIT 20";

        Fixture off = await Setup(SpillOff);
        Fixture on = await Setup(SpillOnBytes);

        CollectionAssert.Contains(await PlanNodes(on, sql), "topk");

        (List<QueryResultRow> offRows, bool offSpilled) = await RunProbed(off, sql);
        (List<QueryResultRow> onRows, bool onSpilled) = await RunProbed(on, sql);

        Assert.IsFalse(offSpilled);
        Assert.IsTrue(onSpilled, "20 retained rows of 16 KiB pass the 32 KiB cap, so the retention hands over");
        Assert.AreEqual(Enumerable.Range(20, 20).Reverse().Select(i => i.ToString()), Column(onRows, "k"));
        CollectionAssert.AreEqual(Column(offRows, "payload"), Column(onRows, "payload"));
    }

    [Test]
    public async Task TopK_WithAComputedKey_HandsOverWithTheSameKeys()
    {
        const string sql = "SELECT k, payload FROM docs ORDER BY k * 3 + 1 LIMIT 15 OFFSET 5";

        Fixture off = await Setup(SpillOff);
        Fixture on = await Setup(SpillOnBytes);

        CollectionAssert.Contains(await PlanNodes(on, sql), "topk");

        (List<QueryResultRow> offRows, _) = await RunProbed(off, sql);
        (List<QueryResultRow> onRows, bool onSpilled) = await RunProbed(on, sql);

        Assert.IsTrue(onSpilled);
        Assert.AreEqual(Enumerable.Range(5, 15).Select(i => i.ToString()), Column(onRows, "k"));
        CollectionAssert.AreEqual(Column(offRows, "payload"), Column(onRows, "payload"));

        // The carrier columns of the spilling selection must not leak into the result.
        Assert.IsTrue(onRows.All(r => r.Row.Keys.All(key => !key.StartsWith('~'))));
    }

    /// <summary>
    /// The spilling selection keeps at most about k rows on disk: it drops a row that cannot enter the
    /// result and deletes a replaced run at once. A disk limit that fails the full external sort of
    /// the same rows therefore does not fail the top-k, which would not be true if the top-k handed
    /// its input to the external sort.
    /// </summary>
    [Test]
    public async Task TopK_OfWideRows_KeepsItsDiskUseBounded()
    {
        // The full sort writes 40 rows of about 4 KiB each, far past 80 KiB. The top-6 selection
        // writes at most two runs of 6 rows at once.
        Fixture f = await Setup(SpillOnBytes with { SpillMaxTotalBytes = 80 * 1024 });

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Run(f, "SELECT k, payload FROM docs ORDER BY k DESC"))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SpillLimitExceeded), "the limit must be tight enough to stop a full sort");

        const string sql = "SELECT k, payload FROM docs ORDER BY k DESC LIMIT 6";
        CollectionAssert.Contains(await PlanNodes(f, sql), "topk");

        (List<QueryResultRow> rows, bool spilled) = await RunProbed(f, sql);

        Assert.IsTrue(spilled, "6 retained rows of 16 KiB pass the 32 KiB cap");
        Assert.AreEqual(Enumerable.Range(34, 6).Reverse().Select(i => i.ToString()), Column(rows, "k"));
        Assert.AreEqual(Enumerable.Range(34, 6).Reverse().Select(i => Wide("p" + i)), Column(rows, "payload"));
    }

    [Test]
    public async Task TopK_UnderTheByteCap_DoesNotSpill()
    {
        Fixture f = await Setup(SpillOnBytes);

        (List<QueryResultRow> rows, bool spilled) = await RunProbed(f, "SELECT k, payload FROM docs ORDER BY k LIMIT 1");

        Assert.AreEqual(0L, rows.Single().Row["k"].LongValue);
        Assert.IsFalse(spilled, "one retained row of 16 KiB stays under the 32 KiB cap");
    }

    // ── GROUP BY and DISTINCT ────────────────────────────────────────────────

    [Test]
    public async Task GroupBy_OnAWideKey_SpillsOnTheByteCapAlone()
    {
        const string sql = "SELECT grp, COUNT(*) AS cnt, SUM(k) AS total FROM docs GROUP BY grp";

        Fixture off = await Setup(SpillOff);
        Fixture on = await Setup(SpillOnBytes);

        (List<QueryResultRow> offRows, bool offSpilled) = await RunProbed(off, sql);
        (List<QueryResultRow> onRows, bool onSpilled) = await RunProbed(on, sql);

        Assert.IsFalse(offSpilled);
        Assert.IsTrue(onSpilled, "10 groups with 8 KiB keys pass a 32 KiB cap long before the 500,000-group cap");
        Assert.AreEqual(GroupCount, onRows.Count);
        Assert.IsTrue(onRows.All(r => r.Row["cnt"].LongValue == RowCount / GroupCount), "every row is counted once");
        CollectionAssert.AreEquivalent(
            offRows.Select(r => (r.Row["grp"].StrValue, r.Row["total"].LongValue)),
            onRows.Select(r => (r.Row["grp"].StrValue, r.Row["total"].LongValue)));
    }

    [Test]
    public async Task Distinct_OfWideTuples_SpillsOnTheByteCapAlone()
    {
        const string sql = "SELECT DISTINCT grp FROM docs";

        Fixture off = await Setup(SpillOff);
        Fixture on = await Setup(SpillOnBytes);

        (List<QueryResultRow> offRows, _) = await RunProbed(off, sql);
        (List<QueryResultRow> onRows, bool onSpilled) = await RunProbed(on, sql);

        Assert.IsTrue(onSpilled);
        Assert.AreEqual(GroupCount, onRows.Count, "each distinct tuple is returned exactly once");
        CollectionAssert.AreEquivalent(Column(offRows, "grp"), Column(onRows, "grp"));
    }

    // ── Hash join ────────────────────────────────────────────────────────────

    [Test]
    public async Task HashJoin_WithAWideBuild_RunsAsGraceHash()
    {
        const string sql = "SELECT a.k AS ak, b.payload AS bp FROM docs a JOIN docs b ON a.k = b.k";

        Fixture off = await Setup(SpillOff);
        Fixture on = await Setup(SpillOnBytes);
        Fixture noCap = await Setup(SpillOnNoByteCap);

        foreach (Fixture f in new[] { off, on, noCap })
            f.Executor.Statistics.ForceHashJoinForTesting = true;

        string onDetail = JoinDetail(await Run(on, "EXPLAIN (ANALYZE) " + sql));
        string noCapDetail = JoinDetail(await Run(noCap, "EXPLAIN (ANALYZE) " + sql));

        Assert.That(onDetail, Does.Contain("ran=grace-hash"), "a 40-row build of 16 KiB rows passes the 32 KiB cap");
        Assert.That(noCapDetail, Does.Not.Contain("ran="), "without the byte cap the same build stays in memory");

        List<QueryResultRow> offRows = await Run(off, sql);
        List<QueryResultRow> onRows = await Run(on, sql);

        Assert.AreEqual(RowCount, onRows.Count);
        CollectionAssert.AreEquivalent(
            offRows.Select(r => (r.Row["ak"].LongValue, r.Row["bp"].StrValue)),
            onRows.Select(r => (r.Row["ak"].LongValue, r.Row["bp"].StrValue)));
    }

    private static string JoinDetail(List<QueryResultRow> explain) =>
        explain.Single(r => r.Row["node"].StrValue == "hash-join").Row["detail"].StrValue ?? "";

    // ── Spill-capable buffers ────────────────────────────────────────────────

    private QueryExecutionContext Context(CamusDBOptions options, StatementProbe probe) =>
        new(options, spillDirectory: _dataDir, probe: probe);

    [Test]
    public async Task RowList_OfWideRows_SpillsOnTheByteCapAndKeepsOrder()
    {
        StatementProbe probe = new();
        await using SpillableRowList list = new(Context(SpillOnBytes, probe));

        for (int i = 0; i < 10; i++)
        {
            await list.AddAsync(new QueryResultRow(ObjectIdGenerator.Generate(), new Dictionary<string, ColumnValue>
            {
                { "n", new(ColumnType.Integer64, (long)i) },
                { "s", new(ColumnType.String, Wide("r" + i)) },
            }));
        }

        await list.SealAsync();

        Assert.IsTrue(probe.Spilled, "ten 8 KiB rows pass the 32 KiB cap");
        Assert.AreEqual(Enumerable.Range(0, 10).Select(i => (long)i), (await list.EnumerateAsync().ToListAsync()).Select(r => r.Row["n"].LongValue));
    }

    [Test]
    public async Task RowList_WithSpillDisabled_IgnoresTheByteCap()
    {
        StatementProbe probe = new();
        await using SpillableRowList list = new(Context(SpillOff, probe));

        for (int i = 0; i < 10; i++)
            await list.AddAsync(new QueryResultRow(default, new Dictionary<string, ColumnValue> { { "s", new(ColumnType.String, Wide("r" + i)) } }));

        await list.SealAsync();

        Assert.IsFalse(probe.Spilled);
        Assert.AreEqual(10, (await list.EnumerateAsync().ToListAsync()).Count);
    }

    [Test]
    public async Task ValueList_OfWideValues_SpillsOnTheByteCap()
    {
        StatementProbe probe = new();
        await using SpillableValueList list = new(Context(SpillOnBytes, probe));

        for (int i = 0; i < 10; i++)
            await list.AddAsync(new ColumnValue(ColumnType.String, Wide("v" + i)));

        await list.SealAsync();

        Assert.IsTrue(list.IsSpilled, "ten 8 KiB values pass the 32 KiB cap");
        Assert.AreEqual(
            Enumerable.Range(0, 10).Select(i => Wide("v" + i)),
            (await list.EnumerateAsync().ToListAsync()).Select(v => v.StrValue));
    }
}
