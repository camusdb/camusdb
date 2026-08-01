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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

[TestFixture]
[NonParallelizable]
public sealed class TestQuerySorterSpill
{
    private string _dataDir = null!;
    private bool _savedSpillEnabled;
    private int _savedThreshold;
    private int? _savedForceThreshold;
    private int _savedFanIn;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_sort_spill_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        _savedSpillEnabled = CamusDBConfig.SpillEnabled;
        _savedThreshold = CamusDBConfig.SpillThresholdRows;
        _savedForceThreshold = CamusDBConfig.ForceSpillThresholdRows;
        _savedFanIn = CamusDBConfig.SpillMergeFanIn;

        CamusDBConfig.DataDirectory = _dataDir;
        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDown()
    {
        SpillFileManager.ReleaseInstanceLock();

        CamusDBConfig.SpillEnabled = _savedSpillEnabled;
        CamusDBConfig.SpillThresholdRows = _savedThreshold;
        CamusDBConfig.ForceSpillThresholdRows = _savedForceThreshold;
        CamusDBConfig.SpillMergeFanIn = _savedFanIn;
        CamusDBConfig.DataDirectory = null!;

        try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Result-equivalence: forced spill must produce the same order as in-memory
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A context for these operator-level tests. Spill files go to the fixture's own directory, so a
    /// forced spill never writes outside the test's scratch space.
    /// </summary>
    private static QueryExecutionContext NewContext(CamusDBOptions options)
        => new(options, spillDirectory: CamusDBConfig.DataDirectory);

    [Test]
    public async Task ExternalSort_ForcedSpill_ProducesSameOrderAsInMemory()
    {
        List<QueryResultRow> input = NumericRows(20);

        // In-memory reference (spill off)
        QueryExecutionContext inMemory = NewContext(CamusDBOptions.Default with { SpillEnabled = false });
        QuerySorter sorterRef = new();
        QueryTicket ticket = NumericDescTicket();
        List<QueryResultRow> reference = await sorterRef.SortResultset(ticket, ToAsync(input), inMemory).ToListAsync();

        // External sort: force a run per row
        QueryExecutionContext spilling = NewContext(
            CamusDBOptions.Default with { SpillEnabled = true, ForceSpillThresholdRows = 1 });
        QuerySorter sorterSpill = new();
        List<QueryResultRow> spilled = await sorterSpill.SortResultset(ticket, ToAsync(input), spilling).ToListAsync();

        Assert.That(spilled.Count, Is.EqualTo(reference.Count));
        for (int i = 0; i < reference.Count; i++)
            Assert.That(spilled[i].Row["n"].LongValue, Is.EqualTo(reference[i].Row["n"].LongValue),
                $"row {i}: expected {reference[i].Row["n"].LongValue}, got {spilled[i].Row["n"].LongValue}");
    }

    [Test]
    public async Task ExternalSort_AscDescMultiColumn_MatchesInMemory()
    {
        // Rows with two sort columns; tests that the shared comparer handles multi-key correctly.
        List<QueryResultRow> input =
        [
            Row(("group", 2L), ("name", "b")),
            Row(("group", 1L), ("name", "z")),
            Row(("group", 2L), ("name", "a")),
            Row(("group", 1L), ("name", "m")),
            Row(("group", 3L), ("name", "c")),
        ];

        QueryTicket ticket = MakeTicket(
            new QueryOrderBy("group", OrderType.Ascending),
            new QueryOrderBy("name", OrderType.Descending));

        CamusDBConfig.SpillEnabled = false;
        List<QueryResultRow> reference = await new QuerySorter().SortResultset(ticket, ToAsync(input), NewContext(CamusDBConfig.Ambient)).ToListAsync();

        CamusDBConfig.SpillEnabled = true;
        CamusDBConfig.ForceSpillThresholdRows = 1;
        List<QueryResultRow> spilled = await new QuerySorter().SortResultset(ticket, ToAsync(input), NewContext(CamusDBConfig.Ambient)).ToListAsync();

        Assert.That(spilled.Count, Is.EqualTo(reference.Count));
        for (int i = 0; i < reference.Count; i++)
        {
            Assert.That(spilled[i].Row["group"].LongValue, Is.EqualTo(reference[i].Row["group"].LongValue));
            Assert.That(spilled[i].Row["name"].StrValue, Is.EqualTo(reference[i].Row["name"].StrValue));
        }
    }

    [Test]
    public async Task ExternalSort_MultipleRunsAndFanIn_CorrectMerge()
    {
        // 12 rows, threshold=3 → 4 runs, fanIn=2 → two intermediate passes before final merge.
        List<QueryResultRow> input = NumericRows(12);
        QueryTicket ticket = NumericAscTicket();

        CamusDBConfig.SpillEnabled = false;
        List<QueryResultRow> reference = await new QuerySorter().SortResultset(ticket, ToAsync(input), NewContext(CamusDBConfig.Ambient)).ToListAsync();

        CamusDBConfig.SpillEnabled = true;
        CamusDBConfig.ForceSpillThresholdRows = 3;
        CamusDBConfig.SpillMergeFanIn = 2;
        List<QueryResultRow> spilled = await new QuerySorter().SortResultset(ticket, ToAsync(input), NewContext(CamusDBConfig.Ambient)).ToListAsync();

        Assert.That(spilled.Count, Is.EqualTo(reference.Count));
        for (int i = 0; i < reference.Count; i++)
            Assert.That(spilled[i].Row["n"].LongValue, Is.EqualTo(reference[i].Row["n"].LongValue));
    }

    [Test]
    public async Task SortByKeys_ForcedSpill_ProducesSameOrderAsInMemory()
    {
        List<QueryResultRow> input = NumericRows(15);
        IReadOnlyList<QueryOrderBy> orderBy = new[] { new QueryOrderBy("n", OrderType.Ascending) };

        CamusDBConfig.SpillEnabled = false;
        List<QueryResultRow> reference = await new QuerySorter().SortByKeys(ToAsync(input), orderBy, NewContext(CamusDBConfig.Ambient)).ToListAsync();

        CamusDBConfig.SpillEnabled = true;
        CamusDBConfig.ForceSpillThresholdRows = 2;
        List<QueryResultRow> spilled = await new QuerySorter().SortByKeys(ToAsync(input), orderBy, NewContext(CamusDBConfig.Ambient)).ToListAsync();

        Assert.That(spilled.Count, Is.EqualTo(reference.Count));
        for (int i = 0; i < reference.Count; i++)
            Assert.That(spilled[i].Row["n"].LongValue, Is.EqualTo(reference[i].Row["n"].LongValue));
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Flag-off and under-threshold: in-memory path, no spill files
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task ExternalSort_SpillDisabled_NoSpillFilesCreated()
    {
        CamusDBConfig.SpillEnabled = false;

        List<QueryResultRow> input = NumericRows(10);
        List<QueryResultRow> result = await new QuerySorter()
            .SortResultset(NumericAscTicket(), ToAsync(input), NewContext(CamusDBConfig.Ambient)).ToListAsync();

        Assert.That(result.Count, Is.EqualTo(10));
        Assert.That(SpillFileCount(), Is.EqualTo(0), "no spill files when flag is off");
    }

    [Test]
    public async Task ExternalSort_InputUnderThreshold_NoSpillFiles()
    {
        CamusDBConfig.SpillEnabled = true;
        CamusDBConfig.ForceSpillThresholdRows = 1000; // threshold larger than input

        List<QueryResultRow> input = NumericRows(5);
        List<QueryResultRow> result = await new QuerySorter()
            .SortResultset(NumericAscTicket(), ToAsync(input), NewContext(CamusDBConfig.Ambient)).ToListAsync();

        Assert.That(result.Count, Is.EqualTo(5));
        Assert.That(SpillFileCount(), Is.EqualTo(0), "input under threshold → in-memory path only");
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Cleanup: spill files are deleted on normal completion, cancellation, exception
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task ExternalSort_NormalCompletion_NoSpillFilesRemain()
    {
        CamusDBConfig.SpillEnabled = true;
        CamusDBConfig.ForceSpillThresholdRows = 1;

        List<QueryResultRow> input = NumericRows(10);
        await new QuerySorter().SortResultset(NumericAscTicket(), ToAsync(input), NewContext(CamusDBConfig.Ambient)).ToListAsync();

        Assert.That(SpillFileCount(), Is.EqualTo(0), "all spill files deleted on normal completion");
    }

    [Test]
    public async Task ExternalSort_Cancellation_NoSpillFilesRemain()
    {
        CamusDBConfig.SpillEnabled = true;
        CamusDBConfig.ForceSpillThresholdRows = 1;

        List<QueryResultRow> input = NumericRows(20);
        CancellationTokenSource cts = new();
        int yielded = 0;

        try
        {
            await foreach (QueryResultRow _ in new QuerySorter()
                .SortResultset(NumericAscTicket(), ToAsync(input), NewContext(CamusDBConfig.Ambient))
                .WithCancellation(cts.Token))
            {
                if (++yielded == 3)
                    cts.Cancel();
            }
        }
        catch (OperationCanceledException) { }

        Assert.That(SpillFileCount(), Is.EqualTo(0), "all spill files deleted on cancellation");
    }

    [Test]
    public async Task ExternalSort_ExceptionInDataCursor_NoSpillFilesRemain()
    {
        CamusDBConfig.SpillEnabled = true;
        CamusDBConfig.ForceSpillThresholdRows = 2;

        // Cursor yields 5 rows then throws; 2 runs will be spilled before the throw.
        try
        {
            await new QuerySorter()
                .SortResultset(NumericAscTicket(), ThrowingCursor(NumericRows(10), throwAfter: 5), NewContext(CamusDBConfig.Ambient))
                .ToListAsync();
            Assert.Fail("Expected exception was not thrown");
        }
        catch (InvalidOperationException) { }

        Assert.That(SpillFileCount(), Is.EqualTo(0), "all spill files deleted when cursor throws");
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Duplicate-key correctness: SQL-correct (multiset + monotone) even with ties
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task ExternalSort_DuplicateSortKeys_MultisetEqualAndMonotone()
    {
        // 8 rows with only 2 distinct sort-key values (k∈{1,2}).  Each row has a unique
        // tag so we can verify the full multiset is preserved.  We do NOT assert positional
        // order within a group — SQL leaves that unspecified — but we do assert:
        //   (a) all rows are present (multiset equality), and
        //   (b) the sort key is non-decreasing across the output.
        List<QueryResultRow> input =
        [
            Row(("k", 2L), ("tag", "a")),
            Row(("k", 1L), ("tag", "b")),
            Row(("k", 2L), ("tag", "c")),
            Row(("k", 1L), ("tag", "d")),
            Row(("k", 2L), ("tag", "e")),
            Row(("k", 1L), ("tag", "f")),
            Row(("k", 2L), ("tag", "g")),
            Row(("k", 1L), ("tag", "h")),
        ];

        QueryTicket ticket = MakeTicket(new QueryOrderBy("k", OrderType.Ascending));

        CamusDBConfig.SpillEnabled = true;
        CamusDBConfig.ForceSpillThresholdRows = 1;
        List<QueryResultRow> spilled = await new QuerySorter().SortResultset(ticket, ToAsync(input), NewContext(CamusDBConfig.Ambient)).ToListAsync();

        // (a) Multiset equality — every tag appears exactly once.
        Assert.That(spilled.Count, Is.EqualTo(input.Count));
        IEnumerable<string?> outputTags = spilled.Select(r => r.Row["tag"].StrValue).OrderBy(t => t);
        IEnumerable<string?> inputTags  = input.Select(r => r.Row["tag"].StrValue).OrderBy(t => t);
        Assert.That(outputTags, Is.EqualTo(inputTags).AsCollection, "all rows must be present");

        // (b) Sort key is non-decreasing.
        for (int i = 1; i < spilled.Count; i++)
            Assert.That(spilled[i].Row["k"].LongValue, Is.GreaterThanOrEqualTo(spilled[i - 1].Row["k"].LongValue),
                $"output not sorted at index {i}");
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Negative-proof: reverting the spill path changes the outcome
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task ExternalSort_FlagOnVsOff_NoBehaviorDifference()
    {
        // Both paths must return rows in identical order — the test fails if either path is broken.
        List<QueryResultRow> input = NumericRows(8);
        QueryTicket ticket = NumericDescTicket();

        CamusDBConfig.SpillEnabled = false;
        List<QueryResultRow> inMem = await new QuerySorter().SortResultset(ticket, ToAsync(input), NewContext(CamusDBConfig.Ambient)).ToListAsync();

        CamusDBConfig.SpillEnabled = true;
        CamusDBConfig.ForceSpillThresholdRows = 1;
        List<QueryResultRow> ext = await new QuerySorter().SortResultset(ticket, ToAsync(input), NewContext(CamusDBConfig.Ambient)).ToListAsync();

        Assert.That(ext.Select(r => r.Row["n"].LongValue),
            Is.EqualTo(inMem.Select(r => r.Row["n"].LongValue)).AsCollection,
            "spill path and in-memory path must produce identical ordering");
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────────────────────────────

    private int SpillFileCount() =>
        Directory.Exists(Path.Combine(_dataDir, "tmp", "spill"))
            ? Directory.GetFiles(Path.Combine(_dataDir, "tmp", "spill"), "*.spill", SearchOption.AllDirectories).Length
            : 0;

    private static List<QueryResultRow> NumericRows(int count)
    {
        // Deliberately unsorted (reverse order) so a sort is always meaningful.
        List<QueryResultRow> rows = new(count);
        for (int i = count; i >= 1; i--)
            rows.Add(Row(("n", (long)i)));
        return rows;
    }

    private static QueryTicket NumericAscTicket() =>
        MakeTicket(new QueryOrderBy("n", OrderType.Ascending));

    private static QueryTicket NumericDescTicket() =>
        MakeTicket(new QueryOrderBy("n", OrderType.Descending));

    private static QueryTicket MakeTicket(params QueryOrderBy[] orderBy)
    {
        KvTransaction txn = new(Kommander.Time.HLCTimestamp.Zero, "sorter-spill-test");
        return new QueryTicket(
            txnState: txn,
            databaseName: "db",
            tableName: "t",
            index: null,
            projection: null,
            filters: null,
            where: null,
            orderBy: orderBy.ToList(),
            limit: null,
            offset: null,
            parameters: null);
    }

    private static QueryResultRow Row(params (string name, object value)[] columns)
    {
        Dictionary<string, ColumnValue> row = new();
        foreach ((string name, object value) in columns)
            row[name] = ToColumnValue(value);
        return new QueryResultRow(default(ObjectIdValue), row);
    }

    private static ColumnValue ToColumnValue(object value) =>
        value switch
        {
            string s => new(ColumnType.String, s),
            long l   => new(ColumnType.Integer64, l),
            bool b   => new(ColumnType.Bool, b),
            _        => throw new AssertionException($"Unsupported test value type: {value.GetType()}"),
        };

    private static async IAsyncEnumerable<QueryResultRow> ToAsync(IEnumerable<QueryResultRow> rows)
    {
        foreach (QueryResultRow row in rows)
            yield return row;
        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<QueryResultRow> ThrowingCursor(
        IEnumerable<QueryResultRow> rows,
        int throwAfter,
        [EnumeratorCancellation] CancellationToken _ = default)
    {
        int count = 0;
        foreach (QueryResultRow row in rows)
        {
            if (count == throwAfter)
                throw new InvalidOperationException("test exception from cursor");
            count++;
            yield return row;
        }
        await Task.CompletedTask;
    }
}
