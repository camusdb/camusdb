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
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

[TestFixture]
// Serial: SpillFileManager's instance lock is process-wide, so two fixtures holding it at once
// would write spill files into each other's directory.
[NonParallelizable]
public sealed class TestQuerySorterSpill
{
    private string _dataDir = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_sort_spill_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDown()
    {
        SpillFileManager.ReleaseInstanceLock();

        try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Result-equivalence: forced spill must produce the same order as in-memory
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A context for these operator-level tests. Spill files go to the fixture's own directory, so a
    /// forced spill never writes outside the test's scratch space.
    /// </summary>
    private QueryExecutionContext NewContext(CamusDBOptions options)
        => new(options, spillDirectory: _dataDir);

    /// <summary>Spill disabled — the fully in-memory sort.</summary>
    private QueryExecutionContext SpillOff => NewContext(CamusDBOptions.Default with { SpillEnabled = false });

    /// <summary>
    /// Spill forced after <paramref name="thresholdRows"/> rows so the external merge sort runs on
    /// inputs small enough for a unit test. Independent of any other configuration in play.
    /// </summary>
    private QueryExecutionContext SpillOn(int thresholdRows, int fanIn = 4) =>
        NewContext(CamusDBOptions.Default with
        {
            SpillEnabled = true,
            ForceSpillThresholdRows = thresholdRows,
            SpillMergeFanIn = fanIn,
        });

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
        List<QueryResultRow> reference = await new QuerySorter().SortResultset(ticket, ToAsync(input), SpillOff).ToListAsync();
        List<QueryResultRow> spilled = await new QuerySorter().SortResultset(ticket, ToAsync(input), SpillOn(1)).ToListAsync();

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
        List<QueryResultRow> reference = await new QuerySorter().SortResultset(ticket, ToAsync(input), SpillOff).ToListAsync();
        List<QueryResultRow> spilled = await new QuerySorter().SortResultset(ticket, ToAsync(input), SpillOn(3, 2)).ToListAsync();

        Assert.That(spilled.Count, Is.EqualTo(reference.Count));
        for (int i = 0; i < reference.Count; i++)
            Assert.That(spilled[i].Row["n"].LongValue, Is.EqualTo(reference[i].Row["n"].LongValue));
    }

    [Test]
    public async Task SortByKeys_ForcedSpill_ProducesSameOrderAsInMemory()
    {
        List<QueryResultRow> input = NumericRows(15);
        IReadOnlyList<QueryOrderBy> orderBy = new[] { new QueryOrderBy("n", OrderType.Ascending) };
        List<QueryResultRow> reference = await new QuerySorter().SortByKeys(ToAsync(input), orderBy, SpillOff).ToListAsync();
        List<QueryResultRow> spilled = await new QuerySorter().SortByKeys(ToAsync(input), orderBy, SpillOn(2)).ToListAsync();

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

        List<QueryResultRow> input = NumericRows(10);
        List<QueryResultRow> result = await new QuerySorter()
            .SortResultset(NumericAscTicket(), ToAsync(input), SpillOff).ToListAsync();

        Assert.That(result.Count, Is.EqualTo(10));
        Assert.That(SpillFileCount(), Is.EqualTo(0), "no spill files when flag is off");
    }

    [Test]
    public async Task ExternalSort_InputUnderThreshold_NoSpillFiles()
    {

        List<QueryResultRow> input = NumericRows(5);
        List<QueryResultRow> result = await new QuerySorter()
            .SortResultset(NumericAscTicket(), ToAsync(input), SpillOn(1000)).ToListAsync();

        Assert.That(result.Count, Is.EqualTo(5));
        Assert.That(SpillFileCount(), Is.EqualTo(0), "input under threshold → in-memory path only");
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Cleanup: spill files are deleted on normal completion, cancellation, exception
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task ExternalSort_NormalCompletion_NoSpillFilesRemain()
    {

        List<QueryResultRow> input = NumericRows(10);
        await new QuerySorter().SortResultset(NumericAscTicket(), ToAsync(input), SpillOn(1)).ToListAsync();

        Assert.That(SpillFileCount(), Is.EqualTo(0), "all spill files deleted on normal completion");
    }

    [Test]
    public async Task ExternalSort_Cancellation_NoSpillFilesRemain()
    {

        List<QueryResultRow> input = NumericRows(20);
        CancellationTokenSource cts = new();
        int yielded = 0;

        try
        {
            await foreach (QueryResultRow _ in new QuerySorter()
                .SortResultset(NumericAscTicket(), ToAsync(input), SpillOn(1))
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

        // Cursor yields 5 rows then throws; 2 runs will be spilled before the throw.
        try
        {
            await new QuerySorter()
                .SortResultset(NumericAscTicket(), ThrowingCursor(NumericRows(10), throwAfter: 5), SpillOn(2))
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
        List<QueryResultRow> spilled = await new QuerySorter().SortResultset(ticket, ToAsync(input), SpillOn(1)).ToListAsync();

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
        List<QueryResultRow> inMem = await new QuerySorter().SortResultset(ticket, ToAsync(input), SpillOff).ToListAsync();
        List<QueryResultRow> ext = await new QuerySorter().SortResultset(ticket, ToAsync(input), SpillOn(1)).ToListAsync();

        Assert.That(ext.Select(r => r.Row["n"].LongValue),
            Is.EqualTo(inMem.Select(r => r.Row["n"].LongValue)).AsCollection,
            "spill path and in-memory path must produce identical ordering");
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Computed ordering keys across the spill round trip
    // ──────────────────────────────────────────────────────────────────────────

    private static CamusDB.Core.SQLParser.NodeAst Identifier(string name) => new(
        CamusDB.Core.SQLParser.NodeType.Identifier, null, null, null, null, null, null, null, name);

    /// <summary>
    /// A computed full sort over positional (<see cref="QueryRow"/>) inputs, forced through spill
    /// runs and intermediate merge passes. The rows must come back in the same order the
    /// in-memory sort produces, positional, under the <b>original</b> layout instance, with no
    /// carrier cell left in the backing array — the spill round trip must not demote the rows to
    /// dictionaries or leak the internal <c>~sort</c> columns.
    /// </summary>
    [Test]
    public async Task ComputedOrdering_ForcedSpill_KeepsRowsPositionalUnderTheInputLayout()
    {
        RowLayout layout = RowLayout.ForColumns(["a", "b"]);

        QueryResultRow Make(long a, long b) =>
            new(default, new QueryRow(default, layout, [new(ColumnType.Integer64, a), new(ColumnType.Integer64, b)]));

        // Duplicate computed keys plus a unique second key, so the total order is deterministic
        // and exact positional equality between the two paths is a fair requirement.
        List<QueryResultRow> input = new(12);
        for (long i = 0; i < 12; i++)
            input.Add(Make((12 - i) % 5, i));

        QueryTicket ticket = MakeTicket(
            new QueryOrderBy("k", OrderType.Ascending, Identifier("a")),
            new QueryOrderBy("b", OrderType.Ascending));

        List<QueryResultRow> reference = await new QuerySorter().SortResultset(ticket, ToAsync(input), SpillOff).ToListAsync();
        List<QueryResultRow> spilled = await new QuerySorter().SortResultset(ticket, ToAsync(input), SpillOn(3, 2)).ToListAsync();

        Assert.That(spilled.Select(r => r.Row["b"].LongValue),
            Is.EqualTo(reference.Select(r => r.Row["b"].LongValue)).AsCollection,
            "spilled and in-memory computed sorts must produce identical row orders");

        foreach (QueryResultRow row in spilled)
        {
            Assert.That(row.Row, Is.InstanceOf<QueryRow>(), "a positional input must stay positional across the spill round trip");

            QueryRow qr = (QueryRow)row.Row;
            Assert.That(ReferenceEquals(qr.Layout, layout), Is.True, "the strip must hand back the original layout instance");
            Assert.That(qr.Count, Is.EqualTo(2));
            Assert.That(qr.Values.Length, Is.EqualTo(2), "no carrier cell may remain in the backing array");
            Assert.That(row.Row.Keys.Any(static key => key.StartsWith('~')), Is.False, "carrier columns must not leak");
        }
    }

    /// <summary>
    /// Mixed row shapes (positional rows interleaved with dictionary rows) push the spill writer
    /// off the value-only format and the carrier onto its dictionary fallback. The result must
    /// still match the in-memory sort exactly, and no row of either shape may expose a
    /// <c>~sort</c> column.
    /// </summary>
    [Test]
    public async Task ComputedOrdering_ForcedSpill_MixedRowShapes_MatchInMemoryAndLeakNoCarrier()
    {
        RowLayout layout = RowLayout.ForColumns(["a", "b"]);

        List<QueryResultRow> input = new(10);
        for (long i = 0; i < 10; i++)
        {
            input.Add(i % 2 == 0
                ? new QueryResultRow(default, new QueryRow(default, layout,
                    [new(ColumnType.Integer64, (10 - i) % 4), new(ColumnType.Integer64, i)]))
                : Row(("a", (10 - i) % 4), ("b", i)));
        }

        QueryTicket ticket = MakeTicket(
            new QueryOrderBy("k", OrderType.Descending, Identifier("a")),
            new QueryOrderBy("b", OrderType.Ascending));

        List<QueryResultRow> reference = await new QuerySorter().SortResultset(ticket, ToAsync(input), SpillOff).ToListAsync();
        List<QueryResultRow> spilled = await new QuerySorter().SortResultset(ticket, ToAsync(input), SpillOn(3, 2)).ToListAsync();

        Assert.That(spilled.Select(r => r.Row["b"].LongValue),
            Is.EqualTo(reference.Select(r => r.Row["b"].LongValue)).AsCollection);

        foreach (QueryResultRow row in spilled)
        {
            Assert.That(row.Row.Keys.Any(static key => key.StartsWith('~')), Is.False, "carrier columns must not leak");
            Assert.That(row.Row.Count, Is.EqualTo(2), "every input column must survive, and nothing more");
        }
    }

    /// <summary>
    /// Randomized parity over the shapes the correctness constraints call out: mixed
    /// ascending/descending keys, NULLs in the computed key, duplicate keys, and a unique
    /// tie-breaking key that makes the total order deterministic. The spilled sort must equal
    /// the in-memory sort exactly, row for row.
    /// </summary>
    [Test]
    public async Task ComputedOrdering_ForcedSpill_RandomizedMixedDirections_MatchInMemoryExactly()
    {
        Random random = new(20260915);
        RowLayout layout = RowLayout.ForColumns(["a", "b", "tie"]);

        List<QueryResultRow> input = new(120);
        for (long i = 0; i < 120; i++)
        {
            ColumnValue a = random.Next(4) == 0
                ? ColumnValue.Null
                : new ColumnValue(ColumnType.Integer64, (long)random.Next(0, 7));

            input.Add(new QueryResultRow(default, new QueryRow(default, layout,
                [a, new(ColumnType.Integer64, (long)random.Next(0, 3)), new(ColumnType.Integer64, i)])));
        }

        QueryTicket ticket = MakeTicket(
            new QueryOrderBy("k", OrderType.Descending, Identifier("a")),
            new QueryOrderBy("b", OrderType.Ascending),
            new QueryOrderBy("tie", OrderType.Ascending));

        List<QueryResultRow> reference = await new QuerySorter().SortResultset(ticket, ToAsync(input), SpillOff).ToListAsync();
        List<QueryResultRow> spilled = await new QuerySorter().SortResultset(ticket, ToAsync(input), SpillOn(7, 2)).ToListAsync();

        Assert.That(reference.Count, Is.EqualTo(120));
        Assert.That(spilled.Select(r => r.Row["tie"].LongValue),
            Is.EqualTo(reference.Select(r => r.Row["tie"].LongValue)).AsCollection,
            "the deterministic total order must be identical on both paths");
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
