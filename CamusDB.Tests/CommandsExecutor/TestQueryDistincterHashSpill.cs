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
using CamusDB.Core.Diagnostics;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Operator-level tests for the spill-aware DISTINCT in <see cref="QueryDistincter"/>.
///
/// <para>The spill path dedups through a hash set exactly like the spill-off path and spills only
/// when the count of <b>distinct</b> tuples reaches the threshold. These tests pin the three
/// properties that make spill on safe as a default: below the threshold it is indistinguishable from
/// spill off (same rows, same order, streaming); above it every distinct tuple is returned exactly
/// once, including duplicates of rows returned before the overflow; and its spill files are deleted
/// on completion, abandonment and cancellation.</para>
/// </summary>
[TestFixture]
// Serial: SpillFileManager's instance lock is process-wide, so two fixtures holding it at once
// would write spill files into each other's directory.
[NonParallelizable]
public sealed class TestQueryDistincterHashSpill
{
    private static readonly RowLayout LayoutA = RowLayout.ForColumns(["city", "n"]);

    // Same columns, reversed ordinals: a row in this layout equals a LayoutA row with the same values.
    private static readonly RowLayout LayoutB = RowLayout.ForColumns(["n", "city"]);

    private string _dataDir = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_dist_hash_spill_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDown()
    {
        SpillFileManager.ReleaseInstanceLock();

        try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    private QueryExecutionContext Context(CamusDBOptions options, StatementProbe? probe = null, CancellationToken ct = default)
        => new(options, ct, spillDirectory: _dataDir, probe: probe);

    private static CamusDBOptions SpillOff => CamusDBOptions.Default with { SpillEnabled = false };

    private static CamusDBOptions SpillOn(int thresholdRows, int fanIn = 4) =>
        CamusDBOptions.Default with
        {
            SpillEnabled = true,
            ForceSpillThresholdRows = thresholdRows,
            SpillMergeFanIn = fanIn,
        };

    [Test]
    public async Task BelowThreshold_SpillOnReturnsSameRowsInSameOrderAsSpillOff()
    {
        List<QueryResultRow> input = Repeated(distinct: 37, copies: 5);

        List<QueryResultRow> off = await Distinct(input, Context(SpillOff));

        StatementProbe probe = new();
        List<QueryResultRow> on = await Distinct(input, Context(SpillOn(1_000), probe));

        Assert.That(off, Has.Count.EqualTo(37));
        Assert.That(on, Has.Count.EqualTo(off.Count));
        for (int i = 0; i < off.Count; i++)
            Assert.That(on[i].Row, Is.SameAs(off[i].Row), $"row {i}: spill on below the threshold must return the same row in the same position");

        Assert.That(probe.Spilled, Is.False);
        AssertNoSpillFiles();
    }

    [Test]
    public async Task LowCardinality_ManyRows_NeverSpills()
    {
        // 5,000 rows but 3 tuples: the trigger counts distinct tuples, not input rows.
        List<QueryResultRow> input = Repeated(distinct: 3, copies: 5_000 / 3);

        StatementProbe probe = new();
        List<QueryResultRow> on = await Distinct(input, Context(SpillOn(10), probe));

        Assert.That(on, Has.Count.EqualTo(3));
        Assert.That(probe.Spilled, Is.False, "a DISTINCT with fewer tuples than the threshold must not spill");
        AssertNoSpillFiles();
    }

    [Test]
    public async Task ForcedSpill_ReturnsEachDistinctTupleExactlyOnce()
    {
        // Every tuple appears three times, spread over the whole input, so duplicates of the rows
        // returned before the overflow keep arriving after it and must still be suppressed.
        List<QueryResultRow> input = Repeated(distinct: 40, copies: 3);

        StatementProbe probe = new();
        List<QueryResultRow> on = await Distinct(input, Context(SpillOn(4), probe));

        Assert.That(probe.Spilled, Is.True, "the test must exercise the spill path");
        AssertExactlyOnce(on, ExpectedKeys(input));

        // The rows returned before the overflow stream in first-seen order, as with spill off.
        List<QueryResultRow> off = await Distinct(input, Context(SpillOff));
        for (int i = 0; i < 4; i++)
            Assert.That(Key(on[i]), Is.EqualTo(Key(off[i])), $"row {i} precedes the overflow and must be in first-seen order");

        AssertNoSpillFiles();
    }

    [Test]
    public async Task ForcedSpill_NullTuplesOnBothSidesOfTheOverflow_Collapse()
    {
        List<QueryResultRow> input =
        [
            RowA(null, 1), RowA("a", 1), RowA(null, 1),
            RowA("b", 1), RowA("c", 1), RowA("d", 1),
            RowA(null, 1), RowA(null, 2), RowA("e", 1), RowA(null, 2), RowA(null, 1),
        ];

        StatementProbe probe = new();
        List<QueryResultRow> on = await Distinct(input, Context(SpillOn(2), probe));

        Assert.That(probe.Spilled, Is.True);
        AssertExactlyOnce(on, ExpectedKeys(input));
        AssertNoSpillFiles();
    }

    [Test]
    public async Task ForcedSpill_DuplicateInOtherLayoutAfterOverflow_IsSuppressed()
    {
        // The carried row is positional (LayoutA); its later duplicates arrive in LayoutB and come
        // back from the spill file as dictionary rows. Equality must hold across all three forms.
        List<QueryResultRow> input =
        [
            RowA("x", 1), RowA("y", 2),
            RowA("z", 3), RowB("x", 1), RowB("w", 4), RowA("y", 2), RowB("z", 3), RowB("w", 4),
        ];

        StatementProbe probe = new();
        List<QueryResultRow> on = await Distinct(input, Context(SpillOn(2, fanIn: 2), probe));

        Assert.That(probe.Spilled, Is.True);
        AssertExactlyOnce(on, ["x|1", "y|2", "z|3", "w|4"]);
        AssertNoSpillFiles();
    }

    [Test]
    public async Task ForcedSpill_RecursivePartitioning_ReturnsEachTupleExactlyOnce()
    {
        // Threshold 2 with fan-in 2 forces partitions far past the threshold, so they repartition.
        List<QueryResultRow> input = Repeated(distinct: 200, copies: 2);

        StatementProbe probe = new();
        List<QueryResultRow> on = await Distinct(input, Context(SpillOn(2, fanIn: 2), probe));

        Assert.That(probe.Spilled, Is.True);
        AssertExactlyOnce(on, ExpectedKeys(input));
        AssertNoSpillFiles();
    }

    [Test]
    public async Task ForcedSpill_BeyondRecursionDepthCap_StillExact()
    {
        // Threshold 1 cannot be met by any partition with more than one tuple, so the recursion runs
        // to its depth cap and the last level dedups in memory.
        List<QueryResultRow> input = Repeated(distinct: 300, copies: 3);

        List<QueryResultRow> on = await Distinct(input, Context(SpillOn(1, fanIn: 2)));

        AssertExactlyOnce(on, ExpectedKeys(input));
        AssertNoSpillFiles();
    }

    [Test]
    public async Task SpillOn_LimitStopsTheInputEarly()
    {
        // The old sort-based spill path read the whole input before its first row. The hash path
        // returns each new tuple at once, so a LIMIT pulls only what it needs.
        CountingSource source = new(Repeated(distinct: 10_000, copies: 1));

        List<QueryResultRow> firstFive = await new QueryDistincter()
            .DistinctResultset(null!, source.Rows(), Context(SpillOn(1_000)))
            .Take(5)
            .ToListAsync();

        Assert.That(firstFive, Has.Count.EqualTo(5));
        Assert.That(source.Pulled, Is.LessThanOrEqualTo(6), "DISTINCT with spill on must stream, not drain its input");
    }

    [Test]
    public async Task ForcedSpill_AbandonedDuringPartitionPhase_DeletesSpillFiles()
    {
        List<QueryResultRow> input = Repeated(distinct: 50, copies: 2);

        int taken = 0;
        await foreach (QueryResultRow _ in new QueryDistincter().DistinctResultset(null!, ToAsync(input), Context(SpillOn(3))))
        {
            // Rows past the threshold come from the partition files, so stopping here leaves the
            // partition phase half done.
            if (++taken == 10)
                break;
        }

        Assert.That(taken, Is.EqualTo(10));
        AssertNoSpillFiles();
    }

    [Test]
    public void ForcedSpill_CancelledDuringPartitionPhase_ThrowsAndDeletesSpillFiles()
    {
        List<QueryResultRow> input = Repeated(distinct: 50, copies: 2);
        using CancellationTokenSource cts = new();

        Assert.That(async () =>
        {
            int taken = 0;
            await foreach (QueryResultRow _ in new QueryDistincter().DistinctResultset(null!, ToAsync(input), Context(SpillOn(3), ct: cts.Token)))
            {
                if (++taken == 10)
                    cts.Cancel();
            }
        }, Throws.InstanceOf<OperationCanceledException>());

        AssertNoSpillFiles();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static async Task<List<QueryResultRow>> Distinct(List<QueryResultRow> input, QueryExecutionContext context)
        => await new QueryDistincter().DistinctResultset(null!, ToAsync(input), context).ToListAsync();

    private static QueryResultRow RowA(string? city, long n) =>
        new(default, new QueryRow(default, LayoutA, [City(city), new(ColumnType.Integer64, n)]));

    private static QueryResultRow RowB(string? city, long n) =>
        new(default, new QueryRow(default, LayoutB, [new(ColumnType.Integer64, n), City(city)]));

    private static ColumnValue City(string? city) =>
        city is null ? new ColumnValue(ColumnType.Null, 0) : new ColumnValue(ColumnType.String, city);

    /// <summary>
    /// <paramref name="distinct"/> tuples, each repeated <paramref name="copies"/> times. Copy k of
    /// every tuple is emitted in its own pass, so all duplicates of an early tuple arrive late.
    /// </summary>
    private static List<QueryResultRow> Repeated(int distinct, int copies)
    {
        List<QueryResultRow> rows = new(distinct * copies);
        for (int c = 0; c < copies; c++)
        {
            for (int d = 0; d < distinct; d++)
                rows.Add(RowA("city" + d, d % 7));
        }
        return rows;
    }

    private static string Key(QueryResultRow row)
    {
        ColumnValue city = row.Row["city"];
        return (city.Type == ColumnType.Null ? "<null>" : city.StrValue) + "|" + row.Row["n"].LongValue;
    }

    private static List<string> ExpectedKeys(List<QueryResultRow> input) =>
        input.Select(Key).Distinct().ToList();

    private static void AssertExactlyOnce(List<QueryResultRow> output, List<string> expected)
    {
        List<string> keys = output.Select(Key).ToList();

        Assert.That(keys, Is.Unique, "a distinct tuple was returned more than once");
        Assert.That(keys, Is.EquivalentTo(expected), "the set of distinct tuples differs from the input's");
    }

    private void AssertNoSpillFiles()
    {
        string spillRoot = Path.Combine(_dataDir, "tmp", "spill");
        if (!Directory.Exists(spillRoot))
            return;

        Assert.That(Directory.GetFiles(spillRoot, "*.spill", SearchOption.AllDirectories), Is.Empty,
            "DISTINCT must delete its spill files");
    }

    private static async IAsyncEnumerable<QueryResultRow> ToAsync(IEnumerable<QueryResultRow> rows)
    {
        foreach (QueryResultRow row in rows)
            yield return row;
        await Task.CompletedTask;
    }

    /// <summary>Async source that counts how many rows the consumer actually pulled.</summary>
    private sealed class CountingSource
    {
        private readonly List<QueryResultRow> rows;

        public CountingSource(List<QueryResultRow> rows) => this.rows = rows;

        public int Pulled { get; private set; }

        public async IAsyncEnumerable<QueryResultRow> Rows([EnumeratorCancellation] CancellationToken ct = default)
        {
            foreach (QueryResultRow row in rows)
            {
                ct.ThrowIfCancellationRequested();
                Pulled++;
                yield return row;
            }
            await Task.CompletedTask;
        }
    }
}
