
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Measures the hash-join key lookup shape: the legacy form allocated a fresh
/// <see cref="ColumnValue"/> array plus a <see cref="CompositeColumnValue"/> wrapper for every
/// build and probe row; the current form fills one reused scratch array and probes the table
/// through the span alternate lookup of the join comparer, so an owned key is allocated only
/// when a new bucket is inserted.
///
/// Three workloads bound the decision:
///   BuildDuplicateHeavy — 100k build rows over 1k distinct keys. Owned-key inserts fall from
///     one per row to one per distinct key.
///   ProbeMostlyMissing — 100k probes, ~90% misses. The legacy form paid two allocations per
///     miss; the alternate form pays none.
///   SmallJoin — 64 build rows + 64 probes. Guards the rejection gate: the alternate lookup's
///     extra dispatch must not regress small joins.
///
/// The loops copy pre-materialized key values per row, mirroring what the executor's
/// key-extraction does, so the two forms differ only in the mechanism under test.
/// </summary>
[Config(typeof(RowDecodeConfig))]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class HashJoinKeyLookupBenchmarks
{
    private const int LargeRows = 100_000;
    private const int DistinctKeys = 1_000;
    private const int SmallRows = 64;

    private static readonly CompositeColumnValueComparer Comparer =
        CompositeColumnValueComparer.Instance;

    // Per-row key values for each workload, pre-materialized so the measured loops do the
    // same reference copies the executor's key extraction does.
    private ColumnValue[][] _duplicateHeavyBuildKeys = null!;
    private ColumnValue[][] _mostlyMissingProbeKeys = null!;
    private ColumnValue[][] _smallBuildKeys = null!;
    private ColumnValue[][] _smallProbeKeys = null!;

    private Dictionary<CompositeColumnValue, List<int>> _probeTable = null!;

    private static ColumnValue[] MakeKey(int i) =>
        [new ColumnValue(ColumnType.Integer64, i), new ColumnValue(ColumnType.String, "k_" + (i & 15))];

    [GlobalSetup]
    public void GlobalSetup()
    {
        _duplicateHeavyBuildKeys = new ColumnValue[LargeRows][];
        for (int i = 0; i < LargeRows; i++)
            _duplicateHeavyBuildKeys[i] = MakeKey(i % DistinctKeys);

        // ~10% of probes land inside [0, DistinctKeys); the rest miss.
        _mostlyMissingProbeKeys = new ColumnValue[LargeRows][];
        for (int i = 0; i < LargeRows; i++)
            _mostlyMissingProbeKeys[i] = MakeKey(i % 10 == 0 ? i % DistinctKeys : DistinctKeys + i);

        _probeTable = new Dictionary<CompositeColumnValue, List<int>>(Comparer);
        for (int i = 0; i < DistinctKeys; i++)
            _probeTable[new CompositeColumnValue(MakeKey(i))] = [i];

        _smallBuildKeys = new ColumnValue[SmallRows][];
        _smallProbeKeys = new ColumnValue[SmallRows][];
        for (int i = 0; i < SmallRows; i++)
        {
            _smallBuildKeys[i] = MakeKey(i);
            _smallProbeKeys[i] = MakeKey(i * 2); // half hit, half miss
        }
    }

    // ── Duplicate-heavy build ─────────────────────────────────────────────────

    [BenchmarkCategory("Build")]
    [Benchmark(Description = "BuildDuplicateHeavy_OwnedKeys", Baseline = true)]
    public int BuildDuplicateHeavy_OwnedKeys()
    {
        Dictionary<CompositeColumnValue, List<int>> table = new(Comparer);

        for (int i = 0; i < LargeRows; i++)
        {
            ColumnValue[] source = _duplicateHeavyBuildKeys[i];
            ColumnValue[] keyValues = new ColumnValue[source.Length];
            for (int k = 0; k < source.Length; k++)
                keyValues[k] = source[k];

            CompositeColumnValue key = new(keyValues);
            if (!table.TryGetValue(key, out List<int>? bucket))
            { bucket = []; table[key] = bucket; }
            bucket.Add(i);
        }

        return table.Count;
    }

    [BenchmarkCategory("Build")]
    [Benchmark(Description = "BuildDuplicateHeavy_AlternateLookup")]
    public int BuildDuplicateHeavy_AlternateLookup()
    {
        Dictionary<CompositeColumnValue, List<int>> table = new(Comparer);
        var lookup = table.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();
        ColumnValue[] scratch = new ColumnValue[2];

        for (int i = 0; i < LargeRows; i++)
        {
            ColumnValue[] source = _duplicateHeavyBuildKeys[i];
            for (int k = 0; k < source.Length; k++)
                scratch[k] = source[k];

            if (!lookup.TryGetValue(scratch.AsSpan(), out List<int>? bucket))
            { bucket = []; lookup[scratch.AsSpan()] = bucket; }
            bucket.Add(i);
        }

        return table.Count;
    }

    // ── Mostly-missing probe ──────────────────────────────────────────────────

    [BenchmarkCategory("Probe")]
    [Benchmark(Description = "ProbeMostlyMissing_OwnedKeys", Baseline = true)]
    public int ProbeMostlyMissing_OwnedKeys()
    {
        int hits = 0;

        for (int i = 0; i < LargeRows; i++)
        {
            ColumnValue[] source = _mostlyMissingProbeKeys[i];
            ColumnValue[] keyValues = new ColumnValue[source.Length];
            for (int k = 0; k < source.Length; k++)
                keyValues[k] = source[k];

            if (_probeTable.TryGetValue(new CompositeColumnValue(keyValues), out _))
                hits++;
        }

        return hits;
    }

    [BenchmarkCategory("Probe")]
    [Benchmark(Description = "ProbeMostlyMissing_AlternateLookup")]
    public int ProbeMostlyMissing_AlternateLookup()
    {
        var lookup = _probeTable.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();
        ColumnValue[] scratch = new ColumnValue[2];
        int hits = 0;

        for (int i = 0; i < LargeRows; i++)
        {
            ColumnValue[] source = _mostlyMissingProbeKeys[i];
            for (int k = 0; k < source.Length; k++)
                scratch[k] = source[k];

            if (lookup.TryGetValue(scratch.AsSpan(), out _))
                hits++;
        }

        return hits;
    }

    // ── Small join (regression gate) ──────────────────────────────────────────

    [BenchmarkCategory("Small")]
    [Benchmark(Description = "SmallJoin_OwnedKeys", Baseline = true)]
    public int SmallJoin_OwnedKeys()
    {
        Dictionary<CompositeColumnValue, List<int>> table = new(Comparer);

        for (int i = 0; i < SmallRows; i++)
        {
            ColumnValue[] source = _smallBuildKeys[i];
            ColumnValue[] keyValues = new ColumnValue[source.Length];
            for (int k = 0; k < source.Length; k++)
                keyValues[k] = source[k];

            CompositeColumnValue key = new(keyValues);
            if (!table.TryGetValue(key, out List<int>? bucket))
            { bucket = []; table[key] = bucket; }
            bucket.Add(i);
        }

        int hits = 0;
        for (int i = 0; i < SmallRows; i++)
        {
            ColumnValue[] source = _smallProbeKeys[i];
            ColumnValue[] keyValues = new ColumnValue[source.Length];
            for (int k = 0; k < source.Length; k++)
                keyValues[k] = source[k];

            if (table.TryGetValue(new CompositeColumnValue(keyValues), out _))
                hits++;
        }

        return hits;
    }

    [BenchmarkCategory("Small")]
    [Benchmark(Description = "SmallJoin_AlternateLookup")]
    public int SmallJoin_AlternateLookup()
    {
        Dictionary<CompositeColumnValue, List<int>> table = new(Comparer);
        var lookup = table.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();
        ColumnValue[] scratch = new ColumnValue[2];

        for (int i = 0; i < SmallRows; i++)
        {
            ColumnValue[] source = _smallBuildKeys[i];
            for (int k = 0; k < source.Length; k++)
                scratch[k] = source[k];

            if (!lookup.TryGetValue(scratch.AsSpan(), out List<int>? bucket))
            { bucket = []; lookup[scratch.AsSpan()] = bucket; }
            bucket.Add(i);
        }

        int hits = 0;
        for (int i = 0; i < SmallRows; i++)
        {
            ColumnValue[] source = _smallProbeKeys[i];
            for (int k = 0; k < source.Length; k++)
                scratch[k] = source[k];

            if (lookup.TryGetValue(scratch.AsSpan(), out _))
                hits++;
        }

        return hits;
    }
}
