/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using BenchmarkDotNet.Attributes;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// Upper bound on what reusing the computed sort-key scratch array could save in the bounded
/// (ORDER BY expression + LIMIT) path.
///
/// <para>
/// The production path evaluates one key array per input row and lets the heap decide whether to
/// retain the row, so a rejected row's array is immediately garbage. The alternative evaluates into
/// one invocation-owned scratch array and only transfers ownership on admission.
/// </para>
///
/// <para>
/// Key evaluation is a constant here on purpose: the point is to isolate the array cost, so the
/// measured gap is the <b>largest</b> saving the change could ever deliver. A real computed ORDER BY
/// key runs through <c>SqlExecutor.EvalExpr</c>, which the EvalExpr benchmarks put at tens of
/// nanoseconds and ~144 B per call — that cost is unchanged by either variant and has to be added
/// back before judging the change worthwhile.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class TopKSortKeyBenchmarks
{
    [Params(10_000)]
    public int RowCount { get; set; }

    [Params(10)]
    public int K { get; set; }

    [Params(1, 2)]
    public int KeyCount { get; set; }

    private ColumnValue[] _values = null!;

    [GlobalSetup]
    public void Setup()
    {
        _values = new ColumnValue[RowCount];

        for (int i = 0; i < RowCount; i++)
            _values[i] = new ColumnValue(ColumnType.Integer64, (long)((i * 7919) % RowCount));
    }

    /// <summary>Current shape: a fresh key array per input row, retained or not.</summary>
    [Benchmark(Baseline = true)]
    public long ArrayPerRow()
    {
        PriorityQueue<ColumnValue[], ColumnValue[]> retained = new(WorstFirst.Instance);

        for (int i = 0; i < _values.Length; i++)
        {
            ColumnValue[] keys = new ColumnValue[KeyCount];

            for (int j = 0; j < KeyCount; j++)
                keys[j] = _values[i];

            if (retained.Count < K)
            {
                retained.Enqueue(keys, keys);
                continue;
            }

            if (Best.Instance.Compare(keys, retained.Peek()) < 0)
                retained.DequeueEnqueue(keys, keys);
        }

        return Drain(retained);
    }

    /// <summary>Fused shape: one scratch array, handed over only when the candidate is admitted.</summary>
    [Benchmark]
    public long ScratchReuse()
    {
        PriorityQueue<ColumnValue[], ColumnValue[]> retained = new(WorstFirst.Instance);
        ColumnValue[] scratch = new ColumnValue[KeyCount];

        for (int i = 0; i < _values.Length; i++)
        {
            for (int j = 0; j < KeyCount; j++)
                scratch[j] = _values[i];

            if (retained.Count < K)
            {
                retained.Enqueue(scratch, scratch);
                scratch = new ColumnValue[KeyCount];
                continue;
            }

            if (Best.Instance.Compare(scratch, retained.Peek()) < 0)
            {
                ColumnValue[] evicted = retained.Peek();
                retained.DequeueEnqueue(scratch, scratch);
                scratch = evicted;
            }
        }

        return Drain(retained);
    }

    private static long Drain(PriorityQueue<ColumnValue[], ColumnValue[]> retained)
    {
        long checksum = 0;

        while (retained.Count > 0)
            checksum += retained.Dequeue()[0].LongValue;

        return checksum;
    }

    private sealed class Best : IComparer<ColumnValue[]>
    {
        internal static readonly Best Instance = new();

        public int Compare(ColumnValue[]? left, ColumnValue[]? right)
        {
            for (int i = 0; i < left!.Length; i++)
            {
                int result = left[i].CompareTo(right![i]);

                if (result != 0)
                    return result;
            }

            return 0;
        }
    }

    private sealed class WorstFirst : IComparer<ColumnValue[]>
    {
        internal static readonly WorstFirst Instance = new();

        public int Compare(ColumnValue[]? left, ColumnValue[]? right) => Best.Instance.Compare(right, left);
    }
}
