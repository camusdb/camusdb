/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Workload;

/// <summary>
/// Maps a worker to the disjoint set of writable rows it — and only it — may update. Ownership is by
/// strided row index (<c>index % workers == workerIndex</c>), which is pairwise disjoint and covers
/// every row even when <c>rows</c> is not a multiple of <c>workers</c>. Because two workers can never
/// select the same writable row, the baseline stays structurally non-conflicting regardless of how
/// operations are scheduled; in this non-conflicting baseline any observed conflict invalidates the run
/// rather than being a contention signal. The mapping is a pure function of (rows, workers), so post-run reconciliation
/// reconstructs exactly which rows each worker touched without reading anything back by scan.
/// </summary>
public sealed class WorkerShard
{
    private readonly long _rows;
    private readonly int _workers;

    public int WorkerIndex { get; }

    /// <summary>Count of writable rows this worker owns.</summary>
    public long OwnedCount { get; }

    public WorkerShard(int workerIndex, int workers, long rows)
    {
        if (workerIndex < 0 || workerIndex >= workers)
            throw new ArgumentOutOfRangeException(nameof(workerIndex));
        if (rows <= 0)
            throw new ArgumentOutOfRangeException(nameof(rows));

        WorkerIndex = workerIndex;
        _workers = workers;
        _rows = rows;
        // Rows 0..rows-1 striped across workers; this worker gets ceil/floor of rows/workers.
        OwnedCount = (rows - workerIndex + (workers - 1)) / workers;
    }

    /// <summary>The global row index of this worker's <paramref name="ordinal"/>-th owned row.</summary>
    public long RowIndexAt(long ordinal)
    {
        if (ordinal < 0 || ordinal >= OwnedCount)
            throw new ArgumentOutOfRangeException(nameof(ordinal));
        return WorkerIndex + (long)_workers * ordinal;
    }

    /// <summary>True when the given global row index belongs to this worker's shard.</summary>
    public bool Owns(long rowIndex) => rowIndex % _workers == WorkerIndex;

    /// <summary>Inverse of <see cref="RowIndexAt"/>: the owned-row ordinal of a global row index in this shard.</summary>
    public long OrdinalOf(long rowIndex) => (rowIndex - WorkerIndex) / _workers;

    /// <summary>Enumerates every global row index this worker owns (used by reconciliation).</summary>
    public IEnumerable<long> OwnedRowIndexes()
    {
        for (long i = WorkerIndex; i < _rows; i += _workers)
            yield return i;
    }
}
