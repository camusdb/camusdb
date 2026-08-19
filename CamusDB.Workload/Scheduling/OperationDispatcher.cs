/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Workload.Operations;
using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Scheduling;

/// <summary>
/// Executes an already-selected operation and times the whole round trip. Selection is deliberately
/// NOT done here: the per-worker PRNG is single-threaded state, and in open-loop mode a worker
/// dispatches operations without awaiting them, so <see cref="OperationSelector"/> must be advanced on
/// the pacing thread before dispatch — never concurrently inside the dispatch task.
/// </summary>
public sealed class OperationDispatcher : IOperationExecutor
{
    private readonly ReadOperation _read;
    private readonly IWriteOperation _write;

    public OperationDispatcher(ReadOperation read, IWriteOperation write)
    {
        _read = read;
        _write = write;
    }

    public async Task<(OperationResult Result, double TotalMs)> ExecuteAsync(
        OperationKind kind, long rowIndex, WorkerShard shard, CancellationToken ct)
    {
        long ts = Stopwatch.GetTimestamp();
        OperationResult result = kind == OperationKind.Read
            ? await _read.ExecuteAsync(rowIndex, ct).ConfigureAwait(false)
            : await _write.ExecuteAsync(shard, rowIndex, ct).ConfigureAwait(false);
        double totalMs = Stopwatch.GetElapsedTime(ts).TotalMilliseconds;
        return (result, totalMs);
    }
}
