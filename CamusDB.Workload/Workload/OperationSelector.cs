/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Util;

namespace CamusDB.Workload.Workload;

/// <summary>
/// Chooses the next operation kind and target row for a single worker from a deterministic per-worker
/// stream. Two guarantees it upholds: (1) a fixed seed reproduces the exact kind sequence and
/// row sequence, so a run is replayable; (2) over a long-enough interval the submitted mix converges
/// to the configured read/write split. Reads may target any row in the table (they never mutate it);
/// writes are constrained to the worker's own <see cref="WorkerShard"/> so writers never collide.
/// </summary>
public sealed class OperationSelector
{
    private readonly DeterministicRandom _rng;
    private readonly int _readPercent;
    private readonly long _totalRows;
    private readonly WorkerShard _shard;

    public OperationSelector(ulong baseSeed, int workerIndex, int readPercent, long totalRows, WorkerShard shard)
    {
        _rng = DeterministicRandom.ForWorker(baseSeed, workerIndex);
        _readPercent = readPercent;
        _totalRows = totalRows;
        _shard = shard;
    }

    /// <summary>Picks the next operation kind (weighted by read percent) and the row index it targets.</summary>
    public (OperationKind Kind, long RowIndex) Next()
    {
        long roll = _rng.NextLong(100);
        if (roll < _readPercent)
        {
            // Read: any row in the full seeded set.
            return (OperationKind.Read, _rng.NextLong(_totalRows));
        }

        // Write: a row this worker exclusively owns.
        long ordinal = _rng.NextLong(_shard.OwnedCount);
        return (OperationKind.Write, _shard.RowIndexAt(ordinal));
    }
}
