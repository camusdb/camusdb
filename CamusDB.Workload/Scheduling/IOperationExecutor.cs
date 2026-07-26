/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Operations;
using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Scheduling;

/// <summary>
/// The seam between a scheduler and the thing that actually runs an operation. Production wiring uses
/// <see cref="OperationDispatcher"/>; tests substitute a fake so the scheduling behavior
/// (lateness/drops, draining, cancellation accounting) can be verified without a live server.
/// </summary>
public interface IOperationExecutor
{
    Task<(OperationResult Result, double TotalMs)> ExecuteAsync(
        OperationKind kind, long rowIndex, WorkerShard shard, CancellationToken ct);
}
