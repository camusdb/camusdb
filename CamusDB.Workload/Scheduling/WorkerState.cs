/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Scheduling;

/// <summary>Per-worker run state: the exclusive writable shard and the deterministic operation stream.</summary>
public sealed class WorkerState
{
    public WorkerShard Shard { get; }
    public OperationSelector Selector { get; }

    public WorkerState(WorkerShard shard, OperationSelector selector)
    {
        Shard = shard;
        Selector = selector;
    }

    /// <summary>Builds the workers for a run: disjoint shards + independent seeded selectors.</summary>
    public static WorkerState[] Build(ulong seed, int workers, int readPercent, long rows)
    {
        WorkerState[] states = new WorkerState[workers];
        for (int w = 0; w < workers; w++)
        {
            WorkerShard shard = new(w, workers, rows);
            OperationSelector selector = new(seed, w, readPercent, rows, shard);
            states[w] = new WorkerState(shard, selector);
        }
        return states;
    }
}
