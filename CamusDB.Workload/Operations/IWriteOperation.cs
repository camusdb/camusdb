/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Operations;

/// <summary>
/// The write half of the mixed workload, abstracted so the run can swap in a different write shape
/// without touching scheduling or metrics. Both implementations report <see cref="OperationKind.Write"/>
/// results and expose the same durable-commit accounting the reconciliation relies on:
/// <list type="bullet">
/// <item><see cref="WriteOperation"/> — the shard-disjoint read-modify-write baseline (no contention).</item>
/// <item><see cref="TransferOperation"/> — a bank-transfer between two rows across the whole keyspace
/// (real contention), whose atomicity is checked post-run by a conserved <c>SUM(balance)</c>.</item>
/// </list>
/// </summary>
public interface IWriteOperation
{
    /// <summary>Rows durably committed across the whole run (warm-up and measured), the quantity the
    /// version-sum reconciliation compares against.</summary>
    long CommittedRows { get; }

    /// <summary>Transactions whose commit round trip failed without a server verdict, across the whole
    /// run; reconciliation widens its expected version-sum delta to admit both outcomes.</summary>
    long IndeterminateTxns { get; }

    Task<OperationResult> ExecuteAsync(WorkerShard shard, long baseRowIndex, CancellationToken ct);
}
