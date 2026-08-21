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

    /// <summary>Extra attempts spent on retryable conflicts across the whole run, counting only the
    /// re-runs (a transfer that succeeds first try adds 0). A conflict absorbed by the retry loop never
    /// reaches <c>metrics.Conflicts</c>, so without this counter contention is invisible: 0 conflicts
    /// means "nothing exhausted its budget", not "nothing contended". The shard-disjoint baseline
    /// cannot conflict and therefore always reports 0.</summary>
    long RetryAttempts { get; }

    /// <summary>Transactions that needed at least one retry, across the whole run. With
    /// <see cref="RetryAttempts"/> this separates "many transfers retried once" from "one transfer
    /// retried many times".</summary>
    long RetriedTxns { get; }

    /// <summary>Highest attempt number any single transaction reached. Equal to the retry budget when
    /// some transfer exhausted it and surfaced a conflict.</summary>
    long MaxAttemptsUsed { get; }

    Task<OperationResult> ExecuteAsync(WorkerShard shard, long baseRowIndex, CancellationToken ct);
}
