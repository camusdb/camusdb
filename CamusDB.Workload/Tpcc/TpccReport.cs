/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using CamusDB.Workload.Metrics;

namespace CamusDB.Workload.Tpcc;

/// <summary>The measured-window figures of one transaction type. Latency is of the committed
/// transactions (and the intended rollbacks of New-Order), retries included.</summary>
public sealed record TpccTransactionStats(
    string Transaction,
    int MixPercent,
    long Executions,
    long Committed,
    long RolledBack,
    long Conflicts,
    long Undecided,
    long Errors,
    long Anomalies,
    long Retries,
    double MeanMs,
    double P50Ms,
    double P95Ms,
    double P99Ms,
    double MaxMs);

public sealed record TpccConditionSummary(string Name, string Rule, long Checked, long Violations, IReadOnlyList<string> Examples);

/// <summary>The content of <c>tpcc.json</c>.</summary>
public sealed record TpccRunReport(
    int Warehouses,
    string Scale,
    string Seed,
    double MeasuredSeconds,
    double NewOrdersPerMinute,
    double TransactionsPerSecond,
    IReadOnlyList<TpccTransactionStats> Transactions,
    IReadOnlyList<TpccConditionSummary> Conditions);

/// <summary>
/// The TPC-C verdict that joins reconciliation. It fails on any anomaly a transaction saw while it ran,
/// on any consistency condition the final state breaks, on a final state that could not be read, and on
/// a transaction type with no commit inside the measured window, since a run says nothing about a
/// transaction it never finished. <see cref="Anomalies"/> counts the whole run; the other counts are of
/// the measured window.
/// </summary>
public sealed record TpccResult(
    bool Passed,
    string Status,
    int Warehouses,
    string Scale,
    long Executions,
    long Committed,
    long NewOrders,
    long RolledBack,
    long Conflicts,
    long Undecided,
    long Errors,
    long Anomalies,
    long ConditionsChecked,
    long ConditionViolations,
    IReadOnlyList<string> Failures);

/// <summary>The consistency conditions over every warehouse, and the baseline of each.</summary>
public sealed record TpccCheck(TpccConsistency Consistency, IReadOnlyDictionary<long, TpccBaseline> Baselines)
{
    /// <summary>
    /// Reads every warehouse and evaluates the conditions. With a <paramref name="ledger"/> and the
    /// baselines of an earlier check it also evaluates the ledger bands.
    /// </summary>
    public static async Task<TpccCheck> RunAsync(
        CamusConnection conn, TpccScale scale, IReadOnlyDictionary<long, TpccBaseline>? before, TpccLedger? ledger,
        TimeSpan retryBudget, CancellationToken ct)
    {
        TpccConsistency consistency = new(scale);
        Dictionary<long, TpccBaseline> baselines = new();

        for (long w = 1; w <= scale.Warehouses; w++)
        {
            TpccWarehouseState state = await TpccConsistency.ReadWarehouseAsync(conn, w, scale, retryBudget, ct).ConfigureAwait(false);
            consistency.CheckWarehouse(state);
            baselines[w] = TpccConsistency.BaselineOf(state);
            if (ledger is not null && before is not null && before.TryGetValue(w, out TpccBaseline? baseline))
                consistency.CheckLedger(state, baseline, ledger);
        }

        return new TpccCheck(consistency, baselines);
    }

    public IReadOnlyList<TpccConditionSummary> Summaries(bool withLedger)
        => (withLedger ? Consistency.Conditions.Concat(Consistency.LedgerConditions) : Consistency.Conditions)
            .Select(c => new TpccConditionSummary(c.Name, c.Rule, c.Checked, c.Violations, c.Examples))
            .ToList();
}

public static class TpccReport
{
    private static readonly (TpccTransaction Type, string Name, int MixPercent)[] Types =
    [
        (TpccTransaction.NewOrder, "New-Order", 45),
        (TpccTransaction.Payment, "Payment", 43),
        (TpccTransaction.OrderStatus, "Order-Status", 4),
        (TpccTransaction.Delivery, "Delivery", 4),
        (TpccTransaction.StockLevel, "Stock-Level", 4),
    ];

    /// <param name="check">The final check, or null when the final state could not be read.</param>
    /// <param name="checkError">Why it could not be read.</param>
    public static (TpccRunReport Report, TpccResult Result) Build(
        TpccScale scale, string seed, IReadOnlyCollection<TpccExecution> executions, long anomaliesWholeRun,
        TpccCheck? check, string? checkError, DateTime measureStartUtc, double measuredSeconds)
    {
        DateTime from = measureStartUtc;
        DateTime until = measureStartUtc.AddSeconds(measuredSeconds);

        Dictionary<TpccTransaction, List<TpccExecution>> byType = new();
        foreach (TpccExecution e in executions)
        {
            // An execution belongs to the window when it finished inside it, the way the scheduler counts
            // a completed operation.
            DateTime finished = e.StartedUtc.AddMilliseconds(e.Milliseconds);
            if (finished < from || finished > until)
                continue;
            if (!byType.TryGetValue(e.Type, out List<TpccExecution>? list))
                byType[e.Type] = list = new List<TpccExecution>();
            list.Add(e);
        }

        List<TpccTransactionStats> stats = new(Types.Length);
        List<string> failures = new();
        long totalExecutions = 0, totalCommitted = 0, totalRolledBack = 0, totalConflicts = 0, totalUndecided = 0, totalErrors = 0;
        long newOrders = 0;

        foreach ((TpccTransaction type, string name, int mix) in Types)
        {
            List<TpccExecution> list = byType.GetValueOrDefault(type) ?? [];
            LatencyHistogram histogram = new();
            long committed = 0, rolledBack = 0, conflicts = 0, undecided = 0, errors = 0, anomalies = 0, retries = 0;
            double sumMs = 0;

            foreach (TpccExecution e in list)
            {
                retries += e.Attempts - 1;
                switch (e.Outcome)
                {
                    case TpccOutcome.Committed:
                    case TpccOutcome.RolledBack:
                        if (e.Outcome == TpccOutcome.Committed)
                            committed++;
                        else
                            rolledBack++;
                        sumMs += e.Milliseconds;
                        histogram.Record(e.Milliseconds);
                        break;
                    case TpccOutcome.Conflict:
                        conflicts++;
                        break;
                    case TpccOutcome.Undecided:
                        undecided++;
                        break;
                    case TpccOutcome.Anomaly:
                        anomalies++;
                        break;
                    default:
                        errors++;
                        break;
                }
            }

            long done = committed + rolledBack;
            stats.Add(new TpccTransactionStats(
                name, mix, list.Count, committed, rolledBack, conflicts, undecided, errors, anomalies, retries,
                done > 0 ? Math.Round(sumMs / done, 3) : 0,
                Math.Round(histogram.Percentile(50), 3), Math.Round(histogram.Percentile(95), 3),
                Math.Round(histogram.Percentile(99), 3), Math.Round(histogram.MaxMilliseconds, 3)));

            totalExecutions += list.Count;
            totalCommitted += committed;
            totalRolledBack += rolledBack;
            totalConflicts += conflicts;
            totalUndecided += undecided;
            totalErrors += errors;
            if (type == TpccTransaction.NewOrder)
                newOrders = committed;

            if (committed == 0)
                failures.Add($"{name} has no commit in the measured window ({list.Count} attempt(s), {conflicts} conflict(s), " +
                             $"{errors} error(s)); the run says nothing about it");
        }

        if (anomaliesWholeRun > 0)
            failures.Insert(0, $"{anomaliesWholeRun:N0} transaction(s) saw a state no interleaving of whole transactions can produce " +
                               "(see tpcc-anomalies.jsonl)");

        long conditionsChecked = 0, violations = 0;
        IReadOnlyList<TpccConditionSummary> conditions = [];
        if (check is null)
        {
            failures.Add($"the final state could not be read, so the consistency conditions were not checked: {checkError}");
        }
        else
        {
            conditions = check.Summaries(withLedger: true);
            foreach (TpccConditionSummary c in conditions)
            {
                conditionsChecked += c.Checked;
                violations += c.Violations;
                if (c.Violations > 0)
                    failures.Add($"consistency condition {c.Name} ({c.Rule}) is broken in {c.Violations:N0} of {c.Checked:N0} case(s); " +
                                 $"first: {c.Examples.FirstOrDefault()}");
            }
        }

        TpccRunReport report = new(
            scale.Warehouses, scale.FractionText, seed, measuredSeconds,
            measuredSeconds > 0 ? Math.Round(newOrders * 60.0 / measuredSeconds, 1) : 0,
            measuredSeconds > 0 ? Math.Round((totalCommitted + totalRolledBack) / measuredSeconds, 2) : 0,
            stats, conditions);

        TpccResult result = new(
            Passed: failures.Count == 0,
            Status: check is null ? "unavailable" : "verified",
            Warehouses: scale.Warehouses,
            Scale: scale.FractionText,
            Executions: totalExecutions,
            Committed: totalCommitted,
            NewOrders: newOrders,
            RolledBack: totalRolledBack,
            Conflicts: totalConflicts,
            Undecided: totalUndecided,
            Errors: totalErrors,
            Anomalies: anomaliesWholeRun,
            ConditionsChecked: conditionsChecked,
            ConditionViolations: violations,
            Failures: failures);

        return (report, result);
    }
}
