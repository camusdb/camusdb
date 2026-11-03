/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using CamusDB.SqlFuzz.Tests.Runner;

namespace CamusDB.SqlFuzz.Tests.Unit;

/// <summary>
/// Test-only: an executor that passes every statement to a real one and then lets a test change the
/// answer of the statements it picks. It plants a fault between an oracle and the engine, so a test can
/// prove that the oracle reports the disagreement the fault causes.
/// </summary>
internal sealed class FaultyExecutor : IStatementExecutor
{
    private readonly IStatementExecutor inner;
    private readonly Func<string, StatementOutcome, StatementOutcome?> fault;

    /// <param name="fault">Gets the SQL and the real outcome; returns a changed outcome, or null to keep the real one.</param>
    public FaultyExecutor(IStatementExecutor inner, Func<string, StatementOutcome, StatementOutcome?> fault)
    {
        this.inner = inner;
        this.fault = fault;
    }

    /// <summary>The count of answers the fault changed.</summary>
    public int Applied { get; private set; }

    public string ArmName => inner.ArmName;

    public async Task<StatementOutcome> ExecuteAsync(StatementRoute route, string sql, KvTransaction? transaction = null)
    {
        StatementOutcome outcome = await inner.ExecuteAsync(route, sql, transaction);
        StatementOutcome? changed = fault(sql, outcome);
        if (changed is null)
            return outcome;
        Applied++;
        return changed;
    }

    public Task<KvTransaction> BeginAsync() => inner.BeginAsync();

    public Task<StatementOutcome> CommitAsync(KvTransaction transaction) => inner.CommitAsync(transaction);

    public Task RollbackAsync(KvTransaction transaction) => inner.RollbackAsync(transaction);

    /// <summary>A fault of the integrity scan: an exception to throw for a table, or null to run the real scan.</summary>
    public Func<string, Exception?>? IntegrityFault { get; init; }

    public Task<IReadOnlyList<string>> CheckIntegrityAsync(string table)
    {
        if (IntegrityFault?.Invoke(table) is { } exception)
        {
            Applied++;
            throw exception;
        }
        return inner.CheckIntegrityAsync(table);
    }

    public bool Poisoned => inner.Poisoned;

    /// <summary>A fault that removes the last row of a result.</summary>
    public static StatementOutcome? DropLastRow(StatementOutcome outcome) =>
        outcome.Kind == OutcomeKind.Rows && outcome.Rows.Count > 0
            ? StatementOutcome.FromRows(outcome.Sql, outcome.Columns, outcome.Rows.Take(outcome.Rows.Count - 1).ToList())
            : null;

    /// <summary>A fault that removes the rows that hold <paramref name="rendered"/> in any cell.</summary>
    public static StatementOutcome? DropRowsWith(StatementOutcome outcome, string rendered)
    {
        if (outcome.Kind != OutcomeKind.Rows)
            return null;
        List<ColumnValue[]> kept = outcome.Rows.Where(r => !r.Any(c => CamusDB.Tests.SqlFuzz.SqlFuzzValues.Render(c) == rendered)).ToList();
        return kept.Count == outcome.Rows.Count ? null : StatementOutcome.FromRows(outcome.Sql, outcome.Columns, kept);
    }
}
