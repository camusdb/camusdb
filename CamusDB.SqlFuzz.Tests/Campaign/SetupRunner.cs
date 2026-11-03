/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Transactions;
using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Oracles;
using CamusDB.SqlFuzz.Tests.Printing;
using CamusDB.SqlFuzz.Tests.Runner;

namespace CamusDB.SqlFuzz.Tests.Campaign;

/// <summary>The outcome of one setup statement, with its position in the setup list.</summary>
internal sealed record SetupOutcome(int Index, SetupStatement Statement, StatementOutcome Outcome);

/// <summary>
/// Runs a list of setup statements in order. A BEGIN marker opens an explicit transaction that the
/// statements up to the next COMMIT or ROLLBACK marker join (writes and reads; DDL and ANALYZE always
/// run on their own); every other statement runs on its own. A read recorded on a plan-equivalence arm
/// runs on that arm, so its caches see the same history. A failure is part of the recorded history and
/// does not stop the list: the campaign recorded the same failure when it ran the statement the first
/// time. The caller classifies the outcomes.
/// </summary>
internal static class SetupRunner
{
    /// <param name="armFor">The executor of a named arm, for a read the history recorded there. Without it, such a read runs on <paramref name="engine"/>.</param>
    public static async Task<List<SetupOutcome>> RunAsync(
        IStatementExecutor engine,
        IReadOnlyList<SetupStatement> setup,
        ISqlPrinter printer,
        Func<bool>? stop = null,
        Func<string, Task<IStatementExecutor>>? armFor = null)
    {
        List<SetupOutcome> outcomes = new(setup.Count);
        KvTransaction? tx = null;
        try
        {
            for (int i = 0; i < setup.Count; i++)
            {
                if (stop?.Invoke() == true || engine.Poisoned)
                    break;

                SetupStatement statement = setup[i];
                switch (statement.Statement)
                {
                    case BeginStmt:
                        if (tx is not null)
                            await engine.RollbackAsync(tx).ConfigureAwait(false);
                        tx = await engine.BeginAsync().ConfigureAwait(false);
                        continue;
                    case CommitStmt:
                        if (tx is not null)
                            outcomes.Add(new SetupOutcome(i, statement, await engine.CommitAsync(tx).ConfigureAwait(false)));
                        tx = null;
                        continue;
                    case RollbackStmt:
                        if (tx is not null)
                            await engine.RollbackAsync(tx).ConfigureAwait(false);
                        tx = null;
                        continue;
                }

                IStatementExecutor target = statement.Arm is { } arm && armFor is not null
                    ? await armFor(arm).ConfigureAwait(false)
                    : engine;
                KvTransaction? joined = statement.Route is StatementRoute.NonQuery or StatementRoute.Query && ReferenceEquals(target, engine) ? tx : null;
                StatementOutcome outcome = await target.ExecuteAsync(statement.Route, printer.Print(statement.Statement), joined).ConfigureAwait(false);
                outcomes.Add(new SetupOutcome(i, statement, outcome));
            }
        }
        finally
        {
            if (tx is not null)
                await engine.RollbackAsync(tx).ConfigureAwait(false);
        }
        return outcomes;
    }
}
