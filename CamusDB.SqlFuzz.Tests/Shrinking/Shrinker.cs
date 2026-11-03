/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;

using CamusDB.SqlFuzz.Tests.Campaign;
using CamusDB.SqlFuzz.Tests.Engine;
using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Oracles;
using CamusDB.SqlFuzz.Tests.Printing;
using CamusDB.SqlFuzz.Tests.Runner;

namespace CamusDB.SqlFuzz.Tests.Shrinking;

/// <summary>
/// Reduces a finding to a short statement list. Each step builds a smaller setup or a simpler check,
/// replays it on a new database, and keeps it only when the check fails again with the same signature
/// (the same oracle and the same kind of disagreement). The steps, in a loop until none helps:
/// <list type="number">
/// <item>Simpler forms of the check itself (<see cref="OracleCheck.Simplify"/>): an expression subtree,
/// fewer output columns, no index hint, one join less.</item>
/// <item>Remove setup statements, in chunks from half the list down to one statement.</item>
/// <item>Remove rows from multi-row INSERT statements.</item>
/// <item>Remove a column from a table, with its index statements and its INSERT values.</item>
/// </list>
/// <para>A hang is not shrunk: each replay would hang again and leave a statement running. A reduction
/// step can also make a statement hang. The replay then poisons the engine (<see cref="Poisoned"/>):
/// the shrinker stops at once, keeps the best reduction so far, and does not touch the engine again,
/// not even to drop the database. The caller replaces the engine. The database calls of a replay have
/// the statement time limit too.</para>
/// <para>After the reduction the shrinker takes the plans of the check again on the reduced case, so the
/// report shows the plans of the statements it prints, not those of the original ones.</para>
/// </summary>
internal sealed class Shrinker
{
    private readonly FuzzEngine engine;
    private readonly ISqlPrinter printer;
    private readonly TimeSpan statementTimeout;
    private readonly int maxAttempts;
    private readonly Func<FuzzArm, IStatementExecutor> executorFactory;

    /// <param name="executorFactory">Wraps each replay database in an executor. The default is a
    /// <see cref="SqlRunner"/>; a unit test passes a wrapper that plants a fault.</param>
    public Shrinker(FuzzEngine engine, TimeSpan statementTimeout, ISqlPrinter? printer = null, int maxAttempts = 600, Func<FuzzArm, IStatementExecutor>? executorFactory = null)
    {
        this.engine = engine;
        this.executorFactory = executorFactory ?? (arm => new SqlRunner(arm, statementTimeout));
        this.statementTimeout = statementTimeout;
        this.printer = printer ?? CamusSqlPrinter.Instance;
        this.maxAttempts = maxAttempts;
    }

    /// <summary>The count of replays the last <see cref="ShrinkAsync"/> ran.</summary>
    public int Attempts { get; private set; }

    /// <summary>True when the last <see cref="ShrinkAsync"/> could replay the finding at all.</summary>
    public bool Reproduced { get; private set; }

    /// <summary>True when a replay left an operation running on the engine. The engine must not be used again.</summary>
    public bool Poisoned { get; private set; }

    public async Task<Finding> ShrinkAsync(Finding finding, TimeSpan budget)
    {
        Attempts = 0;
        Reproduced = false;
        lastDetail = null;
        if (Poisoned)
            return finding;
        if (finding.Signature.EndsWith(":hang", StringComparison.Ordinal) || finding.Signature == "error:hang")
            return finding with { Reproduced = null };

        Stopwatch watch = Stopwatch.StartNew();
        bool OutOfTime() => Poisoned || watch.Elapsed > budget || Attempts >= maxAttempts;

        List<SetupStatement> setup = finding.Setup.ToList();
        OracleCheck check = finding.Check;

        Reproduced = await ReproducesAsync(setup, check, finding.Signature).ConfigureAwait(false);
        if (!Reproduced)
            return finding with { Reproduced = false };

        bool progress = true;
        while (progress && !OutOfTime())
        {
            progress = false;

            foreach (OracleCheck simpler in check.Simplify())
            {
                if (OutOfTime())
                    break;
                if (await ReproducesAsync(setup, simpler, finding.Signature).ConfigureAwait(false))
                {
                    check = simpler;
                    progress = true;
                    break;
                }
            }
            if (progress)
                continue;

            for (int chunk = Math.Max(1, setup.Count / 2); chunk >= 1 && !progress && !OutOfTime(); chunk /= 2)
            {
                for (int start = setup.Count - chunk; start >= 0 && !OutOfTime(); start -= chunk)
                {
                    List<SetupStatement> candidate = setup.Take(start).Concat(setup.Skip(start + chunk)).ToList();
                    if (await ReproducesAsync(candidate, check, finding.Signature).ConfigureAwait(false))
                    {
                        setup = candidate;
                        progress = true;
                        break;
                    }
                }
                if (chunk == 1)
                    break;
            }
            if (progress)
                continue;

            foreach (List<SetupStatement> candidate in FewerRows(setup))
            {
                if (OutOfTime())
                    break;
                if (await ReproducesAsync(candidate, check, finding.Signature).ConfigureAwait(false))
                {
                    setup = candidate;
                    progress = true;
                    break;
                }
            }
            if (progress)
                continue;

            foreach (List<SetupStatement> candidate in FewerColumns(setup))
            {
                if (OutOfTime())
                    break;
                if (await ReproducesAsync(candidate, check, finding.Signature).ConfigureAwait(false))
                {
                    setup = candidate;
                    progress = true;
                    break;
                }
            }
        }

        IReadOnlyList<string> plans = finding.Plans;
        if (!Poisoned && plans.Count > 0)
        {
            IReadOnlyList<string>? reduced = await ReplayAsync(setup, check.PlansAsync).ConfigureAwait(false);
            if (reduced is { Count: > 0 })
                plans = reduced;
        }

        return finding with
        {
            Setup = setup,
            Check = check,
            Detail = lastDetail ?? finding.Detail,
            OriginalSetupCount = finding.Setup.Count,
            Plans = plans,
            Reproduced = true,
        };
    }

    /// <summary>Replays <paramref name="setup"/> and <paramref name="check"/> on a new database and returns the result.</summary>
    public async Task<CheckResult> ReplayAsync(IReadOnlyList<SetupStatement> setup, OracleCheck check) =>
        await ReplayAsync(setup, context => RoundContext.RunGuardedAsync(check, context)).ConfigureAwait(false)
        ?? CheckResult.Fail("error:hang", "hang: the replay could not run, because the engine is poisoned");

    /// <summary>
    /// Replays <paramref name="setup"/> on a new database and runs <paramref name="body"/> on it. Returns
    /// default when the engine is poisoned or a database call passes the time limit.
    /// </summary>
    private async Task<T?> ReplayAsync<T>(IReadOnlyList<SetupStatement> setup, Func<CheckContext, Task<T>> body)
    {
        if (Poisoned)
            return default;

        (bool created, FuzzArm? database) = await BoundedAsync(engine.CreateDatabaseAsync).ConfigureAwait(false);
        if (!created)
            return default;

        List<FuzzArm> arms = new();
        List<IStatementExecutor> runners = new();
        try
        {
            IStatementExecutor runner = executorFactory(database!);
            runners.Add(runner);
            CheckContext context = new(runner, async spec =>
            {
                FuzzArm arm = await engine.CreateArmAsync(database!.DatabaseName, spec.Name, spec.Configure, spec.Prepare).ConfigureAwait(false);
                arms.Add(arm);
                IStatementExecutor armRunner = executorFactory(arm);
                runners.Add(armRunner);
                return armRunner;
            }, printer);

            await SetupRunner.RunAsync(runner, setup, printer,
                stop: () => runners.Any(r => r.Poisoned),
                armFor: name => PlanEquivalenceOracle.ArmByName(name) is { } spec ? context.ArmAsync(spec) : Task.FromResult(runner)).ConfigureAwait(false);
            return await body(context).ConfigureAwait(false);
        }
        finally
        {
            if (runners.Any(r => r.Poisoned))
                Poisoned = true;
            else
            {
                foreach (FuzzArm arm in arms)
                    await FuzzEngine.ReleaseArmAsync(arm).ConfigureAwait(false);
                (bool dropped, _) = await BoundedAsync(async () => { await engine.DropDatabaseAsync(database!.DatabaseName).ConfigureAwait(false); return true; }).ConfigureAwait(false);
                if (!dropped)
                    Poisoned = true;
            }
        }
    }

    /// <summary>An engine call under the statement time limit; a call past it poisons the engine.</summary>
    private async Task<(bool Done, T? Result)> BoundedAsync<T>(Func<Task<T>> call)
    {
        Task<T> run = Task.Run(call);
        if (await Task.WhenAny(run, Task.Delay(statementTimeout)).ConfigureAwait(false) != run)
        {
            Poisoned = true;
            _ = run.ContinueWith(static t => _ = t.Exception, TaskScheduler.Default);
            return (false, default);
        }
        return (true, await run.ConfigureAwait(false));
    }

    private async Task<bool> ReproducesAsync(IReadOnlyList<SetupStatement> setup, OracleCheck check, string signature)
    {
        Attempts++;
        CheckResult result = await ReplayAsync(setup, check).ConfigureAwait(false);
        bool same = !Poisoned && result.Status == CheckStatus.Fail && result.Signature == signature;
        if (same)
            lastDetail = result.Detail;
        return same;
    }

    // The detail of the last reproduction, so the report describes the shrunk form, not the first one.
    private string? lastDetail;

    /// <summary>Setups with fewer rows in one multi-row INSERT: half of its rows, then one row less.</summary>
    private static IEnumerable<List<SetupStatement>> FewerRows(List<SetupStatement> setup)
    {
        for (int i = 0; i < setup.Count; i++)
        {
            if (setup[i].Statement is not InsertStmt { Rows.Count: > 1 } insert)
                continue;

            int half = insert.Rows.Count / 2;
            yield return Replace(setup, i, insert with { Rows = insert.Rows.Take(half).ToList() });
            yield return Replace(setup, i, insert with { Rows = insert.Rows.Skip(half).ToList() });
            for (int r = 0; r < insert.Rows.Count; r++)
                yield return Replace(setup, i, insert with { Rows = insert.Rows.Where((_, j) => j != r).ToList() });
        }
    }

    /// <summary>
    /// Setups with one non-key column fewer: the column leaves its CREATE TABLE, every CREATE INDEX
    /// that names it goes, and its value leaves every INSERT of that table. A statement that still
    /// reads the column fails on replay, so the shrinker rejects a removal that the check needs.
    /// </summary>
    private static IEnumerable<List<SetupStatement>> FewerColumns(List<SetupStatement> setup)
    {
        foreach (CreateTableStmt create in setup.Select(s => s.Statement).OfType<CreateTableStmt>().ToList())
        {
            foreach (ColumnDef column in create.Table.Columns.Where(c => !c.PrimaryKey))
            {
                string table = create.Table.Name;
                List<SetupStatement> candidate = new(setup.Count);
                foreach (SetupStatement statement in setup)
                {
                    switch (statement.Statement)
                    {
                        case CreateTableStmt c when c.Table.Name == table:
                            candidate.Add(statement with { Statement = new CreateTableStmt(c.Table with { Columns = c.Table.Columns.Where(x => x.Name != column.Name).ToList() }) });
                            break;
                        case CreateIndexStmt index when index.Table == table && (index.Index.Columns.Contains(column.Name) || index.Index.Include.Contains(column.Name)):
                            break;
                        case InsertStmt insert when insert.Table == table && insert.Columns.Contains(column.Name):
                        {
                            int position = insert.Columns.ToList().IndexOf(column.Name);
                            candidate.Add(statement with
                            {
                                Statement = insert with
                                {
                                    Columns = insert.Columns.Where((_, j) => j != position).ToList(),
                                    Rows = insert.Rows.Select(r => (IReadOnlyList<Expr>)r.Where((_, j) => j != position).ToList()).ToList(),
                                    OnConflict = insert.OnConflict?.DoUpdate is { } set
                                        ? insert.OnConflict with { DoUpdate = set.Where(a => a.Column != column.Name).ToList() is { Count: > 0 } kept ? kept : null }
                                        : insert.OnConflict,
                                },
                            });
                            break;
                        }
                        default:
                            candidate.Add(statement);
                            break;
                    }
                }
                yield return candidate;
            }
        }
    }

    private static List<SetupStatement> Replace(List<SetupStatement> setup, int index, Statement statement)
    {
        List<SetupStatement> copy = setup.ToList();
        copy[index] = copy[index] with { Statement = statement };
        return copy;
    }
}
