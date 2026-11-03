/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Printing;
using CamusDB.SqlFuzz.Tests.Runner;
using CamusDB.SqlFuzz.Tests.Shrinking;

namespace CamusDB.SqlFuzz.Tests.Oracles;

/// <summary>The five oracles of the fuzzer.</summary>
internal enum OracleKind
{
    /// <summary>Every error is a <see cref="CamusDBException"/> with a specific code, and every statement finishes in time.</summary>
    ErrorClassification,

    /// <summary>Ternary logic partitioning: the parts for <c>p</c>, <c>NOT p</c> and <c>p IS NULL</c> add up to the whole.</summary>
    Tlp,

    /// <summary>The optimized WHERE count equals the count of rows where <c>p</c> is TRUE in a full-scan projection.</summary>
    NoRec,

    /// <summary>One query gives the same rows under every forced index and every engine configuration.</summary>
    PlanEquivalence,

    /// <summary>After each write batch the full scan and every index agree, and a failed statement aborts its transaction.</summary>
    DmlIntegrity,
}

internal enum CheckStatus
{
    Pass,

    /// <summary>The check could not compare, for example because a query gave an expected error. Not a finding.</summary>
    Skip,

    Fail,
}

/// <summary>
/// The result of one oracle check. <see cref="Signature"/> names the kind of disagreement; the
/// shrinker keeps a reduction only when it gives the same signature.
/// </summary>
internal sealed record CheckResult(CheckStatus Status, string Signature, string Detail)
{
    public static CheckResult Pass { get; } = new(CheckStatus.Pass, "", "");

    public static CheckResult Skip(string reason) => new(CheckStatus.Skip, "", reason);

    public static CheckResult Fail(string signature, string detail) => new(CheckStatus.Fail, signature, detail);

    /// <summary>
    /// The result for an outcome that stops a comparison: a finding when the error-classification rule
    /// says so (an unclassified exception, an internal code, a hang, a wire failure), else a skip.
    /// </summary>
    public static CheckResult FromBadOutcome(StatementOutcome outcome)
    {
        if (ErrorClassifier.Classify(outcome) == ErrorVerdict.Finding)
            return Fail("error:" + ErrorClassifier.Signature(outcome), $"{outcome}\n  in: {outcome.Sql}");
        return Skip(outcome.ToString());
    }
}

/// <summary>
/// An engine configuration for the plan-equivalence oracle. <see cref="OptionsText"/> is the C#
/// <c>with</c> body of <see cref="Configure"/>, for the printed regression test.
/// <para><see cref="Prepare"/> sets a test seam of the arm's own executor after it is built, for a
/// path that no option selects (a forced join operator). <see cref="PrepareText"/> is the same
/// statement for the printed test, with <c>{0}</c> in place of the executor.</para>
/// </summary>
internal sealed record ArmSpec(string Name, Func<CamusDBOptions, CamusDBOptions> Configure, string OptionsText)
{
    public Action<CommandExecutor>? Prepare { get; init; }

    public string? PrepareText { get; init; }
}

/// <summary>
/// What a check runs against: the engine under test and, on request, an arm with another
/// configuration on the same database.
/// </summary>
internal sealed class CheckContext
{
    private readonly Func<ArmSpec, Task<IStatementExecutor>>? armFactory;
    private readonly Dictionary<string, IStatementExecutor> arms = new(StringComparer.Ordinal);

    public CheckContext(IStatementExecutor engine, Func<ArmSpec, Task<IStatementExecutor>>? armFactory = null, ISqlPrinter? printer = null)
    {
        Engine = engine;
        this.armFactory = armFactory;
        Printer = printer ?? CamusSqlPrinter.Instance;
    }

    public IStatementExecutor Engine { get; }

    public ISqlPrinter Printer { get; }

    /// <summary>The count of oracle queries sent, for the per-oracle execution counts.</summary>
    public long QueriesSent { get; private set; }

    /// <summary>
    /// The queries sent since the last <see cref="TakeHistory"/>, with the arm each ran on (null for
    /// the engine under test). A read can change what a later statement does, through the bound-query,
    /// plan and probe caches, so the round keeps the reads of passed checks as setup history, and a
    /// finding replays them.
    /// </summary>
    private readonly List<SetupStatement> history = new();

    public IReadOnlyList<SetupStatement> TakeHistory()
    {
        List<SetupStatement> taken = history.ToList();
        history.Clear();
        return taken;
    }

    /// <summary>The arm spec of an arm this context built, by name.</summary>
    public ArmSpec? SpecOf(string name) => specs.GetValueOrDefault(name);

    private readonly Dictionary<string, ArmSpec> specs = new(StringComparer.Ordinal);
    /// <summary>The arm for <paramref name="spec"/>, built once per context.</summary>
    public async Task<IStatementExecutor> ArmAsync(ArmSpec spec)
    {
        if (arms.TryGetValue(spec.Name, out IStatementExecutor? arm))
            return arm;
        if (armFactory is null)
            throw new InvalidOperationException("This context has no arm factory");
        arm = await armFactory(spec).ConfigureAwait(false);
        arms[spec.Name] = arm;
        specs[spec.Name] = spec;
        return arm;
    }

    public Task<StatementOutcome> QueryAsync(SelectStmt query) => QueryAsync(Engine, query);

    public Task<StatementOutcome> QueryAsync(IStatementExecutor executor, SelectStmt query)
    {
        QueriesSent++;
        string? armName = ReferenceEquals(executor, Engine) ? null : arms.FirstOrDefault(kv => ReferenceEquals(kv.Value, executor)).Key;
        history.Add(new SetupStatement(query, StatementRoute.Query) { Arm = armName });
        return executor.ExecuteAsync(StatementRoute.Query, Printer.Print(query));
    }

    /// <summary>The plan of <paramref name="query"/> on <paramref name="executor"/>, one line per EXPLAIN row, or the error text.</summary>
    public async Task<string> ExplainAsync(IStatementExecutor executor, SelectStmt query)
    {
        StatementOutcome outcome = await executor.ExecuteAsync(StatementRoute.Query, Printer.Print(new ExplainStmt(query))).ConfigureAwait(false);
        if (outcome.Kind != OutcomeKind.Rows)
            return "EXPLAIN failed: " + outcome;

        int node = IndexOf(outcome, "node");
        int detail = IndexOf(outcome, "detail");
        return string.Join("\n", outcome.Rows.Select(r =>
            (node >= 0 ? r[node].StrValue : "?") + (detail >= 0 && !string.IsNullOrEmpty(r[detail].StrValue) ? "(" + r[detail].StrValue + ")" : "")));

        static int IndexOf(StatementOutcome o, string name)
        {
            for (int i = 0; i < o.Columns.Count; i++)
                if (o.Columns[i].Name == name)
                    return i;
            return -1;
        }
    }
}

/// <summary>
/// One re-runnable oracle check. The campaign runs it once; on a failure the shrinker runs it again
/// on reduced setups and asks it for simpler forms of itself (<see cref="Simplify"/>); the regression
/// printer asks it for the body of an NUnit test (<see cref="WriteTest"/>). One object serves the three
/// uses, so the printed test checks exactly the relation the campaign checked.
/// </summary>
internal abstract class OracleCheck
{
    public abstract OracleKind Oracle { get; }

    /// <summary>The variant, for example "where" or "aggregate:min" for TLP.</summary>
    public abstract string Variant { get; }

    public abstract Task<CheckResult> RunAsync(CheckContext context);

    /// <summary>Simpler forms of this check: an expression subtree in place of the expression, fewer output columns. The shrinker tries each one in order.</summary>
    public virtual IEnumerable<OracleCheck> Simplify() => [];

    /// <summary>The statements of the check, as SQL, for a finding report.</summary>
    public abstract IEnumerable<string> Describe(ISqlPrinter printer);

    /// <summary>Writes the statements and the assertion of the check into an NUnit test body.</summary>
    public abstract void WriteTest(TestWriter writer, ISqlPrinter printer);

    /// <summary>The configuration arm the check needs, if any; the printed test builds a second executor for it.</summary>
    public virtual ArmSpec? Arm => null;

    /// <summary>The EXPLAIN of the plans the check compares, for a finding report. Empty for a check that compares no plans.</summary>
    public virtual Task<IReadOnlyList<string>> PlansAsync(CheckContext context) => Task.FromResult<IReadOnlyList<string>>([]);
}

/// <summary>
/// One statement of the setup that a check runs after: the tree and the entry point it goes through.
/// <see cref="Arm"/> names the plan-equivalence arm a recorded read ran on; null is the engine under test.
/// </summary>
internal sealed record SetupStatement(Statement Statement, StatementRoute Route)
{
    public string? Arm { get; init; }

    public static SetupStatement Of(Statement statement) => new(statement, RouteOf(statement));

    public static StatementRoute RouteOf(Statement statement) => statement switch
    {
        CreateTableStmt or CreateIndexStmt => StatementRoute.Ddl,
        AnalyzeStmt => StatementRoute.Analyze,
        SelectStmt or ExplainStmt => StatementRoute.Query,
        _ => StatementRoute.NonQuery,
    };
}

/// <summary>
/// A finding: the oracle, the disagreement, the seed and round that gave it, the setup statements,
/// and the check. After shrinking, <see cref="Setup"/> and <see cref="Check"/> hold the reduced forms.
/// </summary>
internal sealed record Finding(
    OracleKind Oracle,
    string Signature,
    string Detail,
    ulong BaseSeed,
    long Round,
    ulong RoundSeed,
    IReadOnlyList<SetupStatement> Setup,
    OracleCheck Check,
    string ProfileName)
{
    /// <summary>EXPLAIN output of the plans that disagree, when the oracle recorded them.</summary>
    public IReadOnlyList<string> Plans { get; init; } = [];

    /// <summary>The setup length before shrinking, for the report.</summary>
    public int OriginalSetupCount { get; init; }

    /// <summary>
    /// True when a replay on a new database gave the finding again, false when it did not, null when no
    /// replay ran (a hang, which is not replayed). A finding that did not reproduce keeps its original
    /// statements, and its report and printed test say so: the cause may be state the history does not
    /// hold.
    /// </summary>
    public bool? Reproduced { get; init; }
}
