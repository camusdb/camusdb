/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Printing;
using CamusDB.SqlFuzz.Tests.Runner;
using CamusDB.SqlFuzz.Tests.Shrinking;
using CamusDB.Tests.SqlFuzz;

namespace CamusDB.SqlFuzz.Tests.Oracles;

/// <summary>
/// One NoREC (non-optimizing reference engine construction) check.
///
/// <list type="bullet">
/// <item>The optimized form puts <c>p</c> in WHERE: <c>SELECT COUNT(*) FROM … WHERE p</c>. The planner
/// can serve it with index seeks, batched probes, the probe memo, pushdown and index-only scans.</item>
/// <item>The reference form puts <c>p</c> in the projection: <c>SELECT SUM(CASE WHEN p THEN 1 ELSE 0
/// END) FROM …</c>. No access path can use a projected expression, so the engine evaluates <c>p</c>
/// on every row of a full scan.</item>
/// </list>
/// <para>The two counts must be equal. The <c>rows</c> variant compares the rows too, because two
/// wrong rows can replace each other and keep the count: the optimized rows must equal the reference
/// rows whose projected flag is 1.</para>
/// </summary>
internal sealed class NoRecCheck : OracleCheck
{
    public NoRecCheck(bool compareRows, FromItem from, IReadOnlyList<SelectItem> items, Expr predicate)
    {
        CompareRows = compareRows;
        From = from;
        Items = items;
        Predicate = predicate;
    }

    public bool CompareRows { get; }

    public FromItem From { get; }

    public IReadOnlyList<SelectItem> Items { get; }

    public Expr Predicate { get; }

    public override OracleKind Oracle => OracleKind.NoRec;

    public override string Variant => CompareRows ? "norec:rows" : "norec:count";

    private Expr Flag => new CaseExpr([new CaseBranch(Predicate, new LiteralExpr(FuzzValue.Of(1L)))], new LiteralExpr(FuzzValue.Of(0L)), FuzzType.Int64);

    public SelectStmt Optimized => CompareRows
        ? new SelectStmt(Items, From, Predicate)
        : new SelectStmt([new SelectItem(new AggregateExpr(AggregateFunc.CountStar, null, FuzzType.Int64), "n")], From, Predicate);

    public SelectStmt Reference => CompareRows
        ? new SelectStmt(Items.Append(new SelectItem(Flag, "norec_flag")).ToList(), From)
        : new SelectStmt([new SelectItem(new AggregateExpr(AggregateFunc.Sum, Flag, FuzzType.Int64), "n")], From);

    public override async Task<CheckResult> RunAsync(CheckContext context)
    {
        StatementOutcome optimized = await context.QueryAsync(Optimized).ConfigureAwait(false);
        if (optimized.Kind != OutcomeKind.Rows || optimized.WireError is not null)
            return CheckResult.FromBadOutcome(optimized);

        StatementOutcome reference = await context.QueryAsync(Reference).ConfigureAwait(false);
        if (reference.Kind != OutcomeKind.Rows || reference.WireError is not null)
            return CheckResult.FromBadOutcome(reference);

        if (!CompareRows)
        {
            long a = Count(optimized);
            long b = Count(reference);
            return a == b ? CheckResult.Pass : CheckResult.Fail("norec:count:mismatch", $"WHERE p counts {a}, SUM(CASE WHEN p …) counts {b}");
        }

        if (SqlFuzzValues.ShapeDifference(
                optimized.Columns.Select(c => (c.Name, c.Type)).ToList(),
                reference.Columns.Take(reference.Columns.Count - 1).Select(c => (c.Name, c.Type)).ToList(),
                "optimized", "reference") is { } shape)
            return CheckResult.Fail("norec:rows:shape", shape);

        List<string> expected = SqlFuzzValues.Multiset(optimized.Rows);
        List<string> actual = SqlFuzzValues.Multiset(reference.Rows
            .Where(r => r[^1].Type == ColumnType.Integer64 && r[^1].LongValue == 1)
            .Select(r => (IReadOnlyList<ColumnValue>)r.Take(r.Length - 1).ToList()));
        string? problem = SqlFuzzValues.Difference(expected, actual, "optimized", "reference");
        return problem is null ? CheckResult.Pass : CheckResult.Fail("norec:rows:mismatch", problem);
    }

    private static long Count(StatementOutcome outcome) =>
        outcome.Rows.Count == 0 || outcome.Rows[0][0].Type == ColumnType.Null ? 0 : outcome.Rows[0][0].LongValue;

    public override IEnumerable<OracleCheck> Simplify()
    {
        foreach (Expr p in ExprShrinker.Candidates(Predicate))
            yield return new NoRecCheck(CompareRows, From, Items, p);
        if (CompareRows)
        {
            yield return new NoRecCheck(false, From, Items, Predicate);
            if (Items.Count > 1)
                for (int i = 0; i < Items.Count; i++)
                    yield return new NoRecCheck(true, From, Items.Where((_, j) => j != i).ToList(), Predicate);
        }
        foreach (FromItem from in FromShrinker.Candidates(From, keepJoin: false))
            yield return new NoRecCheck(CompareRows, from, Items, Predicate);
    }

    public override IEnumerable<string> Describe(ISqlPrinter printer)
    {
        yield return printer.Print(Optimized);
        yield return printer.Print(Reference);
    }

    /// <summary>The EXPLAIN of the optimized and the reference form, in this order.</summary>
    public override async Task<IReadOnlyList<string>> PlansAsync(CheckContext context) =>
    [
        await context.ExplainAsync(context.Engine, Optimized).ConfigureAwait(false),
        await context.ExplainAsync(context.Engine, Reference).ConfigureAwait(false),
    ];

    public override void WriteTest(TestWriter writer, ISqlPrinter printer)
    {
        string a = TestWriter.Literal(printer.Print(Optimized));
        string b = TestWriter.Literal(printer.Print(Reference));
        writer.Line(CompareRows
            ? $"await db.AssertNoRecRows({a},\n            {b});"
            : $"await db.AssertNoRecCount({a},\n            {b});");
    }
}

/// <summary>
/// Makes NoREC checks. Every <see cref="ExplainSampleEvery"/>-th check also records the EXPLAIN of
/// both forms, and counts how often their access paths differ (<see cref="AccessPath"/>). A sample
/// with the same access path is not wrong, because many predicates have no index to use, but it is a
/// check that compared one path with itself: the count shows how much of the oracle's work tests the
/// planner.
/// </summary>
internal sealed class NoRecOracle : IOracle
{
    public const int ExplainSampleEvery = 25;

    private long checks;

    public OracleKind Kind => OracleKind.NoRec;

    /// <summary>EXPLAIN samples: how many were taken and how many showed different plans for the two forms.</summary>
    public long PlanSamples { get; private set; }

    public long PlanSamplesDifferent { get; private set; }

    public async Task RunRoundAsync(RoundContext round)
    {
        for (int i = 0; i < round.CheckCount && !round.ShouldStop; i++)
        {
            NoRecCheck check = Generate(round.Generator);
            CheckResult result = await round.RunAsync(check).ConfigureAwait(false);

            if (++checks % ExplainSampleEvery == 0 && result.Status == CheckStatus.Pass)
            {
                IReadOnlyList<string> plans = await check.PlansAsync(round.Checks).ConfigureAwait(false);
                PlanSamples++;
                if (AccessPath(plans[0]) != AccessPath(plans[1]))
                    PlanSamplesDifferent++;
                round.Log($"NoREC EXPLAIN sample {PlanSamples}:\n  optimized: {round.Checks.Printer.Print(check.Optimized)}\n    {plans[0].Replace("\n", "\n    ")}\n  reference: {round.Checks.Printer.Print(check.Reference)}\n    {plans[1].Replace("\n", "\n    ")}");
            }
        }
    }

    /// <summary>
    /// The access path of a plan: its scan, seek, probe and join nodes, with their details. The other
    /// lines differ between the two forms by construction (the shape hash, the filter, the projection),
    /// so a comparison of the whole text would call every pair of plans different.
    /// </summary>
    public static string AccessPath(string plan) => string.Join("\n", plan.Split('\n').Where(line =>
        !line.StartsWith("plan-info", StringComparison.Ordinal)
        && (line.Contains("scan", StringComparison.Ordinal) || line.Contains("seek", StringComparison.Ordinal)
            || line.Contains("probe", StringComparison.Ordinal) || line.Contains("join", StringComparison.Ordinal)
            || line.Contains("lookup", StringComparison.Ordinal))));

    public static NoRecCheck Generate(StatementGenerator generator)
    {
        (FromItem from, ExprScope scope) = generator.GenerateFrom(SelectOptions.Comparable);
        bool rows = generator.Random.Chance(0.4);
        // The unoptimized form puts the predicate in a projection, where only an uncorrelated subquery runs.
        return new NoRecCheck(rows, from, rows ? generator.GenerateProjection(scope) : [], generator.Expressions.Predicate(scope.ForUncorrelatedSubqueries()));
    }
}
