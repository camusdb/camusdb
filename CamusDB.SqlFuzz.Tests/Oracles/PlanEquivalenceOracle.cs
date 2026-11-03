/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Printing;
using CamusDB.SqlFuzz.Tests.Runner;
using CamusDB.SqlFuzz.Tests.Shrinking;
using CamusDB.Tests.SqlFuzz;

namespace CamusDB.SqlFuzz.Tests.Oracles;

/// <summary>How a plan-equivalence check gets its second plan.</summary>
internal enum PlanSource
{
    /// <summary>An index hint on one source of the query, on the engine under test.</summary>
    Hint,

    /// <summary>The same query on a second engine with another configuration or a forced operator.</summary>
    Arm,

    /// <summary>
    /// The full-scan baseline of a one-table query: the predicate moves from WHERE into the projection
    /// as a flag, so no access path can use it, and the rows whose flag is 1 must equal the rows of the
    /// query. The check confirms with EXPLAIN that the baseline is a full scan of the primary rows.
    /// </summary>
    FullScan,
}

/// <summary>
/// One plan-equivalence check: one query, two plans, the same output shape and the same multiset of
/// rows. See <see cref="PlanSource"/> for where the second plan comes from. A configuration needs its
/// own engine: options are fixed when an engine is built, so a setting changed on the engine under test
/// would do nothing, and the check would compare a result with itself.
/// </summary>
internal sealed class PlanEquivalenceCheck : OracleCheck
{
    private readonly ArmSpec? arm;

    public PlanEquivalenceCheck(SelectStmt query, string? hintAlias, string? hintIndex, ArmSpec? arm, bool fullScan = false)
    {
        Query = query;
        HintAlias = hintAlias;
        HintIndex = hintIndex;
        this.arm = arm;
        Source = fullScan ? PlanSource.FullScan : arm is not null ? PlanSource.Arm : PlanSource.Hint;
    }

    public SelectStmt Query { get; }

    public string? HintAlias { get; }

    public string? HintIndex { get; }

    public PlanSource Source { get; }

    public override ArmSpec? Arm => arm;

    public override OracleKind Oracle => OracleKind.PlanEquivalence;

    public override string Variant => Source switch
    {
        PlanSource.Arm => "plan:arm:" + arm!.Name,
        PlanSource.FullScan => "plan:full-scan",
        _ => HintIndex == "pk" ? "plan:hint:pk" : "plan:hint:index",
    };

    /// <summary>The second query: the query with the hint on its source, the query itself for an arm, or the flagged full-scan form.</summary>
    public SelectStmt Second => Source switch
    {
        PlanSource.Hint => Query with { From = WithHint(Query.From, HintAlias!, HintIndex!) },
        PlanSource.FullScan => Query with
        {
            Where = null,
            Items = Query.Items.Append(new SelectItem(new CaseExpr(
                [new CaseBranch(Query.Where!, new LiteralExpr(FuzzValue.Of(1L)))], new LiteralExpr(FuzzValue.Of(0L)), FuzzType.Int64), "pe_flag")).ToList(),
        },
        _ => Query,
    };

    private static FromItem WithHint(FromItem from, string alias, string index) => from switch
    {
        TableRef t when t.Alias == alias => t with { IndexHint = index },
        JoinFrom j => j with { Left = WithHint(j.Left, alias, index), Right = (TableRef)WithHint(j.Right, alias, index) },
        _ => from,
    };

    private async Task<IStatementExecutor> SecondExecutorAsync(CheckContext context) =>
        arm is null ? context.Engine : await context.ArmAsync(arm).ConfigureAwait(false);

    public override async Task<CheckResult> RunAsync(CheckContext context)
    {
        StatementOutcome first = await context.QueryAsync(Query).ConfigureAwait(false);
        if (first.Kind != OutcomeKind.Rows || first.WireError is not null)
            return CheckResult.FromBadOutcome(first);

        IStatementExecutor executor = await SecondExecutorAsync(context).ConfigureAwait(false);

        if (Source == PlanSource.FullScan)
        {
            string plan = await context.ExplainAsync(executor, Second).ConfigureAwait(false);
            if (!plan.Contains("table-scan", StringComparison.Ordinal) || plan.Contains("index", StringComparison.Ordinal))
                return CheckResult.Skip("the flagged baseline is not a full scan of the primary rows");
        }

        // The plan-cache arm runs the query twice: the first run stores the plan, the second uses it.
        int runs = arm?.Name == "plan-cache" ? 2 : 1;
        for (int run = 0; run < runs; run++)
        {
            StatementOutcome second = await context.QueryAsync(executor, Second).ConfigureAwait(false);
            if (second.Kind != OutcomeKind.Rows || second.WireError is not null)
            {
                // A hint the planner refuses (an index of another table after a shrink) is not a plan.
                return CheckResult.FromBadOutcome(second);
            }

            string label = runs > 1 ? $"{Variant} (run {run + 1})" : Variant;
            IReadOnlyList<(string, Core.Catalogs.Models.ColumnType)> firstShape = first.Columns.Select(c => (c.Name, c.Type)).ToList();
            IReadOnlyList<ColumnValue[]> secondRows = second.Rows;
            IReadOnlyList<(string, Core.Catalogs.Models.ColumnType)> secondShape = second.Columns.Select(c => (c.Name, c.Type)).ToList();
            if (Source == PlanSource.FullScan)
            {
                secondShape = secondShape.Take(secondShape.Count - 1).ToList();
                secondRows = second.Rows
                    .Where(r => r[^1].Type == Core.Catalogs.Models.ColumnType.Integer64 && r[^1].LongValue == 1)
                    .Select(r => r.Take(r.Length - 1).ToArray()).ToList();
            }

            if (SqlFuzzValues.ShapeDifference(firstShape, secondShape, "default plan", label) is { } shape)
                return CheckResult.Fail(Variant + ":shape", shape);
            string? problem = SqlFuzzValues.Difference(SqlFuzzValues.Multiset(first.Rows), SqlFuzzValues.Multiset(secondRows), "default plan", label);
            if (problem is not null)
                return CheckResult.Fail(Variant + ":mismatch", problem);
        }
        return CheckResult.Pass;
    }

    /// <summary>The EXPLAIN of the two plans that disagree.</summary>
    public override async Task<IReadOnlyList<string>> PlansAsync(CheckContext context)
    {
        IStatementExecutor executor = await SecondExecutorAsync(context).ConfigureAwait(false);
        return
        [
            "default plan:\n" + await context.ExplainAsync(context.Engine, Query).ConfigureAwait(false),
            Variant + " plan:\n" + await context.ExplainAsync(executor, Second).ConfigureAwait(false),
        ];
    }

    public override IEnumerable<OracleCheck> Simplify()
    {
        if (Query.Where is not null)
        {
            if (Source != PlanSource.FullScan)
                yield return new PlanEquivalenceCheck(Query with { Where = null }, HintAlias, HintIndex, arm);
            foreach (Expr p in ExprShrinker.Candidates(Query.Where))
                yield return new PlanEquivalenceCheck(Query with { Where = p }, HintAlias, HintIndex, arm, Source == PlanSource.FullScan);
        }
        if (Query.Items.Count > 1)
            for (int i = 0; i < Query.Items.Count; i++)
                yield return new PlanEquivalenceCheck(Query with { Items = Query.Items.Where((_, j) => j != i).ToList() }, HintAlias, HintIndex, arm, Source == PlanSource.FullScan);
        foreach (FromItem from in FromShrinker.Candidates(Query.From, keepJoin: false))
            yield return new PlanEquivalenceCheck(Query with { From = from }, HintAlias, HintIndex, arm, Source == PlanSource.FullScan);
    }

    public override IEnumerable<string> Describe(ISqlPrinter printer)
    {
        yield return printer.Print(Query);
        string prefix = arm is null ? "" : $"[arm {arm.Name}: {string.Join("; ", new[] { arm.OptionsText, arm.PrepareText is null ? "" : string.Format(System.Globalization.CultureInfo.InvariantCulture, arm.PrepareText, "executor") }.Where(t => t.Length > 0))}] ";
        yield return prefix + printer.Print(Second);
    }

    public override void WriteTest(TestWriter writer, ISqlPrinter printer)
    {
        string other = arm is null ? "db" : writer.Arm(arm);
        if (Source == PlanSource.FullScan)
        {
            writer.Line($"await db.AssertNoRecRows({TestWriter.Literal(printer.Print(Query))},\n            {TestWriter.Literal(printer.Print(Second))});");
            return;
        }
        int runs = arm?.Name == "plan-cache" ? 2 : 1;
        for (int run = 0; run < runs; run++)
            writer.Line($"await SqlFuzzReplay.AssertSameRows(db, {TestWriter.Literal(printer.Print(Query))},\n            {other}, {TestWriter.Literal(printer.Print(Second))});");
    }
}

/// <summary>
/// Makes plan-equivalence checks: for each query, every configuration arm, every index hint of every
/// source, and the full-scan baseline of a one-table query. The arms are built once per round, after
/// the data and any ANALYZE, and share the round's database.
///
/// <para><b>Coverage.</b> A configuration name is no evidence that a check compared two different
/// plans: an arm that turns off hash joins changes only queries that picked a hash join. So each passed
/// check also takes the EXPLAIN of both plans, and the report counts the checks whose second plan had
/// another access path than the default one, and how often each operator was the second plan.</para>
/// </summary>
internal sealed class PlanEquivalenceOracle : IOracle
{
    /// <summary>
    /// The configuration arms. The options arms turn one planner or executor path off or on: the
    /// heuristic planner, hash joins off (they fall back to nested loops), the Grace hash join (spill
    /// with a threshold of one row), an index nested-loop batch of one row, the plan cache, the
    /// dictionary row decode, and the query caches off. The forced-operator arms set a test seam of the
    /// arm's own executor (<see cref="CamusDB.Core.Statistics.StatisticsManager"/>), so every equi-join
    /// takes a merge join, a hash join, a nested loop, or an index nested loop where an index allows one.
    /// </summary>
    public static IReadOnlyList<ArmSpec> Arms { get; } =
    [
        new("heuristic-planner", o => o with { CostBasedAccessPathEnabled = false, CostBasedJoinOrderEnabled = false },
            "CostBasedAccessPathEnabled = false, CostBasedJoinOrderEnabled = false"),
        new("no-hash-join", o => o with { HashJoinMaxBuildRows = 0 }, "HashJoinMaxBuildRows = 0"),
        new("grace-hash-join", o => o with { SpillEnabled = true, ForceSpillThresholdRows = 1 }, "SpillEnabled = true, ForceSpillThresholdRows = 1"),
        new("index-batch-1", o => o with { IndexScanFetchBatchSize = 1 }, "IndexScanFetchBatchSize = 1"),
        new("plan-cache", o => o with { PlanCacheEnabled = true }, "PlanCacheEnabled = true"),
        new("row-decode", o => o with { SlotBackedDecode = false }, "SlotBackedDecode = false"),
        new("no-query-caches", o => o with { BoundQueryCacheEnabled = false, PointReadRowIdCacheEnabled = false },
            "BoundQueryCacheEnabled = false, PointReadRowIdCacheEnabled = false"),
        new("force-merge-join", o => o, "")
        {
            Prepare = e => e.Statistics.ForceMergeJoinForTesting = true,
            PrepareText = "{0}.Statistics.ForceMergeJoinForTesting = true",
        },
        new("force-hash-join", o => o, "")
        {
            Prepare = e => e.Statistics.ForceHashJoinForTesting = true,
            PrepareText = "{0}.Statistics.ForceHashJoinForTesting = true",
        },
        new("force-nested-loop", o => o, "")
        {
            Prepare = e => e.Statistics.ForceNestedLoopForTesting = true,
            PrepareText = "{0}.Statistics.ForceNestedLoopForTesting = true",
        },
        new("force-index-nested-loop", o => o, "")
        {
            Prepare = e => e.Statistics.ForceIndexNestedLoopForTesting = true,
            PrepareText = "{0}.Statistics.ForceIndexNestedLoopForTesting = true",
        },
    ];

    /// <summary>The arm with this name, for a replay of a recorded read.</summary>
    public static ArmSpec? ArmByName(string name) => Arms.FirstOrDefault(a => a.Name == name);

    public OracleKind Kind => OracleKind.PlanEquivalence;

    /// <summary>Passed checks whose two plans were compared with EXPLAIN, and how many of them had different access paths.</summary>
    public long PlansCompared { get; private set; }

    public long PlansDifferent { get; private set; }

    /// <summary>How often each operator (the text before the first parenthesis of an EXPLAIN line) appeared in the second plan of a passed check.</summary>
    public Dictionary<string, long> SecondPlanOperators { get; } = new(StringComparer.Ordinal);

    public async Task RunRoundAsync(RoundContext round)
    {
        StatementGenerator generator = round.Generator;
        for (int i = 0; i < round.CheckCount && !round.ShouldStop; i++)
        {
            (FromItem from, ExprScope scope) = generator.GenerateFrom(SelectOptions.Comparable with { AllowHints = false });
            SelectStmt query = new(generator.GenerateProjection(scope), from, generator.Random.Chance(0.9) ? generator.Expressions.Predicate(scope.ForSelectWhere()) : null);

            List<PlanEquivalenceCheck> checks = new();
            foreach (ArmSpec arm in Arms)
                checks.Add(new PlanEquivalenceCheck(query, null, null, arm));
            foreach (TableRef table in Sources(from))
                foreach (string index in generator.State.IndexesOf(table.Table).Select(x => x.Name).Append("pk"))
                    checks.Add(new PlanEquivalenceCheck(query, table.Alias, index, null));
            if (from is TableRef && query.Where is not null)
                checks.Add(new PlanEquivalenceCheck(query, null, null, null, fullScan: true));

            string? defaultPath = null;
            foreach (PlanEquivalenceCheck check in checks)
            {
                CheckResult result = await round.RunAsync(check).ConfigureAwait(false);
                if (result.Status == CheckStatus.Fail || round.ShouldStop)
                    break;
                if (result.Status != CheckStatus.Pass || check.Source == PlanSource.FullScan)
                    continue;

                IReadOnlyList<string> plans = await check.PlansAsync(round.Checks).ConfigureAwait(false);
                // Each plan text starts with a label line ("default plan:"), which is not part of the plan.
                defaultPath ??= NoRecOracle.AccessPath(plans[0][(plans[0].IndexOf('\n') + 1)..]);
                string secondPath = NoRecOracle.AccessPath(plans[1][(plans[1].IndexOf('\n') + 1)..]);
                PlansCompared++;
                if (secondPath != defaultPath)
                    PlansDifferent++;
                foreach (string line in secondPath.Split('\n', StringSplitOptions.RemoveEmptyEntries))
                {
                    string op = line.Split('(')[0].Trim();
                    if (op.Length > 0)
                        SecondPlanOperators[op] = SecondPlanOperators.GetValueOrDefault(op) + 1;
                }
            }
        }
    }

    public static IEnumerable<TableRef> Sources(FromItem from) => from switch
    {
        TableRef t => [t],
        JoinFrom j => Sources(j.Left).Append(j.Right),
        _ => [],
    };
}
