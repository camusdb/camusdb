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

/// <summary>The forms of ternary logic partitioning.</summary>
internal enum TlpVariant
{
    /// <summary><c>Q WHERE p</c>, <c>Q WHERE NOT p</c> and <c>Q WHERE p IS NULL</c> together hold the rows of <c>Q</c>.</summary>
    Where,

    /// <summary>The same for SELECT DISTINCT, combined as a set.</summary>
    Distinct,

    /// <summary>MIN of the MINs, MAX of the MAXs, SUM of the SUMs and SUM of the COUNTs equal the aggregate of the whole.</summary>
    Aggregate,

    /// <summary>The aggregate rule for each group of a GROUP BY.</summary>
    GroupBy,

    /// <summary>The partition on a HAVING predicate.</summary>
    Having,

    /// <summary>The partition on the ON predicate of an INNER join: <c>ON c AND p</c>, <c>ON c AND NOT p</c>, <c>ON c AND p IS NULL</c>.</summary>
    JoinOn,
}

/// <summary>
/// One TLP check. For a predicate <c>p</c>, every row of the base query has exactly one of the
/// values TRUE, FALSE or NULL for <c>p</c>, so the three parts split the rows with no gap and no
/// overlap. A row that a part loses or gains is a three-valued-logic bug, a NULL bug, or a bug in an
/// access path that only one part takes.
///
/// <para>An aggregate is combined from the parts with the rules of <see cref="SqlFuzzValues.Combine"/>.
/// AVG has no such rule and is not partitioned. A FLOAT64 sum compares within a tolerance; every
/// other value compares exactly. The aggregate and GROUP BY forms read <c>SUM(ABS(x))</c> next to each
/// FLOAT64 <c>SUM(x)</c> (<see cref="AbsSumItems"/>), because the error of a float sum scales with the
/// absolute size of its inputs, not with its result (<see cref="SqlFuzzValues.FloatsClose"/>).</para>
/// </summary>
internal sealed class TlpCheck : OracleCheck
{
    public TlpCheck(TlpVariant kind, FromItem from, IReadOnlyList<SelectItem> items, Expr predicate,
        IReadOnlyList<Expr>? groupBy = null, IReadOnlyList<AggregateExpr>? aggregates = null)
    {
        Kind = kind;
        From = from;
        Items = items;
        Predicate = predicate;
        GroupBy = groupBy ?? [];
        Aggregates = aggregates ?? [];
    }

    public TlpVariant Kind { get; }

    public FromItem From { get; }

    /// <summary>The output columns of the WHERE, DISTINCT and JOIN ON forms.</summary>
    public IReadOnlyList<SelectItem> Items { get; }

    /// <summary>The partition predicate: a WHERE predicate, a HAVING predicate, or the extra ON conjunct.</summary>
    public Expr Predicate { get; }

    public IReadOnlyList<Expr> GroupBy { get; }

    /// <summary>The aggregates of the AGGREGATE, GROUP BY and HAVING forms; AGGREGATE uses the first only.</summary>
    public IReadOnlyList<AggregateExpr> Aggregates { get; }

    public override OracleKind Oracle => OracleKind.Tlp;

    public override string Variant => Kind switch
    {
        TlpVariant.Aggregate => "tlp:aggregate:" + FunctionName(Aggregates[0]),
        _ => "tlp:" + Kind.ToString().ToLowerInvariant(),
    };

    /// <summary>The name <see cref="SqlFuzzValues.Combine"/> uses for an aggregate.</summary>
    public static string FunctionName(AggregateExpr aggregate) => aggregate.Func switch
    {
        AggregateFunc.CountStar or AggregateFunc.Count => "count",
        AggregateFunc.Sum => "sum",
        AggregateFunc.Min => "min",
        AggregateFunc.Max => "max",
        _ => "avg",
    };

    private static Expr Not(Expr p) => new UnaryExpr(UnaryOp.Not, p);
    private static Expr IsNull(Expr p) => new UnaryExpr(UnaryOp.IsNull, p);

    /// <summary>The whole query and the three parts.</summary>
    public (SelectStmt Whole, SelectStmt[] Parts) Build()
    {
        Expr[] partitions = [Predicate, Not(Predicate), IsNull(Predicate)];
        switch (Kind)
        {
            case TlpVariant.Where:
            case TlpVariant.Distinct:
            {
                bool distinct = Kind == TlpVariant.Distinct;
                SelectStmt whole = new(Items, From, Distinct: distinct);
                return (whole, partitions.Select(p => whole with { Where = p }).ToArray());
            }

            case TlpVariant.Aggregate:
            {
                SelectStmt whole = new([new SelectItem(Aggregates[0], "agg"), .. AbsSumItems()], From);
                return (whole, partitions.Select(p => whole with { Where = p }).ToArray());
            }

            case TlpVariant.GroupBy:
            {
                SelectStmt whole = new([.. GroupedItems(), .. AbsSumItems()], From, GroupBy: GroupBy);
                return (whole, partitions.Select(p => whole with { Where = p }).ToArray());
            }

            case TlpVariant.Having:
            {
                SelectStmt whole = new(GroupedItems(), From, GroupBy: GroupBy);
                return (whole, partitions.Select(p => whole with { Having = p }).ToArray());
            }

            case TlpVariant.JoinOn:
            {
                JoinFrom join = (JoinFrom)From;
                SelectStmt whole = new(Items, From);
                return (whole, partitions.Select(p => whole with
                {
                    From = join with { On = new BinaryExpr(BinaryOp.And, join.On!, p, FuzzType.Bool) },
                }).ToArray());
            }

            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    /// <summary>
    /// A <c>SUM(ABS(x))</c> column, named <see cref="SqlFuzzValues.AbsSumColumnPrefix"/> and the aggregate
    /// index, for each FLOAT64 <c>SUM(x)</c>. The comparison takes it as the scale of the tolerance.
    /// </summary>
    private IEnumerable<SelectItem> AbsSumItems() =>
        Aggregates
            .Select((a, i) => (Aggregate: a, Index: i))
            .Where(x => x.Aggregate is { Func: AggregateFunc.Sum, Argument.Type: FuzzType.Float64 })
            .Select(x => new SelectItem(
                new AggregateExpr(AggregateFunc.Sum, new FunctionExpr("abs", [x.Aggregate.Argument!], FuzzType.Float64), FuzzType.Float64),
                SqlFuzzValues.AbsSumColumnPrefix + x.Index));

    private IReadOnlyList<SelectItem> GroupedItems() =>
        GroupBy.Select((g, i) => new SelectItem(g, "g" + i))
            .Concat(Aggregates.Select((a, i) => new SelectItem(a, "x" + i)))
            .ToList();

    public override async Task<CheckResult> RunAsync(CheckContext context)
    {
        (SelectStmt whole, SelectStmt[] parts) = Build();

        StatementOutcome wholeOutcome = await context.QueryAsync(whole).ConfigureAwait(false);
        if (wholeOutcome.Kind != OutcomeKind.Rows)
            return CheckResult.FromBadOutcome(wholeOutcome);
        if (wholeOutcome.WireError is not null)
            return CheckResult.FromBadOutcome(wholeOutcome);

        StatementOutcome[] partOutcomes = new StatementOutcome[parts.Length];
        for (int i = 0; i < parts.Length; i++)
        {
            partOutcomes[i] = await context.QueryAsync(parts[i]).ConfigureAwait(false);
            if (partOutcomes[i].Kind != OutcomeKind.Rows || partOutcomes[i].WireError is not null)
                return CheckResult.FromBadOutcome(partOutcomes[i]);
        }

        // Every part has the projection of the whole, so it must have the output shape of the whole.
        foreach (StatementOutcome part in partOutcomes)
            if (SqlFuzzValues.ShapeDifference(Shape(wholeOutcome), Shape(part), "whole", "part") is { } shape)
                return CheckResult.Fail(Variant + ":shape", $"{shape}\n  part: {part.Sql}");

        string? problem;
        switch (Kind)
        {
            case TlpVariant.Where:
            case TlpVariant.Having:
            case TlpVariant.JoinOn:
                problem = SqlFuzzValues.Difference(
                    SqlFuzzValues.Multiset(wholeOutcome.Rows),
                    SqlFuzzValues.Union(partOutcomes.Select(o => SqlFuzzValues.Multiset(o.Rows)).ToArray()),
                    "whole", "parts");
                break;

            case TlpVariant.Distinct:
            {
                // A DISTINCT read must not return a row twice; the whole is compared as it came, so a
                // duplicate in it is a difference, not a row the comparison folds away.
                List<string> wholeRows = SqlFuzzValues.Multiset(wholeOutcome.Rows);
                if (SqlFuzzValues.Duplicate(wholeRows) is { } twice)
                    return CheckResult.Fail("tlp:distinct:duplicate", $"the whole DISTINCT read returned {twice} twice");
                List<List<string>> partRows = partOutcomes.Select(o => SqlFuzzValues.Multiset(o.Rows)).ToList();
                for (int i = 0; i < partRows.Count; i++)
                    if (SqlFuzzValues.Duplicate(partRows[i]) is { } partTwice)
                        return CheckResult.Fail("tlp:distinct:duplicate", $"a DISTINCT part returned {partTwice} twice\n  part: {partOutcomes[i].Sql}");
                problem = SqlFuzzValues.Difference(wholeRows, SqlFuzzValues.SetUnion(partRows.ToArray()), "whole", "parts");
                break;
            }

            case TlpVariant.Aggregate:
            {
                string function = FunctionName(Aggregates[0]);
                ColumnValue expected = First(wholeOutcome);
                double? absSum = wholeOutcome.Rows.Count == 0 ? null : SqlFuzzValues.AbsSum(wholeOutcome.Columns.Select(c => c.Name).ToList(), wholeOutcome.Rows[0], 0);
                ColumnValue? combined = SqlFuzzValues.Combine(function, partOutcomes.Select(First).ToList());
                if (combined is null)
                    return CheckResult.Skip("the parts cannot be combined exactly");
                problem = SqlFuzzValues.SameAggregate(function, expected, combined, absSum)
                    ? null
                    : $"{function}: whole {SqlFuzzValues.Render(expected)}, parts {string.Join(" | ", partOutcomes.Select(o => SqlFuzzValues.Render(First(o))))} combine to {SqlFuzzValues.Render(combined)}";
                break;
            }

            case TlpVariant.GroupBy:
                problem = GroupPartition.Compare(GroupBy.Count, Aggregates.Select(FunctionName).ToList(), ToRows(wholeOutcome), partOutcomes.Select(ToRows).ToList());
                break;

            default:
                problem = null;
                break;
        }

        if (problem is not null)
            return CheckResult.Fail(Variant + ":mismatch", problem);

        // Inputs in the overflow range: the order of the additions decides if a partial sum overflows,
        // so the float sums were accepted, not compared. Count the check as a skip, not as a pass.
        if (HasOverflowRangeSum(wholeOutcome))
            return CheckResult.Skip("a FLOAT64 sum has inputs in the overflow range (SUM(ABS(x)) is infinite); it was not compared");
        return CheckResult.Pass;
    }

    private static IReadOnlyList<(string, Core.Catalogs.Models.ColumnType)> Shape(StatementOutcome outcome) =>
        outcome.Columns.Select(c => (c.Name, c.Type)).ToList();

    private static bool HasOverflowRangeSum(StatementOutcome whole)
    {
        for (int c = 0; c < whole.Columns.Count; c++)
        {
            if (!whole.Columns[c].Name.StartsWith(SqlFuzzValues.AbsSumColumnPrefix, StringComparison.OrdinalIgnoreCase))
                continue;
            foreach (ColumnValue[] row in whole.Rows)
                if (row[c].Type == Core.Catalogs.Models.ColumnType.Float64 && double.IsInfinity(row[c].FloatValue))
                    return true;
        }
        return false;
    }

    private static ColumnValue First(StatementOutcome outcome) => outcome.Rows.Count == 0 ? ColumnValue.Null : outcome.Rows[0][0];

    private static SqlFuzzRows ToRows(StatementOutcome outcome) =>
        new(outcome.Columns.Select(c => c.Name).ToList(), outcome.Rows.Select(r => (IReadOnlyList<ColumnValue>)r).ToList());

    public override IEnumerable<OracleCheck> Simplify()
    {
        foreach (Expr p in ExprShrinker.Candidates(Predicate))
            yield return new TlpCheck(Kind, From, Items, p, GroupBy, Aggregates);

        if (Items.Count > 1)
            for (int i = 0; i < Items.Count; i++)
                yield return new TlpCheck(Kind, From, Items.Where((_, j) => j != i).ToList(), Predicate, GroupBy, Aggregates);

        if (Aggregates.Count > 1)
            for (int i = 0; i < Aggregates.Count; i++)
                yield return new TlpCheck(Kind, From, Items, Predicate, GroupBy, Aggregates.Where((_, j) => j != i).ToList());

        if (GroupBy.Count > 1 && Kind == TlpVariant.GroupBy)
            for (int i = 0; i < GroupBy.Count; i++)
                yield return new TlpCheck(Kind, From, Items, Predicate, GroupBy.Where((_, j) => j != i).ToList(), Aggregates);

        foreach (FromItem from in FromShrinker.Candidates(From, Kind == TlpVariant.JoinOn))
        {
            // The ON partition is valid only for an INNER outermost join; a reduction that loses it
            // would report a "mismatch" that the rule itself causes.
            if (Kind == TlpVariant.JoinOn && from is not JoinFrom { Kind: JoinKind.Inner, On: not null })
                continue;
            yield return new TlpCheck(Kind, from, Items, Predicate, GroupBy, Aggregates);
        }
    }

    public override IEnumerable<string> Describe(ISqlPrinter printer)
    {
        (SelectStmt whole, SelectStmt[] parts) = Build();
        yield return printer.Print(whole);
        foreach (SelectStmt part in parts)
            yield return printer.Print(part);
    }

    public override void WriteTest(TestWriter writer, ISqlPrinter printer)
    {
        (SelectStmt whole, SelectStmt[] parts) = Build();
        string wholeSql = TestWriter.Literal(printer.Print(whole));
        string partSql = string.Join(",\n            ", parts.Select(p => TestWriter.Literal(printer.Print(p))));
        switch (Kind)
        {
            case TlpVariant.Distinct:
                writer.Line($"await db.AssertPartitionDistinct({wholeSql},\n            {partSql});");
                break;
            case TlpVariant.Aggregate:
                writer.Line($"await db.AssertPartitionAggregate({TestWriter.Literal(FunctionName(Aggregates[0]))}, {wholeSql},\n            {partSql});");
                break;
            case TlpVariant.GroupBy:
                writer.Line($"await db.AssertPartitionGroups({GroupBy.Count}, {TestWriter.Array(Aggregates.Select(FunctionName))}, {wholeSql},\n            {partSql});");
                break;
            default:
                writer.Line($"await db.AssertPartitionRows({wholeSql},\n            {partSql});");
                break;
        }
    }
}

/// <summary>Makes TLP checks: a random variant, FROM clause, projection and predicate.</summary>
internal sealed class TlpOracle : IOracle
{
    public OracleKind Kind => OracleKind.Tlp;

    public async Task RunRoundAsync(RoundContext round)
    {
        for (int i = 0; i < round.CheckCount && !round.ShouldStop; i++)
        {
            TlpCheck? check = Generate(round.Generator);
            if (check is not null)
                await round.RunAsync(check).ConfigureAwait(false);
        }
    }

    public static TlpCheck? Generate(StatementGenerator generator)
    {
        FuzzRandom random = generator.Random;
        GeneratorProfile profile = generator.Profile;

        List<TlpVariant> variants = [TlpVariant.Where, TlpVariant.Where, TlpVariant.Aggregate];
        if (profile.Allows(StatementFeature.Distinct))
            variants.Add(TlpVariant.Distinct);
        if (profile.Allows(StatementFeature.GroupBy))
            variants.Add(TlpVariant.GroupBy);
        if (profile.Allows(StatementFeature.GroupBy) && profile.Allows(StatementFeature.Having))
            variants.Add(TlpVariant.Having);
        if (profile.Allows(StatementFeature.Joins) && profile.Allows(JoinKind.Inner))
            variants.Add(TlpVariant.JoinOn);

        TlpVariant kind = random.Pick(variants);

        if (kind == TlpVariant.JoinOn)
        {
            // A two- or three-table chain whose outermost join is INNER, so every row of the join
            // result came from one pair the ON predicate accepted.
            for (int attempt = 0; attempt < 6; attempt++)
            {
                (FromItem joinFrom, ExprScope joinScope) = generator.GenerateFrom(SelectOptions.Comparable with { MaxTables = 3 });
                if (joinFrom is JoinFrom { Kind: JoinKind.Inner, On: not null })
                    // The predicate joins the ON clause, which has no per-row EXISTS stage.
                    return new TlpCheck(kind, joinFrom, generator.GenerateProjection(joinScope), generator.Expressions.Predicate(joinScope.ForUncorrelatedSubqueries()));
            }
            kind = TlpVariant.Where;
        }

        (FromItem from, ExprScope scope) = generator.GenerateFrom(SelectOptions.Comparable);
        // The predicate and its partitions (NOT p, p IS NULL) all sit in a WHERE, so a correlated
        // EXISTS is allowed; the HAVING variant replaces it below.
        Expr predicate = generator.Expressions.Predicate(scope.ForSelectWhere());

        switch (kind)
        {
            case TlpVariant.Aggregate:
            {
                AggregateExpr aggregate;
                int attempts = 0;
                do
                    aggregate = generator.Aggregate(scope);
                while (aggregate.Func == AggregateFunc.Avg && ++attempts < 10);
                if (aggregate.Func == AggregateFunc.Avg)
                    aggregate = new AggregateExpr(AggregateFunc.CountStar, null, FuzzType.Int64);
                return new TlpCheck(kind, from, [], predicate, aggregates: [aggregate]);
            }

            case TlpVariant.GroupBy:
            case TlpVariant.Having:
            {
                List<ScopeColumn> groupable = scope.Columns.Where(c => FuzzTypes.IsOrderable(c.Type)).ToList();
                List<Expr> groupBy = random.Shuffled(groupable).Take(random.Between(1, 2))
                    .Select(c => (Expr)new ColumnExpr(c.Source, c.Column, c.Type)).ToList();
                List<AggregateExpr> aggregates = Enumerable.Range(0, random.Between(1, 3))
                    .Select(_ => generator.Aggregate(scope))
                    .Where(a => a.Func != AggregateFunc.Avg)
                    .ToList();
                if (aggregates.Count == 0)
                    aggregates.Add(new AggregateExpr(AggregateFunc.CountStar, null, FuzzType.Int64));
                if (kind == TlpVariant.Having)
                    predicate = generator.HavingPredicate(scope, groupBy);
                return new TlpCheck(kind, from, [], predicate, groupBy, aggregates);
            }

            default:
                return new TlpCheck(kind, from, generator.GenerateProjection(scope), predicate);
        }
    }
}

/// <summary>Simpler FROM clauses for the shrinker: drop an index hint, or drop the last join.</summary>
internal static class FromShrinker
{
    public static IEnumerable<FromItem> Candidates(FromItem from, bool keepJoin)
    {
        if (from is TableRef { IndexHint: not null } hinted)
            yield return hinted with { IndexHint = null };

        if (from is JoinFrom join)
        {
            if (join.Right.IndexHint is not null)
                yield return join with { Right = join.Right with { IndexHint = null } };
            foreach (FromItem left in Candidates(join.Left, keepJoin: false))
                yield return join with { Left = left };
            if (join.On is not null)
                foreach (Expr on in ExprShrinker.Candidates(join.On))
                    yield return join with { On = on };
            if (!keepJoin || join.Left is JoinFrom)
                yield return join.Left;
        }
    }
}
