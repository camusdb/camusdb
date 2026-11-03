/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.SqlFuzz.Tests.Campaign;
using CamusDB.SqlFuzz.Tests.Engine;
using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Oracles;
using CamusDB.SqlFuzz.Tests.Printing;
using CamusDB.SqlFuzz.Tests.Runner;
using CamusDB.SqlFuzz.Tests.Shrinking;

namespace CamusDB.SqlFuzz.Tests.Unit;

/// <summary>
/// The generator: one seed gives one statement list, a profile removes what it removes, and most
/// generated statements run without an error.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestGenerator
{
    private static List<string> Program(ulong seed, GeneratorProfile profile, int statements)
    {
        StatementGenerator generator = new(new FuzzRandom(seed), profile);
        List<Statement> list = generator.GenerateSchema();
        list.AddRange(generator.GenerateData());
        for (int i = 0; i < statements; i++)
            list.Add(generator.GenerateStatement());
        return list.Select(CamusSqlPrinter.Instance.Print).ToList();
    }

    [Test]
    public void OneSeedGivesOneStatementList()
    {
        for (ulong seed = 1; seed <= 20; seed++)
            Assert.That(Program(seed, GeneratorProfile.CamusDB, 50), Is.EqualTo(Program(seed, GeneratorProfile.CamusDB, 50)), $"seed {seed}");

        Assert.That(Program(1, GeneratorProfile.CamusDB, 50), Is.Not.EqualTo(Program(2, GeneratorProfile.CamusDB, 50)));
    }

    [Test]
    public void TheRandomSourceIsFixed()
    {
        // The sequence is part of the replay contract: a CI seed must give the same statements on every
        // machine and runtime. A change to FuzzRandom breaks every recorded seed.
        FuzzRandom random = new(42);
        // xoshiro256** seeded through SplitMix64 with 42; the values match the reference algorithm.
        Assert.That(new[] { random.NextULong(), random.NextULong(), random.NextULong() },
            Is.EqualTo(new[] { 1546998764402558742UL, 6990951692964543102UL, 12544586762248559009UL }));
        Assert.That(FuzzRandom.DeriveSeed(7, 3), Is.EqualTo(13015481187462834606UL));
        Assert.That(FuzzRandom.DeriveSeed(7, 3), Is.EqualTo(FuzzRandom.DeriveSeed(7, 3)));
        Assert.That(FuzzRandom.DeriveSeed(7, 3), Is.Not.EqualTo(FuzzRandom.DeriveSeed(7, 4)));
    }

    [Test]
    public void AProfileThatRemovesAFormStopsTheGeneratorFromSelectingIt()
    {
        GeneratorProfile profile = GeneratorProfile.CamusDB
            .WithoutOperators(BinaryOp.Concat, BinaryOp.Mod)
            .WithoutOperators(UnaryOp.IsTrue)
            .WithoutFunctions("lower", "substring")
            .WithoutFeatures(ExprFeature.Like, ExprFeature.Case)
            .WithoutFeatures(StatementFeature.WindowFunctions)
            .WithoutJoins(JoinKind.Right)
            .WithoutTypes(FuzzType.Jsonb)
            .Named("restricted");

        List<string> sql = new();
        for (ulong seed = 1; seed <= 40; seed++)
            sql.AddRange(Program(seed, profile, 250));
        string all = string.Join("\n", sql);

        Assert.That(all, Does.Not.Contain(" || "));
        Assert.That(all, Does.Not.Contain(" % "));
        Assert.That(all, Does.Not.Contain(" IS TRUE"));
        Assert.That(all, Does.Not.Contain("lower("));
        Assert.That(all, Does.Not.Contain("substring("));
        Assert.That(all, Does.Not.Contain(" LIKE "));
        Assert.That(all, Does.Not.Contain("(CASE "));
        Assert.That(all, Does.Not.Contain(" OVER ("));
        Assert.That(all, Does.Not.Contain("RIGHT JOIN"));
        Assert.That(all, Does.Not.Contain("jsonb"));

        // The same seeds with the full profile do select them, so the assertions above test the profile.
        string full = string.Join("\n", Enumerable.Range(1, 40).SelectMany(s => Program((ulong)s, GeneratorProfile.CamusDB, 250)));
        Assert.That(full, Does.Contain(" || "));
        Assert.That(full, Does.Contain(" LIKE "));
        Assert.That(full, Does.Contain("(CASE "));
        Assert.That(full, Does.Contain("RIGHT JOIN"));
        Assert.That(full, Does.Contain("jsonb"));
    }

    [Test]
    public void TheTreePrintsAsCamusSql()
    {
        TableDef table = new("t0", [new ColumnDef("c0", FuzzType.Int64, true, true), new ColumnDef("c1", FuzzType.Float64, false, false)]);
        SelectStmt select = new(
            [new SelectItem(new ColumnExpr("a0", "c1", FuzzType.Float64), "o0")],
            new TableRef("t0", "a0", "t0_i0"),
            new BinaryExpr(BinaryOp.Gt, new ColumnExpr("a0", "c1", FuzzType.Float64), new LiteralExpr(FuzzValue.Of(double.NaN)), FuzzType.Bool));

        Assert.That(CamusSqlPrinter.Instance.Print(new CreateTableStmt(table)), Is.EqualTo("CREATE TABLE t0 (c0 int64 PRIMARY KEY, c1 float64)"));
        Assert.That(CamusSqlPrinter.Instance.Print(select),
            Is.EqualTo("SELECT a0.c1 AS o0 FROM t0 AS a0@{FORCE_INDEX=t0_i0} WHERE (a0.c1 > CAST('NaN' AS float64))"));
        Assert.That(CamusSqlPrinter.Instance.Print(new LiteralExpr(FuzzValue.Of(-0.0))), Is.EqualTo("-0.0"));
        Assert.That(CamusSqlPrinter.Instance.Print(new LiteralExpr(FuzzValue.Of(long.MinValue))), Is.EqualTo("-9223372036854775808"));
        Assert.That(CamusSqlPrinter.Instance.Print(new LiteralExpr(FuzzValue.Of(1e300))), Is.EqualTo("1e300"));
        Assert.That(CamusSqlPrinter.Instance.Print(new LiteralExpr(FuzzValue.Null(FuzzType.Date))), Is.EqualTo("CAST(NULL AS date)"));
    }

    /// <summary>
    /// The profiles to check: each operator, function, expression form, statement form, join kind and
    /// set operation on its own, each removable type, and combinations, among them the one that let
    /// INSERT … SELECT bring back a removed + and COALESCE.
    /// </summary>
    private static IEnumerable<GeneratorProfile> ExclusionProfiles()
    {
        GeneratorProfile full = GeneratorProfile.CamusDB;
        foreach (BinaryOp op in full.BinaryOps.Where(o => o is not (BinaryOp.And or BinaryOp.Or or BinaryOp.Eq)))
            yield return full.WithoutOperators(op).Named("without " + op);
        foreach (UnaryOp op in full.UnaryOps.Where(o => o != UnaryOp.Not))
            yield return full.WithoutOperators(op).Named("without " + op);
        foreach (string function in FunctionCatalog.Names)
            yield return full.WithoutFunctions(function).Named("without " + function);
        foreach (ExprFeature feature in Enum.GetValues<ExprFeature>())
            yield return full.WithoutFeatures(feature).Named("without " + feature);
        foreach (StatementFeature feature in Enum.GetValues<StatementFeature>())
            yield return full.WithoutFeatures(feature).Named("without " + feature);
        foreach (JoinKind kind in Enum.GetValues<JoinKind>())
            yield return full.WithoutJoins(kind).Named("without " + kind + " joins");
        foreach (SetOpKind kind in Enum.GetValues<SetOpKind>())
            yield return full.WithoutSetOperations(kind).Named("without " + kind);
        foreach (FuzzType type in new[] { FuzzType.Float64, FuzzType.Numeric, FuzzType.String, FuzzType.Date, FuzzType.Jsonb })
            yield return full.WithoutTypes(type).Named("without " + type);

        yield return full.WithoutOperators(BinaryOp.Add).WithoutFeatures(ExprFeature.Coalesce).Named("without + and COALESCE");
        yield return full.WithoutJoins(Enum.GetValues<JoinKind>()).Named("without any join kind");
        yield return full.WithoutFeatures(Enum.GetValues<ExprFeature>()).Named("without any expression feature");
        yield return full.WithoutOperators(BinaryOp.Add, BinaryOp.Sub, BinaryOp.Mul, BinaryOp.Div, BinaryOp.Mod).Named("without arithmetic");
        yield return full.WithoutOperators(BinaryOp.Lt, BinaryOp.Le, BinaryOp.Gt, BinaryOp.Ge, BinaryOp.Ne).Named("without inequalities");
    }

    /// <summary>
    /// A profile is a contract for the model and reference consumers: no generated tree may hold a form
    /// the profile removed, the helper expressions the generator adds on its own included. The check
    /// walks the whole tree (<see cref="ProfileValidator"/>), for every statement kind the generator
    /// makes, over many seeds.
    /// </summary>
    [Test]
    public void EveryProfileExclusionHoldsOnTheWholeTree()
    {
        List<string> problems = new();
        int profiles = 0;
        foreach (GeneratorProfile profile in ExclusionProfiles())
        {
            profiles++;
            bool reported = false;
            for (ulong seed = 1; seed <= 12 && !reported; seed++)
            {
                StatementGenerator generator = new(new FuzzRandom(seed), profile);
                List<Statement> statements = generator.GenerateSchema();
                statements.AddRange(generator.GenerateData());
                for (int i = 0; i < 60; i++)
                {
                    statements.Add(generator.GenerateStatement());
                    statements.Add(generator.GenerateWrite());
                    statements.Add(generator.GenerateSelect(SelectOptions.Comparable));
                }
                foreach (Statement statement in statements)
                {
                    List<string> violations = ProfileValidator.Violations(statement, profile);
                    if (violations.Count == 0)
                        continue;
                    // The first violation of each profile, so one run shows every profile that leaks.
                    string text = CamusSqlPrinter.Instance.Print(statement);
                    problems.Add($"{profile.Name}, seed {seed}: {string.Join(", ", violations.Distinct())}\n    {(text.Length > 300 ? text[..300] + " …" : text)}");
                    reported = true;
                    break;
                }
            }
        }
        Assert.That(profiles, Is.GreaterThan(50));
        Assert.That(problems, Is.Empty, string.Join("\n", problems));

        // The validator sees the forms: the full profile, checked against a profile without them, has violations.
        GeneratorProfile none = GeneratorProfile.CamusDB.WithoutFeatures(Enum.GetValues<ExprFeature>()).WithoutFeatures(Enum.GetValues<StatementFeature>());
        StatementGenerator full = new(new FuzzRandom(1), GeneratorProfile.CamusDB);
        List<Statement> sample = full.GenerateSchema();
        sample.AddRange(full.GenerateData());
        for (int i = 0; i < 200; i++)
            sample.Add(full.GenerateStatement());
        Assert.That(sample.Sum(s => ProfileValidator.Violations(s, none).Count), Is.GreaterThan(0));
    }

    /// <summary>
    /// At least 90 % of 10,000 generated statements run with no error, and none of them, the setup of
    /// each block included, is a finding of the error-classification rule (known issues are not
    /// findings). Each block of 100 statements runs on a fresh database with a new schema and new data.
    /// </summary>
    [Test]
    public async Task NinetyPercentOfGeneratedStatementsRunWithoutAnError()
    {
        const int total = 10_000;
        const int perDatabase = 100;

        await using FuzzEngine engine = await FuzzEngine.StartAsync();
        int succeeded = 0;
        int run = 0;
        Dictionary<string, int> errors = new(StringComparer.Ordinal);
        Dictionary<string, string> samples = new(StringComparer.Ordinal);
        List<string> unclassified = new();

        for (ulong seed = 1; run < total; seed++)
        {
            StatementGenerator generator = new(new FuzzRandom(FuzzRandom.DeriveSeed(20261102, (long)seed)), GeneratorProfile.CamusDB);
            List<SetupStatement> setup = generator.GenerateSchema().Concat(generator.GenerateData()).Select(SetupStatement.Of).ToList();

            FuzzArm database = await engine.CreateDatabaseAsync();
            SqlRunner runner = new(database, TimeSpan.FromSeconds(20));
            foreach (SetupOutcome outcome in await SetupRunner.RunAsync(runner, setup, CamusSqlPrinter.Instance))
            {
                // The setup is generated too: a defect there is a finding, and a refused CREATE leaves the
                // block without the schema its statements assume.
                if (ErrorClassifier.Classify(outcome.Outcome) == ErrorVerdict.Finding || (outcome.Statement.Route == StatementRoute.Ddl && !outcome.Outcome.IsSuccess))
                    unclassified.Add("setup: " + outcome.Outcome + "\n    " + outcome.Outcome.Sql);
            }

            for (int i = 0; i < perDatabase && run < total; i++, run++)
            {
                Statement statement = generator.GenerateStatement();
                StatementOutcome outcome = await runner.ExecuteAsync(SetupStatement.RouteOf(statement), CamusSqlPrinter.Instance.Print(statement));
                if (outcome.IsSuccess && outcome.WireError is null)
                {
                    succeeded++;
                    continue;
                }

                string key = outcome.Kind + " " + outcome.ErrorCode + ": " + Shape(outcome.ErrorMessage);
                errors[key] = errors.GetValueOrDefault(key) + 1;
                samples.TryAdd(key, outcome.Sql.Length > 400 ? outcome.Sql[..400] + " …" : outcome.Sql);
                if (ErrorClassifier.Classify(outcome) == ErrorVerdict.Finding)
                    unclassified.Add(outcome + "\n    " + outcome.Sql);
            }

            await engine.DropDatabaseAsync(database.DatabaseName);
        }

        double rate = 100.0 * succeeded / run;
        string report = $"{succeeded} of {run} statements ({rate:0.0} %) ran with no error.\n"
            + string.Join("\n", errors.OrderByDescending(kv => kv.Value).Take(25).Select(kv => $"  {kv.Value,5}x {kv.Key}\n         e.g. {samples[kv.Key]}"))
            + (unclassified.Count > 0 ? $"\nfindings ({unclassified.Count}):\n  " + string.Join("\n  ", unclassified.Take(20)) : "");
        TestContext.Progress.WriteLine(report);
        Assert.That(rate, Is.GreaterThanOrEqualTo(90.0), report);
        // A defect is a failure on its own, whatever the rate: a few internal-code outcomes in 10,000
        // statements would not move the rate, and must not hide in it.
        Assert.That(unclassified, Is.Empty, report);
    }

    private static string Shape(string? message)
    {
        if (string.IsNullOrEmpty(message))
            return "";
        string text = System.Text.RegularExpressions.Regex.Replace(message, "'[^']*'", "'…'");
        text = System.Text.RegularExpressions.Regex.Replace(text, "\\d+", "N");
        return text.Length > 140 ? text[..140] : text;
    }
}
