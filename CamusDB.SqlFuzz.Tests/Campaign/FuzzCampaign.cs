/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;

using CamusDB.SqlFuzz.Tests.Engine;
using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Oracles;
using CamusDB.SqlFuzz.Tests.Printing;
using CamusDB.SqlFuzz.Tests.Runner;
using CamusDB.SqlFuzz.Tests.Shrinking;

namespace CamusDB.SqlFuzz.Tests.Campaign;

/// <summary>The result of one oracle campaign.</summary>
internal sealed record CampaignResult(OracleKind Oracle, FuzzSettings Settings, OracleStats Stats, IReadOnlyList<Finding> Findings, IReadOnlyList<string> FindingFiles, long DistinctSignatures, string Report);

/// <summary>
/// Runs one oracle for a time budget, round after round. Each round:
/// <list type="number">
/// <item>derives its seed from the base seed and the round number, so one round replays alone
/// (<c>SQLFUZZ_ROUND</c>);</item>
/// <item>creates a new database, generates a schema and data, and runs ANALYZE on every table in half of
/// the rounds, so the planner works with statistics and MCV lists in some rounds and with its defaults in
/// the others. In some plan-equivalence rounds one table is larger, so the planner meets the sizes at
/// which it picks a merge join;</item>
/// <item>classifies the outcome of every setup statement: an outcome that is a finding of the
/// error-classification rule (an internal code, an unclassified exception, a hang) is a finding with the
/// setup before it, a refused CREATE TABLE or CREATE INDEX is a finding (<see cref="SetupDdlCheck"/>),
/// and a refused data load is counted in the report;</item>
/// <item>runs the oracle's checks;</item>
/// <item>drops the database.</item>
/// </list>
/// <para><b>Evidence first.</b> The first example of each new finding signature is written to the
/// output folder as soon as it is found, before any shrinking, so a later hang or a stopped CI job
/// cannot lose it. After the budget, each distinct finding (up to <see cref="FuzzSettings.MaxFindings"/>)
/// is shrunk and written again in its reduced form. A hang makes the engine unusable: the campaign
/// starts a new engine for the next round, and after a shrink that left an operation running.</para>
/// </summary>
internal sealed class FuzzCampaign
{
    /// <summary>The count of original findings written at once; more distinct signatures are only counted.</summary>
    public const int MaxOriginalFindings = 25;

    private readonly FuzzSettings settings;
    private readonly GeneratorProfile profile;
    private readonly Action<string> log;

    public FuzzCampaign(FuzzSettings settings, GeneratorProfile? profile = null, Action<string>? log = null)
    {
        this.settings = settings;
        this.profile = profile ?? GeneratorProfile.CamusDB;
        this.log = log ?? (_ => { });
    }

    public static IOracle Create(OracleKind kind) => kind switch
    {
        OracleKind.Tlp => new TlpOracle(),
        OracleKind.NoRec => new NoRecOracle(),
        OracleKind.PlanEquivalence => new PlanEquivalenceOracle(),
        OracleKind.ErrorClassification => new ErrorOracle(),
        _ => new DmlIntegrityOracle(),
    };

    /// <summary>Checks per round: enough to pay for the setup of a round, few enough that many schemas are seen. For plan equivalence it is the count of queries; each query has many checks.</summary>
    public static int ChecksPerRound(OracleKind kind) => kind switch
    {
        OracleKind.ErrorClassification => 60,
        OracleKind.PlanEquivalence => 4,
        OracleKind.DmlIntegrity => 8,
        _ => 25,
    };

    public async Task<CampaignResult> RunAsync(OracleKind kind)
    {
        IOracle oracle = Create(kind);
        OracleStats stats = new();
        ISqlPrinter printer = CamusSqlPrinter.Instance;
        Stopwatch watch = Stopwatch.StartNew();

        Dictionary<string, Finding> distinct = new(StringComparer.Ordinal);
        Dictionary<string, long> signatureCounts = new(StringComparer.Ordinal);
        List<string> originalFiles = new();

        FuzzEngine engine = await FuzzEngine.StartAsync().ConfigureAwait(false);
        try
        {
            long round = settings.ReplayRound ?? 0;
            while (true)
            {
                if (settings.ReplayRound is null && watch.Elapsed >= settings.Budget)
                    break;

                ulong roundSeed = FuzzRandom.DeriveSeed(settings.BaseSeed, round);
                (List<CheckFailure> failures, bool poisoned) = await RunRoundAsync(engine, oracle, stats, roundSeed, watch).ConfigureAwait(false);
                stats.Rounds++;

                foreach (CheckFailure failure in failures)
                {
                    signatureCounts[failure.Result.Signature] = signatureCounts.GetValueOrDefault(failure.Result.Signature) + 1;
                    if (distinct.ContainsKey(failure.Result.Signature))
                        continue;
                    Finding finding = new(kind, failure.Result.Signature, failure.Result.Detail,
                        settings.BaseSeed, round, roundSeed, failure.Setup, failure.Check, profile.Name)
                    {
                        Plans = failure.Plans,
                        OriginalSetupCount = failure.Setup.Count,
                    };
                    distinct[failure.Result.Signature] = finding;
                    log($"[{kind}] finding {failure.Result.Signature} in round {round}: {FirstLine(failure.Result.Detail)}");
                    if (originalFiles.Count < MaxOriginalFindings)
                        originalFiles.Add(FindingWriter.Write(settings.OutputDirectory, finding, printer, original: true));
                }

                if (poisoned)
                {
                    log($"[{kind}] a statement hung in round {round}; starting a new engine");
                    engine = await ReplaceAsync(engine).ConfigureAwait(false);
                }

                if (settings.ReplayRound is not null)
                    break;
                round++;
            }

            List<Finding> shrunk = new();
            List<string> files = new();
            Shrinker shrinker = new(engine, settings.StatementTimeout, printer);
            foreach (Finding finding in distinct.Values.Take(settings.MaxFindings))
            {
                Finding reduced = await shrinker.ShrinkAsync(finding, settings.ShrinkBudget).ConfigureAwait(false);
                string status = reduced.Reproduced switch { true => "", false => " (did not reproduce on replay)", null => " (not replayed)" };
                log($"[{kind}] shrunk {finding.Signature}: {finding.Setup.Count} -> {reduced.Setup.Count} setup statements in {shrinker.Attempts} replays{status}");
                shrunk.Add(reduced);
                files.Add(FindingWriter.Write(settings.OutputDirectory, reduced, printer));

                if (shrinker.Poisoned)
                {
                    log($"[{kind}] a replay hung while shrinking {finding.Signature}; starting a new engine");
                    engine = await ReplaceAsync(engine).ConfigureAwait(false);
                    shrinker = new Shrinker(engine, settings.StatementTimeout, printer);
                }
            }

            string report = Report(kind, oracle, stats, signatureCounts, shrunk, files, originalFiles, watch.Elapsed);
            return new CampaignResult(kind, settings, stats, shrunk, files, distinct.Count, report);
        }
        finally
        {
            await engine.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>A new engine in place of one an operation still runs on. The old one is disposed in the background: its stuck operation may never end.</summary>
    private static async Task<FuzzEngine> ReplaceAsync(FuzzEngine engine)
    {
        _ = engine.DisposeAsync().AsTask().ContinueWith(static t => _ = t.Exception, TaskScheduler.Default);
        return await FuzzEngine.StartAsync().ConfigureAwait(false);
    }

    private async Task<(List<CheckFailure> Failures, bool Poisoned)> RunRoundAsync(FuzzEngine engine, IOracle oracle, OracleStats stats, ulong roundSeed, Stopwatch watch)
    {
        FuzzRandom random = new(roundSeed);
        StatementGenerator generator = new(random, profile);
        ISqlPrinter printer = CamusSqlPrinter.Instance;

        List<SetupStatement> setup = generator.GenerateSchema().Select(SetupStatement.Of).ToList();
        string? sized = oracle.Kind == OracleKind.PlanEquivalence && random.Chance(0.3) ? random.Pick(generator.State.Tables).Name : null;
        setup.AddRange(generator.GenerateData(sized).Select(SetupStatement.Of));
        if (profile.Allows(StatementFeature.Analyze) && random.NextBool())
            setup.AddRange(generator.State.Tables.Select(t => SetupStatement.Of(new AnalyzeStmt(t.Name))));

        FuzzArm database = await engine.CreateDatabaseAsync().ConfigureAwait(false);
        List<FuzzArm> arms = new();
        List<SqlRunner> runners = new();
        List<CheckFailure> failures = new();
        try
        {
            SqlRunner runner = new(database, settings.StatementTimeout);
            runners.Add(runner);
            bool Stop() => runners.Any(r => r.Poisoned) || (settings.ReplayRound is null && watch.Elapsed >= settings.Budget);

            CheckContext context = new(runner, async spec =>
            {
                FuzzArm arm = await engine.CreateArmAsync(database.DatabaseName, spec.Name, spec.Configure, spec.Prepare).ConfigureAwait(false);
                arms.Add(arm);
                SqlRunner armRunner = new(arm, settings.StatementTimeout);
                runners.Add(armRunner);
                return armRunner;
            }, printer);

            List<SetupOutcome> outcomes = await SetupRunner.RunAsync(runner, setup, printer, Stop).ConfigureAwait(false);
            bool usable = ClassifySetup(outcomes, setup, stats, failures);

            if (usable && !Stop())
            {
                RoundContext round = new(generator, context, setup, stats, ChecksPerRound(oracle.Kind), Stop, log);
                await oracle.RunRoundAsync(round).ConfigureAwait(false);
                failures.AddRange(round.Failures);
            }

            return (failures, runners.Any(r => r.Poisoned));
        }
        finally
        {
            stats.Statements += runners.Sum(r => r.StatementsExecuted);
            if (!runners.Any(r => r.Poisoned))
            {
                foreach (FuzzArm arm in arms)
                    await FuzzEngine.ReleaseArmAsync(arm).ConfigureAwait(false);
                await engine.DropDatabaseAsync(database.DatabaseName).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Classifies the outcome of each setup statement. Returns false when the round cannot run its
    /// checks: a statement of the schema was refused, or the setup did not finish.
    /// </summary>
    internal static bool ClassifySetup(List<SetupOutcome> outcomes, List<SetupStatement> setup, OracleStats stats, List<CheckFailure> failures)
    {
        bool usable = outcomes.Count == setup.Count;
        foreach (SetupOutcome outcome in outcomes)
        {
            IReadOnlyList<SetupStatement> before = setup.Take(outcome.Index).ToList();
            ErrorVerdict verdict = ErrorClassifier.Classify(outcome.Outcome);
            if (verdict == ErrorVerdict.Finding)
            {
                OracleCheck check = outcome.Statement.Route == StatementRoute.Ddl ? new SetupDdlCheck(outcome.Statement.Statement) : new ErrorCheck(outcome.Statement.Statement);
                CheckResult result = CheckResult.FromBadOutcome(outcome.Outcome);
                stats.Record("setup:" + outcome.Statement.Route.ToString().ToLowerInvariant(), result);
                failures.Add(new CheckFailure(check, result, before, []));
                if (outcome.Statement.Route == StatementRoute.Ddl)
                    usable = false;
                continue;
            }
            if (verdict == ErrorVerdict.KnownIssue)
                stats.Known++;
            if (outcome.Outcome.IsSuccess)
                continue;

            if (outcome.Statement.Route == StatementRoute.Ddl)
            {
                CheckResult result = CheckResult.Fail("setup:ddl-refused:" + outcome.Outcome.ErrorCode, $"{outcome.Outcome}\n  in: {outcome.Outcome.Sql}");
                stats.Record("setup:ddl", result);
                failures.Add(new CheckFailure(new SetupDdlCheck(outcome.Statement.Statement), result, before, []));
                usable = false;
            }
            else
            {
                string key = outcome.Outcome.ErrorCode + " (" + outcome.Statement.Route.ToString().ToLowerInvariant() + ")";
                stats.SetupRefusals[key] = stats.SetupRefusals.GetValueOrDefault(key) + 1;
            }
        }
        return usable;
    }

    private string Report(OracleKind kind, IOracle oracle, OracleStats stats, Dictionary<string, long> signatures, List<Finding> findings, List<string> files, List<string> originals, TimeSpan elapsed)
    {
        System.Text.StringBuilder sb = new();
        sb.AppendLine($"SQL fuzzer: oracle {kind}, profile {profile.Name}, base seed {settings.BaseSeed} (from {settings.SeedSource}){(settings.ReplayRound is { } r ? $", replay of round {r}" : "")}, {elapsed.TotalSeconds:0.0} s");
        sb.AppendLine($"  build {Environment.GetEnvironmentVariable("GITHUB_SHA") ?? "local"}; engine options: shipped defaults with {Shrinking.TestWriter.BaseOptions}; statement limit {settings.StatementTimeout.TotalSeconds:0} s");
        if (kind == OracleKind.PlanEquivalence)
            sb.AppendLine("  arms: " + string.Join("; ", PlanEquivalenceOracle.Arms.Select(a => $"{a.Name} ({string.Join(", ", new[] { a.OptionsText, a.PrepareText is null ? "" : string.Format(System.Globalization.CultureInfo.InvariantCulture, a.PrepareText, "executor") }.Where(t => t.Length > 0))})")));
        sb.Append(stats.Report());
        if (oracle is NoRecOracle norec && norec.PlanSamples > 0)
            sb.AppendLine($"  EXPLAIN samples: {norec.PlanSamples}, the two forms had different access paths in {norec.PlanSamplesDifferent}");
        if (oracle is PlanEquivalenceOracle plan && plan.PlansCompared > 0)
        {
            sb.AppendLine($"  passed checks compared with EXPLAIN: {plan.PlansCompared}; the second plan had another access path in {plan.PlansDifferent}");
            sb.AppendLine("  operators of the second plans: " + string.Join(", ", plan.SecondPlanOperators.OrderByDescending(kv => kv.Value).Select(kv => $"{kv.Key} {kv.Value}")));
        }
        if (signatures.Count > 0)
        {
            sb.AppendLine("  findings by signature:");
            foreach ((string signature, long count) in signatures.OrderByDescending(kv => kv.Value))
                sb.AppendLine($"    {count,5}x {signature}");
        }
        foreach (string file in originals)
            sb.AppendLine($"  original finding: {file}");
        for (int i = 0; i < files.Count; i++)
            sb.AppendLine($"  regression test: {files[i]}{(findings[i].Reproduced == false ? " (did not reproduce; original statements)" : "")}");
        return sb.ToString();
    }

    private static string FirstLine(string text)
    {
        string line = text.Split('\n')[0];
        return line.Length > 200 ? line[..200] + " …" : line;
    }
}
