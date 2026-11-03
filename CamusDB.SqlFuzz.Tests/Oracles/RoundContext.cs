/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Runner;

namespace CamusDB.SqlFuzz.Tests.Oracles;

/// <summary>One oracle: it makes checks for a round and runs them.</summary>
internal interface IOracle
{
    OracleKind Kind { get; }

    /// <summary>Runs checks on the round's database until <see cref="RoundContext.ShouldStop"/> or the round's check count.</summary>
    Task RunRoundAsync(RoundContext round);
}

/// <summary>Execution counts of one oracle, kept per variant, for the CI report.</summary>
internal sealed class OracleStats
{
    private readonly ConcurrentDictionary<string, long[]> variants = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> skipReasons = new(StringComparer.Ordinal);

    public long Rounds { get; set; }

    /// <summary>Every SQL statement the campaign's runners sent: setup, checks and snapshots. Integrity scans are not statements and are not counted.</summary>
    public long Statements { get; set; }

    /// <summary>For the error-classification oracle: statements that ran with no error or an expected error, and statements with an error that is not on the list.</summary>
    public long Accepted { get; set; }

    public long Unlisted { get; set; }

    /// <summary>Statements whose outcome matched a known issue.</summary>
    public long Known { get; set; }

    public Dictionary<string, long> UnlistedCodes { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Setup statements of the rounds (the initial data load and ANALYZE) that the engine refused with
    /// a specific code, by code. Such a refusal is not a defect, but it leaves less data for the checks:
    /// a table whose load failed can be empty, and every relation holds on an empty table.
    /// </summary>
    public Dictionary<string, long> SetupRefusals { get; } = new(StringComparer.Ordinal);

    /// <summary>Checks that compared results (passed or failed), as opposed to skipped ones.</summary>
    public long Completed => Passed + Failed;

    /// <summary>The attempts and the completed comparisons of each variant.</summary>
    public IEnumerable<(string Variant, long Attempts, long Completed)> Variants =>
        variants.Select(kv => (kv.Key, kv.Value.Sum(), kv.Value[0] + kv.Value[2]));

    public void Record(string variant, CheckResult result)
    {
        long[] counts = variants.GetOrAdd(variant, _ => new long[3]);
        counts[(int)result.Status]++;
        if (result.Status == CheckStatus.Skip)
        {
            string reason = result.Detail.Length > 90 ? result.Detail[..90] : result.Detail;
            skipReasons.AddOrUpdate(reason, 1, (_, n) => n + 1);
        }
    }

    public long Checks => variants.Values.Sum(v => v.Sum());
    public long Passed => variants.Values.Sum(v => v[0]);
    public long Skipped => variants.Values.Sum(v => v[1]);
    public long Failed => variants.Values.Sum(v => v[2]);

    public string Report()
    {
        System.Text.StringBuilder sb = new();
        sb.AppendLine($"  rounds={Rounds} checks={Checks} pass={Passed} skip={Skipped} fail={Failed} statements={Statements}");
        foreach ((string variant, long[] counts) in variants.OrderBy(v => v.Key, StringComparer.Ordinal))
            sb.AppendLine($"    {variant,-28} pass={counts[0]} skip={counts[1]} fail={counts[2]}");
        if (Accepted + Unlisted + Known > 0)
        {
            sb.AppendLine($"    statements accepted={Accepted} unlisted-code={Unlisted} known-issue={Known}");
            foreach ((string code, long n) in UnlistedCodes.OrderByDescending(kv => kv.Value).Take(10))
                sb.AppendLine($"      {n,6}x {code}");
        }
        if (SetupRefusals.Count > 0)
        {
            sb.AppendLine($"    setup statements refused: {SetupRefusals.Values.Sum()}");
            foreach ((string code, long n) in SetupRefusals.OrderByDescending(kv => kv.Value).Take(6))
                sb.AppendLine($"      {n,6}x {code}");
        }
        if (!skipReasons.IsEmpty)
        {
            sb.AppendLine("    top skip reasons:");
            foreach ((string reason, long n) in skipReasons.OrderByDescending(kv => kv.Value).Take(8))
                sb.AppendLine($"      {n,6}x {reason}");
        }
        return sb.ToString();
    }
}

/// <summary>A failed check with the setup it ran after.</summary>
internal sealed record CheckFailure(OracleCheck Check, CheckResult Result, IReadOnlyList<SetupStatement> Setup, IReadOnlyList<string> Plans);

/// <summary>
/// Everything an oracle needs for one round: the generator (with the round's schema), the engine
/// through a <see cref="CheckContext"/>, and the setup statements so far. An oracle that writes appends
/// its statements to <see cref="Setup"/>, so a later failure replays the full history.
/// </summary>
internal sealed class RoundContext
{
    private readonly Func<bool> shouldStop;

    public RoundContext(StatementGenerator generator, CheckContext checks, List<SetupStatement> setup, OracleStats stats, int checkCount, Func<bool> shouldStop, Action<string>? log = null)
    {
        Generator = generator;
        Checks = checks;
        Setup = setup;
        Stats = stats;
        CheckCount = checkCount;
        this.shouldStop = shouldStop;
        Log = log ?? (_ => { });
    }

    public StatementGenerator Generator { get; }

    public CheckContext Checks { get; }

    public IStatementExecutor Engine => Checks.Engine;

    public List<SetupStatement> Setup { get; }

    public OracleStats Stats { get; }

    /// <summary>The count of checks to run in this round.</summary>
    public int CheckCount { get; }

    public Action<string> Log { get; }

    public List<CheckFailure> Failures { get; } = new();

    /// <summary>True when the campaign budget is spent or the engine hung.</summary>
    public bool ShouldStop => shouldStop();

    /// <summary>
    /// Runs a check. An operation without an outcome of its own that passed the time limit (BEGIN, an
    /// integrity scan) is a hang finding, the same as a statement that hangs. Any other exception that
    /// escapes the check is a defect of the fuzzer itself, or an engine exception the check did not
    /// expect; it becomes a finding with a <c>harness:</c> signature, so the campaign goes on and the
    /// report shows it.
    /// </summary>
    public static async Task<CheckResult> RunGuardedAsync(OracleCheck check, CheckContext context)
    {
        try
        {
            return await check.RunAsync(context).ConfigureAwait(false);
        }
        catch (StatementHangException hang)
        {
            return CheckResult.Fail("error:hang", $"hang: {hang.Message}");
        }
        catch (Exception ex)
        {
            return CheckResult.Fail("harness:" + ex.GetType().Name, $"the check threw {ex.GetType().Name}: {ex.Message}\n{ex.StackTrace}");
        }
    }

    /// <summary>
    /// Runs one check, records its result, and keeps a failure with a copy of the setup and the plans
    /// the check compares. The reads of a check that did not fail join <see cref="Setup"/> as history,
    /// so a later finding replays them; the reads of a failed check are the check itself.
    /// </summary>
    public async Task<CheckResult> RunAsync(OracleCheck check)
    {
        CheckResult result = await RunGuardedAsync(check, Checks).ConfigureAwait(false);
        IReadOnlyList<SetupStatement> reads = Checks.TakeHistory();
        Stats.Record(check.Variant, result);
        if (result.Status == CheckStatus.Fail)
        {
            IReadOnlyList<string> planText = Engine.Poisoned ? [] : await check.PlansAsync(Checks).ConfigureAwait(false);
            Checks.TakeHistory();
            Failures.Add(new CheckFailure(check, result, Setup.ToList(), planText));
        }
        else
            Setup.AddRange(reads);
        return result;
    }
}
