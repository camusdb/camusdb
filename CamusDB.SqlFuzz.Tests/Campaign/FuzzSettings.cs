/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;

using CamusDB.SqlFuzz.Tests.Oracles;

namespace CamusDB.SqlFuzz.Tests.Campaign;

/// <summary>
/// The settings of a fuzz campaign, read from environment variables so a CI job and a person at a
/// terminal set them the same way. See <c>docs/sql-fuzzer.md</c> for the table.
///
/// <list type="bullet">
/// <item><c>SQLFUZZ_SEED</c>: the base seed. Without it, the seed comes from <c>GITHUB_RUN_NUMBER</c>
/// (a rotating seed in CI), else from the clock. The seed is always printed.</item>
/// <item><c>SQLFUZZ_ROUND</c>: replay mode. Run only this round of the base seed, then stop.</item>
/// <item><c>SQLFUZZ_ORACLE</c>: in replay mode, the oracle to run (<c>tlp</c>, <c>norec</c>,
/// <c>plan</c>, <c>error</c>, <c>dml</c>); the campaign tests of the other oracles do nothing.</item>
/// <item><c>SQLFUZZ_BUDGET_SECONDS</c>: the time budget of each oracle campaign. Default 30.</item>
/// <item><c>SQLFUZZ_SHRINK_SECONDS</c>: the time to shrink one finding. Default 60.</item>
/// <item><c>SQLFUZZ_MAX_FINDINGS</c>: the distinct findings to shrink and print for each oracle. Default 3.</item>
/// <item><c>SQLFUZZ_STATEMENT_TIMEOUT_SECONDS</c>: the time limit of one statement (the hang oracle). Default 10.</item>
/// <item><c>SQLFUZZ_OUTPUT_DIR</c>: where finding files go. Default <c>sqlfuzz-findings</c> in the test output folder.</item>
/// </list>
/// </summary>
internal sealed record FuzzSettings
{
    public ulong BaseSeed { get; init; }

    /// <summary>Where the seed came from, for the report: "SQLFUZZ_SEED", "GITHUB_RUN_NUMBER" or "clock".</summary>
    public string SeedSource { get; init; } = "clock";

    public long? ReplayRound { get; init; }

    public OracleKind? ReplayOracle { get; init; }

    public TimeSpan Budget { get; init; } = TimeSpan.FromSeconds(30);

    public TimeSpan ShrinkBudget { get; init; } = TimeSpan.FromSeconds(60);

    public int MaxFindings { get; init; } = 3;

    public TimeSpan StatementTimeout { get; init; } = TimeSpan.FromSeconds(10);

    public string OutputDirectory { get; init; } = "sqlfuzz-findings";

    public static FuzzSettings FromEnvironment(string defaultOutputDirectory)
    {
        ulong seed;
        string source;
        if (TryULong("SQLFUZZ_SEED", out ulong explicitSeed))
        {
            seed = explicitSeed;
            source = "SQLFUZZ_SEED";
        }
        else if (TryULong("GITHUB_RUN_NUMBER", out ulong runNumber))
        {
            seed = runNumber * 1_000_003UL + (TryULong("GITHUB_RUN_ATTEMPT", out ulong attempt) ? attempt : 0);
            source = "GITHUB_RUN_NUMBER";
        }
        else
        {
            seed = (ulong)DateTime.UtcNow.Ticks;
            source = "clock";
        }

        return new FuzzSettings
        {
            BaseSeed = seed,
            SeedSource = source,
            ReplayRound = TryULong("SQLFUZZ_ROUND", out ulong round) ? (long)round : null,
            ReplayOracle = ParseOracle(Environment.GetEnvironmentVariable("SQLFUZZ_ORACLE")),
            Budget = TimeSpan.FromSeconds(TryULong("SQLFUZZ_BUDGET_SECONDS", out ulong budget) ? budget : 30),
            ShrinkBudget = TimeSpan.FromSeconds(TryULong("SQLFUZZ_SHRINK_SECONDS", out ulong shrink) ? shrink : 60),
            MaxFindings = TryULong("SQLFUZZ_MAX_FINDINGS", out ulong max) ? (int)max : 3,
            StatementTimeout = TimeSpan.FromSeconds(TryULong("SQLFUZZ_STATEMENT_TIMEOUT_SECONDS", out ulong timeout) ? timeout : 10),
            OutputDirectory = Environment.GetEnvironmentVariable("SQLFUZZ_OUTPUT_DIR") is { Length: > 0 } dir ? dir : defaultOutputDirectory,
        };
    }

    public static OracleKind? ParseOracle(string? name) => name?.Trim().ToLowerInvariant() switch
    {
        "tlp" => OracleKind.Tlp,
        "norec" => OracleKind.NoRec,
        "plan" or "plan-equivalence" or "planequivalence" => OracleKind.PlanEquivalence,
        "error" or "errors" or "error-classification" => OracleKind.ErrorClassification,
        "dml" or "dml-integrity" or "integrity" => OracleKind.DmlIntegrity,
        _ => null,
    };

    /// <summary>The value of <c>SQLFUZZ_ORACLE</c> that selects <paramref name="kind"/>.</summary>
    public static string OracleName(OracleKind kind) => kind switch
    {
        OracleKind.Tlp => "tlp",
        OracleKind.NoRec => "norec",
        OracleKind.PlanEquivalence => "plan",
        OracleKind.ErrorClassification => "error",
        _ => "dml",
    };

    private static bool TryULong(string name, out ulong value) =>
        ulong.TryParse(Environment.GetEnvironmentVariable(name), NumberStyles.Integer, CultureInfo.InvariantCulture, out value);
}
