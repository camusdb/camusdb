/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Reflection;

using NUnit.Framework;

namespace CamusDB.PlanQuality.Tests.Harness;

/// <summary>
/// The switches of a harness run, all from environment variables so that CI and a developer use the
/// same entry point (<c>dotnet test</c>):
/// <list type="table">
///   <item><term><c>PLAN_QUALITY_UPDATE_BASELINE=1</c></term><description>Write the baseline from
///   this run, after the reference check. The only way a baseline file changes.</description></item>
///   <item><term><c>PLAN_QUALITY_SCALE</c></term><description>The scale factor of the small tier
///   (default 1). Each scale has its own baseline file.</description></item>
///   <item><term><c>PLAN_QUALITY_ONLY</c></term><description>A comma list of query ids to run, for
///   a local investigation. A partial run never writes a baseline and does not report the queries
///   it left out.</description></item>
///   <item><term><c>PLAN_QUALITY_REPORT_DIR</c></term><description>Where the Markdown report and
///   the raw JSON go (default: <c>plan-quality</c> under the test output directory).</description></item>
///   <item><term><c>PLAN_QUALITY_QUERY_TIMEOUT_SEC</c></term><description>The limit for one query
///   (default 60). A query over the limit fails.</description></item>
/// </list>
/// </summary>
internal sealed record HarnessSettings(
    bool UpdateBaseline,
    int Scale,
    IReadOnlySet<string>? Only,
    string ReportDirectory,
    TimeSpan QueryTimeout)
{
    public static HarnessSettings FromEnvironment()
    {
        string? only = Environment.GetEnvironmentVariable("PLAN_QUALITY_ONLY");
        string reportDirectory = Environment.GetEnvironmentVariable("PLAN_QUALITY_REPORT_DIR")
            ?? Path.Combine(TestContext.CurrentContext.WorkDirectory, "plan-quality");

        return new HarnessSettings(
            UpdateBaseline: Environment.GetEnvironmentVariable("PLAN_QUALITY_UPDATE_BASELINE") == "1",
            Scale: int.Parse(Environment.GetEnvironmentVariable("PLAN_QUALITY_SCALE") ?? "1", CultureInfo.InvariantCulture),
            Only: string.IsNullOrWhiteSpace(only)
                ? null
                : only.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToHashSet(StringComparer.Ordinal),
            ReportDirectory: reportDirectory,
            QueryTimeout: TimeSpan.FromSeconds(int.Parse(
                Environment.GetEnvironmentVariable("PLAN_QUALITY_QUERY_TIMEOUT_SEC") ?? "60", CultureInfo.InvariantCulture)));
    }

    /// <summary>
    /// The source directory of this project, recorded at build time. The corpus and the baselines are
    /// read from the source tree, so a baseline update writes the file that goes into the commit.
    /// </summary>
    public static string ProjectDirectory { get; } =
        typeof(HarnessSettings).Assembly.GetCustomAttributes<AssemblyMetadataAttribute>()
            .Single(a => a.Key == "PlanQualityProjectDirectory").Value!;

    public static string CorpusDirectory(string tier) => Path.Combine(ProjectDirectory, "Corpus", tier);

    public static string BaselinePath(string tier, string scale) =>
        Path.Combine(ProjectDirectory, "Baselines", $"{tier}-sf{scale}.json");
}
