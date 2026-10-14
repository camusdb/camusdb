/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;

using CamusDB.PlanQuality.Tests.Metrics;

namespace CamusDB.PlanQuality.Tests.Baselines;

/// <summary>
/// The baseline entry of one query: the numbers that repeat between runs. Timings are not here,
/// because they change on every run and would make every diff noisy.
/// </summary>
internal sealed record BaselineEntry(
    string Id,
    IReadOnlyList<string> Tags,
    long Rows,
    string ResultHash,
    long Work,
    long StatementRowsRead,
    double QErrorMedian,
    double QErrorMax,
    string? JoinOrder,
    long JoinStepsCosted,
    IReadOnlyList<string> Plan)
{
    /// <summary>The entry for a measurement. The q-errors are rounded to 2 decimals, so the file does not change on noise in the last digits.</summary>
    public static BaselineEntry From(QueryMeasurement measurement) => new(
        measurement.Id,
        measurement.Tags,
        measurement.ResultRows,
        measurement.ResultHash,
        measurement.Work,
        measurement.StatementRowsRead,
        Math.Round(measurement.QErrorMedian, 2),
        Math.Round(measurement.QErrorMax, 2),
        measurement.JoinOrder,
        measurement.JoinStepsCosted,
        measurement.Plan);
}

/// <summary>
/// The baseline of one tier at one scale, stored as JSON next to the corpus. The data set, the seed
/// and the scale are part of the file: a baseline compares only with a run on the same data.
///
/// <para><b>The file is written so that a diff shows only the queries that changed.</b> Entries are
/// sorted by id, properties have a fixed order, the plan is one JSON string for each node, and the
/// file ends with a newline. Only the explicit update switch writes it (see
/// <see cref="Harness.HarnessSettings.UpdateBaseline"/>).</para>
/// </summary>
internal sealed record BaselineFile(
    string Tier,
    string DataSet,
    string Seed,
    string Scale,
    IReadOnlyList<BaselineEntry> Queries)
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
        // Plans hold quotes, '<' and '>'; keep them readable in the diff.
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    public static BaselineFile Create(string tier, string dataSet, string seed, string scale, IEnumerable<QueryMeasurement> measurements) =>
        new(tier, dataSet, seed, scale,
            measurements.Where(m => m.Error is null)
                .Select(BaselineEntry.From)
                .OrderBy(e => e.Id, StringComparer.Ordinal)
                .ToList());

    /// <summary>The baseline at <paramref name="path"/>, or null when there is no file yet.</summary>
    public static BaselineFile? TryLoad(string path)
    {
        if (!File.Exists(path))
            return null;

        return JsonSerializer.Deserialize<BaselineFile>(File.ReadAllText(path), JsonOptions)
            ?? throw new InvalidDataException($"empty baseline file: {path}");
    }

    public string ToJson()
    {
        string json = JsonSerializer.Serialize(this, JsonOptions);
        return json.Replace("\r\n", "\n", StringComparison.Ordinal) + "\n";
    }

    public void Save(string path)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, ToJson(), new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
    }
}
