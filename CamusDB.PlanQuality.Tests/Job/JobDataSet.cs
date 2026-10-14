/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

using CamusDB.PlanQuality.Tests.Corpus;
using CamusDB.PlanQuality.Tests.Engine;

namespace CamusDB.PlanQuality.Tests.Job;

/// <summary>
/// The Join Order Benchmark (JOB) data set, as <c>scripts/plan-quality/job/prepare.sh</c> leaves it
/// in a work directory:
/// <list type="bullet">
///   <item><c>bench/data/schema.sql</c>, <c>bench/data/fkindexes.sql</c> and
///   <c>bench/queries/*.sql</c>, downloaded at a pinned commit. The JOB repository has no license,
///   so none of it is committed.</item>
///   <item><c>data/data/*.tsv</c> and <c>data/types.tsv</c>: synthetic IMDB rows from the seeded
///   generator, with string values built from the query literals so the filters select rows.</item>
///   <item><c>data/sqlite_results.json</c>: the one result row of each query on a SQLite copy of the
///   same rows, the oracle.</item>
/// </list>
/// </summary>
internal sealed partial class JobDataSet
{
    private const int BatchSize = 1000;

    private JobDataSet(string benchDirectory, string dataDirectory)
    {
        BenchDirectory = benchDirectory;
        DataDirectory = dataDirectory;
    }

    public string BenchDirectory { get; }

    public string DataDirectory { get; }

    public static JobDataSet Open(string workDirectory)
    {
        JobDataSet set = new(Path.Combine(workDirectory, "bench"), Path.Combine(workDirectory, "data"));
        foreach (string required in new[]
                 {
                     Path.Combine(set.BenchDirectory, "data", "schema.sql"),
                     Path.Combine(set.BenchDirectory, "queries", "1a.sql"),
                     Path.Combine(set.DataDirectory, "types.tsv"),
                     Path.Combine(set.DataDirectory, "sqlite_results.json"),
                 })
        {
            if (!File.Exists(required))
                throw new FileNotFoundException($"the JOB work directory is not prepared; run scripts/plan-quality/job/prepare.sh first (missing {required})");
        }
        return set;
    }

    [GeneratedRegex(@"CREATE TABLE \w+ \(.*?\);", RegexOptions.Singleline)]
    private static partial Regex CreateTable();

    [GeneratedRegex(@"character varying\(\d+\)")]
    private static partial Regex CharacterVarying();

    [GeneratedRegex(@"\binteger\b")]
    private static partial Regex Integer();

    [GeneratedRegex(@"\btext\b")]
    private static partial Regex Text();

    [GeneratedRegex(@"\b\w+ AS \w+\b")]
    private static partial Regex TableAlias();

    /// <summary>
    /// Creates the schema, the foreign-key indexes and the rows, then runs <c>ANALYZE</c> on each
    /// table. A <c>CREATE TABLE</c> that CamusDB does not parse as written (the PostgreSQL types) is
    /// retried with the types converted.
    /// </summary>
    public async Task LoadAsync(PlanQualityEngine engine)
    {
        string schema = File.ReadAllText(Path.Combine(BenchDirectory, "data", "schema.sql"));
        List<string> tables = new();
        foreach (Match match in CreateTable().Matches(schema))
        {
            string sql = match.Value.TrimEnd(';');
            tables.Add(sql.Split(' ', 4)[2]);
            try
            {
                await engine.ExecuteDdlAsync(sql).ConfigureAwait(false);
            }
            catch (Exception)
            {
                string converted = Text().Replace(Integer().Replace(CharacterVarying().Replace(sql, "string"), "int64"), "string");
                await engine.ExecuteDdlAsync(converted).ConfigureAwait(false);
            }
        }

        foreach (string line in File.ReadAllLines(Path.Combine(BenchDirectory, "data", "fkindexes.sql")))
        {
            if (!string.IsNullOrWhiteSpace(line))
                await engine.ExecuteDdlAsync(line.Trim().TrimEnd(';')).ConfigureAwait(false);
        }

        Dictionary<(string Table, string Column), string> types = File.ReadAllLines(Path.Combine(DataDirectory, "types.tsv"))
            .Select(l => l.Split('\t'))
            .ToDictionary(p => (p[0], p[1]), p => p[2]);

        foreach (string table in tables)
        {
            Stopwatch tableTime = Stopwatch.StartNew();
            long tableRows = 0;
            string file = Path.Combine(DataDirectory, "data", table + ".tsv");
            using StreamReader reader = new(file);
            string[] header = reader.ReadLine()!.Split('\t');
            List<Dictionary<string, ColumnValue>> batch = new(BatchSize);

            for (string? line = reader.ReadLine(); line is not null; line = reader.ReadLine())
            {
                string[] cells = line.Split('\t');
                Dictionary<string, ColumnValue> row = new(header.Length, StringComparer.Ordinal);
                for (int i = 0; i < header.Length; i++)
                {
                    if (cells[i] == "\\N")
                        continue;

                    row[header[i]] = types[(table, header[i])] == "int"
                        ? new ColumnValue(ColumnType.Integer64, long.Parse(cells[i], CultureInfo.InvariantCulture))
                        : new ColumnValue(ColumnType.String, cells[i]);
                }

                batch.Add(row);
                tableRows++;
                if (batch.Count == BatchSize)
                {
                    await engine.InsertAsync(table, batch).ConfigureAwait(false);
                    batch = new(BatchSize);
                }
            }

            if (batch.Count > 0)
                await engine.InsertAsync(table, batch).ConfigureAwait(false);

            TestContext.Progress.WriteLine(FormattableString.Invariant($"  {table}: {tableRows} rows in {tableTime.Elapsed.TotalSeconds:0.0} s"));
        }

        foreach (string table in tables)
            await engine.AnalyzeAsync(table).ConfigureAwait(false);
    }

    /// <summary>
    /// The 113 queries in benchmark order (1a, 1b, …, 33c), tagged <c>job</c>, <c>tables:N</c> and,
    /// above the 12-table limit of the join-order search, <c>over-dp-limit</c>.
    /// </summary>
    public List<CorpusQuery> Queries()
    {
        List<CorpusQuery> queries = new();
        foreach (string file in Directory.GetFiles(Path.Combine(BenchDirectory, "queries"), "*.sql")
                     .OrderBy(f => int.Parse(Regex.Match(Path.GetFileName(f), @"^\d+").Value, CultureInfo.InvariantCulture))
                     .ThenBy(f => f, StringComparer.Ordinal))
        {
            string sql = File.ReadAllText(file).Trim().TrimEnd(';');
            string from = sql.Split("WHERE", 2, StringSplitOptions.None)[0];
            int tables = TableAlias().Matches(from).Count;

            List<string> tags = ["job", "join", $"tables:{tables}"];
            if (tables > 12)
                tags.Add("over-dp-limit");

            queries.Add(new CorpusQuery(Path.GetFileNameWithoutExtension(file), tags, sql, Path.GetFileName(file)));
        }
        return queries;
    }

    /// <summary>The SQLite result row of each query, rendered as the harness renders CamusDB's row; null for an SQL NULL.</summary>
    public Dictionary<string, List<string?>> OracleRows()
    {
        using JsonDocument document = JsonDocument.Parse(File.ReadAllText(Path.Combine(DataDirectory, "sqlite_results.json")));
        Dictionary<string, List<string?>> rows = new(StringComparer.Ordinal);

        foreach (JsonProperty query in document.RootElement.EnumerateObject())
        {
            if (!query.Value.TryGetProperty("row", out JsonElement row))
                continue;

            rows[query.Name] = row.EnumerateArray().Select(v => v.ValueKind switch
            {
                JsonValueKind.Null => null,
                JsonValueKind.String => v.GetString(),
                JsonValueKind.Number => v.GetRawText(),
                _ => v.GetRawText(),
            }).ToList();
        }

        return rows;
    }

    /// <summary>A CamusDB value in the oracle's text form.</summary>
    public static string? OracleText(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => null,
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Float64 => value.FloatValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Bool => value.BoolValue ? "1" : "0",
        _ => value.StrValue ?? value.ToString(),
    };
}
