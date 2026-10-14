/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace CamusDB.PlanQuality.Tests.Corpus;

/// <summary>One query of a corpus: a stable id, its tags, and the SQL text.</summary>
/// <param name="Id">The key of the query in the baseline. Renaming a query is a removal plus an addition.</param>
/// <param name="Tags">The shape tags. The report groups regressions by them; they do not change how the query runs.</param>
/// <param name="Sql">The statement, without a trailing semicolon.</param>
/// <param name="Source">The file and line of the query, for error messages.</param>
internal sealed record CorpusQuery(string Id, IReadOnlyList<string> Tags, string Sql, string Source);

/// <summary>
/// Reads a corpus from <c>.sql</c> files. A file holds any number of queries, and each one starts
/// with a header:
/// <code>
/// -- query: join-star-orders-5
/// -- tags: join, join:star, tables:5
/// SELECT ...;
/// </code>
/// Other lines that start with <c>--</c> are comments. An id must be unique in the corpus, and a
/// query must have at least one tag, so the report can group it. Files are read in ordinal name
/// order, so the corpus has the same order on every machine.
/// </summary>
internal static class CorpusReader
{
    private const string QueryHeader = "-- query:";

    private const string TagsHeader = "-- tags:";

    public static List<CorpusQuery> ReadDirectory(string directory)
    {
        if (!Directory.Exists(directory))
            throw new DirectoryNotFoundException($"corpus directory not found: {directory}");

        List<CorpusQuery> queries = new();
        foreach (string file in Directory.GetFiles(directory, "*.sql").OrderBy(f => f, StringComparer.Ordinal))
            queries.AddRange(Parse(File.ReadAllText(file), Path.GetFileName(file)));

        HashSet<string> ids = new(StringComparer.Ordinal);
        foreach (CorpusQuery query in queries)
        {
            if (!ids.Add(query.Id))
                throw new InvalidDataException($"duplicate query id '{query.Id}' at {query.Source}");
        }

        return queries;
    }

    public static List<CorpusQuery> Parse(string text, string fileName)
    {
        List<CorpusQuery> queries = new();
        string[] lines = text.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');

        string? id = null;
        List<string> tags = new();
        StringBuilder sql = new();
        int startLine = 0;

        void Flush()
        {
            if (id is null)
            {
                if (sql.ToString().Trim().Length > 0)
                    throw new InvalidDataException($"{fileName}: SQL before the first '{QueryHeader}' header");
                return;
            }

            string statement = sql.ToString().Trim().TrimEnd(';').Trim();
            string source = $"{fileName}:{startLine}";

            if (statement.Length == 0)
                throw new InvalidDataException($"query '{id}' at {source} has no SQL");
            if (tags.Count == 0)
                throw new InvalidDataException($"query '{id}' at {source} has no '{TagsHeader}' line");

            queries.Add(new CorpusQuery(id, tags.ToArray(), statement, source));
        }

        for (int i = 0; i < lines.Length; i++)
        {
            string trimmed = lines[i].Trim();

            if (trimmed.StartsWith(QueryHeader, StringComparison.Ordinal))
            {
                Flush();
                id = trimmed[QueryHeader.Length..].Trim();
                if (id.Length == 0)
                    throw new InvalidDataException($"{fileName}:{i + 1}: empty query id");
                tags = new();
                sql.Clear();
                startLine = i + 1;
                continue;
            }

            if (trimmed.StartsWith(TagsHeader, StringComparison.Ordinal))
            {
                foreach (string tag in trimmed[TagsHeader.Length..].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                    tags.Add(tag);
                continue;
            }

            if (trimmed.StartsWith("--", StringComparison.Ordinal))
                continue;

            sql.Append(lines[i]).Append('\n');
        }

        Flush();
        return queries;
    }
}
