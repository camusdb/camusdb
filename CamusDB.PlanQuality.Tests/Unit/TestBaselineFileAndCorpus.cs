/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.PlanQuality.Tests.Baselines;
using CamusDB.PlanQuality.Tests.Corpus;
using CamusDB.PlanQuality.Tests.Harness;
using CamusDB.PlanQuality.Tests.Metrics;

namespace CamusDB.PlanQuality.Tests.Unit;

/// <summary>The baseline file format and the corpus file format.</summary>
[TestFixture]
[Category("PlanQuality")]
public sealed class TestBaselineFileAndCorpus
{
    private static QueryMeasurement M(string id, string? error = null) =>
        new(id, ["t1", "t2"], 3, "abc", 42, 21, 1.234, 7.891, "cost-based", 5, ["hash-join(on=a=b)", "table-scan(table=x)"], 9.9, 8.8, [], error);

    [Test]
    public void Baseline_RoundTripsAndSortsByIdAndLeavesOutFailedQueries()
    {
        BaselineFile file = BaselineFile.Create("small", "retail", "7", "1", [M("b"), M("a"), M("c", error: "boom")]);
        string path = Path.Combine(Path.GetTempPath(), "pq-baseline-" + Guid.NewGuid().ToString("n") + ".json");

        try
        {
            file.Save(path);
            string first = File.ReadAllText(path);

            BaselineFile loaded = BaselineFile.TryLoad(path)!;
            Assert.That(loaded.Queries.Select(q => q.Id), Is.EqualTo(new[] { "a", "b" }));
            Assert.AreEqual(1.23, loaded.Queries[0].QErrorMedian, "q-errors are rounded to 2 decimals");
            Assert.AreEqual(42, loaded.Queries[0].Work);
            Assert.AreEqual(21, loaded.Queries[0].StatementRowsRead);
            Assert.That(loaded.Queries[0].Plan, Is.EqualTo(new[] { "hash-join(on=a=b)", "table-scan(table=x)" }));

            loaded.Save(path);
            Assert.AreEqual(first, File.ReadAllText(path), "an unchanged baseline is written byte for byte the same");
            Assert.IsTrue(first.EndsWith("}\n", StringComparison.Ordinal));
            Assert.IsFalse(first.Contains("PlanningMs", StringComparison.OrdinalIgnoreCase), "timings never go into a baseline");
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Test]
    public void Baseline_MissingFile_IsNull()
    {
        Assert.IsNull(BaselineFile.TryLoad(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("n") + ".json")));
    }

    [Test]
    public void Corpus_ParsesHeadersTagsAndComments()
    {
        const string text = """
            -- a file comment
            -- query: one
            -- tags: single-table, range
            -- a query comment
            SELECT a
            FROM t;

            -- query: two
            -- tags: join
            -- tags: tables:2
            SELECT b FROM t JOIN u ON u.id = t.u_id
            """;

        List<CorpusQuery> queries = CorpusReader.Parse(text, "x.sql");

        Assert.AreEqual(2, queries.Count);
        Assert.AreEqual("one", queries[0].Id);
        Assert.That(queries[0].Tags, Is.EqualTo(new[] { "single-table", "range" }));
        Assert.AreEqual("SELECT a\nFROM t", queries[0].Sql);
        Assert.That(queries[1].Tags, Is.EqualTo(new[] { "join", "tables:2" }));
        Assert.AreEqual("x.sql:2", queries[0].Source);
    }

    [TestCase("SELECT 1;\n-- query: a\n-- tags: t\nSELECT 2", "before the first")]
    [TestCase("-- query: a\nSELECT 2", "no '-- tags:' line")]
    [TestCase("-- query: a\n-- tags: t\n", "has no SQL")]
    public void Corpus_RejectsMalformedFiles(string text, string message)
    {
        InvalidDataException ex = Assert.Throws<InvalidDataException>(() => CorpusReader.Parse(text, "bad.sql"))!;
        Assert.That(ex.Message, Does.Contain(message));
    }

    [Test]
    public void SmallCorpus_HasUniqueIdsAndTagsAndCoversEveryShapeGroup()
    {
        List<CorpusQuery> corpus = CorpusReader.ReadDirectory(HarnessSettings.CorpusDirectory("small"));
        HashSet<string> tags = corpus.SelectMany(q => q.Tags).ToHashSet(StringComparer.Ordinal);

        Assert.That(corpus.Count, Is.GreaterThanOrEqualTo(90));
        foreach (string required in new[]
                 {
                     "single-table", "point-lookup", "range", "in-list", "or", "like", "order-limit", "group-by",
                     "distinct", "descending-index", "join:chain", "join:star", "join:cycle", "join:clique",
                     "join:snowflake", "comma-join", "over-dp-limit", "left-join", "right-join", "cross-join",
                     "exists", "not-exists", "in-subquery", "not-in", "derived-table", "view", "correlated",
                     "tables:17",
                 })
        {
            Assert.That(tags, Does.Contain(required), $"no query has the tag '{required}'");
        }
    }
}
