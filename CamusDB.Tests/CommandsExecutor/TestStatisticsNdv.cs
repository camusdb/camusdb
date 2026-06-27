
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;

using NUnit.Framework;

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Statistics;
using CamusDB.Core.Statistics.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Validates the distinct-value count (NDV) model and persistence:
///   1. ColumnNdv and KeyNdv JSON round-trip via MetaJsonContext.
///   2. TableStatistics.ColumnNdv / KeyNdv persist alongside existing fields without damage.
///   3. Old blobs without NDV fields deserialize to null (backward-compatible).
///   4. KeyTupleSignature produces the expected comma-separated string.
///   5. A uniform column reports NDV ≈ row count; a low-cardinality column reports small NDV.
///   6. Key-tuple NDV is keyed by the same signature StatisticsManager.KeyTupleSignature returns.
/// </summary>
[TestFixture]
public sealed class TestStatisticsNdv
{
    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static byte[] SerializeStats(TableStatistics s)
        => MetaJsonSerializer.Serialize(s, MetaJsonContext.Default.TableStatistics);

    private static TableStatistics? DeserializeStats(byte[] bytes)
        => MetaJsonSerializer.DeserializeCompat(bytes, MetaJsonContext.Default.TableStatistics);

    // ─────────────────────────────────────────────────────────────────────────
    // Tests
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public void ColumnNdvJsonRoundTrip()
    {
        var stats = new TableStatistics
        {
            RowCount  = 1000,
            ColumnNdv = new Dictionary<string, long> { ["age"] = 80, ["city"] = 5 },
        };

        byte[] bytes = SerializeStats(stats);
        TableStatistics? rt = DeserializeStats(bytes);

        Assert.That(rt,                          Is.Not.Null);
        Assert.That(rt!.ColumnNdv,               Is.Not.Null);
        Assert.That(rt.ColumnNdv!["age"],         Is.EqualTo(80));
        Assert.That(rt.ColumnNdv["city"],         Is.EqualTo(5));
    }

    [Test]
    public void KeyNdvJsonRoundTrip()
    {
        string sig = StatisticsManager.KeyTupleSignature(["city", "year"]);

        var stats = new TableStatistics
        {
            RowCount = 500,
            KeyNdv   = new Dictionary<string, long> { [sig] = 40 },
        };

        byte[] bytes = SerializeStats(stats);
        TableStatistics? rt = DeserializeStats(bytes);

        Assert.That(rt,              Is.Not.Null);
        Assert.That(rt!.KeyNdv,      Is.Not.Null);
        Assert.That(rt.KeyNdv![sig], Is.EqualTo(40));
    }

    [Test]
    public void OldBlobWithoutNdvDeserializesToNull()
    {
        var oldStats = new TableStatistics { RowCount = 99 };
        byte[] bytes = SerializeStats(oldStats);
        TableStatistics? rt = DeserializeStats(bytes);

        Assert.That(rt!.RowCount,  Is.EqualTo(99));
        Assert.That(rt.ColumnNdv,  Is.Null);
        Assert.That(rt.KeyNdv,     Is.Null);
    }

    [Test]
    public void AllFieldsCoexistWithoutDamage()
    {
        string sig = StatisticsManager.KeyTupleSignature(["region", "category"]);

        var stats = new TableStatistics
        {
            RowCount         = 2000,
            IndexEntryCounts = new Dictionary<string, long> { ["idx_price"] = 1990 },
            ColumnStats      = new Dictionary<string, ColumnMinMax>
            {
                ["price"] = new()
                {
                    Min = new ScalarBound { Type = ColumnType.Integer64, LongValue = 1   },
                    Max = new ScalarBound { Type = ColumnType.Integer64, LongValue = 9999 },
                },
            },
            Histograms = new Dictionary<string, ColumnHistogram>
            {
                ["price"] = new ColumnHistogram
                {
                    TotalRows = 2000,
                    Buckets   = [ new() { UpperBound = new ScalarBound { Type = ColumnType.Integer64, LongValue = 9999 }, CumulativeRows = 2000, DistinctInBucket = 300 } ],
                },
            },
            ColumnNdv = new Dictionary<string, long> { ["price"] = 300, ["region"] = 8, ["category"] = 12 },
            KeyNdv    = new Dictionary<string, long> { [sig] = 60 },
        };

        byte[] bytes = SerializeStats(stats);
        TableStatistics? rt = DeserializeStats(bytes);

        Assert.That(rt!.RowCount,                           Is.EqualTo(2000));
        Assert.That(rt.IndexEntryCounts!["idx_price"],       Is.EqualTo(1990));
        Assert.That(rt.ColumnStats!["price"].Min!.LongValue, Is.EqualTo(1));
        Assert.That(rt.Histograms!["price"].TotalRows,       Is.EqualTo(2000));
        Assert.That(rt.ColumnNdv!["price"],                  Is.EqualTo(300));
        Assert.That(rt.ColumnNdv["region"],                  Is.EqualTo(8));
        Assert.That(rt.KeyNdv![sig],                         Is.EqualTo(60));
    }

    [Test]
    public void KeyTupleSignatureSingleColumn()
    {
        string sig = StatisticsManager.KeyTupleSignature(["id"]);
        Assert.That(sig, Is.EqualTo("id"));
    }

    [Test]
    public void KeyTupleSignatureMultipleColumns()
    {
        string sig = StatisticsManager.KeyTupleSignature(["city", "year", "category"]);
        Assert.That(sig, Is.EqualTo("city,year,category"));
    }

    [Test]
    public void KeyTupleSignatureOrderMatters()
    {
        string ab = StatisticsManager.KeyTupleSignature(["a", "b"]);
        string ba = StatisticsManager.KeyTupleSignature(["b", "a"]);
        Assert.That(ab, Is.Not.EqualTo(ba));
    }

    [Test]
    public void UniformColumnNdvApproximatesRowCount()
    {
        // Simulate ANALYZE result for a table with 1000 rows and a column whose every value
        // is distinct (e.g. a primary key) — NDV should equal row count.
        long rowCount = 1000;
        long ndv = rowCount; // exact scan: all distinct

        var stats = new TableStatistics
        {
            RowCount  = rowCount,
            ColumnNdv = new Dictionary<string, long> { ["pk"] = ndv },
        };

        byte[] bytes = SerializeStats(stats);
        TableStatistics? rt = DeserializeStats(bytes);

        Assert.That(rt!.ColumnNdv!["pk"], Is.EqualTo(rowCount));
    }

    [Test]
    public void LowCardinalityColumnNdvIsSmall()
    {
        // A boolean-like column with 2 distinct values in 10 000 rows.
        var stats = new TableStatistics
        {
            RowCount  = 10_000,
            ColumnNdv = new Dictionary<string, long> { ["status"] = 2 },
        };

        byte[] bytes = SerializeStats(stats);
        TableStatistics? rt = DeserializeStats(bytes);

        long ndv = rt!.ColumnNdv!["status"];
        Assert.That(ndv, Is.LessThan(10));
        Assert.That(ndv, Is.GreaterThan(0));
    }

    [Test]
    public void CompositeKeyNdvIsAtMostProductOfColumnNdvs()
    {
        // NDV of a composite key (city=8, year=25) is at most 8*25=200 but may be less
        // when the columns are correlated. This test verifies the model can express both.
        string sig = StatisticsManager.KeyTupleSignature(["city", "year"]);

        long cityNdv  = 8;
        long yearNdv  = 25;
        long keyNdv   = 60; // correlated: not all (city, year) combinations exist

        var stats = new TableStatistics
        {
            RowCount  = 10_000,
            ColumnNdv = new Dictionary<string, long> { ["city"] = cityNdv, ["year"] = yearNdv },
            KeyNdv    = new Dictionary<string, long> { [sig] = keyNdv },
        };

        byte[] bytes = SerializeStats(stats);
        TableStatistics? rt = DeserializeStats(bytes);

        long rtKeyNdv = rt!.KeyNdv![sig];
        Assert.That(rtKeyNdv, Is.EqualTo(60));
        Assert.That(rtKeyNdv, Is.LessThanOrEqualTo(rt.ColumnNdv!["city"] * rt.ColumnNdv["year"]));
    }

    [Test]
    public void KeyTupleSignatureRejectsCommaInColumnName()
    {
        // A column name containing the delimiter ',' would produce the same signature as a
        // two-column tuple, silently corrupting the persisted stats key. The guard must throw.
        Assert.Throws<ArgumentException>(() => StatisticsManager.KeyTupleSignature(["city,year"]));
        Assert.Throws<ArgumentException>(() => StatisticsManager.KeyTupleSignature(["a", "b,c"]));
    }
}
