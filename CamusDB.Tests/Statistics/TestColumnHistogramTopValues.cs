/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.Statistics.Models;

namespace CamusDB.Tests.Statistics;

/// <summary>
/// The most common values a histogram keeps next to its buckets. An equality on a listed value
/// reads its counted rows, the join distribution takes the list as its heavy values, and the
/// list survives the JSON form the statistics are persisted in. A histogram without a list keeps
/// the whole-bucket rule.
/// </summary>
[TestFixture]
public sealed class TestColumnHistogramTopValues
{
    private static ScalarBound Int(long v) => new() { Type = ColumnType.Integer64, LongValue = v };
    private static ScalarBound Str(string v) => new() { Type = ColumnType.String, StrValue = v };

    /// <summary>
    /// 1,000 rows over 10 buckets of 100: the values 7 and 42 hold 30 rows each, below a bucket, so
    /// no bucket holds either alone; the list names them.
    /// </summary>
    private static ColumnHistogram WithList() => new()
    {
        TotalRows = 1000,
        MinValue = Int(1),
        Buckets = Enumerable.Range(1, 10).Select(i => new ColumnHistogramBucket { UpperBound = Int(i * 100), CumulativeRows = i * 100, DistinctInBucket = 71 }).ToList(),
        TopValues = [new() { Value = Int(7), Rows = 30 }, new() { Value = Int(42), Rows = 30 }],
    };

    [Test]
    public void EqualityFraction_OfAListedValue_IsItsCountedRows()
    {
        ColumnHistogram hist = WithList();

        Assert.That(hist.TopValueRows(Int(42)), Is.EqualTo(30));
        Assert.That(hist.EqualityFraction(Int(42)), Is.EqualTo(0.03).Within(1e-9), "30 of 1,000 rows; the bucket density would say 100 / 71");
        Assert.That(hist.IsHeavyValue(Int(42)), Is.True);

        Assert.That(hist.TopValueRows(Int(43)), Is.Null);
        Assert.That(hist.EqualityFraction(Int(43)), Is.EqualTo(100.0 / 71 / 1000).Within(1e-9), "an unlisted value: its bucket's density");
        Assert.That(hist.IsHeavyValue(Int(43)), Is.False);
    }

    [Test]
    public void AValueOfAnotherType_IsNeverListed()
    {
        ColumnHistogram hist = WithList();

        // The bound comparison across types is not an equality, so the search must not match.
        Assert.That(hist.TopValueRows(Str("42")), Is.Null);
        Assert.That(hist.IsHeavyValue(Str("42")), Is.False);
    }

    [Test]
    public void JoinDistribution_TakesTheListAsItsHeavyValues()
    {
        JoinKeyDistribution? distribution = JoinKeyDistribution.FromHistogram(WithList(), distinct: 712);

        Assert.That(distribution, Is.Not.Null);
        Assert.That(distribution!.HeavyValues.Select(h => (h.Value.LongValue, h.Fraction)), Is.EqualTo(new[] { (7L, 0.03), (42L, 0.03) }));
        Assert.That(distribution.ResidualFraction, Is.EqualTo(0.94).Within(1e-9));
        Assert.That(distribution.ResidualDistinct, Is.EqualTo(710));

        // Two columns skewed on the same two values: 2 × 0.03² plus the residuals over 710 values.
        double selectivity = JoinKeyDistribution.JoinSelectivity(distribution, distribution)!.Value;
        Assert.That(selectivity, Is.EqualTo(2 * 0.03 * 0.03 + 0.94 * 0.94 / 710).Within(1e-12));
        Assert.That(selectivity * 712, Is.EqualTo(2.17).Within(0.01), "more than twice the textbook 1 / NDV");
    }

    [Test]
    public void JoinDistribution_OfAListWithEveryValue_HasNoResidual()
    {
        ColumnHistogram status = new()
        {
            TotalRows = 100,
            MinValue = Str("fail"),
            Buckets = [new() { UpperBound = Str("ok"), CumulativeRows = 100, DistinctInBucket = 3 }],
            TopValues = [new() { Value = Str("fail"), Rows = 5 }, new() { Value = Str("ok"), Rows = 80 }, new() { Value = Str("warn"), Rows = 15 }],
        };

        JoinKeyDistribution distribution = JoinKeyDistribution.FromHistogram(status, distinct: 3)!;

        Assert.That(distribution.ResidualFraction, Is.EqualTo(0.0));
        Assert.That(JoinKeyDistribution.JoinSelectivity(distribution, distribution), Is.EqualTo(0.05 * 0.05 + 0.8 * 0.8 + 0.15 * 0.15).Within(1e-12), "the exact self-join");
    }

    [Test]
    public void AListBuiltOutOfOrder_IsSortedForTheMerge()
    {
        ColumnHistogram hist = WithList();
        hist.TopValues = [new() { Value = Int(42), Rows = 30 }, new() { Value = Int(7), Rows = 30 }];

        JoinKeyDistribution distribution = JoinKeyDistribution.FromHistogram(hist, distinct: 712)!;

        Assert.That(distribution.HeavyValues.Select(h => h.Value.LongValue), Is.EqualTo(new[] { 7L, 42L }));
    }

    [Test]
    public void WithoutAList_TheWholeBucketRuleStands()
    {
        ColumnHistogram hist = WithList();
        hist.TopValues = null;

        Assert.That(hist.IsHeavyValue(Int(42)), Is.False);
        Assert.That(hist.EqualityFraction(Int(42)), Is.EqualTo(100.0 / 71 / 1000).Within(1e-9));
        Assert.That(JoinKeyDistribution.FromHistogram(hist, distinct: 712)!.HeavyValues, Is.Empty);
    }

    [Test]
    public void TheList_SurvivesThePersistedJsonForm()
    {
        byte[] bytes = MetaJsonSerializer.Serialize(WithList(), MetaJsonContext.Default.ColumnHistogram);
        ColumnHistogram back = MetaJsonSerializer.DeserializeCompat(bytes, MetaJsonContext.Default.ColumnHistogram);

        Assert.That(back.TopValues, Is.Not.Null);
        Assert.That(back.TopValues!.Select(v => (v.Value!.LongValue, v.Rows)), Is.EqualTo(new[] { (7L, 30L), (42L, 30L) }));

        // A histogram persisted before the list existed reads back with none.
        ColumnHistogram bare = WithList();
        bare.TopValues = null;
        byte[] bareBytes = MetaJsonSerializer.Serialize(bare, MetaJsonContext.Default.ColumnHistogram);
        Assert.That(System.Text.Encoding.UTF8.GetString(bareBytes), Does.Not.Contain("mcv"));
        Assert.That(MetaJsonSerializer.DeserializeCompat(bareBytes, MetaJsonContext.Default.ColumnHistogram).TopValues, Is.Null);
    }
}
