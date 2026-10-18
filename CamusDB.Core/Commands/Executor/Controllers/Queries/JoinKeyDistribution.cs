/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Statistics.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The value distribution of one join-key column, in the form an equi-join estimate can multiply
/// across tables: the <b>heavy values</b>, each with the fraction of the rows that hold it, and
/// the <b>residual</b>, the rest of the rows spread evenly over the rest of the distinct values.
///
/// <para>The textbook estimate <c>|A ⋈ B| = |A| · |B| / max(ndvA, ndvB)</c> assumes every value
/// is as common as every other. When both sides are skewed on the same values (customers and
/// stores both concentrate in the popular cities), the true join size
/// <c>Σ_v f_A(v) · f_B(v)</c> is many times that, because the common values multiply. The
/// heavy values make that sum visible; the residual keeps the textbook assumption for the
/// values the statistics cannot tell apart.</para>
///
/// <para>The heavy values are the most common values that <c>ANALYZE</c> keeps next to the
/// histogram (<see cref="ColumnHistogram.TopValues"/>), each with its counted rows; when the
/// column has no more distinct values than the list holds, the list is the whole distribution
/// and the residual is empty. A histogram built before the list existed gives the heavy values
/// the buckets can resolve: a value that holds at least a whole bucket of the sorted rows fills
/// buckets whose only distinct value it is (<see cref="ColumnHistogramBucket.DistinctInBucket"/>
/// is 1), and the rows of those buckets are its frequency; the partial bucket where its run
/// starts is not counted, so that fraction is a slight underestimate, and a value below one
/// bucket of rows stays in the residual. The list matters because the skew that sizes a join is
/// mostly below the bucket line: on the Join Order Benchmark the most popular movie holds about
/// one percent of the cast rows and the next ones half a percent, and the join of two such
/// columns is three times the textbook estimate.</para>
///
/// <para>Fractions, not row counts: a histogram can come from a sample, and a join input can be
/// filtered, so the distribution describes shares of the column and the caller multiplies by
/// the rows of each input, as it does for the textbook estimate.</para>
/// </summary>
internal sealed class JoinKeyDistribution
{
    /// <summary>One heavy value with its share of the non-null rows.</summary>
    internal readonly record struct HeavyValue(ScalarBound Value, double Fraction);

    /// <summary>The heavy values in ascending value order, with no two equal.</summary>
    public IReadOnlyList<HeavyValue> HeavyValues { get; }

    /// <summary>The share of the rows that no heavy value holds.</summary>
    public double ResidualFraction { get; }

    /// <summary>The distinct values the residual rows spread over; at least 1.</summary>
    public double ResidualDistinct { get; }

    private JoinKeyDistribution(IReadOnlyList<HeavyValue> heavyValues, double residualFraction, double residualDistinct)
    {
        HeavyValues = heavyValues;
        ResidualFraction = residualFraction;
        ResidualDistinct = residualDistinct;
    }

    /// <summary>
    /// A column whose values are all equally common, <paramref name="distinct"/> of them: a unique
    /// key, or a column with a distinct count and no histogram. Its join selectivity against any
    /// other distribution is the textbook one.
    /// </summary>
    public static JoinKeyDistribution Uniform(long distinct) =>
        new([], 1.0, Math.Max(1L, distinct));

    /// <summary>
    /// The distribution of a column from its histogram and its distinct count: the most common
    /// values when the histogram keeps them, else the values that fill whole buckets. Returns null
    /// for an empty histogram.
    /// </summary>
    public static JoinKeyDistribution? FromHistogram(ColumnHistogram histogram, long distinct)
    {
        if (histogram.TotalRows <= 0 || histogram.Buckets.Count == 0)
            return null;

        if (histogram.TopValues is { Count: > 0 } top)
            return FromTopValues(histogram, top, distinct);

        List<HeavyValue> heavy = [];
        long heavyRows = 0;
        long previousCumulative = 0;

        foreach (ColumnHistogramBucket bucket in histogram.Buckets)
        {
            long rows = bucket.CumulativeRows - previousCumulative;
            previousCumulative = bucket.CumulativeRows;

            if (bucket.UpperBound is not { } value || rows <= 0 || bucket.DistinctInBucket != 1)
                continue;

            heavyRows += rows;

            // A value that fills several buckets in a row: one heavy value, the rows added up.
            if (heavy.Count > 0 && heavy[^1].Value.CompareTo(value) == 0)
            {
                heavy[^1] = new HeavyValue(value, heavy[^1].Fraction + (double)rows / histogram.TotalRows);
                continue;
            }

            heavy.Add(new HeavyValue(value, (double)rows / histogram.TotalRows));
        }

        double residualFraction = Math.Max(0.0, (double)(histogram.TotalRows - heavyRows) / histogram.TotalRows);
        double residualDistinct = Math.Max(1.0, distinct - heavy.Count);

        return new JoinKeyDistribution(heavy, residualFraction, residualDistinct);
    }

    /// <summary>
    /// The distribution from the histogram's most common values: each listed value with its share,
    /// the rest of the rows as the residual over the rest of the distinct values. The list is
    /// stored in ascending value order; a list that is not (a histogram built by hand) is sorted
    /// here, because <see cref="JoinSelectivity"/> merges two lists in one pass.
    /// </summary>
    private static JoinKeyDistribution FromTopValues(ColumnHistogram histogram, List<ColumnTopValue> top, long distinct)
    {
        List<HeavyValue> heavy = new(top.Count);
        long heavyRows = 0;
        bool ordered = true;

        foreach (ColumnTopValue entry in top)
        {
            if (entry.Value is not { } value || entry.Rows <= 0)
                continue;

            if (heavy.Count > 0 && heavy[^1].Value.CompareTo(value) >= 0)
                ordered = false;

            heavy.Add(new HeavyValue(value, (double)entry.Rows / histogram.TotalRows));
            heavyRows += entry.Rows;
        }

        if (!ordered)
            heavy.Sort((a, b) => a.Value.CompareTo(b.Value));

        double residualFraction = Math.Max(0.0, (double)(histogram.TotalRows - heavyRows) / histogram.TotalRows);
        double residualDistinct = Math.Max(1.0, distinct - heavy.Count);

        return new JoinKeyDistribution(heavy, residualFraction, residualDistinct);
    }

    /// <summary>
    /// The selectivity of the equi-join of two columns on one value, the factor that turns the
    /// product of their row counts into the join size: <c>Σ_v f_A(v) · f_B(v)</c> over the union
    /// of the heavy values, where a side without <c>v</c> among its heavy values contributes the
    /// average frequency of one residual value, plus the two residuals joined with the textbook
    /// estimate (each side's residual share, divided by the larger residual distinct count).
    ///
    /// <para>With no heavy value on either side this is exactly the textbook selectivity
    /// <c>1 / max(dA, dB)</c>. It is the Postgres-style estimate over most-common-value lists.
    /// It is a pair estimate on purpose: the join-order search applies it one pair at a time
    /// inside a class of equal columns, because the mean of a product of several heavy-tailed
    /// frequencies is dominated by the few most common values and is wrong by orders of magnitude
    /// as soon as a leaf filter excludes them.</para>
    ///
    /// <para>Returns null when the heavy values of the two sides have different types, because a
    /// comparison across types is not an equality the estimate can trust; the caller then uses
    /// the textbook estimate.</para>
    /// </summary>
    public static double? JoinSelectivity(JoinKeyDistribution a, JoinKeyDistribution b)
    {
        IReadOnlyList<HeavyValue> heavyA = a.HeavyValues;
        IReadOnlyList<HeavyValue> heavyB = b.HeavyValues;

        if (heavyA.Count > 0 && heavyB.Count > 0 && heavyA[0].Value.Type != heavyB[0].Value.Type)
            return null;

        double residualA = a.ResidualFraction / a.ResidualDistinct;
        double residualB = b.ResidualFraction / b.ResidualDistinct;

        double sum = 0.0;
        int i = 0;
        int j = 0;

        while (i < heavyA.Count || j < heavyB.Count)
        {
            int order = i == heavyA.Count ? 1
                : j == heavyB.Count ? -1
                : heavyA[i].Value.CompareTo(heavyB[j].Value);

            if (order == 0)
            {
                sum += heavyA[i].Fraction * heavyB[j].Fraction;
                i++;
                j++;
            }
            else if (order < 0)
            {
                sum += heavyA[i].Fraction * residualB;
                i++;
            }
            else
            {
                sum += residualA * heavyB[j].Fraction;
                j++;
            }
        }

        double residual = a.ResidualFraction * b.ResidualFraction / Math.Max(a.ResidualDistinct, b.ResidualDistinct);

        return Math.Clamp(sum + residual, 0.0, 1.0);
    }
}
