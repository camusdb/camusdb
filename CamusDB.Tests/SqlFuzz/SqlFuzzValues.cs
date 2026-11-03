/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.SqlFuzz;

/// <summary>
/// Canonical text for values and rows, multiset comparison, and the combination of aggregate parts.
/// The SQL fuzzer (<c>CamusDB.SqlFuzz.Tests</c>, which links this file) and the regression tests it
/// prints into this project use this one copy, so a printed test compares exactly as the oracle did.
///
/// <para><b>The value policy.</b> Two cells are equal when the SQL rules say so and the engine has no
/// freedom to return either form:</para>
/// <list type="bullet">
/// <item>-0.0 and 0.0 have one text, because <c>-0 = 0</c> and an index key stores the canonical zero.
/// An index-only read can return the zero of the key where the row holds -0.0.</item>
/// <item>NaN has one text, because NaN equals NaN in CamusDB.</item>
/// <item>A NUMERIC has the text of its exact value.</item>
/// <item>The type is part of the text. An INT64 1 and a FLOAT64 1 are different cells, and so are a
/// FLOAT32 1 and a FLOAT64 1, or a DATE and a DATETIME.</item>
/// </list>
/// <para>No comparison here is approximate, except <see cref="FloatsClose"/>, which only the combination
/// of FLOAT64 sums uses: the order of the additions changes the last bits of a sum.</para>
/// </summary>
internal static class SqlFuzzValues
{
    public static string Render(ColumnValue value)
    {
        switch (value.Type)
        {
            case ColumnType.Null:
                return "NULL";
            case ColumnType.Integer64:
                return "i:" + value.LongValue.ToString(CultureInfo.InvariantCulture);
            case ColumnType.Float64:
                return "f:" + RenderFloat(value.FloatValue);
            case ColumnType.Float32:
                return "g:" + RenderFloat(value.FloatValue);
            case ColumnType.Bool:
                return value.BoolValue ? "b:t" : "b:f";
            case ColumnType.Numeric:
                return "n:" + value.NumericValue;
            case ColumnType.Date:
                return "d:" + value.IsoValue;
            case ColumnType.DateTime:
                return "t:" + value.IsoValue;
            case ColumnType.Jsonb:
                return "j:" + value.JsonValue;
            case ColumnType.Bytes:
                return "x:" + Convert.ToHexString(value.BytesValue ?? []);
            case ColumnType.Uuid:
                return "u:" + value.UuidValue;
            case ColumnType.Array:
                return "a[" + RenderCells(value.ArrayValues ?? []) + "]";
            case ColumnType.Id:
                return "o:" + value.StrValue;
            default:
                return "s:" + value.StrValue;
        }
    }

    public static string RenderFloat(double value)
    {
        if (double.IsNaN(value))
            return "NaN";
        if (value == 0)
            return "0";
        return value.ToString("R", CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// One row as one string. Each cell is written as its length, a colon and its text, so no cell text
    /// can imitate a cell boundary: the rows <c>['a␟s:b', 'c']</c> and <c>['a', 'b␟s:c']</c> have
    /// different texts. The same form holds the items of an array.
    /// </summary>
    public static string RenderRow(IReadOnlyList<ColumnValue> row) => RenderCells(row);

    private static string RenderCells(IReadOnlyList<ColumnValue> cells)
    {
        StringBuilder sb = new();
        for (int i = 0; i < cells.Count; i++)
        {
            if (i > 0)
                sb.Append(' ');
            string text = Render(cells[i]);
            sb.Append(text.Length.ToString(CultureInfo.InvariantCulture)).Append(':').Append(text);
        }
        return sb.ToString();
    }

    /// <summary>
    /// Null when two reads have the same output shape (the same count of columns, and the same type in
    /// each position), else the difference. Rows alone cannot show it: two empty results, or two
    /// all-NULL results, have equal rows whatever their columns are. Names are not compared, because a
    /// transformed query may name its columns in another way.
    /// </summary>
    public static string? ShapeDifference(IReadOnlyList<(string Name, ColumnType Type)> expected, IReadOnlyList<(string Name, ColumnType Type)> actual, string expectedName = "expected", string actualName = "actual")
    {
        if (expected.Count != actual.Count)
            return $"{expectedName} has {expected.Count} column(s), {actualName} has {actual.Count}";
        for (int i = 0; i < expected.Count; i++)
            if (expected[i].Type != actual[i].Type)
                return $"column {i + 1} is {expected[i].Type} in {expectedName} and {actual[i].Type} in {actualName}";
        return null;
    }

    /// <summary>The rows as a sorted list of row texts: the canonical form of a multiset.</summary>
    public static List<string> Multiset(IEnumerable<IReadOnlyList<ColumnValue>> rows)
    {
        List<string> rendered = rows.Select(RenderRow).ToList();
        rendered.Sort(StringComparer.Ordinal);
        return rendered;
    }

    /// <summary>Several row-text lists as one sorted multiset.</summary>
    public static List<string> Union(params IEnumerable<string>[] parts)
    {
        List<string> all = parts.SelectMany(p => p).ToList();
        all.Sort(StringComparer.Ordinal);
        return all;
    }

    /// <summary>The first row text that a sorted multiset holds twice, or null when every row is unique.</summary>
    public static string? Duplicate(List<string> sorted)
    {
        for (int i = 1; i < sorted.Count; i++)
            if (string.Equals(sorted[i], sorted[i - 1], StringComparison.Ordinal))
                return sorted[i];
        return null;
    }

    /// <summary>Several row-text lists as one sorted set.</summary>
    public static List<string> SetUnion(params IEnumerable<string>[] parts) =>
        new SortedSet<string>(parts.SelectMany(p => p), StringComparer.Ordinal).ToList();

    /// <summary>
    /// Compares two sorted multisets. Returns null when they are equal, else a short report of the rows
    /// that only one side has, with their counts.
    /// </summary>
    public static string? Difference(List<string> expected, List<string> actual, string expectedName = "expected", string actualName = "actual")
    {
        if (expected.Count == actual.Count && expected.SequenceEqual(actual, StringComparer.Ordinal))
            return null;

        Dictionary<string, int> counts = new(StringComparer.Ordinal);
        foreach (string row in expected)
            counts[row] = counts.GetValueOrDefault(row) + 1;
        foreach (string row in actual)
            counts[row] = counts.GetValueOrDefault(row) - 1;

        List<string> onlyExpected = counts.Where(kv => kv.Value > 0).Select(kv => $"{kv.Value}x {kv.Key}").Take(5).ToList();
        List<string> onlyActual = counts.Where(kv => kv.Value < 0).Select(kv => $"{-kv.Value}x {kv.Key}").Take(5).ToList();

        StringBuilder sb = new();
        sb.Append(CultureInfo.InvariantCulture, $"{expectedName} has {expected.Count} row(s), {actualName} has {actual.Count}.");
        if (onlyExpected.Count > 0)
            sb.Append(CultureInfo.InvariantCulture, $" Only in {expectedName}: {string.Join("; ", onlyExpected)}.");
        if (onlyActual.Count > 0)
            sb.Append(CultureInfo.InvariantCulture, $" Only in {actualName}: {string.Join("; ", onlyActual)}.");
        return sb.ToString();
    }

    /// <summary>The name prefix of the column that holds <c>SUM(ABS(x))</c> for the FLOAT64 sum of aggregate <c>i</c>.</summary>
    public const string AbsSumColumnPrefix = "abs_sum";

    /// <summary>
    /// The FLOAT64 rule for a sum combined from parts: equal when both are NaN, both the same infinity,
    /// or within a relative 1e-9 (absolute 1e-12 near zero).
    ///
    /// <para><paramref name="absSum"/> is <c>SUM(ABS(x))</c> over the same rows, when the caller has it.
    /// Two orders of the additions of a float sum can differ by up to about <c>2·(n−1)·2⁻⁵³·Σ|x|</c>, so
    /// the tolerance scales with <c>Σ|x|</c>, not with the result: with cancellation the result is much
    /// smaller than the inputs. An example is <c>1e300 + -1e300 + 1.0</c>, which is 1 in one order and 0
    /// in another. An infinite <paramref name="absSum"/> means the inputs reach the overflow range, where
    /// the order also decides if a partial sum overflows, so any two results are accepted; the caller
    /// reports such a comparison as not made. A NaN <paramref name="absSum"/> means a NaN input, and both
    /// sums must then be NaN, so the scale is ignored.</para>
    /// </summary>
    public static bool FloatsClose(double a, double b, double? absSum = null)
    {
        if (absSum is { } scale && double.IsInfinity(scale))
            return true;
        if (absSum is { } nan && double.IsNaN(nan))
            absSum = null;
        if (double.IsNaN(a) || double.IsNaN(b))
            return double.IsNaN(a) && double.IsNaN(b);
        if (double.IsInfinity(a) || double.IsInfinity(b))
            return a == b;
        double diff = Math.Abs(a - b);
        double magnitude = Math.Max(Math.Max(Math.Abs(a), Math.Abs(b)), absSum ?? 0);
        return diff <= 1e-12 || diff <= 1e-9 * magnitude;
    }

    /// <summary>The value of the <see cref="AbsSumColumnPrefix"/> column of aggregate <paramref name="aggregate"/> in <paramref name="row"/>, if the read has it.</summary>
    public static double? AbsSum(IReadOnlyList<string> columns, IReadOnlyList<ColumnValue> row, int aggregate)
    {
        for (int i = 0; i < columns.Count; i++)
            if (string.Equals(columns[i], AbsSumColumnPrefix + aggregate.ToString(CultureInfo.InvariantCulture), StringComparison.OrdinalIgnoreCase))
                return row[i].Type is ColumnType.Float64 or ColumnType.Float32 ? row[i].FloatValue : null;
        return null;
    }

    /// <summary>
    /// Combines the values of one aggregate over the parts of a partition, as the aggregate over the
    /// whole would compute it. <paramref name="function"/> is <c>count</c>, <c>sum</c>, <c>min</c> or
    /// <c>max</c>. Returns null when the combination cannot be computed exactly, for example an INT64
    /// sum that overflows, so the caller skips the comparison instead of reporting it.
    /// </summary>
    public static ColumnValue? Combine(string function, IReadOnlyList<ColumnValue> parts)
    {
        List<ColumnValue> present = parts.Where(p => p.Type != ColumnType.Null).ToList();
        switch (function)
        {
            case "count":
            {
                long total = 0;
                foreach (ColumnValue part in parts)
                    total += part.Type == ColumnType.Null ? 0 : part.LongValue;
                return new ColumnValue(ColumnType.Integer64, total);
            }

            case "sum":
            {
                if (present.Count == 0)
                    return ColumnValue.Null;
                switch (present[0].Type)
                {
                    case ColumnType.Integer64:
                        try
                        {
                            long total = 0;
                            foreach (ColumnValue part in present)
                                total = checked(total + part.LongValue);
                            return new ColumnValue(ColumnType.Integer64, total);
                        }
                        catch (OverflowException)
                        {
                            return null;
                        }
                    case ColumnType.Numeric:
                    {
                        Int128 total = 0;
                        foreach (ColumnValue part in present)
                            total += part.NumericUnscaled;
                        return ColumnValue.FromNumeric(total);
                    }
                    case ColumnType.Float64:
                    case ColumnType.Float32:
                    {
                        double total = 0;
                        foreach (ColumnValue part in present)
                            total += part.FloatValue;
                        return new ColumnValue(ColumnType.Float64, total);
                    }
                    default:
                        return null;
                }
            }

            case "min":
            case "max":
            {
                if (present.Count == 0)
                    return ColumnValue.Null;
                ColumnValue best = present[0];
                for (int i = 1; i < present.Count; i++)
                {
                    int cmp = present[i].CompareTo(best);
                    if (function == "min" ? cmp < 0 : cmp > 0)
                        best = present[i];
                }
                return best;
            }

            default:
                return null;
        }
    }

    /// <summary>
    /// True when an aggregate of the whole equals the value combined from the parts. A FLOAT64 sum uses
    /// <see cref="FloatsClose"/>, scaled by <paramref name="absSum"/> when the caller has it; every other
    /// value is compared by its canonical text.
    /// </summary>
    public static bool SameAggregate(string function, ColumnValue whole, ColumnValue combined, double? absSum = null)
    {
        if (function == "sum" && whole.Type is ColumnType.Float64 or ColumnType.Float32 && combined.Type is ColumnType.Float64 or ColumnType.Float32)
            return FloatsClose(whole.FloatValue, combined.FloatValue, absSum);
        return Render(whole) == Render(combined);
    }
}
