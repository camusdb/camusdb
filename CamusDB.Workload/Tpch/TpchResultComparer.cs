/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace CamusDB.Workload.Tpch;

public enum TpchCellKind : byte
{
    Null,
    Integer,

    /// <summary>A NUMERIC value, kept as normalized decimal text so no digit goes through a double.</summary>
    Exact,

    /// <summary>A float64 value, compared with a tolerance.</summary>
    Float,
    Text,
}

public readonly record struct TpchCell(TpchCellKind Kind, string Text, double Number = 0)
{
    public static TpchCell Null { get; } = new(TpchCellKind.Null, "");

    public static TpchCell Integer(long value) => new(TpchCellKind.Integer, value.ToString(CultureInfo.InvariantCulture));

    public static TpchCell Exact(string decimalText) => new(TpchCellKind.Exact, TpchResultComparer.NormalizeNumeric(decimalText));

    public static TpchCell Float(double value) => new(TpchCellKind.Float, value.ToString("R", CultureInfo.InvariantCulture), value);

    public static TpchCell String(string value) => new(TpchCellKind.Text, value);

    public override string ToString() => Kind == TpchCellKind.Null ? "NULL" : Text;
}

/// <summary>The rows of one query execution in canonical order, with a digest for the artifacts.</summary>
public sealed record TpchResultSet(IReadOnlyList<TpchCell[]> Rows, string Digest);

/// <summary>
/// Puts a query result into a canonical form and compares two of them.
///
/// <para>Rows are compared as a multiset: they are sorted here, by their exact cells first, so two
/// executions whose plans emit ties in a different order still compare equal. Integers, strings and
/// NUMERIC values must be identical. Float64 values (averages and ratios the server computes in
/// floating point) may differ in their last bits between plans, so they are compared with a relative
/// tolerance of 1e-9.</para>
/// </summary>
public static class TpchResultComparer
{
    public const double FloatTolerance = 1e-9;

    public static string NormalizeNumeric(string text)
    {
        text = text.Trim();
        if (text.Contains('.', StringComparison.Ordinal))
            text = text.TrimEnd('0').TrimEnd('.');
        if (text is "" or "-" or "-0")
            return "0";
        return text;
    }

    public static TpchResultSet Canonical(List<TpchCell[]> rows)
    {
        rows.Sort(CompareRows);

        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        StringBuilder line = new();
        foreach (TpchCell[] row in rows)
        {
            line.Clear();
            foreach (TpchCell cell in row)
            {
                // Floats enter the digest rounded, so the digest is stable where the comparison is.
                line.Append((int)cell.Kind).Append(':')
                    .Append(cell.Kind == TpchCellKind.Float ? cell.Number.ToString("G9", CultureInfo.InvariantCulture) : cell.Text)
                    .Append('\u001f');
            }
            line.Append('\n');
            hash.AppendData(Encoding.UTF8.GetBytes(line.ToString()));
        }

        return new TpchResultSet(rows, Convert.ToHexString(hash.GetHashAndReset())[..16].ToLowerInvariant());
    }

    /// <summary>Null when the two results are equal, otherwise one sentence naming the first difference.</summary>
    public static string? Difference(TpchResultSet expected, TpchResultSet actual)
    {
        if (expected.Rows.Count != actual.Rows.Count)
            return $"{actual.Rows.Count} row(s), expected {expected.Rows.Count}";

        for (int r = 0; r < expected.Rows.Count; r++)
        {
            TpchCell[] want = expected.Rows[r];
            TpchCell[] got = actual.Rows[r];
            if (want.Length != got.Length)
                return $"row {r + 1} has {got.Length} column(s), expected {want.Length}";

            for (int c = 0; c < want.Length; c++)
            {
                if (!CellsEqual(want[c], got[c]))
                    return $"row {r + 1} column {c + 1} is {got[c]}, expected {want[c]} " +
                           $"(row: {Render(got)}; expected: {Render(want)})";
            }
        }
        return null;
    }

    public static bool CellsEqual(TpchCell a, TpchCell b)
    {
        if (a.Kind != b.Kind)
            return false;
        if (a.Kind != TpchCellKind.Float)
            return string.Equals(a.Text, b.Text, StringComparison.Ordinal);
        return FloatsClose(a.Number, b.Number);
    }

    public static bool FloatsClose(double a, double b)
    {
        if (double.IsNaN(a) || double.IsNaN(b))
            return double.IsNaN(a) && double.IsNaN(b);
        if (a == b)
            return true;
        return Math.Abs(a - b) <= FloatTolerance * Math.Max(Math.Abs(a), Math.Abs(b)) + 1e-12;
    }

    public static string Render(TpchCell[] row) => string.Join(" | ", row.Select(c => c.ToString()));

    private static int CompareRows(TpchCell[] a, TpchCell[] b)
    {
        int shared = Math.Min(a.Length, b.Length);

        // Exact cells decide the order first. A float that differs in its last bits must not move a row.
        for (int i = 0; i < shared; i++)
        {
            int kinds = a[i].Kind.CompareTo(b[i].Kind);
            if (kinds != 0)
                return kinds;
            if (a[i].Kind == TpchCellKind.Float)
                continue;
            int text = string.CompareOrdinal(a[i].Text, b[i].Text);
            if (text != 0)
                return text;
        }
        for (int i = 0; i < shared; i++)
        {
            if (a[i].Kind != TpchCellKind.Float)
                continue;
            int number = a[i].Number.CompareTo(b[i].Number);
            if (number != 0)
                return number;
        }
        return a.Length.CompareTo(b.Length);
    }
}
