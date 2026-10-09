/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// The one hash of a <see cref="ColumnValue"/> payload that every hash-keyed operator uses: the join
/// key comparer, the GROUP BY key comparer, the DISTINCT row comparer and the SQL value comparer
/// behind IN sets. Each of those defines equality as "same type and
/// <see cref="ColumnValue.CompareTo"/> returns 0", so this hash must give equal hashes for any two
/// values that compare equal:
/// <list type="bullet">
/// <item>Float32 hashes at single precision, as <see cref="ColumnValue.CompareTo"/> compares it.</item>
/// <item>String and Id hash ordinally, as they compare.</item>
/// <item>Uuid and Numeric hash both 64-bit halves. With the low half only, values that differ in the
///   high half share one bucket.</item>
/// <item>An array hashes its count and each element, and a NULL element hashes as its type only.</item>
/// </list>
///
/// <para>A type with no arm here still gives correct results, because equality still separates the
/// values. But every value of that type goes into one bucket, and a hash join, GROUP BY, DISTINCT or
/// IN set on it slows toward quadratic work. A new <see cref="ColumnType"/> member must get an arm;
/// <c>TestColumnTypeCoverage</c> fails for a member whose distinct values share one hash.</para>
///
/// <para>The hash is process-local: <see cref="HashCode"/> takes a random seed for each process. Never
/// persist it, and never compare it between nodes.</para>
/// </summary>
internal static class ColumnValueHash
{
    /// <summary>The hash of one value, type included.</summary>
    public static int Of(ColumnValue value)
    {
        HashCode hash = new();
        Add(ref hash, value);
        return hash.ToHashCode();
    }

    /// <summary>Adds the type and the payload of <paramref name="value"/> to <paramref name="hash"/>.</summary>
    public static void Add(ref HashCode hash, ColumnValue value)
    {
        hash.Add((int)value.Type);

        switch (value.Type)
        {
            case ColumnType.Null:
                break;

            case ColumnType.Array:
            {
                IReadOnlyList<ColumnValue> elements = value.ArrayValues ?? [];
                hash.Add(elements.Count);

                // Nested arrays are rejected, so an element is NULL or a scalar.
                foreach (ColumnValue element in elements)
                {
                    hash.Add((int)element.Type);
                    if (element.Type != ColumnType.Null)
                        AddScalar(ref hash, element);
                }
                break;
            }

            default:
                AddScalar(ref hash, value);
                break;
        }
    }

    private static void AddScalar(ref HashCode hash, ColumnValue value)
    {
        switch (value.Type)
        {
            case ColumnType.String:
            case ColumnType.Id:
                hash.Add(value.StrValue, StringComparer.Ordinal);
                break;

            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                hash.Add(value.LongValue);
                break;

            // double and float hashes treat 0 and -0 as one value, and every NaN as one value, as
            // CompareTo does.
            case ColumnType.Float64:
                hash.Add(value.FloatValue);
                break;

            case ColumnType.Float32:
                hash.Add((float)value.FloatValue);
                break;

            case ColumnType.Bool:
                hash.Add(value.BoolValue);
                break;

            case ColumnType.Bytes:
                hash.AddBytes(value.BytesValue ?? []);
                break;

            case ColumnType.Uuid:
            case ColumnType.Numeric:
                hash.Add(value.UuidHigh);
                hash.Add(value.LongValue);
                break;
        }
    }
}
