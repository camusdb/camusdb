/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Widens a numeric value to the numeric type an expression declares. The declared type of an
/// expression is the arithmetic supertype of its branches
/// (<see cref="MixedNumericComparison.ArithmeticResultType"/>), but a value carries the type of the
/// branch that produced it. <c>COALESCE(p, 0)</c> on a NUMERIC <c>p</c> declares NUMERIC, and a row
/// with <c>p = NULL</c> evaluates it to an INT64 zero, because a NULL value has no declared type.
///
/// <para>The mismatch is not only cosmetic. A join plans its hash, merge and index strategies from
/// the declared types of the key columns, and those strategies treat two value types as unequal.
/// With an INT64 zero on one side and a NUMERIC zero on the other, a hash join drops a row that the
/// nested loop keeps. So the value of a derived-table column is widened to its declared type where
/// the derived table is filled (<see cref="WidenToDeclaredTypes"/>), and COALESCE widens its result
/// itself.</para>
///
/// <para>The rule only widens: a value converts only when the declared type is the supertype of
/// the value type and the declared type. A NUMERIC value never narrows to INT64, and a FLOAT32 value
/// never becomes NUMERIC. Widening to a float type is the same conversion that the mixed comparison
/// applies, so a widened value compares as the original value compared before.</para>
/// </summary>
public static class NumericWidening
{
    /// <summary>
    /// Returns <paramref name="value"/> widened to <paramref name="declared"/>, or the value unchanged
    /// when it is not numeric, already has the type, or the conversion would narrow it.
    /// </summary>
    public static ColumnValue Widen(ColumnValue value, ColumnType declared)
    {
        if (value.Type == declared || !MixedNumericComparison.IsNumeric(value.Type) || !MixedNumericComparison.IsNumeric(declared))
            return value;

        if (MixedNumericComparison.ArithmeticResultType(value.Type, declared) != declared)
            return value;

        return declared switch
        {
            ColumnType.Numeric => ColumnValue.FromNumeric(NumericMath.FromInt64(value.LongValue)),
            ColumnType.Float32 => new ColumnValue(ColumnType.Float32, (double)(float)value.LongValue),
            ColumnType.Float64 => new ColumnValue(ColumnType.Float64, MixedNumericComparison.ToDouble(value)),
            _ => value,
        };
    }

    /// <summary>
    /// The row keys of the columns in <paramref name="columns"/> whose declared type is numeric, with
    /// that type. Empty when the schema has no numeric column, so <see cref="WidenToDeclaredTypes"/>
    /// costs nothing for it.
    /// </summary>
    public static (string RowKey, ColumnType Type)[] NumericColumns(IReadOnlyList<DerivedColumnSchema> columns)
    {
        int count = 0;
        foreach (DerivedColumnSchema column in columns)
            if (MixedNumericComparison.IsNumeric(column.Type))
                count++;

        if (count == 0)
            return [];

        (string, ColumnType)[] result = new (string, ColumnType)[count];
        int i = 0;
        foreach (DerivedColumnSchema column in columns)
            if (MixedNumericComparison.IsNumeric(column.Type))
                result[i++] = (column.RowKey, column.Type);

        return result;
    }

    /// <summary>
    /// Returns <paramref name="row"/> with each numeric cell of <paramref name="numericColumns"/>
    /// widened to its declared type. The row is returned unchanged, with no allocation, when no cell
    /// needs a change, which is the usual case.
    /// </summary>
    public static QueryResultRow WidenToDeclaredTypes(QueryResultRow row, (string RowKey, ColumnType Type)[] numericColumns)
    {
        int first = -1;
        for (int i = 0; i < numericColumns.Length; i++)
        {
            if (row.Row.TryGetValue(numericColumns[i].RowKey, out ColumnValue? value)
                && !ReferenceEquals(Widen(value, numericColumns[i].Type), value))
            {
                first = i;
                break;
            }
        }

        if (first < 0)
            return row;

        if (row.Row is QueryRow queryRow)
        {
            ColumnValue[] values = new ColumnValue[queryRow.Count];
            for (int i = 0; i < values.Length; i++)
                values[i] = queryRow.GetColumnValue(i);

            for (int i = first; i < numericColumns.Length; i++)
            {
                int ordinal = queryRow.Layout.IndexOf(numericColumns[i].RowKey);
                if (ordinal >= 0)
                    values[ordinal] = Widen(values[ordinal], numericColumns[i].Type);
            }

            return new QueryResultRow(row.RowId, new QueryRow(queryRow.RowId, queryRow.Layout, values));
        }

        Dictionary<string, ColumnValue> copy = new(row.Row, StringComparer.OrdinalIgnoreCase);
        for (int i = first; i < numericColumns.Length; i++)
        {
            string key = numericColumns[i].RowKey;
            if (copy.TryGetValue(key, out ColumnValue? value))
                copy[key] = Widen(value, numericColumns[i].Type);
        }

        return new QueryResultRow(row.RowId, copy);
    }
}
