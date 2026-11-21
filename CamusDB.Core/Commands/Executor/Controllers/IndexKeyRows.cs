/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Gives the write paths the values of an index's expression key parts.
///
/// <para>Every path that builds an index key from a row reads the value at each key position by the
/// name in <see cref="TableIndexSchema.Columns"/>. At an expression position that name is a hidden key
/// name (<see cref="BoundIndexExpression.KeyName"/>) that no row holds. <see cref="For"/> returns a
/// small map with the value at each key position under the name the key builders read: the column
/// value at a column position, and the expression value under its hidden name at an expression
/// position. An index without expressions gets the row itself, so a plain index pays nothing.</para>
///
/// <para>The map holds the key positions only. It is for key building only, and it must never be
/// encoded, returned to a client or handed to a trigger: it holds names that are not columns. INCLUDE
/// values are read from the original row.</para>
///
/// <para><b>An absent column.</b> An INSERT leaves out a nullable column, which then holds NULL, so the
/// insert path passes <c>absentIsNull</c>. Every other path decodes the columns the index reads
/// (<see cref="AddReadColumns"/>), so an absent column there is a bug, and it fails the statement
/// instead of indexing a NULL that the row does not hold.</para>
/// </summary>
internal static class IndexKeyRows
{
    /// <summary>
    /// The value at each key position of <paramref name="index"/> for <paramref name="row"/>, under the
    /// name in <see cref="TableIndexSchema.Columns"/>, or <paramref name="row"/> itself when the index
    /// has no expressions. A column position that the row does not hold stays absent, as it is in the
    /// row. An error the expression raises fails the statement, as the same error in a SET or a CHECK
    /// does.
    /// </summary>
    public static Dictionary<string, ColumnValue> For(TableIndexSchema index, Dictionary<string, ColumnValue> row, bool absentIsNull = false)
    {
        if (index.Expressions is null)
            return row;

        string[] names = index.Columns;
        Dictionary<string, ColumnValue> keyRow = new(names.Length, row.Comparer);

        for (int i = 0; i < names.Length; i++)
        {
            if (index.ExpressionAt(i) is { } expression)
                keyRow[expression.KeyName] = Evaluate(index, expression, row, absentIsNull);
            else if (row.TryGetValue(names[i], out ColumnValue? value))
                keyRow[names[i]] = value;
        }

        return keyRow;
    }

    /// <summary>
    /// The value of <paramref name="expression"/> for <paramref name="row"/>, of the type the index
    /// declared for it, or NULL.
    /// </summary>
    public static ColumnValue Evaluate(TableIndexSchema index, BoundIndexExpression expression, IReadOnlyDictionary<string, ColumnValue> row, bool absentIsNull = false)
    {
        IReadOnlyDictionary<string, ColumnValue> source = row;
        string[] referenced = expression.ReferencedColumns;

        for (int i = 0; i < referenced.Length; i++)
        {
            if (row.ContainsKey(referenced[i]))
                continue;

            if (!absentIsNull)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Column '{referenced[i]}' was not read for the expression {expression.Sql} of index '{index.Name}'");

            // The expression reads only these columns, so a map of them, with NULL for each absent one,
            // replaces the row without a copy of every other column.
            source = InputsWithNulls(referenced, row);
            break;
        }

        ColumnValue value = SqlExecutor.EvalExpr(expression.Expression, source, parameters: null);
        return Conform(index, expression, value);
    }

    private static Dictionary<string, ColumnValue> InputsWithNulls(string[] referenced, IReadOnlyDictionary<string, ColumnValue> row)
    {
        Dictionary<string, ColumnValue> inputs = new(referenced.Length, StringComparer.OrdinalIgnoreCase);

        foreach (string column in referenced)
            inputs[column] = row.TryGetValue(column, out ColumnValue? value) ? value : ColumnValue.Null;

        return inputs;
    }

    /// <summary>
    /// Adds to <paramref name="columns"/> every column the key of <paramref name="index"/> reads: its key
    /// columns, and the columns its expressions read. A path that decodes only some columns of a row must
    /// call this, and not add <see cref="TableIndexSchema.Columns"/> alone, which holds hidden names.
    /// </summary>
    public static void AddReadColumns(TableIndexSchema index, ICollection<string> columns)
    {
        string[] names = index.Columns;

        for (int i = 0; i < names.Length; i++)
        {
            if (index.ExpressionAt(i) is { } expression)
            {
                foreach (string column in expression.ReferencedColumns)
                    columns.Add(column);
            }
            else
                columns.Add(names[i]);
        }
    }

    /// <summary>
    /// True when a column the key of <paramref name="index"/> reads is not <see cref="Identical"/> in
    /// <paramref name="oldRow"/> and <paramref name="newRow"/>. An absent column reads as NULL. An UPDATE
    /// that changes none of them leaves the index alone without evaluating any expression.
    /// </summary>
    public static bool InputsChanged(TableIndexSchema index, Dictionary<string, ColumnValue> oldRow, Dictionary<string, ColumnValue> newRow)
    {
        string[] names = index.Columns;

        for (int i = 0; i < names.Length; i++)
        {
            if (index.ExpressionAt(i) is { } expression)
            {
                foreach (string column in expression.ReferencedColumns)
                {
                    if (ValueChanged(oldRow, newRow, column))
                        return true;
                }
            }
            else if (ValueChanged(oldRow, newRow, names[i]))
                return true;
        }

        return false;
    }

    private static bool ValueChanged(Dictionary<string, ColumnValue> oldRow, Dictionary<string, ColumnValue> newRow, string column)
    {
        ColumnValue oldValue = oldRow.TryGetValue(column, out ColumnValue? o) ? o : ColumnValue.Null;
        ColumnValue newValue = newRow.TryGetValue(column, out ColumnValue? n) ? n : ColumnValue.Null;

        return !Identical(oldValue, newValue);
    }

    /// <summary>
    /// True when <paramref name="a"/> and <paramref name="b"/> hold the same representation, so no
    /// function can tell them apart. SQL equality (<see cref="ColumnValue.CompareTo"/>) is weaker, and a
    /// skip decided by it leaves a stale value behind: <c>0.0 = -0.0</c>, but <c>atan2</c> gives pi for
    /// one and -pi for the other; the jsonb documents <c>1</c> and <c>1.0</c> are equal, but
    /// <c>-&gt;&gt;</c> gives another text for each; and the intervals <c>'1 month'</c> and
    /// <c>'30 days'</c> are equal but add to a date differently. Two NaN values with different bits count
    /// as different, which costs at most one unneeded rewrite.
    /// </summary>
    public static bool Identical(ColumnValue a, ColumnValue b)
    {
        if (ReferenceEquals(a, b))
            return true;

        if (a.Type != b.Type)
            return false;

        switch (a.Type)
        {
            case ColumnType.Null:
                return true;

            case ColumnType.Float64:
            case ColumnType.Float32:
                return BitConverter.DoubleToInt64Bits(a.FloatValue) == BitConverter.DoubleToInt64Bits(b.FloatValue);

            case ColumnType.Bytes:
            case ColumnType.Jsonb:
                return (a.BytesValue ?? []).AsSpan().SequenceEqual(b.BytesValue ?? []);

            case ColumnType.Interval:
                return a.UuidHigh == b.UuidHigh && a.LongValue == b.LongValue;

            case ColumnType.Array:
            {
                IReadOnlyList<ColumnValue> left = a.ArrayValues ?? [];
                IReadOnlyList<ColumnValue> right = b.ArrayValues ?? [];

                if (a.ArrayElementType != b.ArrayElementType || left.Count != right.Count)
                    return false;

                for (int i = 0; i < left.Count; i++)
                {
                    if (!Identical(left[i], right[i]))
                        return false;
                }

                return true;
            }

            // Ordinal strings, integers, dates, uuids, booleans and numerics: equality is identity
            // (a numeric has one bit pattern for each value).
            default:
                return a.CompareTo(b) == 0;
        }
    }

    /// <summary>
    /// True when <paramref name="changedColumn"/> is a column the key of <paramref name="index"/> reads,
    /// directly or through an expression.
    /// </summary>
    public static bool KeyReads(TableIndexSchema index, string changedColumn)
    {
        string[] names = index.Columns;

        for (int i = 0; i < names.Length; i++)
        {
            if (index.ExpressionAt(i) is { } expression)
            {
                foreach (string column in expression.ReferencedColumns)
                {
                    if (string.Equals(column, changedColumn, StringComparison.OrdinalIgnoreCase))
                        return true;
                }
            }
            else if (string.Equals(names[i], changedColumn, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    /// <summary>
    /// The text that names key position <paramref name="position"/> of <paramref name="index"/> to a
    /// person: the column name, or the expression in parentheses.
    /// </summary>
    public static string DisplayName(TableIndexSchema index, int position) =>
        index.ExpressionAt(position) is { } expression ? "(" + expression.Sql + ")" : index.Columns[position];

    /// <summary>
    /// The value with the type the index declared for the expression. The type comes from the
    /// expression's static type, so the two agree except where an operator widens an integer to a
    /// float. Any other mismatch would encode a key that the index decodes as another type, so it fails
    /// the statement instead.
    /// </summary>
    private static ColumnValue Conform(TableIndexSchema index, BoundIndexExpression expression, ColumnValue value)
    {
        if (value.Type == ColumnType.Null || value.Type == expression.Type)
            return value;

        if (expression.Type == ColumnType.Float64 && value.Type == ColumnType.Integer64)
            return new ColumnValue(ColumnType.Float64, (double)value.LongValue);

        throw IndexExpressionBinder.Refused(
            index.Name,
            $"the expression {expression.Sql} gave a value of type {value.Type}, but the index holds values of type {expression.Type}");
    }
}
