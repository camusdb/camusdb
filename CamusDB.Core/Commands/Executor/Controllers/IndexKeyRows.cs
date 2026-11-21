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
/// name (<see cref="BoundIndexExpression.KeyName"/>) that no row holds. <see cref="For"/> returns a copy
/// of the row with the value of each expression under its hidden name, so the existing key builders
/// read it like a column. An index without expressions gets the row itself, so a plain index pays
/// nothing.</para>
///
/// <para>The copy is for key building only. It must never be encoded, returned to a client or handed to
/// a trigger: it holds names that are not columns. INCLUDE values are read from the original row.</para>
///
/// <para><b>An absent column.</b> An INSERT leaves out a nullable column, which then holds NULL, so the
/// insert path passes <c>absentIsNull</c>. Every other path decodes the columns the index reads
/// (<see cref="AddReadColumns"/>), so an absent column there is a bug, and it fails the statement
/// instead of indexing a NULL that the row does not hold.</para>
/// </summary>
internal static class IndexKeyRows
{
    /// <summary>
    /// <paramref name="row"/> with the value of each expression key part of <paramref name="index"/>
    /// under its hidden key name, or <paramref name="row"/> itself when the index has no expressions.
    /// An error the expression raises fails the statement, as the same error in a SET or a CHECK does.
    /// </summary>
    public static Dictionary<string, ColumnValue> For(TableIndexSchema index, Dictionary<string, ColumnValue> row, bool absentIsNull = false)
    {
        if (index.Expressions is not { } expressions)
            return row;

        Dictionary<string, ColumnValue> keyRow = new(row, row.Comparer);

        foreach (BoundIndexExpression? expression in expressions)
        {
            if (expression is not null)
                keyRow[expression.KeyName] = Evaluate(index, expression, row, absentIsNull);
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

        foreach (string column in expression.ReferencedColumns)
        {
            if (row.ContainsKey(column))
                continue;

            if (!absentIsNull)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Column '{column}' was not read for the expression {expression.Sql} of index '{index.Name}'");

            if (ReferenceEquals(source, row))
                source = new Dictionary<string, ColumnValue>(row, StringComparer.OrdinalIgnoreCase);

            ((Dictionary<string, ColumnValue>)source)[column] = ColumnValue.Null;
        }

        ColumnValue value = SqlExecutor.EvalExpr(expression.Expression, source, parameters: null);
        return Conform(index, expression, value);
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
    /// True when a column the key of <paramref name="index"/> reads holds another value in
    /// <paramref name="newRow"/> than in <paramref name="oldRow"/>. An absent column reads as NULL. An
    /// UPDATE that changes none of them leaves the index alone without evaluating any expression.
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

        // A column keeps its type, so a different type only comes with NULL, which CompareTo handles.
        return oldValue.Type != newValue.Type && oldValue.Type != ColumnType.Null && newValue.Type != ColumnType.Null
            || oldValue.CompareTo(newValue) != 0;
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
