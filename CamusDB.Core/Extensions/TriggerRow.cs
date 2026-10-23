/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Extensions;

namespace CamusDB.Core.Extensions;

/// <summary>
/// The engine side of <see cref="IRow"/> and <see cref="IWritableRow"/>: a view over a row dictionary
/// with the value mapping of <see cref="ExtensionValueMapping"/>. It reads the dictionary directly,
/// without a copy, which is why the contract tells extension code not to keep it.
///
/// <para><b>Names.</b> Column names compare case-insensitively, the same as SQL. A row dictionary may
/// be keyed in the case the user wrote, so a lookup tries the exact key first and then scans.</para>
///
/// <para><b>Writes</b> (BEFORE INSERT and BEFORE UPDATE only) go into the same dictionary the
/// statement writes, under the schema's spelling of the column name, and are recorded in
/// <see cref="ChangedColumns"/> so the statement can coerce exactly those values to their column
/// types before it validates the row. A write to a column the table does not have fails at once.</para>
/// </summary>
internal sealed class TriggerRow : IWritableRow
{
    private readonly IReadOnlyDictionary<string, ColumnValue> values;

    private readonly Dictionary<string, ColumnValue>? writable;

    private readonly IReadOnlyList<TableColumnSchema>? columns;

    private List<TableColumnSchema>? changed;

    /// <summary>A read-only row.</summary>
    public TriggerRow(IReadOnlyDictionary<string, ColumnValue> values)
    {
        this.values = values;
    }

    /// <summary>A writable row over the statement's own dictionary.</summary>
    public TriggerRow(Dictionary<string, ColumnValue> values, IReadOnlyList<TableColumnSchema> columns)
    {
        this.values = values;
        writable = values;
        this.columns = columns;
    }

    public bool IsWritable => writable is not null;

    /// <summary>The columns a trigger set, or null when it set none.</summary>
    public IReadOnlyList<TableColumnSchema>? ChangedColumns => changed;

    public IEnumerable<string> ColumnNames => values.Keys;

    public bool Contains(string column) => TryGet(column, out _);

    public bool IsNull(string column) => !TryGet(column, out ColumnValue? value) || value.Type == ColumnType.Null;

    public object? GetValue(string column) => TryGet(column, out ColumnValue? value) ? ExtensionValueMapping.ToObject(value) : null;

    public long? GetInt64(string column) => Read(column, ColumnType.Integer64, static v => v.LongValue);

    public double? GetDouble(string column)
    {
        if (!TryGet(column, out ColumnValue? value) || value.Type == ColumnType.Null)
            return null;

        return value.Type switch
        {
            ColumnType.Float64 or ColumnType.Float32 => value.FloatValue,
            ColumnType.Integer64 => value.LongValue,
            _ => throw WrongType(column, value, "Float64"),
        };
    }

    public float? GetSingle(string column) => Read(column, ColumnType.Float32, static v => (float)v.FloatValue);

    public bool? GetBoolean(string column) => Read(column, ColumnType.Bool, static v => v.BoolValue);

    public string? GetString(string column)
    {
        if (!TryGet(column, out ColumnValue? value) || value.Type == ColumnType.Null)
            return null;

        return value.Type is ColumnType.String or ColumnType.Id ? value.StrValue : throw WrongType(column, value, "String");
    }

    public Guid? GetGuid(string column) => Read(column, ColumnType.Uuid, static v => v.ToGuid());

    public byte[]? GetBytes(string column)
    {
        if (!TryGet(column, out ColumnValue? value) || value.Type == ColumnType.Null)
            return null;

        // A copy: the stored array is shared by OLD, NEW and the index comparison (see ExtensionValueMapping).
        return value.Type == ColumnType.Bytes
            ? value.BytesValue is { } bytes ? ExtensionValueMapping.CopyBytes(bytes) : null
            : throw WrongType(column, value, "Bytes");
    }

    public DateTime? GetDateTime(string column)
    {
        if (!TryGet(column, out ColumnValue? value) || value.Type == ColumnType.Null)
            return null;

        return value.Type is ColumnType.Date or ColumnType.DateTime
            ? new DateTime(value.LongValue, DateTimeKind.Utc)
            : throw WrongType(column, value, "DateTime");
    }

    public decimal? GetDecimal(string column)
    {
        if (!TryGet(column, out ColumnValue? value) || value.Type == ColumnType.Null)
            return null;

        return value.Type switch
        {
            ColumnType.Numeric => ExtensionValueMapping.UnscaledToDecimal(value.NumericUnscaled),
            ColumnType.Integer64 => value.LongValue,
            _ => throw WrongType(column, value, "Numeric"),
        };
    }

    public IReadOnlyList<T>? GetArray<T>(string column)
    {
        if (!TryGet(column, out ColumnValue? value) || value.Type == ColumnType.Null)
            return null;

        if (value.Type != ColumnType.Array)
            throw WrongType(column, value, "Array");

        IReadOnlyList<ColumnValue> items = value.ArrayValues ?? [];
        T[] copy = new T[items.Count];

        for (int i = 0; i < copy.Length; i++)
        {
            // The boxed form of each element, unboxed to T: a long element reads as long or long?.
            object? element = items[i].Type == ColumnType.Null ? null : ExtensionValueMapping.ToObject(items[i]);

            if (element is null)
            {
                if (default(T) is not null)
                    throw new InvalidCastException($"Column '{column}' has a NULL element at position {i + 1}, which {typeof(T)} cannot hold");

                copy[i] = default!;
            }
            else
                copy[i] = element is T typed
                    ? typed
                    : throw new InvalidCastException($"Column '{column}' holds an array of {value.ArrayElementType}, not of {typeof(T)}");
        }

        return copy;
    }

    public void SetNull(string column) => Write(column, ColumnValue.Null);

    public void SetArray<T>(string column, IReadOnlyList<T>? values) => Write(column, ExtensionValueMapping.FromObject(values));

    public void Set(string column, long value) => Write(column, ExtensionValueMapping.FromInt64(value));

    public void Set(string column, double value) => Write(column, ExtensionValueMapping.FromDouble(value));

    public void Set(string column, float value) => Write(column, ExtensionValueMapping.FromSingle(value));

    public void Set(string column, bool value) => Write(column, ExtensionValueMapping.FromBoolean(value));

    public void Set(string column, string? value) => Write(column, ExtensionValueMapping.FromString(value));

    public void Set(string column, Guid value) => Write(column, ExtensionValueMapping.FromGuid(value));

    public void Set(string column, byte[]? value) => Write(column, ExtensionValueMapping.FromBytes(value));

    public void Set(string column, DateTime value) => Write(column, ExtensionValueMapping.FromDateTime(value));

    public void Set(string column, decimal value) => Write(column, ExtensionValueMapping.FromDecimal(value));

    private T? Read<T>(string column, ColumnType expected, Func<ColumnValue, T> read) where T : struct
    {
        if (!TryGet(column, out ColumnValue? value) || value.Type == ColumnType.Null)
            return null;

        return value.Type == expected ? read(value) : throw WrongType(column, value, expected.ToString());
    }

    private bool TryGet(string column, out ColumnValue value)
    {
        if (values.TryGetValue(column, out value!))
            return true;

        foreach (KeyValuePair<string, ColumnValue> entry in values)
        {
            if (string.Equals(entry.Key, column, StringComparison.OrdinalIgnoreCase))
            {
                value = entry.Value;
                return true;
            }
        }

        value = null!;
        return false;
    }

    /// <summary>Writes an engine value, for the typed write-back; the same checks and bookkeeping as a setter.</summary>
    internal void WriteValue(string column, ColumnValue value) => Write(column, value);

    private void Write(string column, ColumnValue value)
    {
        if (writable is null)
            throw new InvalidOperationException("This row is read-only: only a BEFORE INSERT or BEFORE UPDATE trigger can change NEW");

        TableColumnSchema? schemaColumn = null;

        foreach (TableColumnSchema candidate in columns!)
        {
            if (string.Equals(candidate.Name, column, StringComparison.OrdinalIgnoreCase))
            {
                schemaColumn = candidate;
                break;
            }
        }

        if (schemaColumn is null)
            throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"A trigger set column '{column}', which the table does not have");

        // Replace the existing key whatever its case, so the row never holds the column twice.
        string key = schemaColumn.Name;

        foreach (string existing in writable.Keys)
        {
            if (string.Equals(existing, column, StringComparison.OrdinalIgnoreCase))
            {
                key = existing;
                break;
            }
        }

        writable[key] = value;
        (changed ??= []).Add(schemaColumn);
    }

    private static InvalidCastException WrongType(string column, ColumnValue value, string expected) =>
        new($"Column '{column}' holds a {value.Type} value, not {expected}");
}
