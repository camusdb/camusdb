
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Encodes query result values into the compact-raw JSON-native form used by the positional
/// wire format. The response carries an ordered column schema once; each row is then a bare
/// positional array whose entries are decoded by the client using the declared column type,
/// so per-cell type tags and column names are not repeated per row.
/// <para>
/// The encoding is deliberately minimal: JSON-native tokens (number/string/bool/null) for the
/// scalar types, and a compact raw form for the rest — base64 for <see cref="ColumnType.Bytes"/>,
/// raw <see cref="System.DateTime.Ticks"/> for <see cref="ColumnType.Date"/>/<see cref="ColumnType.DateTime"/>,
/// and the two big-endian 64-bit halves <c>[high, low]</c> for <see cref="ColumnType.Uuid"/>. A
/// null cell is always JSON null regardless of column type.
/// </para>
/// </summary>
public static class CompactRowEncoder
{
    /// <summary>
    /// Encodes a <see cref="QueryResultRow"/>'s cells into a positional array aligned to
    /// <paramref name="schema"/>: <c>result[c]</c> is the value for <c>schema[c]</c>. Each column
    /// is resolved from the row by its schema name, so the schema names MUST match the keys the
    /// row cursor produced (bare for single-source, <c>alias.column</c> for joins) — a name absent
    /// from the row encodes as null. See <see cref="DerivedTableSchemaBuilder"/> for how the schema
    /// names are kept aligned with the cursor keys.
    /// </summary>
    public static object?[] EncodeRow(IReadOnlyDictionary<string, ColumnValue> row, IReadOnlyList<DerivedColumnSchema> schema)
    {
        object?[] result = new object?[schema.Count];

        for (int i = 0; i < schema.Count; i++)
        {
            row.TryGetValue(schema[i].Name, out ColumnValue? value);
            result[i] = EncodeValue(value);
        }

        return result;
    }

    /// <summary>
    /// Encodes a single <see cref="ColumnValue"/> to its compact-raw JSON-native representation.
    /// A null value or a <see cref="ColumnType.Null"/> cell is always JSON null.
    /// </summary>
    public static object? EncodeValue(ColumnValue? value)
    {
        if (value is null || value.Type == ColumnType.Null)
            return null;

        return value.Type switch
        {
            ColumnType.Id        => value.StrValue,
            ColumnType.String    => value.StrValue,
            ColumnType.Integer64 => value.LongValue,
            ColumnType.Bool      => value.BoolValue,
            ColumnType.Float64   => value.FloatValue,
            ColumnType.Float32   => (float)value.FloatValue,
            ColumnType.Bytes     => value.BytesValue is null ? null : Convert.ToBase64String(value.BytesValue),
            ColumnType.Date      => value.LongValue,
            ColumnType.DateTime  => value.LongValue,
            ColumnType.Uuid      => new long[] { value.UuidHigh, value.LongValue },
            ColumnType.Array     => EncodeArray(value),
            _                    => null,
        };
    }

    private static object?[] EncodeArray(ColumnValue array)
    {
        if (array.ArrayValues is null)
            return [];

        object?[] result = new object?[array.ArrayValues.Count];

        for (int i = 0; i < array.ArrayValues.Count; i++)
            result[i] = EncodeValue(array.ArrayValues[i]);

        return result;
    }
}
