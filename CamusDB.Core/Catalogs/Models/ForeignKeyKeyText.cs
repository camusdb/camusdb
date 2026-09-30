/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Renders the key of a foreign-key error the way PostgreSQL does: <c>(city, country)=(lima, pe)</c>.
/// Every foreign-key message — the validation pass, a child insert without a parent, a parent delete
/// with children — names the key through this one format, so a user sees the same shape wherever the
/// constraint is checked.
/// </summary>
internal static class ForeignKeyKeyText
{
    /// <summary>
    /// Renders <paramref name="columnNames"/> and <paramref name="values"/>, both in constraint order.
    /// </summary>
    internal static string Format(string[] columnNames, ColumnValue[] values)
    {
        StringBuilder text = new();
        text.Append('(').Append(string.Join(", ", columnNames)).Append(")=(");

        for (int i = 0; i < values.Length; i++)
        {
            if (i > 0)
                text.Append(", ");
            text.Append(RenderValue(values[i]));
        }

        return text.Append(')').ToString();
    }

    /// <summary>
    /// One value in the literal forms the rest of the engine displays: ISO-8601 for dates, an
    /// <c>X'…'</c> literal for bytes, the canonical string for a uuid, and a string without quotes.
    /// </summary>
    private static string RenderValue(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "NULL",
        ColumnType.Id => value.StrValue ?? "",
        ColumnType.String => value.StrValue ?? "",
        ColumnType.Bool => value.BoolValue ? "true" : "false",
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Float64 => value.FloatValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Float32 => ((float)value.FloatValue).ToString(CultureInfo.InvariantCulture),
        ColumnType.Date or ColumnType.DateTime => value.IsoValue ?? "",
        ColumnType.Bytes => SqlStringLiteral.QuoteBytes(value.BytesValue ?? []),
        ColumnType.Uuid => value.UuidValue ?? "",
        _ => value.ToString() ?? "",
    };
}
