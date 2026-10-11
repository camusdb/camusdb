
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The one text form of each scalar type. <c>CAST(x AS string)</c>, <c>x::string</c>,
/// <c>to_string(x)</c> and <c>concat(…)</c> all convert through here, so the conversions cannot
/// drift apart again: before this type existed, the cast and <c>concat</c> each kept their own type
/// table, and the two tables accepted different sets of types.
///
/// <para>Every form uses the invariant culture and depends on nothing but the value, so the
/// conversion is deterministic and is safe in a column <c>DEFAULT</c>, a stored <c>CHECK</c> and an
/// index expression. Where a type can be cast back from text, the form is one the string cast
/// parses, so <c>CAST(CAST(x AS string) AS T)</c> returns <c>x</c>:</para>
/// <list type="bullet">
///   <item><description>Date — <c>yyyy-MM-dd</c>, the same as <see cref="ColumnValue.IsoValue"/>.</description></item>
///   <item><description>DateTime — the ISO-8601 round-trip form in UTC,
///   <c>2026-10-11T08:30:00.0000000Z</c>, the same as <see cref="ColumnValue.IsoValue"/>.</description></item>
///   <item><description>Bytes — <c>\x</c> followed by lowercase hex, the PostgreSQL
///   <c>bytea_output = hex</c> form. The string-to-bytes cast accepts it next to <c>0x…</c>.</description></item>
///   <item><description>Float64 and Float32 — the shortest text that parses back to the same value
///   at that precision.</description></item>
///   <item><description>Uuid — lowercase hyphenated; Numeric — the canonical decimal text.</description></item>
/// </list>
///
/// <para>An array has no text form. <see cref="TryFormat"/> returns false for it, and each caller
/// raises its own error.</para>
/// </summary>
internal static class ScalarTextForm
{
    /// <summary>
    /// Converts a non-NULL scalar value to its text form. Returns false for a type that has no text
    /// form (an array, or NULL, which every caller handles before it calls this).
    /// </summary>
    public static bool TryFormat(ColumnValue value, out string text)
    {
        switch (value.Type)
        {
            case ColumnType.String:
            case ColumnType.Id:
                text = value.StrValue!;
                return true;

            case ColumnType.Integer64:
                text = value.LongValue.ToString(CultureInfo.InvariantCulture);
                return true;

            case ColumnType.Float64:
                text = value.FloatValue.ToString(CultureInfo.InvariantCulture);
                return true;

            case ColumnType.Float32:
                text = ((float)value.FloatValue).ToString(CultureInfo.InvariantCulture);
                return true;

            case ColumnType.Bool:
                text = value.BoolValue ? "true" : "false";
                return true;

            case ColumnType.Date:
            case ColumnType.DateTime:
                text = value.IsoValue!;
                return true;

            case ColumnType.Bytes:
                text = FormatBytes(value.BytesValue ?? []);
                return true;

            case ColumnType.Uuid:
                text = value.UuidValue!;
                return true;

            case ColumnType.Numeric:
                text = value.NumericValue!;
                return true;

            default:
                text = "";
                return false;
        }
    }

    /// <summary>Writes <c>\x</c> and the lowercase hex digits into one string allocation.</summary>
    private static string FormatBytes(byte[] bytes)
        => string.Create(2 + bytes.Length * 2, bytes, static (span, source) =>
        {
            span[0] = '\\';
            span[1] = 'x';
            Convert.TryToHexStringLower(source, span[2..], out _);
        });
}
