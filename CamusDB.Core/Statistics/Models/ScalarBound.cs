
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Statistics.Models;

/// <summary>
/// JSON-serializable typed scalar value used to persist column min/max bounds.
///
/// <see cref="CamusDB.Core.CommandsExecutor.Models.ColumnValue"/> is a runtime type whose
/// constructor enforces invariants and is not directly JSON-deserializable. <see cref="ScalarBound"/>
/// is a plain data-bag that round-trips safely through <c>System.Text.Json</c> source generation.
///
/// Only the payload field matching <see cref="Type"/> carries a meaningful value; the others
/// default to zero/null and are ignored by the cost model.
/// </summary>
public sealed class ScalarBound
{
    [JsonPropertyName("t")]
    public ColumnType Type { get; set; }

    [JsonPropertyName("l")]
    public long LongValue { get; set; }

    [JsonPropertyName("f")]
    public double FloatValue { get; set; }

    [JsonPropertyName("s")]
    public string? StrValue { get; set; }

    /// <summary>High 64 bits of a <see cref="ColumnType.Uuid"/> bound; the low 64 live in <see cref="LongValue"/>.</summary>
    [JsonPropertyName("u")]
    public long UuidHigh { get; set; }

    public static ScalarBound FromColumnValue(ColumnValue v) => new()
    {
        Type = v.Type,
        LongValue = v.LongValue,
        FloatValue = v.FloatValue,
        StrValue = v.StrValue,
        UuidHigh = v.UuidHigh,
    };

    // Signed comparison: negative = this < other, 0 = equal, positive = this > other.
    // Only meaningful when both sides have the same type.
    public int CompareTo(ScalarBound other)
    {
        if (Type != other.Type)
            return 0; // mixed types — caller must not compare

        return Type switch
        {
            ColumnType.Integer64 => LongValue.CompareTo(other.LongValue),
            ColumnType.Float64   => FloatValue.CompareTo(other.FloatValue),
            ColumnType.Float32   => ((float)FloatValue).CompareTo((float)other.FloatValue),
            ColumnType.Date      => LongValue.CompareTo(other.LongValue),
            ColumnType.DateTime  => LongValue.CompareTo(other.LongValue),
            ColumnType.String    => string.Compare(StrValue, other.StrValue, StringComparison.Ordinal),
            ColumnType.Id        => string.Compare(StrValue, other.StrValue, StringComparison.Ordinal),
            ColumnType.Uuid      => ((ulong)UuidHigh).CompareTo((ulong)other.UuidHigh) is int h and not 0
                                        ? h
                                        : ((ulong)LongValue).CompareTo((ulong)other.LongValue),
            _                    => 0,
        };
    }
}
