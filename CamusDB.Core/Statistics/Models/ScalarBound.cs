
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

    /// <summary>
    /// High 64 bits of a <see cref="ColumnType.Uuid"/> or <see cref="ColumnType.Numeric"/> bound; the
    /// low 64 live in <see cref="LongValue"/>. For NUMERIC the two halves are the signed unscaled
    /// <see cref="Int128"/> (value × 10⁹).
    /// </summary>
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

    /// <summary>
    /// Signed comparison: negative = this &lt; other, 0 = equal, positive = this &gt; other. Exact
    /// for two bounds of one type. Two numeric bounds of different types (a predicate constant
    /// <c>5</c> against a NUMERIC or FLOAT64 histogram) compare as doubles, which is close enough for
    /// an estimate; any other mixed pair returns 0 and the caller must not rely on it.
    /// </summary>
    public int CompareTo(ScalarBound other)
    {
        if (Type != other.Type)
            return TryToDouble(out double left) && other.TryToDouble(out double right) ? left.CompareTo(right) : 0;

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
            ColumnType.Numeric   => NumericUnscaled().CompareTo(other.NumericUnscaled()),
            _                    => 0,
        };
    }

    /// <summary>
    /// The bound as a double, for interpolation and for comparing numeric bounds of different types.
    /// False for a type with no numeric value. A NUMERIC past 2⁵³ loses digits here, which only
    /// blurs an estimate.
    /// </summary>
    public bool TryToDouble(out double value)
    {
        switch (Type)
        {
            case ColumnType.Integer64:
                value = LongValue;
                return true;

            case ColumnType.Float64:
            case ColumnType.Float32:
                value = FloatValue;
                return true;

            case ColumnType.Numeric:
                value = NumericMath.ToDouble(NumericUnscaled());
                return true;

            default:
                value = 0;
                return false;
        }
    }

    /// <summary>The signed unscaled value of a NUMERIC bound. A method, so JSON does not persist it.</summary>
    private Int128 NumericUnscaled() => ((Int128)UuidHigh << 64) | (ulong)LongValue;
}
