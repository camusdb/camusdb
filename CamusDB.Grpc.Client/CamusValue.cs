
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using Google.Protobuf;

namespace CamusDB.Grpc.Client;

/// <summary>
/// Converts ordinary .NET values into the wire <see cref="Value"/> a prepared execution binds.
///
/// <para>Only the mapping that is unambiguous is done here. Where a .NET type could reasonably mean
/// more than one column type — a <see cref="string"/> that is meant as an <c>oid</c> or a
/// <c>uuid</c>, say — the caller is expected to build the <see cref="Value"/> itself and pass it
/// through, which this method allows. Guessing would silently coerce a key column and produce a
/// query that quietly matches nothing.</para>
/// </summary>
public static class CamusValue
{
    /// <summary>
    /// Maps <paramref name="value"/> onto the wire representation, passing an already-built
    /// <see cref="Value"/> through unchanged. A <see cref="decimal"/> binds as NUMERIC with all of its
    /// digits; the server rounds more than 9 fraction digits half away from zero. A NUMERIC parameter
    /// also binds into a FLOAT64 or FLOAT32 column, which converts it.
    /// </summary>
    public static Value From(object? value) => value switch
    {
        null            => new Value { NullValue = NullValue.Unset },
        Value ready     => ready,
        string s        => new Value { StringValue = s },
        bool b          => new Value { BoolValue = b },
        long l          => new Value { Int64Value = l },
        int i           => new Value { Int64Value = i },
        short sh        => new Value { Int64Value = sh },
        byte by         => new Value { Int64Value = by },
        double d        => new Value { Float64Value = d },
        float f         => new Value { Float32Value = f },
        decimal m       => new Value { NumericValue = m.ToString(CultureInfo.InvariantCulture) },
        byte[] bytes    => new Value { BytesValue = ByteString.CopyFrom(bytes) },
        DateTime dt     => new Value { DatetimeValue = dt.ToUniversalTime().Ticks },
        DateOnly date   => new Value { DateValue = date.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc).Ticks },
        Guid guid       => new Value { UuidValue = ByteString.CopyFrom(ToBigEndian(guid)) },
        _ => throw new ArgumentException(
            $"Cannot bind a value of type {value.GetType().Name}; build a Value explicitly for it",
            nameof(value)),
    };

    /// <summary>
    /// Reads a NUMERIC cell as a <see cref="decimal"/>. Returns false when the value is not a NUMERIC
    /// (NULL included), or when a <see cref="decimal"/> cannot hold it exactly.
    ///
    /// <para>A NUMERIC holds 38 significant digits, 9 of them after the point. A <see cref="decimal"/>
    /// holds 28 or 29 significant digits and a magnitude below about 7.9 × 10²⁸. A value outside that
    /// envelope, for example <c>12345678901234567890.123456789</c>, gives false here and never a
    /// rounded number: read <see cref="Value.NumericValue"/>, which is the exact canonical text.</para>
    /// </summary>
    public static bool TryGetDecimal(Value value, out decimal result)
    {
        result = 0;

        if (value.KindCase != Value.KindOneofCase.NumericValue)
            return false;

        string text = value.NumericValue;

        if (!decimal.TryParse(text, NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint,
                CultureInfo.InvariantCulture, out decimal parsed))
            return false;

        // decimal.TryParse rounds digits it cannot hold. The server sends the canonical text, with no
        // trailing zeros, and a parsed decimal keeps the scale of its text, so the two spellings are
        // equal exactly when nothing was rounded.
        if (!string.Equals(parsed.ToString(CultureInfo.InvariantCulture), text, StringComparison.Ordinal))
            return false;

        result = parsed;
        return true;
    }

    /// <summary>
    /// Lays a <see cref="Guid"/> out as the 16 big-endian bytes the server decodes (high half then
    /// low half), rather than the mixed-endian layout <see cref="Guid.ToByteArray()"/> produces by
    /// default on little-endian machines.
    /// </summary>
    private static byte[] ToBigEndian(Guid guid)
    {
        byte[] bytes = new byte[16];
        guid.TryWriteBytes(bytes, bigEndian: true, out _);
        return bytes;
    }
}
