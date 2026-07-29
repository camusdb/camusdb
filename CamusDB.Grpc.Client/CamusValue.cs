
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
    /// <see cref="Value"/> through unchanged.
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
        decimal m       => new Value { Float64Value = (double)m },
        byte[] bytes    => new Value { BytesValue = ByteString.CopyFrom(bytes) },
        DateTime dt     => new Value { DatetimeValue = dt.ToUniversalTime().Ticks },
        DateOnly date   => new Value { DateValue = date.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc).Ticks },
        Guid guid       => new Value { UuidValue = ByteString.CopyFrom(ToBigEndian(guid)) },
        _ => throw new ArgumentException(
            $"Cannot bind a value of type {value.GetType().Name}; build a Value explicitly for it",
            nameof(value)),
    };

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
