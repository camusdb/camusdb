
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;

namespace CamusDB.Core.Util.ObjectIds;

public sealed class ObjectId
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static char ToHexChar(int value)
    {
        return (char)(value + (value < 10 ? '0' : 'a' - 10));
    }

    public static string ToString(ObjectIdValue objectId)
    {
        return ToString(objectId.a, objectId.b, objectId.c);
    }

    public static string ToString(int _a, int _b, int _c)
        => string.Create(24, (_a, _b, _c), static (span, s) => WriteHex(span, s.Item1, s.Item2, s.Item3));

    /// <summary>
    /// Writes the 24-char lowercase-hex representation of the id into the first 24 chars of
    /// <paramref name="dest"/>. Lets callers compose a key (prefix + id) directly into a destination
    /// span with no intermediate string. <paramref name="dest"/> must have room for 24 chars.
    /// </summary>
    public static void WriteHex(Span<char> dest, int _a, int _b, int _c)
    {
        dest[0] = ToHexChar((_a >> 28) & 0x0f);
        dest[1] = ToHexChar((_a >> 24) & 0x0f);
        dest[2] = ToHexChar((_a >> 20) & 0x0f);
        dest[3] = ToHexChar((_a >> 16) & 0x0f);
        dest[4] = ToHexChar((_a >> 12) & 0x0f);
        dest[5] = ToHexChar((_a >> 8) & 0x0f);
        dest[6] = ToHexChar((_a >> 4) & 0x0f);
        dest[7] = ToHexChar(_a & 0x0f);

        dest[8] = ToHexChar((_b >> 28) & 0x0f);
        dest[9] = ToHexChar((_b >> 24) & 0x0f);
        dest[10] = ToHexChar((_b >> 20) & 0x0f);
        dest[11] = ToHexChar((_b >> 16) & 0x0f);
        dest[12] = ToHexChar((_b >> 12) & 0x0f);
        dest[13] = ToHexChar((_b >> 8) & 0x0f);
        dest[14] = ToHexChar((_b >> 4) & 0x0f);
        dest[15] = ToHexChar(_b & 0x0f);

        dest[16] = ToHexChar((_c >> 28) & 0x0f);
        dest[17] = ToHexChar((_c >> 24) & 0x0f);
        dest[18] = ToHexChar((_c >> 20) & 0x0f);
        dest[19] = ToHexChar((_c >> 16) & 0x0f);
        dest[20] = ToHexChar((_c >> 12) & 0x0f);
        dest[21] = ToHexChar((_c >> 8) & 0x0f);
        dest[22] = ToHexChar((_c >> 4) & 0x0f);
        dest[23] = ToHexChar(_c & 0x0f);
    }

    /// <summary>
    /// Writes the 24-character lowercase-hex form of an ObjectId directly as ASCII bytes into
    /// <paramref name="dest"/>. Every hex character is ASCII, so each byte is the char cast to
    /// <c>byte</c>. Used to compose a unique-index KV value (row-id payload) without first
    /// materializing a 24-char string and a separate UTF-8 array.
    /// </summary>
    public static void WriteHexAscii(Span<byte> dest, int _a, int _b, int _c)
    {
        dest[0] = (byte)ToHexChar((_a >> 28) & 0x0f);
        dest[1] = (byte)ToHexChar((_a >> 24) & 0x0f);
        dest[2] = (byte)ToHexChar((_a >> 20) & 0x0f);
        dest[3] = (byte)ToHexChar((_a >> 16) & 0x0f);
        dest[4] = (byte)ToHexChar((_a >> 12) & 0x0f);
        dest[5] = (byte)ToHexChar((_a >> 8) & 0x0f);
        dest[6] = (byte)ToHexChar((_a >> 4) & 0x0f);
        dest[7] = (byte)ToHexChar(_a & 0x0f);

        dest[8] = (byte)ToHexChar((_b >> 28) & 0x0f);
        dest[9] = (byte)ToHexChar((_b >> 24) & 0x0f);
        dest[10] = (byte)ToHexChar((_b >> 20) & 0x0f);
        dest[11] = (byte)ToHexChar((_b >> 16) & 0x0f);
        dest[12] = (byte)ToHexChar((_b >> 12) & 0x0f);
        dest[13] = (byte)ToHexChar((_b >> 8) & 0x0f);
        dest[14] = (byte)ToHexChar((_b >> 4) & 0x0f);
        dest[15] = (byte)ToHexChar(_b & 0x0f);

        dest[16] = (byte)ToHexChar((_c >> 28) & 0x0f);
        dest[17] = (byte)ToHexChar((_c >> 24) & 0x0f);
        dest[18] = (byte)ToHexChar((_c >> 20) & 0x0f);
        dest[19] = (byte)ToHexChar((_c >> 16) & 0x0f);
        dest[20] = (byte)ToHexChar((_c >> 12) & 0x0f);
        dest[21] = (byte)ToHexChar((_c >> 8) & 0x0f);
        dest[22] = (byte)ToHexChar((_c >> 4) & 0x0f);
        dest[23] = (byte)ToHexChar(_c & 0x0f);
    }

    private static bool TryParseHexChar(char c, out int value)
    {
        if (c is >= '0' and <= '9')
        {
            value = c - '0';
            return true;
        }

        if (c is >= 'a' and <= 'f')
        {
            value = 10 + (c - 'a');
            return true;
        }

        if (c is >= 'A' and <= 'F')
        {
            value = 10 + (c - 'A');
            return true;
        }

        value = 0;
        return false;
    }

    /// <summary>
    /// Parses a 24-character lowercase-hex id from a <see cref="string"/>. Delegates to the
    /// <see cref="ReadOnlySpan{Char}"/> overload so no intermediate <c>byte[]</c> is allocated; every
    /// caller passes a canonical 24-char id (row ids, index start offsets, and CAST which validates
    /// length first). Non-24-char input throws <see cref="FormatException"/>, which CAST already handles.
    /// </summary>
    public static ObjectIdValue ToValue(string s)
    {
        if (s is null)
            throw new FormatException("String should contain only hexadecimal digits.");

        return ToValue(s.AsSpan());
    }

    /// <summary>
    /// Parses a 24-character lowercase-hex span directly to an <see cref="ObjectIdValue"/>
    /// without allocating an intermediate <see cref="string"/> or <c>byte[]</c>. Use this at
    /// call sites that already hold the id as a key-suffix span (e.g. scan paths in
    /// <see cref="CamusDB.Core.Storage.Kv.KvTableStore"/>) to avoid <c>.ToString()</c> before parsing.
    /// </summary>
    public static ObjectIdValue ToValue(ReadOnlySpan<char> s)
    {
        if (s.Length != 24)
            throw new FormatException("String should contain only hexadecimal digits.");

        if (!TryParseHex8(s, 0, out int a) || !TryParseHex8(s, 8, out int b) || !TryParseHex8(s, 16, out int c))
            throw new FormatException("String should contain only hexadecimal digits.");

        return new(a, b, c);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryParseHex8(ReadOnlySpan<char> s, int offset, out int result)
    {
        result = 0;
        for (int i = 0; i < 8; i++)
        {
            if (!TryParseHexChar(s[offset + i], out int nibble))
                return false;
            result = (result << 4) | nibble;
        }
        return true;
    }

    /// <summary>
    /// Parses the 24-byte ASCII lowercase-hex row-id form stored in unique-index values directly to an
    /// <see cref="ObjectIdValue"/>, without materializing an intermediate <see cref="string"/> via
    /// <c>Encoding.UTF8.GetString</c>. Each byte is one ASCII hex digit. Use this on the unique-index
    /// scan/probe paths in <see cref="CamusDB.Core.Storage.Kv.KvTableStore"/>.
    /// </summary>
    public static ObjectIdValue ToValue(ReadOnlySpan<byte> asciiHex)
    {
        if (asciiHex.Length != 24)
            throw new FormatException("String should contain only hexadecimal digits.");

        if (!TryParseHex8(asciiHex, 0, out int a) || !TryParseHex8(asciiHex, 8, out int b) || !TryParseHex8(asciiHex, 16, out int c))
            throw new FormatException("String should contain only hexadecimal digits.");

        return new(a, b, c);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryParseHex8(ReadOnlySpan<byte> s, int offset, out int result)
    {
        result = 0;
        for (int i = 0; i < 8; i++)
        {
            if (!TryParseHexChar((char)s[offset + i], out int nibble))
                return false;
            result = (result << 4) | nibble;
        }
        return true;
    }
}
