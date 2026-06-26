
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

    private static bool TryParseHexString(string s, out byte[] bytes)
    {
        bytes = Array.Empty<byte>();

        if (s == null)
            return false;

        byte[] buffer = new byte[(s.Length + 1) / 2];

        int i = 0;
        int j = 0;

        if ((s.Length % 2) == 1) // if s has an odd length assume an implied leading "0"
        {
            if (!TryParseHexChar(s[i++], out int y))
                return false;

            buffer[j++] = (byte)y;
        }

        while (i < s.Length)
        {
            if (!TryParseHexChar(s[i++], out int x))
                return false;

            if (!TryParseHexChar(s[i++], out int y))
                return false;

            buffer[j++] = (byte)((x << 4) | y);
        }

        bytes = buffer;
        return true;
    }

    public static ObjectIdValue ToValue(string s)
    {
        if (!TryParseHexString(s, out byte[] bytes))
            throw new FormatException("String should contain only hexadecimal digits.");

        int a = (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3];
        int b = (bytes[4] << 24) | (bytes[5] << 16) | (bytes[6] << 8) | bytes[7];
        int c = (bytes[8] << 24) | (bytes[9] << 16) | (bytes[10] << 8) | bytes[11];

        return new(a, b, c);
    }
}
