﻿
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
    public static char ToHexChar(int value)
    {
        return (char)(value + (value < 10 ? '0' : 'a' - 10));
    }

    public static string ToString(ObjectIdValue objectId)
    {
        return ToString(objectId.a, objectId.b, objectId.c);
    }

    public static string ToString(int _a, int _b, int _c)
    {
        var c = new char[24];

        c[0] = ToHexChar((_a >> 28) & 0x0f);
        c[1] = ToHexChar((_a >> 24) & 0x0f);
        c[2] = ToHexChar((_a >> 20) & 0x0f);
        c[3] = ToHexChar((_a >> 16) & 0x0f);
        c[4] = ToHexChar((_a >> 12) & 0x0f);
        c[5] = ToHexChar((_a >> 8) & 0x0f);
        c[6] = ToHexChar((_a >> 4) & 0x0f);
        c[7] = ToHexChar(_a & 0x0f);

        c[8] = ToHexChar((_b >> 28) & 0x0f);
        c[9] = ToHexChar((_b >> 24) & 0x0f);
        c[10] = ToHexChar((_b >> 20) & 0x0f);
        c[11] = ToHexChar((_b >> 16) & 0x0f);
        c[12] = ToHexChar((_b >> 12) & 0x0f);
        c[13] = ToHexChar((_b >> 8) & 0x0f);
        c[14] = ToHexChar((_b >> 4) & 0x0f);
        c[15] = ToHexChar(_b & 0x0f);

        c[16] = ToHexChar((_c >> 28) & 0x0f);
        c[17] = ToHexChar((_c >> 24) & 0x0f);
        c[18] = ToHexChar((_c >> 20) & 0x0f);
        c[19] = ToHexChar((_c >> 16) & 0x0f);
        c[20] = ToHexChar((_c >> 12) & 0x0f);
        c[21] = ToHexChar((_c >> 8) & 0x0f);
        c[22] = ToHexChar((_c >> 4) & 0x0f);
        c[23] = ToHexChar(_c & 0x0f);

        return new string(c);
    }

    private static bool TryParseHexChar(char c, out int value)
    {
        if (c >= '0' && c <= '9')
        {
            value = c - '0';
            return true;
        }

        if (c >= 'a' && c <= 'f')
        {
            value = 10 + (c - 'a');
            return true;
        }

        if (c >= 'A' && c <= 'F')
        {
            value = 10 + (c - 'A');
            return true;
        }

        value = 0;
        return false;
    }

    public static bool TryParseHexString(string s, out byte[] bytes)
    {
        bytes = Array.Empty<byte>();

        if (s == null)
            return false;

        var buffer = new byte[(s.Length + 1) / 2];

        var i = 0;
        var j = 0;

        if ((s.Length % 2) == 1) // if s has an odd length assume an implied leading "0"
        {
            int y;

            if (!TryParseHexChar(s[i++], out y))
                return false;

            buffer[j++] = (byte)y;
        }

        while (i < s.Length)
        {
            int x, y;

            if (!TryParseHexChar(s[i++], out x))
                return false;

            if (!TryParseHexChar(s[i++], out y))
                return false;

            buffer[j++] = (byte)((x << 4) | y);
        }

        bytes = buffer;
        return true;
    }

    public static ObjectIdValue ToValue(string s, int offset = 0)
    {
        byte[] bytes;

        if (!TryParseHexString(s, out bytes))
            throw new FormatException("String should contain only hexadecimal digits.");

        int a = (bytes[offset] << 24) | (bytes[offset + 1] << 16) | (bytes[offset + 2] << 8) | bytes[offset + 3];
        int b = (bytes[offset + 4] << 24) | (bytes[offset + 5] << 16) | (bytes[offset + 6] << 8) | bytes[offset + 7];
        int c = (bytes[offset + 8] << 24) | (bytes[offset + 9] << 16) | (bytes[offset + 10] << 8) | bytes[offset + 11];

        return new(a, b, c);
    }
}
