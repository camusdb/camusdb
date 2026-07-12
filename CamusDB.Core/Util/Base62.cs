
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Util;

/// <summary>
/// Encodes non-negative integers as short base-62 strings using the alphabet
/// <c>0–9 A–Z a–z</c>. The output contains none of the key-separator characters
/// (<c>/</c>, <c>:</c>, <c>~</c>), so it is safe to embed verbatim in any KV key position.
/// Used to produce compact, globally-unique ids for databases and tables that are
/// dramatically shorter than 24-hex ObjectIds.
/// </summary>
public static class Base62
{
    private const string Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    /// <summary>
    /// Encodes <paramref name="value"/> as a base-62 string. Returns the shortest
    /// representation with no leading zeros (value 1 → "1", value 62 → "A0").
    /// Always at least one character. Non-positive values encode as "0".
    /// </summary>
    public static string Encode(long value)
    {
        if (value <= 0)
            return "0";

        Span<char> buf = stackalloc char[11]; // ceil(log₆₂(long.MaxValue)) ≤ 11
        int pos = 11;
        while (value > 0)
        {
            buf[--pos] = Alphabet[(int)(value % 62)];
            value /= 62;
        }
        return new string(buf[pos..]);
    }
}
