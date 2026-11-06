
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Counts and slices a string by Unicode code point, the unit that SQL text functions and a
/// <c>CHAR(n)</c> limit count in, as in PostgreSQL. A .NET string is UTF-16, so a character outside the
/// Basic Multilingual Plane takes two units (a surrogate pair). A slice by unit can cut such a pair, and
/// half of a pair has no UTF-8 form, so a write could not store the value.
///
/// <para>Every method first searches for a surrogate, because almost no text holds one, and that search
/// is vectorized. Text with no surrogate costs one such search and no loop. An unpaired surrogate counts
/// as one code point: it cannot come from a literal or from storage, but it can come from a parameter.</para>
///
/// <para>The sort order of strings stays by UTF-16 unit. That is a separate rule, and every index key
/// depends on it.</para>
/// </summary>
internal static class TextCodePoints
{
    private const char FirstSurrogate = '\uD800';

    private const char LastSurrogate = '\uDFFF';

    /// <summary>True when the text holds any surrogate, paired or not.</summary>
    public static bool HasSurrogate(ReadOnlySpan<char> text)
        => text.IndexOfAnyInRange(FirstSurrogate, LastSurrogate) >= 0;

    /// <summary>The number of code points in the text.</summary>
    public static int Count(ReadOnlySpan<char> text)
    {
        int first = text.IndexOfAnyInRange(FirstSurrogate, LastSurrogate);

        if (first < 0)
            return text.Length;

        int count = first;

        for (int i = first; i < text.Length; i += UnitsAt(text, i))
            count++;

        return count;
    }

    /// <summary>
    /// The length to compare with a column limit of <paramref name="limit"/> characters. A text with no
    /// more units than the limit is inside it, because a code point takes at least one unit, so a value
    /// that fits costs no search. The result is an exact code point count only above the limit, which is
    /// the only case where a caller reports it.
    /// </summary>
    public static int LengthForLimit(string text, int limit)
        => text.Length > limit ? Count(text) : text.Length;

    /// <summary>
    /// The UTF-16 offset of the code point at the 0-based <paramref name="codePoints"/>, or the length of
    /// the text when the text is shorter. The offset is never inside a pair of the text.
    /// </summary>
    public static int UnitOffset(ReadOnlySpan<char> text, long codePoints)
    {
        int first = text.IndexOfAnyInRange(FirstSurrogate, LastSurrogate);

        if (first < 0 || codePoints <= first)
            return (int)Math.Min(codePoints, first < 0 ? text.Length : first);

        long remaining = codePoints - first;
        int offset = first;

        while (remaining > 0 && offset < text.Length)
        {
            offset += UnitsAt(text, offset);
            remaining--;
        }

        return offset;
    }

    /// <summary>True when the text holds half of a surrogate pair.</summary>
    public static bool HasUnpairedSurrogate(ReadOnlySpan<char> text)
    {
        int first = text.IndexOfAnyInRange(FirstSurrogate, LastSurrogate);

        if (first < 0)
            return false;

        for (int i = first; i < text.Length; i++)
        {
            if (!char.IsSurrogate(text[i]))
                continue;

            if (UnitsAt(text, i) == 1)
                return true;

            i++;
        }

        return false;
    }

    /// <summary>True when <paramref name="offset"/> falls between the two units of a pair.</summary>
    public static bool IsInsidePair(ReadOnlySpan<char> text, int offset)
        => offset > 0 && offset < text.Length && char.IsHighSurrogate(text[offset - 1]) && char.IsLowSurrogate(text[offset]);

    /// <summary>The number of units of the code point that starts at <paramref name="offset"/>: 2 for a pair and 1 otherwise.</summary>
    public static int UnitsAt(ReadOnlySpan<char> text, int offset)
        => char.IsHighSurrogate(text[offset]) && offset + 1 < text.Length && char.IsLowSurrogate(text[offset + 1]) ? 2 : 1;

    /// <summary>The number of units of the code point that ends just before <paramref name="end"/>: 2 for a pair and 1 otherwise.</summary>
    public static int UnitsBefore(ReadOnlySpan<char> text, int end)
        => end >= 2 && char.IsLowSurrogate(text[end - 1]) && char.IsHighSurrogate(text[end - 2]) ? 2 : 1;
}
