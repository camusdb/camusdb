
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The one SQL pattern matcher, used by <c>LIKE</c>, <c>ILIKE</c>, <c>NOT LIKE</c>, a CHECK condition and
/// every <c>SHOW ... LIKE</c>. It follows PostgreSQL: <c>%</c> matches any run of characters, <c>_</c>
/// matches one character, and a backslash makes the next pattern character a literal.
///
/// <para><b>Characters.</b> <c>_</c> and the search after <c>%</c> move by code point, so <c>_</c> never
/// matches half of a surrogate pair and <c>'🙂' LIKE '__'</c> is false. Literals compare by UTF-16 unit,
/// because text and pattern move together there. Half of a pair, which only a parameter can carry,
/// counts as one character.</para>
///
/// <para><b>An error that depends on the text.</b> A pattern that ends with the escape character fails
/// only when the match reaches that character, as in PostgreSQL. So <c>'b' LIKE 'a\'</c> is false, and
/// <c>'a\' LIKE 'a\'</c> fails. A statement mistake usually does not depend on the rows, but this one is a
/// value error of the pattern, and the pattern can come from a column. The cardinality estimate reads a
/// pattern with no row, so it uses <see cref="MatchesForEstimate"/>, which never raises this error.</para>
///
/// <para><b>Why no regular expression.</b> The older path that built one had no single-line option, so
/// <c>.*</c> stopped at a newline, and the answer depended on the shape of the pattern. The walk has no
/// such case, needs no timeout, and costs at most text length times pattern length, because only the
/// last <c>%</c> can be retried: text that a later <c>%</c> skipped cannot help an earlier one.</para>
///
/// <para><b>ILIKE.</b> Case folding is <see cref="StringComparison.OrdinalIgnoreCase"/>, so it also folds
/// non-ASCII letters. PostgreSQL in the C locale folds only ASCII. This is a decided difference.</para>
/// </summary>
internal static class LikeMatcher
{
    private const char Escape = '\\';

    private const char AnyRun = '%';

    private const char AnyOne = '_';

    private enum WalkResult : byte
    {
        NoMatch,
        Match,
        PatternEndsWithEscape,
    }

    /// <summary>
    /// True when <paramref name="text"/> matches <paramref name="pattern"/>. Throws
    /// <see cref="CamusDBErrorCodes.InvalidInput"/> when the match reaches an escape character with
    /// nothing after it.
    /// </summary>
    public static bool Matches(string text, string pattern, bool ignoreCase)
    {
        if (TryMatchWithoutWalk(text, pattern, ignoreCase, out bool matched))
            return matched;

        return Walk(text, pattern, ignoreCase) switch
        {
            WalkResult.Match => true,
            WalkResult.NoMatch => false,
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "LIKE pattern must not end with escape character"),
        };
    }

    /// <summary>
    /// <see cref="Matches"/> for a histogram bound. The estimate reads values that the statement may never
    /// reach, so a pattern that would fail counts as no match, and only a row can raise the error.
    /// </summary>
    public static bool MatchesForEstimate(string text, string pattern, bool ignoreCase)
    {
        if (TryMatchWithoutWalk(text, pattern, ignoreCase, out bool matched))
            return matched;

        return Walk(text, pattern, ignoreCase) == WalkResult.Match;
    }

    /// <summary>
    /// The result of the walk alone, with no shortcut. A test compares it with <see cref="Matches"/>, so a
    /// shortcut cannot change an answer.
    /// </summary>
    internal static bool? WalkOnly(string text, string pattern, bool ignoreCase) =>
        Walk(text, pattern, ignoreCase) switch
        {
            WalkResult.Match => true,
            WalkResult.NoMatch => false,
            _ => null,
        };

    /// <summary>
    /// The common patterns need no walk: a pattern with no <c>_</c> and no escape character is an equality,
    /// a prefix, a suffix or a search. These comparisons are vectorized. They compare by unit at any offset,
    /// so half of a pair in the pattern could match inside a pair of the text. Such a pattern goes to the
    /// walk, which counts it as one character.
    /// </summary>
    private static bool TryMatchWithoutWalk(string text, string pattern, bool ignoreCase, out bool matched)
    {
        ReadOnlySpan<char> value = pattern;
        matched = false;

        if (value.IndexOfAny(AnyOne, Escape) >= 0 || TextCodePoints.HasUnpairedSurrogate(value))
            return false;

        StringComparison comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        int first = value.IndexOf(AnyRun);

        if (first < 0)
        {
            matched = text.Equals(pattern, comparison);
            return true;
        }

        int last = value.LastIndexOf(AnyRun);

        if (first == last)
        {
            if (last == value.Length - 1)
            {
                matched = text.AsSpan().StartsWith(value[..last], comparison);
                return true;
            }

            if (first == 0)
            {
                matched = text.AsSpan().EndsWith(value[1..], comparison);
                return true;
            }

            return false;
        }

        if (first == 0 && last == value.Length - 1 && value[1..last].IndexOf(AnyRun) < 0)
        {
            matched = text.AsSpan().Contains(value[1..last], comparison);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Walks text and pattern together. A <c>%</c> records a place to come back to, and a mismatch after it
    /// moves that place one code point forward. The end of the text with pattern left is final, because
    /// the rest of the pattern needs at least one character and a later place has fewer.
    /// </summary>
    private static WalkResult Walk(ReadOnlySpan<char> text, ReadOnlySpan<char> pattern, bool ignoreCase)
    {
        int t = 0;
        int p = 0;
        int retryText = -1;
        int retryPattern = -1;

        while (true)
        {
            bool mismatch = false;

            while (t < text.Length && p < pattern.Length)
            {
                char current = pattern[p];

                if (current == AnyRun)
                {
                    p++;

                    // A run of % and _ is N characters and then any run, so _ consumes its character here.
                    while (p < pattern.Length)
                    {
                        if (pattern[p] == AnyRun)
                        {
                            p++;
                        }
                        else if (pattern[p] == AnyOne)
                        {
                            if (t >= text.Length)
                                return WalkResult.NoMatch;

                            t += TextCodePoints.UnitsAt(text, t);
                            p++;
                        }
                        else
                        {
                            break;
                        }
                    }

                    if (p == pattern.Length)
                        return WalkResult.Match;

                    if (pattern[p] == Escape && p + 1 == pattern.Length)
                        return WalkResult.PatternEndsWithEscape;

                    retryText = t;
                    retryPattern = p;
                    continue;
                }

                if (current == AnyOne)
                {
                    t += TextCodePoints.UnitsAt(text, t);
                    p++;
                    continue;
                }

                if (current == Escape)
                {
                    p++;

                    if (p == pattern.Length)
                        return WalkResult.PatternEndsWithEscape;
                }

                int units = CompareCharacter(text, t, pattern, p, ignoreCase);

                if (units == 0)
                {
                    mismatch = true;
                    break;
                }

                t += units;
                p += units;
            }

            // The pattern ended with text left: this is a mismatch too.
            if (!mismatch && t < text.Length)
                mismatch = true;

            if (!mismatch)
            {
                while (p < pattern.Length && pattern[p] == AnyRun)
                    p++;

                return p == pattern.Length ? WalkResult.Match : WalkResult.NoMatch;
            }

            if (retryPattern < 0)
                return WalkResult.NoMatch;

            retryText += TextCodePoints.UnitsAt(text, retryText);

            if (retryText >= text.Length)
                return WalkResult.NoMatch;

            t = retryText;
            p = retryPattern;
        }
    }

    /// <summary>
    /// The number of units that match at these offsets, or 0 for no match. With <paramref name="ignoreCase"/>
    /// a pair compares as one character, because a letter outside the Basic Multilingual Plane folds as a
    /// pair and a unit alone does not fold.
    /// </summary>
    private static int CompareCharacter(ReadOnlySpan<char> text, int t, ReadOnlySpan<char> pattern, int p, bool ignoreCase)
    {
        if (!ignoreCase)
            return text[t] == pattern[p] ? 1 : 0;

        int units = TextCodePoints.UnitsAt(text, t);

        if (units != TextCodePoints.UnitsAt(pattern, p))
            return 0;

        return text.Slice(t, units).Equals(pattern.Slice(p, units), StringComparison.OrdinalIgnoreCase) ? units : 0;
    }

    /// <summary>
    /// Rewrites a pattern with an ESCAPE character into the backslash form, which is how <c>x LIKE p ESCAPE e</c>
    /// runs: the parser builds <c>x LIKE like_escape(p, e)</c>, as PostgreSQL does. So the matcher knows one
    /// escape character, and no expression walk needs a third operand.
    ///
    /// <para>An empty <paramref name="escape"/> means no escape character, so each backslash doubles and acts as
    /// a literal. Otherwise each escape character becomes a backslash, and a backslash doubles unless it
    /// directly follows an escape character. An escape string of more than one code point is refused.</para>
    /// </summary>
    public static string RewriteEscape(string pattern, string escape)
    {
        if (escape.Length == 0)
            return pattern.Replace("\\", "\\\\", StringComparison.Ordinal);

        if (TextCodePoints.Count(escape) != 1)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Invalid escape string: an ESCAPE string must be empty or one character");

        if (escape[0] == Escape)
            return pattern;

        StringBuilder rewritten = new(pattern.Length + 1);
        bool afterEscape = false;
        int offset = 0;

        while (offset < pattern.Length)
        {
            int units = TextCodePoints.UnitsAt(pattern, offset);
            ReadOnlySpan<char> character = pattern.AsSpan(offset, units);

            if (!afterEscape && character.SequenceEqual(escape))
            {
                rewritten.Append(Escape);
                afterEscape = true;
            }
            else if (character[0] == Escape)
            {
                rewritten.Append(Escape);

                if (!afterEscape)
                    rewritten.Append(Escape);

                afterEscape = false;
            }
            else
            {
                rewritten.Append(character);
                afterEscape = false;
            }

            offset += units;
        }

        return rewritten.ToString();
    }
}
