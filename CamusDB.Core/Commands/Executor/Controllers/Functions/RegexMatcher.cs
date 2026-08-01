
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// Thread-safe compiled-regex cache and match helper for the <c>~</c>, <c>~*</c>, <c>!~</c>,
/// and <c>!~*</c> operators, and the <c>regexp_*</c> scalar functions. Caches up to
/// <see cref="CamusDBOptions.RegexCacheMaxEntries"/> compiled <see cref="Regex"/> instances
/// keyed by <c>(pattern, RegexOptions)</c>. When full, new patterns are compiled and evaluated
/// without being stored — queries never fail because the cache is full. Every match runs under
/// <see cref="CamusDBOptions.RegexMatchTimeoutMs"/> to guard against ReDoS on pathological
/// patterns.
///
/// <para>The underlying regex engine is .NET <see cref="System.Text.RegularExpressions"/>,
/// not POSIX ERE. It is a superset for common constructs (character classes, quantifiers,
/// anchors, alternation, groups). Users should use <c>\p{L}</c> / <c>[a-zA-Z]</c> rather
/// than POSIX named classes like <c>[[:alpha:]]</c>.</para>
///
/// <para>Matching is unanchored — the pattern matches if it occurs anywhere in the subject,
/// matching PostgreSQL semantics. Anchor explicitly with <c>^</c> / <c>$</c>.</para>
/// </summary>
internal static class RegexMatcher
{
    // Cache keyed by (pattern, RegexOptions) so functions using the full options set
    // share the same compiled Regex as the operators that used the old bool-ci key.
    private static readonly ConcurrentDictionary<(string pattern, RegexOptions opts), Regex> Cache = new();

    /// <summary>
    /// Evaluates whether <paramref name="subject"/> contains a match for <paramref name="pattern"/>.
    /// Throws <see cref="CamusDBException"/> with <see cref="CamusDBErrorCodes.InvalidInput"/>
    /// when the pattern is malformed or the match exceeds the configured timeout.
    /// </summary>
    public static bool IsMatch(string subject, string pattern, bool ignoreCase)
    {
        RegexOptions opts = RegexOptions.CultureInvariant;
        if (ignoreCase) opts |= RegexOptions.IgnoreCase;
        Regex re = GetOrCompile(pattern, opts);
        try
        {
            return re.IsMatch(subject);
        }
        catch (RegexMatchTimeoutException)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Regular expression match exceeded the {CamusDBConfig.RegexMatchTimeoutMs}ms time limit");
        }
    }

    /// <summary>
    /// Returns the compiled <see cref="Regex"/> for <paramref name="pattern"/> with the given
    /// <paramref name="opts"/> (always OR-ed with <c>CultureInvariant</c>). Used by the
    /// <c>regexp_*</c> scalar functions so they can call <c>.Match</c>, <c>.Matches</c>,
    /// <c>.Replace</c>, and <c>.Split</c> directly while sharing the same cache and timeout.
    /// </summary>
    public static Regex GetRegex(string pattern, RegexOptions opts) =>
        GetOrCompile(pattern, opts | RegexOptions.CultureInvariant);

    /// <summary>
    /// Parses a PostgreSQL-style flags string into <see cref="RegexOptions"/> and a global flag.
    /// <para>Supported flags: <c>i</c> (ignore case), <c>c</c> (case-sensitive, cancels i),
    /// <c>m</c>/<c>n</c> (multiline), <c>s</c> (singleline / dot matches newline),
    /// <c>x</c> (ignore pattern whitespace), <c>g</c> (global — only meaningful for
    /// <c>regexp_replace</c> and <c>regexp_count</c>; not a <see cref="RegexOptions"/> flag).</para>
    /// Unknown flag characters produce an <see cref="CamusDBErrorCodes.InvalidInput"/> error.
    /// </summary>
    public static (RegexOptions Options, bool Global) ParseFlags(string? flags)
    {
        RegexOptions opts = RegexOptions.None;
        bool global = false;

        if (string.IsNullOrEmpty(flags))
            return (opts, global);

        foreach (char ch in flags)
        {
            switch (ch)
            {
                case 'i': opts |= RegexOptions.IgnoreCase; break;
                case 'c': opts &= ~RegexOptions.IgnoreCase; break;
                case 'm':
                case 'n': opts |= RegexOptions.Multiline; break;
                case 's': opts |= RegexOptions.Singleline; break;
                case 'x': opts |= RegexOptions.IgnorePatternWhitespace; break;
                case 'g': global = true; break;
                default:
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Invalid regular expression option: '{ch}'");
            }
        }

        return (opts, global);
    }

    /// <summary>
    /// Wraps a <see cref="RegexMatchTimeoutException"/> thrown by <paramref name="action"/> with an
    /// <see cref="CamusDBErrorCodes.InvalidInput"/> <see cref="CamusDBException"/>. Used by the
    /// scalar functions to give a consistent timeout error.
    /// </summary>
    public static T GuardTimeout<T>(Func<T> action)
    {
        try
        {
            return action();
        }
        catch (RegexMatchTimeoutException)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Regular expression match exceeded the {CamusDBConfig.RegexMatchTimeoutMs}ms time limit");
        }
    }

    /// <summary>
    /// Compiles <paramref name="pattern"/> (warming the cache as a side effect) and throws
    /// <see cref="CamusDBException"/> with <see cref="CamusDBErrorCodes.InvalidInput"/> if it is
    /// malformed. Called at DDL time so a bad regex in a CHECK constraint fails at CREATE/ALTER
    /// rather than at the first INSERT.
    /// </summary>
    public static void ValidatePattern(string pattern, bool ignoreCase)
    {
        RegexOptions opts = RegexOptions.CultureInvariant;
        if (ignoreCase) opts |= RegexOptions.IgnoreCase;
        GetOrCompile(pattern, opts);
    }

    private static Regex GetOrCompile(string pattern, RegexOptions opts)
    {
        // CultureInvariant is always forced; callers may or may not include it already.
        opts |= RegexOptions.CultureInvariant;

        if (Cache.TryGetValue((pattern, opts), out Regex? cached))
            return cached;

        Regex re;
        try
        {
            re = new Regex(pattern, opts, TimeSpan.FromMilliseconds(CamusDBConfig.RegexMatchTimeoutMs));
        }
        catch (ArgumentException ex)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Invalid regular expression: {ex.Message}");
        }

        if (Cache.Count < CamusDBConfig.RegexCacheMaxEntries)
            Cache.TryAdd((pattern, opts), re);

        return re;
    }
}
