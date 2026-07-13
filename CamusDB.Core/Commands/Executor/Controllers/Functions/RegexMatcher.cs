
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
/// and <c>!~*</c> operators. Caches up to <see cref="CamusDBConfig.RegexCacheMaxEntries"/>
/// compiled <see cref="Regex"/> instances keyed by <c>(pattern, ignoreCase)</c>. When full,
/// new patterns are compiled and evaluated without being stored — queries never fail because
/// the cache is full. Every match runs under <see cref="CamusDBConfig.RegexMatchTimeoutMs"/>
/// to guard against ReDoS on pathological patterns.
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
    private static readonly ConcurrentDictionary<(string pattern, bool ci), Regex> Cache = new();

    /// <summary>
    /// Evaluates whether <paramref name="subject"/> contains a match for <paramref name="pattern"/>.
    /// Throws <see cref="CamusDBException"/> with <see cref="CamusDBErrorCodes.InvalidInput"/>
    /// when the pattern is malformed or the match exceeds the configured timeout.
    /// </summary>
    public static bool IsMatch(string subject, string pattern, bool ignoreCase)
    {
        Regex re = GetOrCompile(pattern, ignoreCase);
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
    /// Compiles <paramref name="pattern"/> (warming the cache as a side effect) and throws
    /// <see cref="CamusDBException"/> with <see cref="CamusDBErrorCodes.InvalidInput"/> if it is
    /// malformed. Called at DDL time so a bad regex in a CHECK constraint fails at CREATE/ALTER
    /// rather than at the first INSERT.
    /// </summary>
    public static void ValidatePattern(string pattern, bool ignoreCase) => GetOrCompile(pattern, ignoreCase);

    private static Regex GetOrCompile(string pattern, bool ignoreCase)
    {
        if (Cache.TryGetValue((pattern, ignoreCase), out Regex? cached))
            return cached;

        RegexOptions opts = RegexOptions.CultureInvariant;
        if (ignoreCase)
            opts |= RegexOptions.IgnoreCase;

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
            Cache.TryAdd((pattern, ignoreCase), re);

        return re;
    }
}
