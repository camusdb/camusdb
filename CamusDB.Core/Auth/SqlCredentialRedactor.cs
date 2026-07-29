/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace CamusDB.Core.Auth;

/// <summary>
/// Redacts the cleartext password out of <c>CREATE USER</c> / <c>ALTER USER … IDENTIFIED … BY …</c>
/// SQL before it reaches any log, trace, or metric. A statement text is otherwise safe to log, but the
/// password literal is a credential — a request body containing <c>IDENTIFIED BY 'secret'</c> must never
/// place that secret in application logs. Every transport must pass SQL through this before
/// <c>LogExecutingSql</c>/request-body logging.
///
/// <para>Only the literal after <c>BY</c> is replaced (with <c>'***'</c>); a parameterized password
/// (<c>BY @p</c>) has no literal here and is left alone — the caller is responsible for never logging
/// the bound parameter values of a credential-bearing statement, which this class cannot see.</para>
///
/// <para>This is a hand-written scan rather than a regular expression, and that is deliberate. It is a
/// <em>second</em> reader of the dialect's literal syntax, and every time it has been expressed as a
/// regex it has silently fallen behind the lexer — first missing backslash escapes, then missing the
/// double-quoted, <c>E"…"</c>, and backtick-quoted-plugin forms that the grammar accepts
/// (<c>auth_secret : string | placeholder</c>, plugin is <c>any_identifier</c>). Falling behind here
/// does not fail a test, it leaks a password. The scan below therefore accepts <b>every</b> literal
/// shape the lexer produces, and errs toward over-redaction: an unterminated literal redacts to the
/// end of the input rather than leaving a tail behind.</para>
///
/// <para>Redaction runs before validation, so it must also handle statements that will later be
/// rejected — a bad statement's password is still a password.</para>
/// </summary>
public static class SqlCredentialRedactor
{
    private const string Mask = "'***'";

    public static string Redact(string? sql)
    {
        if (string.IsNullOrEmpty(sql))
            return sql ?? "";

        // Cheap guard: only pay for the scan when the statement could carry a credential literal.
        if (sql.IndexOf("identified", StringComparison.OrdinalIgnoreCase) < 0)
            return sql;

        StringBuilder? result = null;
        int copied = 0;
        int i = 0;

        while (i < sql.Length)
        {
            int keywordEnd = MatchIdentifiedBy(sql, i);

            if (keywordEnd < 0)
            {
                i++;
                continue;
            }

            int literalEnd = SkipLiteral(sql, keywordEnd);

            if (literalEnd < 0)
            {
                // Not a literal (a placeholder, or something unparseable) — leave it as-is.
                i = keywordEnd;
                continue;
            }

            result ??= new StringBuilder(sql.Length);
            result.Append(sql, copied, keywordEnd - copied).Append(Mask);
            copied = literalEnd;
            i = literalEnd;
        }

        if (result is null)
            return sql;

        return result.Append(sql, copied, sql.Length - copied).ToString();
    }

    /// <summary>
    /// Matches <c>IDENTIFIED [WITH &lt;plugin&gt;] BY</c> starting at <paramref name="start"/> and
    /// returns the index just past the whitespace following <c>BY</c>, or -1 if it does not match.
    /// The plugin is any identifier form the grammar accepts, including a backtick-quoted one.
    /// </summary>
    private static int MatchIdentifiedBy(string sql, int start)
    {
        int i = SkipKeyword(sql, start, "IDENTIFIED");

        if (i < 0)
            return -1;

        i = SkipWhitespace(sql, i);

        int afterWith = SkipKeyword(sql, i, "WITH");

        if (afterWith >= 0)
        {
            i = SkipWhitespace(sql, afterWith);
            i = SkipPluginName(sql, i);

            if (i < 0)
                return -1;

            i = SkipWhitespace(sql, i);
        }

        int afterBy = SkipKeyword(sql, i, "BY");

        if (afterBy < 0)
            return -1;

        return SkipWhitespace(sql, afterBy);
    }

    /// <summary>
    /// Skips the authentication-plugin name: a backtick-quoted identifier or a bare one. Returns -1
    /// when neither is present, so a malformed clause does not cause the scan to mask the wrong span.
    /// </summary>
    private static int SkipPluginName(string sql, int start)
    {
        if (start >= sql.Length)
            return -1;

        if (sql[start] == '`')
        {
            int close = sql.IndexOf('`', start + 1);
            return close < 0 ? -1 : close + 1;
        }

        int i = start;

        while (i < sql.Length && (char.IsLetterOrDigit(sql[i]) || sql[i] == '_'))
            i++;

        return i == start ? -1 : i;
    }

    /// <summary>
    /// Skips a complete string literal at <paramref name="start"/> and returns the index just past it,
    /// or -1 when there is no literal there (a placeholder, an identifier, end of input).
    ///
    /// <para>Covers every form the lexer produces: either delimiter, with or without the <c>E</c>
    /// prefix. In an <c>E</c> literal a backslash consumes the next character; in a plain literal it
    /// does not. A doubled delimiter is an embedded delimiter in both. An unterminated literal returns
    /// the end of the input, so the tail is masked rather than logged.</para>
    /// </summary>
    private static int SkipLiteral(string sql, int start)
    {
        int i = start;
        bool escapeForm = false;

        if (i < sql.Length && (sql[i] == 'E' || sql[i] == 'e') && i + 1 < sql.Length && IsDelimiter(sql[i + 1]))
        {
            escapeForm = true;
            i++;
        }

        if (i >= sql.Length || !IsDelimiter(sql[i]))
            return -1;

        char quote = sql[i];
        i++;

        while (i < sql.Length)
        {
            char c = sql[i];

            if (escapeForm && c == '\\')
            {
                i += 2;
                continue;
            }

            if (c == quote)
            {
                if (i + 1 < sql.Length && sql[i + 1] == quote)
                {
                    i += 2;
                    continue;
                }

                return i + 1;
            }

            i++;
        }

        // Unterminated: mask through the end rather than leave a partial credential behind.
        return sql.Length;
    }

    /// <summary>
    /// Matches <paramref name="keyword"/> case-insensitively at <paramref name="start"/>, requiring a
    /// non-identifier character (or end of input) after it so <c>IDENTIFIEDX</c> does not match.
    /// Returns the index just past the keyword, or -1.
    /// </summary>
    private static int SkipKeyword(string sql, int start, string keyword)
    {
        if (start < 0 || start + keyword.Length > sql.Length)
            return -1;

        if (string.Compare(sql, start, keyword, 0, keyword.Length, StringComparison.OrdinalIgnoreCase) != 0)
            return -1;

        int end = start + keyword.Length;

        if (end < sql.Length && (char.IsLetterOrDigit(sql[end]) || sql[end] == '_'))
            return -1;

        return end;
    }

    private static int SkipWhitespace(string sql, int start)
    {
        int i = start;

        while (i < sql.Length && char.IsWhiteSpace(sql[i]))
            i++;

        return i;
    }

    private static bool IsDelimiter(char c) => c == '\'' || c == '"';
}
