
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

using CamusDB.Core;

namespace CamusDB.Core.SQLParser;

/// <summary>
/// The single authority on how a SQL string literal maps to and from its value.
///
/// <para>The dialect has two literal forms, following PostgreSQL:</para>
/// <list type="bullet">
///   <item><b>Plain</b> — <c>'…'</c> or <c>"…"</c>. No escape processing at all: a backslash is an
///     ordinary character and the only special sequence is a doubled delimiter. This is what makes
///     <c>'\d+'</c> a usable regex and <c>'C:\Users'</c> a usable path without doubling anything.</item>
///   <item><b>Escape</b> — <c>E'…'</c> or <c>E"…"</c>. Backslash introduces an escape, so control
///     characters have a spelling. Only needed for values the plain form cannot carry.</item>
/// </list>
///
/// <para><see cref="Decode"/> and <see cref="Quote"/> are exact inverses over <em>values</em>: every
/// string survives <c>Quote</c> → lex → <c>Decode</c> unchanged, including one holding control
/// characters, a trailing backslash, or both quote characters. That round-trip is what makes
/// <c>SHOW CREATE TABLE</c> output re-executable and a dump reloadable, so every site that renders a
/// value back into SQL must go through <see cref="Quote"/> rather than hand-rolling a
/// <c>Replace("'", "''")</c>. Note the inverse holds value→literal→value, not the other way: a value
/// with no control characters has both a plain and an escape spelling, and <c>Quote</c> picks the
/// plain one.</para>
///
/// <para>The escape set mirrors the lexer's <c>EscString</c>/<c>EscStringSingle</c> productions (see
/// <c>SQLParser.Language.analyzer.lex</c>). An unrecognized escape yields the character itself, as in
/// PostgreSQL — the strict alternative would reject <c>E'\d'</c>, and since the plain form already
/// serves that need there is nothing to gain by failing here.</para>
/// </summary>
public static class SqlStringLiteral
{
    /// <summary>Named single-character escapes, in both directions.</summary>
    private const string EscapeChars = "\\'\"0abfnrtv";
    private const string EscapeValues = "\\'\"\0\a\b\f\n\r\t\v";

    /// <summary>
    /// Decodes a string-literal token — <c>yytext</c>, outer quotes and any <c>E</c> prefix
    /// included — into its value. The prefix is what selects the rules, so this must be given the
    /// token exactly as the lexer produced it.
    ///
    /// <para>Input that is not a quoted token is returned as-is; callers pass AST text that is
    /// occasionally already bare (an identifier position, a synthesized node).</para>
    /// </summary>
    /// <exception cref="CamusDBException">An escape-form token contains a truncated numeric escape
    /// or an unpaired surrogate.</exception>
    public static string Decode(string raw)
    {
        if (raw.Length >= 3 && (raw[0] == 'E' || raw[0] == 'e') && IsDelimiter(raw[1]) && raw[^1] == raw[1])
            return DecodeEscaped(raw, 1);

        if (raw.Length >= 2 && raw[0] == raw[^1] && IsDelimiter(raw[0]))
            return DecodePlain(raw, 0);

        return raw;
    }

    /// <summary>
    /// Decodes a plain literal: the body is taken verbatim apart from a doubled delimiter, which
    /// stands for one delimiter character. A backslash is an ordinary character here.
    /// </summary>
    private static string DecodePlain(string raw, int start)
    {
        char quote = raw[start];
        StringBuilder decoded = new(raw.Length - start - 2);

        for (int i = start + 1; i < raw.Length - 1; i++)
        {
            decoded.Append(raw[i]);

            if (raw[i] == quote && i + 1 < raw.Length - 1 && raw[i + 1] == quote)
                i++;
        }

        return decoded.ToString();
    }

    /// <summary>Decodes an <c>E'…'</c> literal, where a backslash introduces an escape.</summary>
    private static string DecodeEscaped(string raw, int start)
    {
        char quote = raw[start];
        StringBuilder decoded = new(raw.Length - start - 2);

        for (int i = start + 1; i < raw.Length - 1; i++)
        {
            char c = raw[i];

            if (c == quote && i + 1 < raw.Length - 1 && raw[i + 1] == quote)
            {
                decoded.Append(quote);
                i++;
                continue;
            }

            if (c != '\\')
            {
                decoded.Append(c);
                continue;
            }

            i = DecodeEscape(raw, i, decoded);
        }

        ValidateSurrogates(decoded);

        return decoded.ToString();
    }

    /// <summary>
    /// Decodes the escape starting at the backslash at <paramref name="start"/> into
    /// <paramref name="decoded"/> and returns the index of its final character, so the caller's loop
    /// increment lands on the next one.
    ///
    /// <para>Octal is tried before the named escapes so that <c>\012</c> is one octal escape rather
    /// than <c>\0</c> followed by two digits — the same longest-match rule the lexer applies.</para>
    /// </summary>
    private static int DecodeEscape(string raw, int start, StringBuilder decoded)
    {
        int last = raw.Length - 2;

        if (start + 1 > last)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "String literal ends with an incomplete escape");

        char kind = raw[start + 1];

        if (IsOctal(kind) && start + 3 <= last && IsOctal(raw[start + 2]) && IsOctal(raw[start + 3]))
        {
            int value = ((kind - '0') << 6) | ((raw[start + 2] - '0') << 3) | (raw[start + 3] - '0');
            decoded.Append((char)value);
            return start + 3;
        }

        switch (kind)
        {
            case 'x': return DecodeHex(raw, start, 2, decoded);
            case 'u': return DecodeHex(raw, start, 4, decoded);
            case 'U': return DecodeHex(raw, start, 8, decoded);
        }

        int named = EscapeChars.IndexOf(kind);

        // An unrecognized escape yields the character itself, matching PostgreSQL's E'…' rule.
        decoded.Append(named < 0 ? kind : EscapeValues[named]);
        return start + 1;
    }

    /// <summary>Decodes a fixed-width hex escape (<c>\xHH</c>, <c>\uHHHH</c>, <c>\UHHHHHHHH</c>).</summary>
    private static int DecodeHex(string raw, int start, int digits, StringBuilder decoded)
    {
        int last = raw.Length - 2;

        if (start + 1 + digits > last)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Escape '\\{raw[start + 1]}' in string literal needs {digits} hex digits");

        // Accumulated as long, not int: \UHHHHHHHH permits eight digits, and a signed 32-bit
        // accumulator wraps negative above 0x7FFFFFFF — which would slip past the range check below
        // and get cast to an unrelated UTF-16 code unit instead of being rejected.
        long value = 0;

        for (int i = start + 2; i <= start + 1 + digits; i++)
        {
            int digit = HexDigit(raw[i]);

            if (digit < 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Escape '\\{raw[start + 1]}' in string literal has a non-hex digit '{raw[i]}'");

            value = (value << 4) | (uint)digit;
        }

        if (value > 0x10FFFF)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Escape '\\{raw[start + 1]}' in string literal is outside the Unicode range");

        // Above the BMP the value needs a surrogate pair; below it, appending the char directly also
        // lets a deliberately-written lone surrogate through to the pairing check.
        if (value > 0xFFFF)
            decoded.Append(char.ConvertFromUtf32((int)value));
        else
            decoded.Append((char)value);

        return start + 1 + digits;
    }

    /// <summary>
    /// Rejects an unpaired surrogate. Such a value cannot be encoded as UTF-8 by the row serializer,
    /// so it must not get past the parser — the failure would otherwise surface much later, as a
    /// corrupt row rather than a bad literal.
    /// </summary>
    private static void ValidateSurrogates(StringBuilder decoded)
    {
        for (int i = 0; i < decoded.Length; i++)
        {
            if (!char.IsSurrogate(decoded[i]))
                continue;

            bool paired = char.IsHighSurrogate(decoded[i])
                && i + 1 < decoded.Length
                && char.IsLowSurrogate(decoded[i + 1]);

            if (!paired)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "String literal contains an unpaired surrogate");

            i++;
        }
    }

    /// <summary>
    /// Renders a value as a literal that <see cref="Decode"/> maps back to the same value.
    ///
    /// <para>The plain form is preferred, so ordinary text — including backslashes — is emitted
    /// exactly as it reads. Only a value carrying a control character, which the lexer's plain string
    /// body excludes, falls back to <c>E'…'</c>.</para>
    /// </summary>
    public static string Quote(string value)
    {
        return NeedsEscapeForm(value) ? QuoteEscaped(value) : QuotePlain(value);
    }

    /// <summary>
    /// True when the value holds a character the plain literal body cannot carry — only a control
    /// character, which the lexer excludes outright.
    ///
    /// <para>Characters above the BMP are <em>not</em> in this set: the scanner runs with the unicode
    /// option, so a surrogate pair lexes inside a plain literal and an emoji stays readable in
    /// <c>SHOW CREATE TABLE</c> output rather than being expanded into escapes.</para>
    /// </summary>
    private static bool NeedsEscapeForm(string value)
    {
        foreach (char c in value)
        {
            if (char.IsControl(c))
                return true;
        }

        return false;
    }

    private static string QuotePlain(string value)
    {
        StringBuilder sb = new(value.Length + 2);

        sb.Append('\'');

        foreach (char c in value)
        {
            if (c == '\'')
                sb.Append('\'');

            sb.Append(c);
        }

        return sb.Append('\'').ToString();
    }

    private static string QuoteEscaped(string value)
    {
        // A control character expands to \uHHHH — six characters — so sizing at value.Length would
        // force repeated growth of the backing buffer for control-heavy text. Reserve the worst case
        // plus the E'…' wrapper; this path only runs for values that already contain a control
        // character, so the over-reservation is bounded and rare.
        StringBuilder sb = new((value.Length * 6) + 3);

        sb.Append("E'");

        foreach (char c in value)
        {
            int named = EscapeValues.IndexOf(c);

            // '"' is in the escape table for decoding, but needs no escape inside a single-quoted
            // literal and only hurts readability there.
            if (named >= 0 && c != '"')
            {
                sb.Append('\\').Append(EscapeChars[named]);
                continue;
            }

            if (char.IsControl(c))
            {
                sb.Append("\\u").Append(((int)c).ToString("x4"));
                continue;
            }

            sb.Append(c);
        }

        return sb.Append('\'').ToString();
    }

    /// <summary>
    /// Decodes an <c>X'4D5A'</c> hex-string token into its bytes. Case-insensitive on both the
    /// prefix and the digits; <c>X''</c> is the empty byte string.
    ///
    /// <para>An odd digit count is rejected rather than padded: padding would have to guess whether
    /// the missing nibble belongs at the front or the back, and either guess silently produces bytes
    /// the author did not write.</para>
    /// </summary>
    /// <exception cref="CamusDBException">The token is not a well-formed hex string.</exception>
    public static byte[] DecodeBytes(string raw)
    {
        if (raw.Length < 3 || (raw[0] != 'X' && raw[0] != 'x') || raw[1] != '\'' || raw[^1] != '\'')
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Malformed bytes literal: {raw}");

        int digits = raw.Length - 3;

        if (digits % 2 != 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Bytes literal has an odd number of hex digits ({digits}); each byte needs two");

        byte[] result = new byte[digits / 2];

        for (int i = 0; i < result.Length; i++)
        {
            int hi = HexDigit(raw[2 + (i * 2)]);
            int lo = HexDigit(raw[3 + (i * 2)]);

            if (hi < 0 || lo < 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Bytes literal contains a non-hex digit: {raw}");

            result[i] = (byte)((hi << 4) | lo);
        }

        return result;
    }

    /// <summary>
    /// Renders bytes as an <c>X'…'</c> literal — the form that re-parses back to
    /// <see cref="Catalogs.Models.ColumnType.Bytes"/> without depending on a target column type to
    /// coerce it.
    /// </summary>
    public static string QuoteBytes(ReadOnlySpan<byte> value) => "X'" + Convert.ToHexString(value) + "'";

    private static bool IsDelimiter(char c) => c == '\'' || c == '"';

    private static bool IsOctal(char c) => c >= '0' && c <= '7';

    private static int HexDigit(char c) => c switch
    {
        >= '0' and <= '9' => c - '0',
        >= 'a' and <= 'f' => c - 'a' + 10,
        >= 'A' and <= 'F' => c - 'A' + 10,
        _ => -1
    };
}
