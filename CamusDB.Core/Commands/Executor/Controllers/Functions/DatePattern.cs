
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Globalization;
using System.Text;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The two pattern languages a <see cref="DatePattern"/> compiles from. Both compile to the same
/// token list, so formatting and parsing behave the same way for the same field.
/// </summary>
internal enum DatePatternDialect
{
    /// <summary>PostgreSQL template patterns (<c>YYYY-MM-DD HH24:MI:SS</c>), used by <c>to_char</c>, <c>to_timestamp</c> and <c>to_date</c>.</summary>
    PostgreSql,

    /// <summary>MySQL <c>%</c> specifiers (<c>%Y-%m-%d %H:%i:%s</c>), used by <c>date_format</c> and <c>str_to_date</c>.</summary>
    MySql,
}

/// <summary>
/// A compiled date pattern: formats a UTC value to text and strictly parses text back to a UTC value.
/// </summary>
/// <remarks>
/// <para>A pattern is compiled once per distinct pattern string and dialect, through <see cref="Get"/>,
/// and then reused for every row. The pattern is nearly always a literal, so a small cache keyed by
/// the pattern text removes the compile from the row loop. The cache is bounded: it is cleared when it
/// is full, and a very long pattern is never cached.</para>
/// <para>All values are UTC. Month and day names are English only and do not depend on the server
/// culture. The time zone fields print <c>UTC</c> and <c>+00</c>.</para>
/// <para>Parsing is strict on purpose. Text that does not match the pattern is an error that names the
/// text and the pattern; nothing is guessed. A field that the other fields already decide (a day name,
/// a quarter, a week number) is checked against the result, not ignored. The documented differences
/// from PostgreSQL and MySQL are in <c>docs/date-patterns.md</c>.</para>
/// </remarks>
internal sealed class DatePattern
{
    private const int CacheCapacity = 256;

    private const int MaxCachedPatternLength = 256;

    private const int MaxRetainedBufferCapacity = 1024;

    /// <summary>The width a full month or day name pads to without <c>FM</c>: the length of "September" and "Wednesday".</summary>
    private const int FullNameWidth = 9;

    private static readonly ConcurrentDictionary<string, DatePattern> PostgreSqlCache = new(StringComparer.Ordinal);

    private static readonly ConcurrentDictionary<string, DatePattern> MySqlCache = new(StringComparer.Ordinal);

    [ThreadStatic]
    private static StringBuilder? formatBuffer;

    private static readonly string[] MonthNames =
        ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];

    /// <summary>Indexed by <see cref="DayOfWeek"/>, so Sunday is 0.</summary>
    private static readonly string[] DayNames =
        ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

    private static readonly string[][] MonthFull = CaseForms(MonthNames, 0);
    private static readonly string[][] MonthAbbr = CaseForms(MonthNames, 3);
    private static readonly string[][] DayFull = CaseForms(DayNames, 0);
    private static readonly string[][] DayAbbr = CaseForms(DayNames, 3);

    /// <summary>
    /// The PostgreSQL keywords, longest first, so <c>MONTH</c> wins over <c>MON</c> and <c>MM</c>, and
    /// <c>DDD</c> wins over <c>DD</c> and <c>D</c>. Matching is case-sensitive, as in PostgreSQL: a
    /// numeric field has an upper and a lower spelling, and a name field has three, which set the case
    /// of the printed name.
    /// </summary>
    private static readonly (string Text, DatePatternField Field, NameCase Case)[] PostgreSqlKeywords = BuildPostgreSqlKeywords();

    private readonly Token[] tokens;

    private DatePattern(string text, Token[] tokens)
    {
        Text = text;
        this.tokens = tokens;
    }

    /// <summary>The pattern as the caller wrote it. Error messages quote it.</summary>
    public string Text { get; }

    /// <summary>
    /// Returns the compiled form of <paramref name="pattern"/>, from the cache when possible.
    /// A pattern that does not compile throws <see cref="CamusDBErrorCodes.InvalidInput"/> and is
    /// not cached, so the next call reports the same error.
    /// </summary>
    public static DatePattern Get(string calledName, string pattern, DatePatternDialect dialect)
    {
        ConcurrentDictionary<string, DatePattern> cache = dialect == DatePatternDialect.PostgreSql ? PostgreSqlCache : MySqlCache;

        if (cache.TryGetValue(pattern, out DatePattern? compiled))
            return compiled;

        compiled = dialect == DatePatternDialect.PostgreSql
            ? CompilePostgreSql(calledName, pattern)
            : CompileMySql(calledName, pattern);

        if (pattern.Length <= MaxCachedPatternLength)
        {
            if (cache.Count >= CacheCapacity)
                cache.Clear();

            cache.TryAdd(pattern, compiled);
        }

        return compiled;
    }

    // ── Compile ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Compiles a PostgreSQL template. A keyword becomes a field; <c>FM</c> before a keyword turns off
    /// the zero and blank padding of that one field; <c>"text"</c> is printed and matched as written,
    /// with <c>\"</c> for a double quote; a run of spaces is its own token; any other character is
    /// literal text.
    /// </summary>
    private static DatePattern CompilePostgreSql(string calledName, string pattern)
    {
        List<Token> tokens = [];
        StringBuilder literal = new();
        int i = 0;

        while (i < pattern.Length)
        {
            char c = pattern[i];

            if (c == '"')
            {
                FlushLiteral(tokens, literal);
                i = ReadQuotedText(calledName, pattern, i, literal);
                FlushLiteral(tokens, literal);
                continue;
            }

            if (c == '\\' && i + 1 < pattern.Length && pattern[i + 1] == '"')
            {
                literal.Append('"');
                i += 2;
                continue;
            }

            if (c == ' ')
            {
                FlushLiteral(tokens, literal);
                int start = i;
                while (i < pattern.Length && pattern[i] == ' ')
                    i++;

                tokens.Add(Token.ForSpaces(pattern[start..i]));
                continue;
            }

            bool fill = true;
            int keywordStart = i;

            if (pattern.AsSpan(i).StartsWith("FM", StringComparison.Ordinal) || pattern.AsSpan(i).StartsWith("fm", StringComparison.Ordinal))
            {
                // FM changes the next field only. Without a field after it, PostgreSQL ignores it.
                fill = false;
                i += 2;

                if (!TryMatchKeyword(pattern, i, out _))
                    continue;
            }

            if (TryMatchKeyword(pattern, i, out (string Text, DatePatternField Field, NameCase Case) keyword))
            {
                FlushLiteral(tokens, literal);
                i += keyword.Text.Length;
                tokens.Add(new Token(keyword.Field, pattern[keywordStart..i], fill, keyword.Case, null));
                continue;
            }

            literal.Append(c);
            i++;
        }

        FlushLiteral(tokens, literal);
        return new DatePattern(pattern, tokens.ToArray());
    }

    private static int ReadQuotedText(string calledName, string pattern, int openQuote, StringBuilder literal)
    {
        int i = openQuote + 1;

        while (i < pattern.Length)
        {
            char c = pattern[i];

            if (c == '\\' && i + 1 < pattern.Length && pattern[i + 1] == '"')
            {
                literal.Append('"');
                i += 2;
                continue;
            }

            if (c == '"')
                return i + 1;

            literal.Append(c);
            i++;
        }

        throw InvalidPattern(calledName, pattern, $"the quoted text that starts at position {openQuote + 1} has no closing '\"'");
    }

    private static bool TryMatchKeyword(string pattern, int index, out (string Text, DatePatternField Field, NameCase Case) keyword)
    {
        ReadOnlySpan<char> rest = pattern.AsSpan(index);

        foreach ((string Text, DatePatternField Field, NameCase Case) candidate in PostgreSqlKeywords)
        {
            if (rest.StartsWith(candidate.Text, StringComparison.Ordinal))
            {
                keyword = candidate;
                return true;
            }
        }

        keyword = default;
        return false;
    }

    /// <summary>
    /// Compiles a MySQL pattern. Each <c>%x</c> specifier becomes a field and <c>%%</c> is a literal
    /// percent sign. An unknown specifier is an error: MySQL prints the letter instead, which hides a
    /// typo in the pattern.
    /// </summary>
    private static DatePattern CompileMySql(string calledName, string pattern)
    {
        List<Token> tokens = [];
        StringBuilder literal = new();
        int i = 0;

        while (i < pattern.Length)
        {
            char c = pattern[i];

            if (c == ' ')
            {
                FlushLiteral(tokens, literal);
                int start = i;
                while (i < pattern.Length && pattern[i] == ' ')
                    i++;

                tokens.Add(Token.ForSpaces(pattern[start..i]));
                continue;
            }

            if (c != '%')
            {
                literal.Append(c);
                i++;
                continue;
            }

            if (i + 1 >= pattern.Length)
                throw InvalidPattern(calledName, pattern, "the pattern ends with a '%' that has no specifier letter");

            char specifier = pattern[i + 1];
            string source = pattern.Substring(i, 2);
            i += 2;

            if (specifier == '%')
            {
                literal.Append('%');
                continue;
            }

            (DatePatternField field, bool fill) = specifier switch
            {
                'Y' => (DatePatternField.Year, true),
                'y' => (DatePatternField.YearLast2, true),
                'm' => (DatePatternField.Month, true),
                'c' => (DatePatternField.Month, false),
                'd' => (DatePatternField.Day, true),
                'e' => (DatePatternField.Day, false),
                'H' => (DatePatternField.Hour24, true),
                'h' or 'I' => (DatePatternField.Hour12, true),
                'i' => (DatePatternField.Minute, true),
                's' or 'S' => (DatePatternField.Second, true),
                'f' => (DatePatternField.Microsecond, true),
                'p' => (DatePatternField.Meridian, true),
                'M' => (DatePatternField.MonthName, false),
                'b' => (DatePatternField.MonthAbbr, true),
                'W' => (DatePatternField.DayName, false),
                'a' => (DatePatternField.DayAbbr, true),
                'j' => (DatePatternField.DayOfYear, true),
                'u' => (DatePatternField.MondayWeek, true),
                'v' => (DatePatternField.IsoWeek, true),
                _ => throw InvalidPattern(
                    calledName,
                    pattern,
                    $"unknown specifier '{source}' at position {i - 1}"),
            };

            FlushLiteral(tokens, literal);
            NameCase nameCase = field == DatePatternField.Meridian ? NameCase.Upper : NameCase.Capitalized;
            tokens.Add(new Token(field, source, fill, nameCase, null));
        }

        FlushLiteral(tokens, literal);
        return new DatePattern(pattern, tokens.ToArray());
    }

    private static void FlushLiteral(List<Token> tokens, StringBuilder literal)
    {
        if (literal.Length == 0)
            return;

        tokens.Add(Token.ForLiteral(literal.ToString()));
        literal.Clear();
    }

    // ── Format ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Prints <paramref name="utc"/> with this pattern. The text is built in a per-thread buffer, so a
    /// call allocates the result string only. The seventh fraction digit (100 ns) has no field and is
    /// not printed.
    /// </summary>
    public string Format(DateTime utc)
    {
        StringBuilder sb = formatBuffer ??= new StringBuilder(64);
        sb.Clear();

        foreach (Token token in tokens)
        {
            switch (token.Field)
            {
                case DatePatternField.Literal:
                case DatePatternField.Spaces:
                    sb.Append(token.Literal);
                    break;

                case DatePatternField.Year:
                    AppendNumber(sb, utc.Year, 4, token.Fill);
                    break;

                case DatePatternField.YearLast3:
                    AppendNumber(sb, utc.Year % 1000, 3, token.Fill);
                    break;

                case DatePatternField.YearLast2:
                    AppendNumber(sb, utc.Year % 100, 2, token.Fill);
                    break;

                case DatePatternField.YearLast1:
                    AppendNumber(sb, utc.Year % 10, 1, token.Fill);
                    break;

                case DatePatternField.IsoYear:
                    AppendNumber(sb, ISOWeek.GetYear(utc), 4, token.Fill);
                    break;

                case DatePatternField.Month:
                    AppendNumber(sb, utc.Month, 2, token.Fill);
                    break;

                case DatePatternField.MonthName:
                    AppendName(sb, MonthFull[(int)token.Case][utc.Month - 1], token.Fill);
                    break;

                case DatePatternField.MonthAbbr:
                    sb.Append(MonthAbbr[(int)token.Case][utc.Month - 1]);
                    break;

                case DatePatternField.Day:
                    AppendNumber(sb, utc.Day, 2, token.Fill);
                    break;

                case DatePatternField.DayOfYear:
                    AppendNumber(sb, utc.DayOfYear, 3, token.Fill);
                    break;

                case DatePatternField.DayOfWeekSunday1:
                    AppendNumber(sb, (int)utc.DayOfWeek + 1, 1, token.Fill);
                    break;

                case DatePatternField.IsoDayOfWeek:
                    AppendNumber(sb, IsoDayOfWeek(utc.DayOfWeek), 1, token.Fill);
                    break;

                case DatePatternField.DayName:
                    AppendName(sb, DayFull[(int)token.Case][(int)utc.DayOfWeek], token.Fill);
                    break;

                case DatePatternField.DayAbbr:
                    sb.Append(DayAbbr[(int)token.Case][(int)utc.DayOfWeek]);
                    break;

                case DatePatternField.WeekFromJanuary1:
                    AppendNumber(sb, WeekFromJanuary1(utc), 2, token.Fill);
                    break;

                case DatePatternField.IsoWeek:
                    AppendNumber(sb, ISOWeek.GetWeekOfYear(utc), 2, token.Fill);
                    break;

                case DatePatternField.MondayWeek:
                    AppendNumber(sb, MondayWeek(utc), 2, token.Fill);
                    break;

                case DatePatternField.Quarter:
                    AppendNumber(sb, (utc.Month - 1) / 3 + 1, 1, token.Fill);
                    break;

                case DatePatternField.Hour12:
                    AppendNumber(sb, utc.Hour % 12 == 0 ? 12 : utc.Hour % 12, 2, token.Fill);
                    break;

                case DatePatternField.Hour24:
                    AppendNumber(sb, utc.Hour, 2, token.Fill);
                    break;

                case DatePatternField.Minute:
                    AppendNumber(sb, utc.Minute, 2, token.Fill);
                    break;

                case DatePatternField.Second:
                    AppendNumber(sb, utc.Second, 2, token.Fill);
                    break;

                case DatePatternField.Millisecond:
                    AppendNumber(sb, utc.Millisecond, 3, token.Fill);
                    break;

                case DatePatternField.Microsecond:
                    AppendNumber(sb, (int)(utc.Ticks % TimeSpan.TicksPerSecond / TimeSpan.TicksPerMicrosecond), 6, token.Fill);
                    break;

                case DatePatternField.Meridian:
                    sb.Append(utc.Hour < 12
                        ? token.Case == NameCase.Lower ? "am" : "AM"
                        : token.Case == NameCase.Lower ? "pm" : "PM");
                    break;

                case DatePatternField.TimeZone:
                    sb.Append(token.Case == NameCase.Lower ? "utc" : "UTC");
                    break;

                case DatePatternField.Offset:
                    sb.Append("+00");
                    break;

                default:
                    throw new InvalidOperationException($"Unhandled date pattern field {token.Field}");
            }
        }

        string result = sb.ToString();

        // A very long pattern must not pin a large buffer on the thread after the call.
        if (sb.Capacity > MaxRetainedBufferCapacity)
            formatBuffer = null;

        return result;
    }

    private static void AppendNumber(StringBuilder sb, int value, int width, bool fill)
    {
        Span<char> digits = stackalloc char[11];
        value.TryFormat(digits, out int written, default, CultureInfo.InvariantCulture);

        if (fill && written < width)
            sb.Append('0', width - written);

        sb.Append(digits[..written]);
    }

    private static void AppendName(StringBuilder sb, string name, bool fill)
    {
        sb.Append(name);

        if (fill && name.Length < FullNameWidth)
            sb.Append(' ', FullNameWidth - name.Length);
    }

    // ── Parse ───────────────────────────────────────────────────────────────────

    /// <summary>
    /// Reads <paramref name="text"/> with this pattern and returns the UTC value it names. The whole
    /// text must match: a missing field, an extra character, a value out of range, two fields that
    /// disagree, and a checked field (day name, quarter, week) that does not match the date are each an
    /// <see cref="CamusDBErrorCodes.InvalidInput"/> error that quotes the text and the pattern.
    /// </summary>
    /// <remarks>
    /// <para>A numeric field reads one digit up to its full width, so <c>3/5/2026</c> matches
    /// <c>MM/DD/YYYY</c>. A run of spaces in the pattern matches a run of one or more spaces. Literal
    /// text and quoted text must match exactly.</para>
    /// <para>The date comes from the year, month and day; from the year and day of year; or, when the
    /// pattern has <c>IYYY</c>, from the ISO year, ISO week and ISO day. A pattern without a year field
    /// is an error, because a date with no year is not a value CamusDB can hold.</para>
    /// </remarks>
    public DateTime Parse(string calledName, string text)
    {
        Fields fields = Fields.Empty;
        ReadOnlySpan<char> input = text;
        int pos = 0;

        for (int t = 0; t < tokens.Length; t++)
        {
            Token token = tokens[t];

            switch (token.Field)
            {
                case DatePatternField.Literal:
                    if (!input[pos..].StartsWith(token.Literal!, StringComparison.Ordinal))
                        throw ParseError(calledName, text, $"expected '{token.Literal}' at position {pos + 1}, found {Describe(input, pos)}");

                    pos += token.Literal!.Length;
                    break;

                case DatePatternField.Spaces:
                    if (pos >= input.Length || input[pos] != ' ')
                        throw ParseError(calledName, text, $"expected a space at position {pos + 1}, found {Describe(input, pos)}");

                    while (pos < input.Length && input[pos] == ' ')
                        pos++;

                    break;

                case DatePatternField.Year:
                    fields.SetYear(this, calledName, text, token, ReadNumber(calledName, text, input, ref pos, token, 4, 0, 9999));
                    break;

                case DatePatternField.YearLast3:
                    fields.SetYear(this, calledName, text, token, ToNearYear(ReadNumber(calledName, text, input, ref pos, token, 3, 0, 999)));
                    break;

                case DatePatternField.YearLast2:
                    fields.SetYear(this, calledName, text, token, ToNearYear(ReadNumber(calledName, text, input, ref pos, token, 2, 0, 99)));
                    break;

                case DatePatternField.YearLast1:
                    fields.SetYear(this, calledName, text, token, ToNearYear(ReadNumber(calledName, text, input, ref pos, token, 1, 0, 9)));
                    break;

                case DatePatternField.IsoYear:
                    Set(ref fields.IsoYear, ReadNumber(calledName, text, input, ref pos, token, 4, 1, 9999), calledName, text, token);
                    break;

                case DatePatternField.Month:
                    Set(ref fields.Month, ReadNumber(calledName, text, input, ref pos, token, 2, 1, 12), calledName, text, token);
                    break;

                case DatePatternField.MonthName:
                    Set(ref fields.Month, ReadName(calledName, text, input, ref pos, token, MonthNames, 0, NextIsSpaces(t)) + 1, calledName, text, token);
                    break;

                case DatePatternField.MonthAbbr:
                    Set(ref fields.Month, ReadName(calledName, text, input, ref pos, token, MonthNames, 3, false) + 1, calledName, text, token);
                    break;

                case DatePatternField.Day:
                    Set(ref fields.Day, ReadNumber(calledName, text, input, ref pos, token, 2, 1, 31), calledName, text, token);
                    break;

                case DatePatternField.DayOfYear:
                    Set(ref fields.DayOfYear, ReadNumber(calledName, text, input, ref pos, token, 3, 1, 366), calledName, text, token);
                    fields.DayOfYearSource = token.Source;
                    break;

                case DatePatternField.DayOfWeekSunday1:
                    fields.SetDayOfWeek(this, calledName, text, token, ReadNumber(calledName, text, input, ref pos, token, 1, 1, 7) - 1);
                    break;

                case DatePatternField.IsoDayOfWeek:
                    fields.SetDayOfWeek(this, calledName, text, token, ReadNumber(calledName, text, input, ref pos, token, 1, 1, 7) % 7);
                    break;

                case DatePatternField.DayName:
                    fields.SetDayOfWeek(this, calledName, text, token, ReadName(calledName, text, input, ref pos, token, DayNames, 0, NextIsSpaces(t)));
                    break;

                case DatePatternField.DayAbbr:
                    fields.SetDayOfWeek(this, calledName, text, token, ReadName(calledName, text, input, ref pos, token, DayNames, 3, false));
                    break;

                case DatePatternField.WeekFromJanuary1:
                    Set(ref fields.WeekFromJanuary1, ReadNumber(calledName, text, input, ref pos, token, 2, 1, 53), calledName, text, token);
                    fields.WeekFromJanuary1Source = token.Source;
                    break;

                case DatePatternField.IsoWeek:
                    Set(ref fields.IsoWeek, ReadNumber(calledName, text, input, ref pos, token, 2, 1, 53), calledName, text, token);
                    fields.IsoWeekSource = token.Source;
                    break;

                case DatePatternField.MondayWeek:
                    Set(ref fields.MondayWeek, ReadNumber(calledName, text, input, ref pos, token, 2, 0, 53), calledName, text, token);
                    fields.MondayWeekSource = token.Source;
                    break;

                case DatePatternField.Quarter:
                    Set(ref fields.Quarter, ReadNumber(calledName, text, input, ref pos, token, 1, 1, 4), calledName, text, token);
                    fields.QuarterSource = token.Source;
                    break;

                case DatePatternField.Hour12:
                    Set(ref fields.Hour12, ReadNumber(calledName, text, input, ref pos, token, 2, 1, 12), calledName, text, token);
                    break;

                case DatePatternField.Hour24:
                    Set(ref fields.Hour24, ReadNumber(calledName, text, input, ref pos, token, 2, 0, 23), calledName, text, token);
                    break;

                case DatePatternField.Minute:
                    Set(ref fields.Minute, ReadNumber(calledName, text, input, ref pos, token, 2, 0, 59), calledName, text, token);
                    break;

                case DatePatternField.Second:
                    Set(ref fields.Second, ReadNumber(calledName, text, input, ref pos, token, 2, 0, 59), calledName, text, token);
                    break;

                case DatePatternField.Millisecond:
                    Set(ref fields.Millisecond, ReadFraction(calledName, text, input, ref pos, token, 3), calledName, text, token);
                    break;

                case DatePatternField.Microsecond:
                    Set(ref fields.Microsecond, ReadFraction(calledName, text, input, ref pos, token, 6), calledName, text, token);
                    break;

                case DatePatternField.Meridian:
                    Set(ref fields.Meridian, ReadMeridian(calledName, text, input, ref pos, token), calledName, text, token);
                    fields.MeridianSource = token.Source;
                    break;

                case DatePatternField.TimeZone:
                    ReadExact(calledName, text, input, ref pos, token, "UTC");
                    break;

                case DatePatternField.Offset:
                    ReadExact(calledName, text, input, ref pos, token, "+00");
                    break;

                default:
                    throw new InvalidOperationException($"Unhandled date pattern field {token.Field}");
            }
        }

        if (pos < input.Length)
            throw ParseError(calledName, text, $"unexpected text {Describe(input, pos)} at position {pos + 1} after the end of the pattern");

        return Build(calledName, text, ref fields);
    }

    private bool NextIsSpaces(int tokenIndex)
        => tokenIndex + 1 < tokens.Length && tokens[tokenIndex + 1].Field == DatePatternField.Spaces;

    private DateTime Build(string calledName, string text, ref Fields fields)
    {
        DateTime date;
        bool isoDate = fields.IsoYear >= 0;

        if (isoDate)
        {
            if (fields.Year >= 0 || fields.Month >= 0 || fields.Day >= 0 || fields.DayOfYear >= 0)
                throw ParseError(calledName, text, "the pattern mixes ISO week date fields (IYYY, IW, ID) with Gregorian date fields; use one convention only");

            int week = fields.IsoWeek >= 0 ? fields.IsoWeek : 1;
            int weeksInYear = ISOWeek.GetWeeksInYear(fields.IsoYear);

            if (week > weeksInYear)
                throw ParseError(calledName, text, $"ISO year {fields.IsoYear} has {weeksInYear} weeks, so week {week} is out of range");

            DayOfWeek dayOfWeek = fields.DayOfWeek >= 0 ? (DayOfWeek)fields.DayOfWeek : DayOfWeek.Monday;

            try
            {
                date = ISOWeek.ToDateTime(fields.IsoYear, week, dayOfWeek);
            }
            catch (ArgumentOutOfRangeException)
            {
                throw ParseError(calledName, text, "the date is outside the years 1-9999");
            }
        }
        else
        {
            if (fields.Year < 0)
                throw ParseError(calledName, text, "the pattern has no year field, and a date needs a year");

            if (fields.Year < 1)
                throw ParseError(calledName, text, $"year {fields.Year} is outside the years 1-9999");

            if (fields.DayOfYear >= 0 && fields.Month < 0 && fields.Day < 0)
            {
                int daysInYear = DateTime.IsLeapYear(fields.Year) ? 366 : 365;
                if (fields.DayOfYear > daysInYear)
                    throw ParseError(calledName, text, $"year {fields.Year} has {daysInYear} days, so day of year {fields.DayOfYear} is out of range");

                date = new DateTime(fields.Year, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(fields.DayOfYear - 1);
            }
            else
            {
                int month = fields.Month >= 0 ? fields.Month : 1;
                int day = fields.Day >= 0 ? fields.Day : 1;
                int daysInMonth = DateTime.DaysInMonth(fields.Year, month);

                if (day > daysInMonth)
                    throw ParseError(calledName, text, $"day {day} is out of range for {MonthNames[month - 1]} {fields.Year}, which has {daysInMonth} days");

                date = new DateTime(fields.Year, month, day, 0, 0, 0, DateTimeKind.Utc);
            }
        }

        int hour = 0;

        if (fields.Meridian >= 0 && fields.Hour12 < 0)
            throw ParseError(calledName, text, $"'{fields.MeridianSource}' needs a 12-hour field (HH, HH12, %h or %I) in the pattern");

        if (fields.Hour12 >= 0)
            hour = fields.Hour12 % 12 + (fields.Meridian == 1 ? 12 : 0);

        if (fields.Hour24 >= 0)
        {
            if (fields.Hour12 >= 0 && hour != fields.Hour24)
                throw ParseError(calledName, text, $"the 12-hour field gives hour {hour}, which conflicts with the 24-hour field, which gives {fields.Hour24}");

            hour = fields.Hour24;
        }

        // MS and US both name the whole fraction of the second (to_char prints 020 and 020000 for
        // 20 ms), so when both are present they must agree. PostgreSQL adds them instead, which does
        // not read back what to_char printed.
        long fractionTicks = 0;

        if (fields.Microsecond >= 0)
        {
            if (fields.Millisecond >= 0 && fields.Microsecond / 1000 != fields.Millisecond)
                throw ParseError(calledName, text, $"the millisecond field gives {fields.Millisecond}, which conflicts with the microsecond field, which gives {fields.Microsecond}");

            fractionTicks = fields.Microsecond * TimeSpan.TicksPerMicrosecond;
        }
        else if (fields.Millisecond >= 0)
        {
            fractionTicks = fields.Millisecond * TimeSpan.TicksPerMillisecond;
        }

        long ticks = date.Ticks
                     + hour * TimeSpan.TicksPerHour
                     + Math.Max(fields.Minute, 0) * TimeSpan.TicksPerMinute
                     + Math.Max(fields.Second, 0) * TimeSpan.TicksPerSecond
                     + fractionTicks;

        DateTime result = new(ticks, DateTimeKind.Utc);

        CheckFields(calledName, text, ref fields, result, isoDate);

        return result;
    }

    /// <summary>
    /// Checks the fields that the other fields already decide. PostgreSQL ignores most of them when it
    /// parses; here a day name, quarter or week that does not match the date is an error, because the
    /// text then names two different dates.
    /// </summary>
    private void CheckFields(string calledName, string text, ref Fields fields, DateTime result, bool isoDate)
    {
        string resultDate = result.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        if (!isoDate && fields.DayOfWeek >= 0 && (int)result.DayOfWeek != fields.DayOfWeek)
            throw ParseError(calledName, text, $"'{fields.DayOfWeekSource}' gives {DayNames[fields.DayOfWeek]}, but {resultDate} is a {DayNames[(int)result.DayOfWeek]}");

        if (!isoDate && fields.IsoWeek >= 0 && ISOWeek.GetWeekOfYear(result) != fields.IsoWeek)
            throw ParseError(calledName, text, $"'{fields.IsoWeekSource}' gives ISO week {fields.IsoWeek}, but {resultDate} is in ISO week {ISOWeek.GetWeekOfYear(result)}");

        if (fields.WeekFromJanuary1 >= 0 && WeekFromJanuary1(result) != fields.WeekFromJanuary1)
            throw ParseError(calledName, text, $"'{fields.WeekFromJanuary1Source}' gives week {fields.WeekFromJanuary1}, but {resultDate} is in week {WeekFromJanuary1(result)}");

        if (fields.MondayWeek >= 0 && MondayWeek(result) != fields.MondayWeek)
            throw ParseError(calledName, text, $"'{fields.MondayWeekSource}' gives week {fields.MondayWeek}, but {resultDate} is in week {MondayWeek(result)}");

        if (fields.Quarter >= 0 && (result.Month - 1) / 3 + 1 != fields.Quarter)
            throw ParseError(calledName, text, $"'{fields.QuarterSource}' gives quarter {fields.Quarter}, but {resultDate} is in quarter {(result.Month - 1) / 3 + 1}");

        if (fields.DayOfYear >= 0 && result.DayOfYear != fields.DayOfYear)
            throw ParseError(calledName, text, $"'{fields.DayOfYearSource}' gives day of year {fields.DayOfYear}, but {resultDate} is day {result.DayOfYear}");
    }

    private int ReadNumber(
        string calledName,
        string text,
        ReadOnlySpan<char> input,
        ref int pos,
        Token token,
        int maxDigits,
        int min,
        int max)
    {
        int start = pos;
        int value = 0;

        while (pos - start < maxDigits && pos < input.Length && char.IsAsciiDigit(input[pos]))
        {
            value = value * 10 + (input[pos] - '0');
            pos++;
        }

        if (pos == start)
            throw ParseError(calledName, text, $"expected a number for '{token.Source}' at position {start + 1}, found {Describe(input, start)}");

        if (value < min || value > max)
            throw ParseError(calledName, text, $"'{token.Source}' value {value.ToString(CultureInfo.InvariantCulture)} is out of range {min}-{max}");

        return value;
    }

    /// <summary>
    /// Reads a fraction field and returns it in units of that field (milliseconds for a width of 3,
    /// microseconds for 6). The digits are the digits after the decimal point, so <c>MS</c> reads
    /// <c>5</c> as 500 milliseconds, as PostgreSQL does.
    /// </summary>
    private int ReadFraction(string calledName, string text, ReadOnlySpan<char> input, ref int pos, Token token, int width)
    {
        int start = pos;
        int value = ReadNumber(calledName, text, input, ref pos, token, width, 0, int.MaxValue);

        for (int digits = pos - start; digits < width; digits++)
            value *= 10;

        return value;
    }

    private int ReadName(
        string calledName,
        string text,
        ReadOnlySpan<char> input,
        ref int pos,
        Token token,
        string[] names,
        int abbreviation,
        bool nextIsSpaces)
    {
        ReadOnlySpan<char> rest = input[pos..];

        for (int i = 0; i < names.Length; i++)
        {
            ReadOnlySpan<char> name = abbreviation > 0 ? names[i].AsSpan(0, abbreviation) : names[i];

            if (!rest.StartsWith(name, StringComparison.OrdinalIgnoreCase))
                continue;

            pos += name.Length;

            // A padded full name ("May      ") is followed by blanks up to nine characters. When the
            // pattern has spaces next, those spaces match the padding too, so leave it for them.
            if (abbreviation == 0 && token.Fill && !nextIsSpaces)
            {
                int padEnd = pos + FullNameWidth - name.Length;
                while (pos < padEnd && pos < input.Length && input[pos] == ' ')
                    pos++;
            }

            return i;
        }

        string expected = names == MonthNames
            ? abbreviation > 0 ? "a three-letter month name" : "a month name"
            : abbreviation > 0 ? "a three-letter day name" : "a day name";

        throw ParseError(calledName, text, $"expected {expected} for '{token.Source}' at position {pos + 1}, found {Describe(input, pos)}");
    }

    private int ReadMeridian(string calledName, string text, ReadOnlySpan<char> input, ref int pos, Token token)
    {
        ReadOnlySpan<char> rest = input[pos..];

        if (rest.StartsWith("AM", StringComparison.OrdinalIgnoreCase))
        {
            pos += 2;
            return 0;
        }

        if (rest.StartsWith("PM", StringComparison.OrdinalIgnoreCase))
        {
            pos += 2;
            return 1;
        }

        throw ParseError(calledName, text, $"expected AM or PM for '{token.Source}' at position {pos + 1}, found {Describe(input, pos)}");
    }

    private void ReadExact(string calledName, string text, ReadOnlySpan<char> input, ref int pos, Token token, string expected)
    {
        if (!input[pos..].StartsWith(expected, StringComparison.OrdinalIgnoreCase))
            throw ParseError(calledName, text, $"expected '{expected}' for '{token.Source}' at position {pos + 1}, found {Describe(input, pos)}; all values are UTC");

        pos += expected.Length;
    }

    private void Set(ref int slot, int value, string calledName, string text, Token token)
    {
        if (slot >= 0 && slot != value)
            throw ParseError(calledName, text, $"'{token.Source}' gives {value.ToString(CultureInfo.InvariantCulture)}, which conflicts with an earlier field that gives {slot.ToString(CultureInfo.InvariantCulture)}");

        slot = value;
    }

    private static string Describe(ReadOnlySpan<char> input, int pos)
    {
        if (pos >= input.Length)
            return "the end of the text";

        int length = Math.Min(10, input.Length - pos);
        return "'" + input.Slice(pos, length).ToString() + (length < input.Length - pos ? "...'" : "'");
    }

    // ── Shared date rules ───────────────────────────────────────────────────────

    /// <summary>
    /// Turns a year written with fewer than four digits into the nearest year to 2020, with the
    /// PostgreSQL rule: 0-69 → 2000-2069, 70-99 → 1970-1999, 100-519 → 2100-2519, 520-999 →
    /// 1520-1999. MySQL <c>%y</c> uses the same rule for two digits.
    /// </summary>
    private static int ToNearYear(int year)
    {
        if (year < 70)
            return year + 2000;

        if (year < 100)
            return year + 1900;

        if (year < 520)
            return year + 2000;

        return year + 1000;
    }

    /// <summary>The PostgreSQL <c>WW</c> week: week 1 is January 1-7, whatever day of the week January 1 is.</summary>
    private static int WeekFromJanuary1(DateTime value) => (value.DayOfYear - 1) / 7 + 1;

    private static int IsoDayOfWeek(DayOfWeek dayOfWeek) => dayOfWeek == DayOfWeek.Sunday ? 7 : (int)dayOfWeek;

    /// <summary>
    /// The MySQL <c>%u</c> week (<c>WEEK(x, 1)</c>): weeks start on Monday, and week 1 is the first week
    /// with four or more days in the year, the same week 1 as ISO-8601. Days before it are week 0, and
    /// the count does not wrap into the next year, so the range is 0-53.
    /// </summary>
    private static int MondayWeek(DateTime value)
    {
        DateTime january4 = new(value.Year, 1, 4);
        DateTime week1Monday = january4.AddDays(-(((int)january4.DayOfWeek + 6) % 7));
        DateTime day = value.Date;

        return day < week1Monday ? 0 : (day - week1Monday).Days / 7 + 1;
    }

    private static string[][] CaseForms(string[] names, int length)
    {
        string[] capitalized = new string[names.Length];
        string[] upper = new string[names.Length];
        string[] lower = new string[names.Length];

        for (int i = 0; i < names.Length; i++)
        {
            string name = length > 0 ? names[i][..length] : names[i];
            capitalized[i] = name;
            upper[i] = name.ToUpperInvariant();
            lower[i] = name.ToLowerInvariant();
        }

        // Indexed by NameCase.
        return [upper, capitalized, lower];
    }

    private static (string, DatePatternField, NameCase)[] BuildPostgreSqlKeywords()
    {
        List<(string Text, DatePatternField Field, NameCase Case)> keywords =
        [
            ("MONTH", DatePatternField.MonthName, NameCase.Upper),
            ("Month", DatePatternField.MonthName, NameCase.Capitalized),
            ("month", DatePatternField.MonthName, NameCase.Lower),
            ("MON", DatePatternField.MonthAbbr, NameCase.Upper),
            ("Mon", DatePatternField.MonthAbbr, NameCase.Capitalized),
            ("mon", DatePatternField.MonthAbbr, NameCase.Lower),
            ("DAY", DatePatternField.DayName, NameCase.Upper),
            ("Day", DatePatternField.DayName, NameCase.Capitalized),
            ("day", DatePatternField.DayName, NameCase.Lower),
            ("DY", DatePatternField.DayAbbr, NameCase.Upper),
            ("Dy", DatePatternField.DayAbbr, NameCase.Capitalized),
            ("dy", DatePatternField.DayAbbr, NameCase.Lower),
            ("AM", DatePatternField.Meridian, NameCase.Upper),
            ("PM", DatePatternField.Meridian, NameCase.Upper),
            ("am", DatePatternField.Meridian, NameCase.Lower),
            ("pm", DatePatternField.Meridian, NameCase.Lower),
            ("TZ", DatePatternField.TimeZone, NameCase.Upper),
            ("tz", DatePatternField.TimeZone, NameCase.Lower),
            ("OF", DatePatternField.Offset, NameCase.Upper),
        ];

        (string, DatePatternField)[] numeric =
        [
            ("IYYY", DatePatternField.IsoYear),
            ("YYYY", DatePatternField.Year),
            ("YYY", DatePatternField.YearLast3),
            ("YY", DatePatternField.YearLast2),
            ("Y", DatePatternField.YearLast1),
            ("HH24", DatePatternField.Hour24),
            ("HH12", DatePatternField.Hour12),
            ("HH", DatePatternField.Hour12),
            ("MI", DatePatternField.Minute),
            ("MM", DatePatternField.Month),
            ("MS", DatePatternField.Millisecond),
            ("SS", DatePatternField.Second),
            ("US", DatePatternField.Microsecond),
            ("DDD", DatePatternField.DayOfYear),
            ("DD", DatePatternField.Day),
            ("D", DatePatternField.DayOfWeekSunday1),
            ("ID", DatePatternField.IsoDayOfWeek),
            ("IW", DatePatternField.IsoWeek),
            ("WW", DatePatternField.WeekFromJanuary1),
            ("Q", DatePatternField.Quarter),
        ];

        foreach ((string text, DatePatternField field) in numeric)
        {
            keywords.Add((text, field, NameCase.Upper));
            keywords.Add((text.ToLowerInvariant(), field, NameCase.Lower));
        }

        // Longest first; the sort is stable, so equal lengths keep the order above.
        return keywords.OrderByDescending(k => k.Text.Length).ToArray();
    }

    private CamusDBException ParseError(string calledName, string text, string reason)
        => new(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' could not parse '{text}' with pattern '{Text}': {reason}");

    private static CamusDBException InvalidPattern(string calledName, string pattern, string reason)
        => new(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' could not use pattern '{pattern}': {reason}");

    /// <summary>The printed case of a name, a meridian or a time zone field. The value indexes the arrays from <see cref="CaseForms"/>.</summary>
    private enum NameCase : byte
    {
        Upper = 0,
        Capitalized = 1,
        Lower = 2,
    }

    /// <summary>
    /// One compiled element of a pattern. <see cref="Source"/> is the spelling in the pattern
    /// (<c>MM</c>, <c>%m</c>), which error messages quote. <see cref="Fill"/> is false for a field
    /// under <c>FM</c> and for the MySQL fields that do not pad (<c>%c</c>, <c>%e</c>, <c>%M</c>,
    /// <c>%W</c>).
    /// </summary>
    private readonly record struct Token(DatePatternField Field, string Source, bool Fill, NameCase Case, string? Literal)
    {
        public static Token ForLiteral(string text) => new(DatePatternField.Literal, text, false, NameCase.Upper, text);

        public static Token ForSpaces(string text) => new(DatePatternField.Spaces, text, false, NameCase.Upper, text);
    }

    /// <summary>
    /// The values read from the text, each -1 until a field sets it. A second field that sets the same
    /// value must agree with the first.
    /// </summary>
    private struct Fields
    {
        public int Year;
        public int IsoYear;
        public int Month;
        public int Day;
        public int DayOfYear;
        public int DayOfWeek;
        public int IsoWeek;
        public int WeekFromJanuary1;
        public int MondayWeek;
        public int Quarter;
        public int Hour12;
        public int Hour24;
        public int Minute;
        public int Second;
        public int Millisecond;
        public int Microsecond;
        public int Meridian;

        public string? DayOfYearSource;
        public string? DayOfWeekSource;
        public string? IsoWeekSource;
        public string? WeekFromJanuary1Source;
        public string? MondayWeekSource;
        public string? QuarterSource;
        public string? MeridianSource;

        public static Fields Empty => new()
        {
            Year = -1,
            IsoYear = -1,
            Month = -1,
            Day = -1,
            DayOfYear = -1,
            DayOfWeek = -1,
            IsoWeek = -1,
            WeekFromJanuary1 = -1,
            MondayWeek = -1,
            Quarter = -1,
            Hour12 = -1,
            Hour24 = -1,
            Minute = -1,
            Second = -1,
            Millisecond = -1,
            Microsecond = -1,
            Meridian = -1,
        };

        public void SetYear(DatePattern pattern, string calledName, string text, Token token, int year)
            => pattern.Set(ref Year, year, calledName, text, token);

        public void SetDayOfWeek(DatePattern pattern, string calledName, string text, Token token, int dayOfWeek)
        {
            pattern.Set(ref DayOfWeek, dayOfWeek, calledName, text, token);
            DayOfWeekSource ??= token.Source;
        }
    }
}

/// <summary>The fields a <see cref="DatePattern"/> token can print and read.</summary>
internal enum DatePatternField : byte
{
    /// <summary>Text printed and matched exactly: plain characters, quoted text, and <c>%%</c>.</summary>
    Literal,

    /// <summary>A run of spaces. It prints as written and matches one or more spaces.</summary>
    Spaces,

    Year,
    YearLast3,
    YearLast2,
    YearLast1,
    IsoYear,
    Month,
    MonthName,
    MonthAbbr,
    Day,
    DayOfYear,

    /// <summary>1 = Sunday to 7 = Saturday (PostgreSQL <c>D</c>).</summary>
    DayOfWeekSunday1,

    /// <summary>1 = Monday to 7 = Sunday (PostgreSQL <c>ID</c>).</summary>
    IsoDayOfWeek,

    DayName,
    DayAbbr,

    /// <summary>PostgreSQL <c>WW</c>: week 1 is January 1-7.</summary>
    WeekFromJanuary1,

    IsoWeek,

    /// <summary>MySQL <c>%u</c>: Monday weeks, 0-53.</summary>
    MondayWeek,

    Quarter,
    Hour12,
    Hour24,
    Minute,
    Second,

    /// <summary>The fraction of the second in milliseconds, 000-999.</summary>
    Millisecond,

    /// <summary>The fraction of the second in microseconds, 000000-999999.</summary>
    Microsecond,

    Meridian,
    TimeZone,
    Offset,
}
