/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// One <see cref="Catalogs.Models.ColumnType.Interval"/> value: a number of months, a number of days
/// and a number of microseconds. The three parts stay separate, as in PostgreSQL, because a month and a
/// day have no fixed length when they are added to a date: <c>2024-01-31 + 1 month</c> is
/// <c>2024-02-29</c>, and only the time part is a fixed number of microseconds.
/// </summary>
public readonly record struct IntervalParts(int Months, int Days, long Micros);

/// <summary>
/// The one implementation of every <see cref="Catalogs.Models.ColumnType.Interval"/> rule: parse,
/// format, order, and the arithmetic with dates, datetimes, numbers and other intervals. The rules are
/// the PostgreSQL rules, so a query that comes from PostgreSQL gives the same values.
///
/// <para><b>An interval is an expression value only.</b> A table cannot store it. A query that returns
/// one sends its ISO-8601 text (<see cref="FormatIso"/>) to the client as a string.</para>
///
/// <para><b>Order.</b> Two intervals compare by one total, as PostgreSQL <c>interval_cmp_value</c>
/// does: a month counts as 30 days and a day as 24 hours. Thus <c>'1 month' = '30 days'</c> and
/// <c>'1 day' = '24 hours'</c> are true, and a hash must use the same total
/// (<see cref="ComparisonKey"/>), so that equal values hash alike.</para>
///
/// <para><b>Range.</b> The months and the days are 32-bit, the microseconds are 64-bit. A result that
/// does not fit is an <see cref="CamusDBErrorCodes.InvalidInput"/> error ("interval out of range"),
/// never a wrapped value. A datetime result before year 1 or after year 9999 is the same error code
/// with "timestamp out of range".</para>
/// </summary>
public static class IntervalMath
{
    public const long MicrosPerSecond = 1_000_000;
    public const long MicrosPerMinute = 60 * MicrosPerSecond;
    public const long MicrosPerHour = 60 * MicrosPerMinute;
    public const long MicrosPerDay = 24 * MicrosPerHour;

    /// <summary>The length of a month in days when a fraction of a month spills into days, and in comparisons.</summary>
    public const int DaysPerMonth = 30;

    public const string IntervalOutOfRangeMessage = "interval out of range";
    public const string TimestampOutOfRangeMessage = "timestamp out of range";

    public static IntervalParts Zero => default;

    /// <summary>
    /// The column type a client sees for a result column: an interval column is a string column,
    /// because every transport sends an interval as its ISO-8601 text. Every other type is unchanged.
    /// </summary>
    public static Catalogs.Models.ColumnType ClientColumnType(Catalogs.Models.ColumnType type) =>
        type == Catalogs.Models.ColumnType.Interval ? Catalogs.Models.ColumnType.String : type;

    // ─── Order ─────────────────────────────────────────────────────────────

    /// <summary>
    /// The total that orders and hashes an interval: <c>(months * 30 + days) * 86400000000 + micros</c>.
    /// It is exact in <see cref="Int128"/> for every value.
    /// </summary>
    public static Int128 ComparisonKey(IntervalParts value) =>
        ((Int128)value.Months * DaysPerMonth + value.Days) * MicrosPerDay + value.Micros;

    public static int Compare(IntervalParts left, IntervalParts right) =>
        ComparisonKey(left).CompareTo(ComparisonKey(right));

    // ─── Arithmetic between intervals ──────────────────────────────────────

    public static IntervalParts Add(IntervalParts left, IntervalParts right)
    {
        try
        {
            return new IntervalParts(
                checked(left.Months + right.Months),
                checked(left.Days + right.Days),
                checked(left.Micros + right.Micros));
        }
        catch (OverflowException)
        {
            throw OutOfRange();
        }
    }

    public static IntervalParts Subtract(IntervalParts left, IntervalParts right) => Add(left, Negate(right));

    public static IntervalParts Negate(IntervalParts value)
    {
        if (value.Months == int.MinValue || value.Days == int.MinValue || value.Micros == long.MinValue)
            throw OutOfRange();

        return new IntervalParts(-value.Months, -value.Days, -value.Micros);
    }

    /// <summary>
    /// <c>interval * factor</c>, the PostgreSQL <c>interval_mul</c> algorithm. Each part is multiplied
    /// and truncated. The fraction that a month part loses moves into days (30 days a month), and the
    /// fraction that a day part loses moves into microseconds. A NaN or infinite factor, or a result
    /// that does not fit, is an error.
    /// </summary>
    public static IntervalParts Multiply(IntervalParts value, double factor)
    {
        if (!double.IsFinite(factor))
            throw OutOfRange();

        double monthsExact = value.Months * factor;
        double daysExact = value.Days * factor;

        if (!FitsInt32(monthsExact) || !FitsInt32(daysExact))
            throw OutOfRange();

        int months = (int)monthsExact;
        int days = (int)daysExact;

        double monthRemainderDays = RoundToMicros((monthsExact - months) * DaysPerMonth);
        double secondRemainder = RoundToMicros((daysExact - days + monthRemainderDays - (int)monthRemainderDays) * 86400.0);

        double carry = 0;
        if (Math.Abs(secondRemainder) >= 86400.0)
        {
            carry = Math.Truncate(secondRemainder / 86400.0);
            secondRemainder -= carry * 86400.0;
        }

        double daysTotal = days + carry + (int)monthRemainderDays;
        double microsExact = Math.Round(value.Micros * factor + secondRemainder * MicrosPerSecond, MidpointRounding.ToEven);

        if (!FitsInt32(daysTotal) || !FitsInt64(microsExact))
            throw OutOfRange();

        return new IntervalParts(months, (int)daysTotal, (long)microsExact);
    }

    /// <summary>
    /// <c>interval / divisor</c>, the PostgreSQL <c>interval_div</c> algorithm: the same steps as
    /// <see cref="Multiply"/>, with a division. A zero divisor is the division-by-zero error.
    /// </summary>
    public static IntervalParts Divide(IntervalParts value, double divisor)
    {
        if (divisor == 0.0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, NumericMath.DivisionByZeroMessage);

        if (!double.IsFinite(divisor))
            throw OutOfRange();

        double monthsExact = value.Months / divisor;
        double daysExact = value.Days / divisor;

        if (!FitsInt32(monthsExact) || !FitsInt32(daysExact))
            throw OutOfRange();

        int months = (int)monthsExact;
        int days = (int)daysExact;

        double monthRemainderDays = RoundToMicros((monthsExact - months) * DaysPerMonth);
        double secondRemainder = RoundToMicros((daysExact - days + monthRemainderDays - (int)monthRemainderDays) * 86400.0);

        double carry = 0;
        if (Math.Abs(secondRemainder) >= 86400.0)
        {
            carry = Math.Truncate(secondRemainder / 86400.0);
            secondRemainder -= carry * 86400.0;
        }

        double daysTotal = days + carry + (int)monthRemainderDays;
        double microsExact = Math.Round(value.Micros / divisor + secondRemainder * MicrosPerSecond, MidpointRounding.ToEven);

        if (!FitsInt32(daysTotal) || !FitsInt64(microsExact))
            throw OutOfRange();

        return new IntervalParts(months, (int)daysTotal, (long)microsExact);
    }

    // ─── Arithmetic with datetimes ─────────────────────────────────────────

    /// <summary>
    /// <c>datetime + interval</c>: the months first (the day of the month is clamped to the last day of
    /// the new month, so <c>2024-01-31 + 1 month</c> is <c>2024-02-29</c>), then the days, then the
    /// microseconds. All values are UTC, so a day is always 24 hours. The ticks are UTC
    /// <see cref="DateTime.Ticks"/>.
    /// </summary>
    public static long AddToTicks(long ticks, IntervalParts interval)
    {
        try
        {
            DateTime value = new(ticks, DateTimeKind.Utc);

            if (interval.Months != 0)
                value = value.AddMonths(interval.Months);

            if (interval.Days != 0)
                value = value.AddTicks(checked(interval.Days * TimeSpan.TicksPerDay));

            if (interval.Micros != 0)
                value = value.AddTicks(checked(interval.Micros * TimeSpan.TicksPerMicrosecond));

            return value.Ticks;
        }
        catch (Exception e) when (e is ArgumentOutOfRangeException or OverflowException)
        {
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, TimestampOutOfRangeMessage);
        }
    }

    /// <summary>
    /// <c>datetime - datetime</c>, the PostgreSQL rule: the difference in microseconds, with whole days
    /// moved into the day part (<c>justify_hours</c>). The months part is always zero. A tick
    /// difference finer than a microsecond is truncated toward zero.
    /// </summary>
    public static IntervalParts BetweenTicks(long leftTicks, long rightTicks)
    {
        // Both values are inside year 1..9999, so the difference fits in a long.
        long micros = (leftTicks - rightTicks) / TimeSpan.TicksPerMicrosecond;
        long days = micros / MicrosPerDay;
        return new IntervalParts(0, (int)days, micros - days * MicrosPerDay);
    }

    /// <summary>
    /// <c>age(a, b)</c>, the PostgreSQL rule: a "symbolic" difference in years, months and days, not a
    /// count of days. Each calendar field of <paramref name="leftTicks"/> minus the same field of
    /// <paramref name="rightTicks"/>; a negative field borrows from the next larger field, and a day
    /// borrows the length of the month of the earlier value. So <c>age('2001-04-10', '1957-06-13')</c>
    /// is 43 years 9 months 27 days. When the left value is the earlier one, the fields are computed on
    /// the swapped values and the result is negated, so <c>age(a, b) = -age(b, a)</c>. A tick
    /// fraction finer than a microsecond is truncated.
    /// </summary>
    public static IntervalParts Age(long leftTicks, long rightTicks)
    {
        DateTime left = new(leftTicks, DateTimeKind.Utc);
        DateTime right = new(rightTicks, DateTimeKind.Utc);
        bool flip = leftTicks < rightTicks;

        long fraction = left.Ticks % TimeSpan.TicksPerSecond / TimeSpan.TicksPerMicrosecond
                        - right.Ticks % TimeSpan.TicksPerSecond / TimeSpan.TicksPerMicrosecond;
        int second = left.Second - right.Second;
        int minute = left.Minute - right.Minute;
        int hour = left.Hour - right.Hour;
        int day = left.Day - right.Day;
        int month = left.Month - right.Month;
        int year = left.Year - right.Year;

        if (flip)
        {
            fraction = -fraction;
            second = -second;
            minute = -minute;
            hour = -hour;
            day = -day;
            month = -month;
            year = -year;
        }

        while (fraction < 0) { fraction += MicrosPerSecond; second--; }
        while (second < 0) { second += 60; minute--; }
        while (minute < 0) { minute += 60; hour--; }
        while (hour < 0) { hour += 24; day--; }

        DateTime earlier = flip ? left : right;
        while (day < 0) { day += DateTime.DaysInMonth(earlier.Year, earlier.Month); month--; }
        while (month < 0) { month += 12; year--; }

        if (flip)
        {
            fraction = -fraction;
            second = -second;
            minute = -minute;
            hour = -hour;
            day = -day;
            month = -month;
            year = -year;
        }

        // Years are inside 1..9999, so every part fits: no overflow check is necessary.
        long micros = ((hour * 60L + minute) * 60L + second) * MicrosPerSecond + fraction;
        return new IntervalParts(year * 12 + month, day, micros);
    }

    /// <summary>
    /// <c>make_interval(years, months, weeks, days, hours, mins, secs)</c>, the PostgreSQL rule: the
    /// months are <c>years * 12 + months</c>, the days are <c>weeks * 7 + days</c>, and the time is the
    /// hours, the minutes and the seconds in microseconds. The seconds round to a whole microsecond
    /// (half to even). A part that does not fit, or seconds that are not finite, is "interval out of
    /// range".
    /// </summary>
    public static IntervalParts Make(long years, long months, long weeks, long days, long hours, long minutes, double seconds)
    {
        if (!double.IsFinite(seconds))
            throw OutOfRange();

        double secondMicros = Math.Round(seconds * MicrosPerSecond, MidpointRounding.ToEven);
        if (!FitsInt64(secondMicros))
            throw OutOfRange();

        try
        {
            int monthPart = checked((int)(checked(years * 12) + months));
            int dayPart = checked((int)(checked(weeks * 7) + days));
            long micros = checked(hours * MicrosPerHour + minutes * MicrosPerMinute + (long)secondMicros);
            return new IntervalParts(monthPart, dayPart, micros);
        }
        catch (OverflowException)
        {
            throw OutOfRange();
        }
    }

    /// <summary>
    /// <c>justify_days</c>: each 30 days become one month. When the months and the days then have
    /// different signs, one month moves into the days, so <c>'1 month -1 day'</c> gives <c>'29 days'</c>.
    /// The time part does not change.
    /// </summary>
    public static IntervalParts JustifyDays(IntervalParts value)
    {
        int wholeMonths = value.Days / DaysPerMonth;
        int days = value.Days - wholeMonths * DaysPerMonth;
        int months = AddInt32(value.Months, wholeMonths);

        if (months > 0 && days < 0) { days += DaysPerMonth; months--; }
        else if (months < 0 && days > 0) { days -= DaysPerMonth; months++; }

        return new IntervalParts(months, days, value.Micros);
    }

    /// <summary>
    /// <c>justify_hours</c>: each 24 hours of the time part become one day. When the days and the time
    /// then have different signs, one day moves into the time. The months do not change.
    /// </summary>
    public static IntervalParts JustifyHours(IntervalParts value)
    {
        long wholeDays = value.Micros / MicrosPerDay;
        long micros = value.Micros - wholeDays * MicrosPerDay;
        int days = AddInt32(value.Days, wholeDays);

        if (days > 0 && micros < 0) { micros += MicrosPerDay; days--; }
        else if (days < 0 && micros > 0) { micros -= MicrosPerDay; days++; }

        return new IntervalParts(value.Months, days, micros);
    }

    /// <summary>
    /// <c>justify_interval</c>: <see cref="JustifyHours"/> and then <see cref="JustifyDays"/>, with
    /// the PostgreSQL sign rules across all three parts, so that every non-zero part has the sign of
    /// the whole value: <c>'1 mon -1 hour'</c> gives <c>'29 days 23:00:00'</c>.
    /// </summary>
    public static IntervalParts JustifyInterval(IntervalParts value)
    {
        long wholeDays = value.Micros / MicrosPerDay;
        long micros = value.Micros - wholeDays * MicrosPerDay;
        int days = AddInt32(value.Days, wholeDays);

        int wholeMonths = days / DaysPerMonth;
        days -= wholeMonths * DaysPerMonth;
        int months = AddInt32(value.Months, wholeMonths);

        if (months > 0 && (days < 0 || (days == 0 && micros < 0)))
        {
            days += DaysPerMonth;
            months--;
        }
        else if (months < 0 && (days > 0 || (days == 0 && micros > 0)))
        {
            days -= DaysPerMonth;
            months++;
        }

        if (days > 0 && micros < 0) { micros += MicrosPerDay; days--; }
        else if (days < 0 && micros > 0) { micros -= MicrosPerDay; days++; }

        return new IntervalParts(months, days, micros);
    }

    /// <summary>
    /// <c>date_bin(stride, source, origin)</c>, the PostgreSQL rule: the start of the bin of width
    /// <paramref name="stride"/> that holds <paramref name="sourceTicks"/>, where the bins are aligned
    /// to <paramref name="originTicks"/>. A source before the origin rounds down too, to the earlier
    /// bin. A stride with a month part is refused, because a month has no fixed length; a stride of
    /// zero or less is refused. A result outside year 1..9999 is "timestamp out of range".
    /// </summary>
    public static long BinTicks(IntervalParts stride, long sourceTicks, long originTicks)
    {
        if (stride.Months != 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "timestamps cannot be binned into intervals containing months or years");

        Int128 strideMicros = (Int128)stride.Days * MicrosPerDay + stride.Micros;
        if (strideMicros > long.MaxValue || strideMicros < long.MinValue)
            throw OutOfRange();

        if (strideMicros <= 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "stride must be greater than zero");

        Int128 strideTicks = strideMicros * TimeSpan.TicksPerMicrosecond;
        Int128 difference = (Int128)sourceTicks - originTicks;
        Int128 remainder = difference % strideTicks;
        Int128 result = originTicks + difference - remainder;
        if (remainder < 0)
            result -= strideTicks;

        if (result < 0 || result > DateTime.MaxValue.Ticks)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, TimestampOutOfRangeMessage);

        return (long)result;
    }

    /// <summary>
    /// The total length in microseconds as PostgreSQL's <c>EXTRACT(epoch FROM interval)</c> counts it:
    /// a year is 365.25 days, a month that is not part of a whole year is 30 days, and a day is 24
    /// hours. The result can exceed a long, so it is an <see cref="Int128"/>.
    /// </summary>
    public static Int128 EpochMicros(IntervalParts value)
    {
        const long MicrosPerYear = 31_557_600L * MicrosPerSecond; // 365.25 days

        return (Int128)(value.Months / 12) * MicrosPerYear
               + (Int128)(value.Months % 12) * DaysPerMonth * MicrosPerDay
               + (Int128)value.Days * MicrosPerDay
               + value.Micros;
    }

    private static int AddInt32(int left, long right)
    {
        long sum = left + right;
        if (sum < int.MinValue || sum > int.MaxValue)
            throw OutOfRange();

        return (int)sum;
    }

    // ─── Construction from a unit ──────────────────────────────────────────

    /// <summary>
    /// The interval of <paramref name="amount"/> of one unit, with the PostgreSQL rules for a fraction:
    /// a fraction of a year, a decade, a century or a millennium rounds to whole months; a fraction of
    /// a month moves into days (30 days a month) and then into microseconds; a fraction of a week or a
    /// day moves into microseconds; a time unit rounds to whole microseconds.
    /// </summary>
    public static IntervalParts FromUnit(decimal amount, IntervalUnit unit)
    {
        Accumulator accumulator = new();
        accumulator.Add(amount, unit);
        return accumulator.ToParts();
    }

    /// <summary>
    /// Reads a unit name: the PostgreSQL names, their plurals and short forms (<c>year</c>, <c>mons</c>,
    /// <c>hrs</c>, <c>us</c>), case-insensitive. Returns false for any other word.
    /// </summary>
    public static bool TryParseUnit(ReadOnlySpan<char> word, out IntervalUnit unit)
    {
        string lower = word.ToString().ToLowerInvariant();

        unit = lower switch
        {
            "millennium" or "millennia" or "millenniums" or "mil" or "mils" => IntervalUnit.Millennium,
            "century" or "centuries" or "c" or "cent" => IntervalUnit.Century,
            "decade" or "decades" or "dec" or "decs" => IntervalUnit.Decade,
            "year" or "years" or "y" or "yr" or "yrs" => IntervalUnit.Year,
            "quarter" or "quarters" => IntervalUnit.Quarter,
            "month" or "months" or "mon" or "mons" => IntervalUnit.Month,
            "week" or "weeks" or "w" => IntervalUnit.Week,
            "day" or "days" or "d" => IntervalUnit.Day,
            "hour" or "hours" or "h" or "hr" or "hrs" => IntervalUnit.Hour,
            "minute" or "minutes" or "m" or "min" or "mins" => IntervalUnit.Minute,
            "second" or "seconds" or "s" or "sec" or "secs" => IntervalUnit.Second,
            "millisecond" or "milliseconds" or "ms" or "msec" or "msecs" => IntervalUnit.Millisecond,
            "microsecond" or "microseconds" or "us" or "usec" or "usecs" => IntervalUnit.Microsecond,
            _ => IntervalUnit.None,
        };

        return unit != IntervalUnit.None;
    }

    // ─── Parse ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Parses interval text: the ISO-8601 form (<c>P1Y2M3DT4H5M6.5S</c>, <c>P1W</c>, a sign or a
    /// fraction on any number) or the PostgreSQL form (<c>1 year 2 mons 3 days 04:05:06</c>,
    /// <c>-1 day +2 hours</c>, <c>1.5 days</c>, <c>@ 3 hours ago</c>). A number with no unit is a
    /// number of seconds. Throws <see cref="CamusDBErrorCodes.InvalidInput"/> for other text.
    /// </summary>
    public static IntervalParts Parse(string text)
    {
        if (TryParse(text, out IntervalParts value, out bool outOfRange))
            return value;

        if (outOfRange)
            throw OutOfRange();

        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"invalid input syntax for type interval: \"{text}\"");
    }

    public static bool TryParse(string text, out IntervalParts value, out bool outOfRange)
    {
        value = default;
        outOfRange = false;

        ReadOnlySpan<char> span = text.AsSpan().Trim();
        if (span.IsEmpty)
            return false;

        try
        {
            Accumulator accumulator = new();

            bool parsed = span[0] is 'P' or 'p'
                ? ParseIso(span, ref accumulator)
                : ParseVerbose(span, ref accumulator);

            if (!parsed)
                return false;

            value = accumulator.ToParts();
            return true;
        }
        catch (Exception e) when (e is OverflowException || e is CamusDBException { Message: IntervalOutOfRangeMessage })
        {
            outOfRange = true;
            return false;
        }
    }

    private static bool ParseIso(ReadOnlySpan<char> span, ref Accumulator accumulator)
    {
        int i = 1;
        bool timePart = false;
        bool any = false;
        bool anyTime = false;

        while (i < span.Length)
        {
            if (span[i] is 'T' or 't')
            {
                if (timePart)
                    return false;

                timePart = true;
                i++;
                continue;
            }

            if (!TryReadNumber(span, ref i, out decimal number) || i >= span.Length)
                return false;

            char designator = char.ToUpperInvariant(span[i++]);

            IntervalUnit unit = (designator, timePart) switch
            {
                ('Y', false) => IntervalUnit.Year,
                ('M', false) => IntervalUnit.Month,
                ('W', false) => IntervalUnit.Week,
                ('D', false) => IntervalUnit.Day,
                ('H', true) => IntervalUnit.Hour,
                ('M', true) => IntervalUnit.Minute,
                ('S', true) => IntervalUnit.Second,
                _ => IntervalUnit.None,
            };

            if (unit == IntervalUnit.None)
                return false;

            accumulator.Add(number, unit);
            any = true;
            anyTime |= timePart;
        }

        // A 'T' must have at least one time designator after it.
        return any && (!timePart || anyTime);
    }

    private static bool ParseVerbose(ReadOnlySpan<char> span, ref Accumulator accumulator)
    {
        int i = 0;
        bool any = false;
        bool ago = false;

        SkipSpaces(span, ref i);
        if (i < span.Length && span[i] == '@')
        {
            i++;
            SkipSpaces(span, ref i);
        }

        while (i < span.Length)
        {
            if (ago)
                return false;

            if (char.IsLetter(span[i]))
            {
                int wordStart = i;
                while (i < span.Length && char.IsLetter(span[i]))
                    i++;

                if (!span[wordStart..i].Equals("ago", StringComparison.OrdinalIgnoreCase) || !any)
                    return false;

                ago = true;
                SkipSpaces(span, ref i);
                continue;
            }

            int numberStart = i;
            if (!TryReadNumber(span, ref i, out decimal number))
                return false;

            // A time field: [+-]hh:mm[:ss[.ffffff]]. Its sign applies to all of it.
            if (i < span.Length && span[i] == ':')
            {
                i = numberStart;
                if (!TryReadTime(span, ref i, ref accumulator))
                    return false;

                any = true;
                SkipSpaces(span, ref i);
                continue;
            }

            SkipSpaces(span, ref i);

            int unitStart = i;
            while (i < span.Length && char.IsLetter(span[i]))
                i++;

            if (unitStart == i)
            {
                // A number with no unit is seconds, and only as the last item.
                if (i < span.Length)
                    return false;

                accumulator.Add(number, IntervalUnit.Second);
                any = true;
                break;
            }

            if (!TryParseUnit(span[unitStart..i], out IntervalUnit unit))
            {
                // "ago" right after a bare number is not a unit.
                return false;
            }

            accumulator.Add(number, unit);
            any = true;
            SkipSpaces(span, ref i);
        }

        if (!any)
            return false;

        if (ago)
            accumulator.Negate();

        return true;
    }

    private static bool TryReadTime(ReadOnlySpan<char> span, ref int i, ref Accumulator accumulator)
    {
        bool negative = false;
        if (i < span.Length && span[i] is '+' or '-')
        {
            negative = span[i] == '-';
            i++;
        }

        if (!TryReadDigits(span, ref i, out decimal hours) || i >= span.Length || span[i] != ':')
            return false;

        i++;
        if (!TryReadDigits(span, ref i, out decimal minutes) || minutes >= 60)
            return false;

        decimal seconds = 0;
        if (i < span.Length && span[i] == ':')
        {
            i++;
            int secondsStart = i;
            while (i < span.Length && (char.IsAsciiDigit(span[i]) || span[i] == '.'))
                i++;

            if (!decimal.TryParse(span[secondsStart..i], NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out seconds) || seconds >= 60)
                return false;
        }

        decimal sign = negative ? -1 : 1;
        accumulator.Add(sign * hours, IntervalUnit.Hour);
        accumulator.Add(sign * minutes, IntervalUnit.Minute);
        accumulator.Add(sign * seconds, IntervalUnit.Second);
        return true;
    }

    private static bool TryReadDigits(ReadOnlySpan<char> span, ref int i, out decimal value)
    {
        int start = i;
        while (i < span.Length && char.IsAsciiDigit(span[i]))
            i++;

        value = 0;
        return i > start && decimal.TryParse(span[start..i], NumberStyles.None, CultureInfo.InvariantCulture, out value);
    }

    /// <summary>Reads <c>[+-]digits[.digits]</c> or <c>[+-].digits</c>.</summary>
    private static bool TryReadNumber(ReadOnlySpan<char> span, ref int i, out decimal value)
    {
        int start = i;

        if (i < span.Length && span[i] is '+' or '-')
            i++;

        int digitsStart = i;
        bool point = false;
        while (i < span.Length && (char.IsAsciiDigit(span[i]) || (span[i] == '.' && !point)))
        {
            if (span[i] == '.')
                point = true;
            i++;
        }

        value = 0;
        if (i == digitsStart || (i == digitsStart + 1 && point))
            return false;

        if (!decimal.TryParse(span[start..i], NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out value))
            throw new OverflowException();

        return true;
    }

    private static void SkipSpaces(ReadOnlySpan<char> span, ref int i)
    {
        while (i < span.Length && char.IsWhiteSpace(span[i]))
            i++;
    }

    // ─── Format ────────────────────────────────────────────────────────────

    /// <summary>
    /// The ISO-8601 text of an interval, in the PostgreSQL <c>iso_8601</c> output style:
    /// <c>P1Y2M3DT4H5M6.789S</c>. The years and the months share the sign of the months part, and the
    /// hours, minutes and seconds share the sign of the microseconds part. A zero interval is
    /// <c>PT0S</c>. This is the text a client receives and the result of <c>CAST(x AS string)</c>.
    /// </summary>
    public static string FormatIso(IntervalParts value)
    {
        if (value.Months == 0 && value.Days == 0 && value.Micros == 0)
            return "PT0S";

        StringBuilder text = new(32);
        text.Append('P');

        int years = value.Months / 12;
        int months = value.Months % 12;

        if (years != 0)
            text.Append(years.ToString(CultureInfo.InvariantCulture)).Append('Y');

        if (months != 0)
            text.Append(months.ToString(CultureInfo.InvariantCulture)).Append('M');

        if (value.Days != 0)
            text.Append(value.Days.ToString(CultureInfo.InvariantCulture)).Append('D');

        if (value.Micros != 0)
        {
            long micros = value.Micros;
            long hours = micros / MicrosPerHour;
            micros -= hours * MicrosPerHour;
            long minutes = micros / MicrosPerMinute;
            micros -= minutes * MicrosPerMinute;
            long seconds = micros / MicrosPerSecond;
            long fraction = micros - seconds * MicrosPerSecond;

            text.Append('T');

            if (hours != 0)
                text.Append(hours.ToString(CultureInfo.InvariantCulture)).Append('H');

            if (minutes != 0)
                text.Append(minutes.ToString(CultureInfo.InvariantCulture)).Append('M');

            if (seconds != 0 || fraction != 0)
            {
                if (seconds < 0 || fraction < 0)
                    text.Append('-');

                text.Append(Math.Abs(seconds).ToString(CultureInfo.InvariantCulture));

                if (fraction != 0)
                    text.Append('.').Append(Math.Abs(fraction).ToString("D6", CultureInfo.InvariantCulture).TrimEnd('0'));

                text.Append('S');
            }
        }

        return text.ToString();
    }

    // ─── Helpers ───────────────────────────────────────────────────────────

    private static CamusDBException OutOfRange() => new(CamusDBErrorCodes.InvalidInput, IntervalOutOfRangeMessage);

    private static bool FitsInt32(double value) => value >= int.MinValue && value <= int.MaxValue;

    // 2^63 is exact as a double; a value at or past it does not fit in a long.
    private static bool FitsInt64(double value) => value >= -9223372036854775808.0 && value < 9223372036854775808.0;

    /// <summary>Rounds to six decimal places, as the PostgreSQL <c>TSROUND</c> macro does.</summary>
    private static double RoundToMicros(double value) => Math.Round(value * 1e6, MidpointRounding.ToEven) / 1e6;

    /// <summary>
    /// Sums unit amounts with exact decimal arithmetic and applies the fraction rules once at the end
    /// of each <see cref="Add"/>. The months and the days are whole numbers after each step; only the
    /// microseconds keep a fraction until <see cref="ToParts"/> rounds them.
    /// </summary>
    private struct Accumulator
    {
        private decimal months;
        private decimal days;
        private decimal micros;

        public void Add(decimal amount, IntervalUnit unit)
        {
            switch (unit)
            {
                case IntervalUnit.Millennium: AddYears(amount, 1000); break;
                case IntervalUnit.Century: AddYears(amount, 100); break;
                case IntervalUnit.Decade: AddYears(amount, 10); break;
                case IntervalUnit.Year: AddYears(amount, 1); break;

                case IntervalUnit.Quarter:
                    AddMonths(amount * 3);
                    break;

                case IntervalUnit.Month:
                    AddMonths(amount);
                    break;

                case IntervalUnit.Week:
                    AddDays(amount * 7);
                    break;

                case IntervalUnit.Day:
                    AddDays(amount);
                    break;

                case IntervalUnit.Hour: micros += amount * MicrosPerHour; break;
                case IntervalUnit.Minute: micros += amount * MicrosPerMinute; break;
                case IntervalUnit.Second: micros += amount * MicrosPerSecond; break;
                case IntervalUnit.Millisecond: micros += amount * 1000; break;
                case IntervalUnit.Microsecond: micros += amount; break;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"invalid interval unit: {unit}");
            }
        }

        public void Negate()
        {
            months = -months;
            days = -days;
            micros = -micros;
        }

        public readonly IntervalParts ToParts()
        {
            decimal roundedMicros = decimal.Round(micros, MidpointRounding.ToEven);

            if (months < int.MinValue || months > int.MaxValue
                || days < int.MinValue || days > int.MaxValue
                || roundedMicros < long.MinValue || roundedMicros > long.MaxValue)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, IntervalOutOfRangeMessage);

            return new IntervalParts((int)months, (int)days, (long)roundedMicros);
        }

        // A fraction of a year rounds to whole months, as PostgreSQL does: '1.1 years' is 1 year 1 month.
        private void AddYears(decimal amount, int scale)
        {
            decimal whole = decimal.Truncate(amount);
            months += whole * 12 * scale + decimal.Round((amount - whole) * 12 * scale, MidpointRounding.ToEven);
        }

        // A fraction of a month moves into days (30 a month), and the rest of that into microseconds.
        private void AddMonths(decimal amount)
        {
            decimal whole = decimal.Truncate(amount);
            months += whole;
            AddDays((amount - whole) * DaysPerMonth);
        }

        // A fraction of a day moves into microseconds (24 hours a day).
        private void AddDays(decimal amount)
        {
            decimal whole = decimal.Truncate(amount);
            days += whole;
            micros += (amount - whole) * MicrosPerDay;
        }
    }
}

/// <summary>The units an interval literal, <c>make_interval</c> and <c>DATE_ADD(d, INTERVAL n unit)</c> accept.</summary>
public enum IntervalUnit
{
    None,
    Millennium,
    Century,
    Decade,
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
}
