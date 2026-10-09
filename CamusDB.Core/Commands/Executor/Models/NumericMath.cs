/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Numerics;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>How <see cref="NumericMath.Round"/> treats the digits it drops.</summary>
public enum NumericRounding
{
    /// <summary>Half away from zero: <c>round</c>.</summary>
    HalfAwayFromZero,

    /// <summary>Drop the digits: <c>trunc</c>.</summary>
    TowardZero,

    /// <summary>Toward negative infinity: <c>floor</c>.</summary>
    Floor,

    /// <summary>Toward positive infinity: <c>ceil</c>.</summary>
    Ceiling,
}

/// <summary>Outcome of <see cref="NumericMath.TryParse"/> and the other conversions into NUMERIC.</summary>
public enum NumericConversionStatus
{
    /// <summary>The input is a valid number inside the NUMERIC range (after rounding).</summary>
    Ok,

    /// <summary>The input is not a number (bad syntax, NaN or infinity).</summary>
    Invalid,

    /// <summary>The input is a number, but its magnitude is larger than the NUMERIC range.</summary>
    Overflow,
}

/// <summary>
/// The one implementation of <see cref="Catalogs.Models.ColumnType.Numeric"/> arithmetic. Every
/// parse, format, rounding and range rule for the type lives here, so the row writer, CAST, the
/// arithmetic evaluator and the planner cannot drift apart.
///
/// <para><b>Representation.</b> A NUMERIC value is an <see cref="Int128"/> "unscaled" integer equal to
/// the value times 10⁹. The type has fixed precision 38 and fixed scale 9, as Spanner GoogleSQL
/// <c>NUMERIC</c>, so the unscaled magnitude is at most 10³⁸ − 1 (29 integer digits, 9 fractional
/// digits). Because the scale is fixed, every value has exactly one representation: equality is bit
/// equality, ordering is a signed <see cref="Int128"/> compare, and a hash of the two 64-bit halves is
/// consistent with both. No code may produce an unscaled value outside
/// [<see cref="MinUnscaled"/>, <see cref="MaxUnscaled"/>]; every entry point here checks the range.</para>
///
/// <para><b>Rounding.</b> A value with more than nine fractional digits rounds half away from zero, the
/// Spanner rule for a CAST to NUMERIC. Multiplication and division round their exact result the same
/// way. A result outside the range is an error (<see cref="CamusDBErrorCodes.NumericValueOutOfRange"/>),
/// never a truncation or a wrap.</para>
/// </summary>
public static class NumericMath
{
    /// <summary>Count of digits after the decimal point. Fixed for the type.</summary>
    public const int Scale = 9;

    /// <summary>Total count of significant digits. Fixed for the type.</summary>
    public const int Precision = 38;

    /// <summary>10^<see cref="Scale"/>: the unscaled representation of the value 1.</summary>
    public const long ScaleFactor = 1_000_000_000L;

    /// <summary>The largest unscaled value: 10³⁸ − 1, which is 99999999999999999999999999999.999999999.</summary>
    public static readonly Int128 MaxUnscaled = BuildMaxUnscaled();

    /// <summary>The smallest unscaled value: −(10³⁸ − 1).</summary>
    public static readonly Int128 MinUnscaled = -MaxUnscaled;

    private static readonly UInt128 MaxMagnitude = (UInt128)MaxUnscaled;

    /// <summary>The largest magnitude that can be multiplied by <see cref="ScaleFactor"/> without a UInt128 overflow.</summary>
    private static readonly UInt128 MaxScalableMagnitude = UInt128.MaxValue / ScaleFactor;

    private static readonly Int128[] PowersOfTen = BuildPowersOfTen();

    private static readonly double TwoPow53 = 9007199254740992.0;

    /// <summary>The exponent value past which <see cref="TryParse"/> stops accumulating digits (10¹⁵).</summary>
    private const long ExponentClamp = 1_000_000_000_000_000;

    /// <summary>Room for the canonical text of any Int128: sign, 30 integer digits, point, 9 fraction digits.</summary>
    private const int MaxTextLength = 48;

    private static Int128 BuildMaxUnscaled()
    {
        Int128 value = 1;
        for (int i = 0; i < Precision; i++)
            value *= 10;
        return value - 1;
    }

    private static Int128[] BuildPowersOfTen()
    {
        Int128[] powers = new Int128[Precision + 1];
        powers[0] = 1;
        for (int i = 1; i < powers.Length; i++)
            powers[i] = powers[i - 1] * 10;
        return powers;
    }

    /// <summary>True when <paramref name="unscaled"/> is inside the NUMERIC range.</summary>
    public static bool IsInRange(Int128 unscaled) => unscaled >= MinUnscaled && unscaled <= MaxUnscaled;

    // ── Parse ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Parses a decimal number: optional sign, digits with an optional decimal point, and an optional
    /// exponent (<c>1.5e3</c>). Surrounding white space is ignored. Digits past the ninth fractional
    /// digit round half away from zero, so <c>1.0000000005</c> gives <c>1.000000001</c>. The input is
    /// read exactly — never through <see cref="double"/> — which is why a literal reaches a NUMERIC
    /// column without a binary rounding error.
    /// </summary>
    public static NumericConversionStatus TryParse(ReadOnlySpan<char> text, out Int128 unscaled)
    {
        unscaled = 0;
        text = text.Trim();

        if (text.IsEmpty)
            return NumericConversionStatus.Invalid;

        int i = 0;
        bool negative = false;

        if (text[0] is '+' or '-')
        {
            negative = text[0] == '-';
            i++;
        }

        int mantissaStart = i;
        int integerDigits = 0;
        int fractionDigits = 0;
        bool seenPoint = false;

        for (; i < text.Length; i++)
        {
            char c = text[i];

            if (c is >= '0' and <= '9')
            {
                if (seenPoint)
                    fractionDigits++;
                else
                    integerDigits++;
            }
            else if (c == '.' && !seenPoint)
            {
                seenPoint = true;
            }
            else
            {
                break;
            }
        }

        int mantissaEnd = i;
        int totalDigits = integerDigits + fractionDigits;

        if (totalDigits == 0)
            return NumericConversionStatus.Invalid;

        long exponent = 0;

        if (i < text.Length)
        {
            if (text[i] is not ('e' or 'E'))
                return NumericConversionStatus.Invalid;

            i++;

            bool exponentNegative = false;

            if (i < text.Length && text[i] is '+' or '-')
            {
                exponentNegative = text[i] == '-';
                i++;
            }

            if (i >= text.Length)
                return NumericConversionStatus.Invalid;

            for (; i < text.Length; i++)
            {
                char c = text[i];

                if (c is < '0' or > '9')
                    return NumericConversionStatus.Invalid;

                // Clamp far above anything the mantissa can offset: the fraction length of a string
                // is below 2³¹, so a clamped exponent still decides the outcome (overflow or zero)
                // after the shift below subtracts it. The clamp keeps that arithmetic free of long
                // overflow. A clamp near the fraction length would let a long run of fractional
                // digits cancel a huge exponent and turn an overflow into an ordinary value.
                if (exponent < ExponentClamp)
                    exponent = exponent * 10 + (c - '0');
            }

            if (exponentNegative)
                exponent = -exponent;
        }

        // The mantissa digits M give value = M × 10^(exponent − fractionDigits), so
        // unscaled = M × 10^shift. A negative shift drops |shift| trailing digits.
        long shift = exponent - fractionDigits + Scale;
        long keep = shift >= 0 ? totalDigits : totalDigits + shift;

        UInt128 magnitude = 0;
        bool roundUp = false;
        long seen = 0;

        for (int j = mantissaStart; j < mantissaEnd; j++)
        {
            char c = text[j];

            if (c == '.')
                continue;

            uint digit = (uint)(c - '0');

            if (seen < keep)
            {
                if (magnitude > (MaxMagnitude - digit) / 10)
                    return NumericConversionStatus.Overflow;

                magnitude = magnitude * 10 + digit;
            }
            else if (seen == keep)
            {
                // The first dropped digit decides the rounding: 5 or more is at least half a unit,
                // and half away from zero rounds a tie up in magnitude.
                roundUp = digit >= 5;
            }

            seen++;
        }

        if (shift > 0 && magnitude != 0)
        {
            if (shift > Precision)
                return NumericConversionStatus.Overflow;

            UInt128 factor = (UInt128)PowersOfTen[shift];

            if (magnitude > MaxMagnitude / factor)
                return NumericConversionStatus.Overflow;

            magnitude *= factor;
        }

        if (roundUp)
            magnitude++;

        if (magnitude > MaxMagnitude)
            return NumericConversionStatus.Overflow;

        unscaled = negative ? -(Int128)magnitude : (Int128)magnitude;
        return NumericConversionStatus.Ok;
    }

    // ── Format ───────────────────────────────────────────────────────────────

    /// <summary>
    /// The canonical text of a value: an optional minus sign, the integer digits, and the fractional
    /// digits with trailing zeros removed (no decimal point when the fraction is zero). So the value 1.1
    /// gives <c>"1.1"</c>, 5 gives <c>"5"</c>, and there is never an exponent. This is the form CAST to
    /// STRING returns and the form the HTTP and gRPC surfaces carry, as Spanner does.
    /// </summary>
    public static string Format(Int128 unscaled)
    {
        Span<char> buffer = stackalloc char[MaxTextLength];
        int written = Format(unscaled, buffer);
        return new string(buffer[..written]);
    }

    /// <summary>Writes the canonical text of <paramref name="unscaled"/> into <paramref name="destination"/> (at least 41 chars) and returns the length.</summary>
    public static int Format(Int128 unscaled, Span<char> destination)
    {
        int pos = 0;

        if (unscaled < 0)
            destination[pos++] = '-';

        UInt128 magnitude = unscaled < 0 ? (UInt128)(-unscaled) : (UInt128)unscaled;
        UInt128 integerPart = magnitude / ScaleFactor;
        ulong fraction = (ulong)(magnitude % ScaleFactor);

        integerPart.TryFormat(destination[pos..], out int integerLength, default, CultureInfo.InvariantCulture);
        pos += integerLength;

        if (fraction == 0)
            return pos;

        destination[pos++] = '.';

        Span<char> digits = destination.Slice(pos, Scale);
        for (int i = Scale - 1; i >= 0; i--)
        {
            digits[i] = (char)('0' + (int)(fraction % 10));
            fraction /= 10;
        }

        int length = Scale;
        while (digits[length - 1] == '0')
            length--;

        return pos + length;
    }

    // ── Conversions ──────────────────────────────────────────────────────────

    /// <summary>The unscaled form of an integer. Always in range: |long| &lt; 10¹⁹ &lt; 10²⁹.</summary>
    public static Int128 FromInt64(long value) => (Int128)value * ScaleFactor;

    /// <summary>
    /// Converts a <see cref="double"/> exactly: the binary value is expanded without loss and then
    /// rounded half away from zero to nine fractional digits, the Spanner rule for a CAST of FLOAT64 to
    /// NUMERIC. NaN and the infinities are <see cref="NumericConversionStatus.Invalid"/>.
    /// </summary>
    public static NumericConversionStatus TryFromDouble(double value, out Int128 unscaled)
    {
        unscaled = 0;

        if (!double.IsFinite(value))
            return NumericConversionStatus.Invalid;

        if (value == 0)
            return NumericConversionStatus.Ok;

        long bits = BitConverter.DoubleToInt64Bits(value);
        bool negative = bits < 0;
        int biasedExponent = (int)((bits >> 52) & 0x7FF);
        ulong fraction = (ulong)bits & 0xF_FFFF_FFFF_FFFFUL;

        // value = mantissa × 2^exponent, exactly.
        ulong mantissa = biasedExponent == 0 ? fraction : fraction | (1UL << 52);
        int exponent = biasedExponent == 0 ? -1074 : biasedExponent - 1075;

        UInt128 magnitude;

        if (exponent >= 0)
        {
            // mantissa ≥ 2^52 for a normal value, so a shift of 45 or more is at least 2^97 > 10²⁹.
            if (exponent >= 45)
                return NumericConversionStatus.Overflow;

            UInt128 integer = (UInt128)mantissa << exponent;

            if (integer > MaxMagnitude / ScaleFactor)
                return NumericConversionStatus.Overflow;

            magnitude = integer * ScaleFactor;
        }
        else
        {
            int k = -exponent;
            UInt128 numerator = (UInt128)mantissa * ScaleFactor; // < 2^83

            if (k > 84)
            {
                magnitude = 0; // numerator / 2^k < 0.5
            }
            else
            {
                magnitude = numerator >> k;
                UInt128 remainder = numerator - (magnitude << k);

                if (remainder >= (UInt128.One << (k - 1)))
                    magnitude++;
            }
        }

        if (magnitude > MaxMagnitude)
            return NumericConversionStatus.Overflow;

        unscaled = negative ? -(Int128)magnitude : (Int128)magnitude;
        return NumericConversionStatus.Ok;
    }

    /// <summary>
    /// The closest <see cref="double"/>. Exact enough for every value: below 2⁵³ the division by the
    /// exact power 10⁹ rounds once; above it the canonical text is parsed, which also rounds once.
    /// </summary>
    public static double ToDouble(Int128 unscaled)
    {
        if (unscaled <= (Int128)(long)TwoPow53 && unscaled >= -(Int128)(long)TwoPow53)
            return (double)(long)unscaled / ScaleFactor;

        Span<char> text = stackalloc char[MaxTextLength];
        int length = Format(unscaled, text);
        return double.Parse(text[..length], NumberStyles.Float, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// The closest <see cref="float"/>, rounded once from the exact value. Going through
    /// <see cref="ToDouble"/> first would round twice: a value just above a float midpoint can round
    /// onto the midpoint as a double and then to the wrong float (100000004.000000001 must give
    /// 100000008, not 100000000). The exact decimal text is parsed straight to single precision
    /// instead, which the runtime rounds correctly; the text is formatted on the stack.
    /// </summary>
    public static float ToSingle(Int128 unscaled)
    {
        Span<char> text = stackalloc char[MaxTextLength];
        int length = Format(unscaled, text);
        return float.Parse(text[..length], NumberStyles.Float, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Rounds half away from zero to an integer, the Spanner rule for CAST of NUMERIC to INT64. Returns
    /// false when the integer does not fit in <see cref="long"/>.
    /// </summary>
    public static bool TryToInt64(Int128 unscaled, out long value)
    {
        Int128 quotient = unscaled / ScaleFactor;
        Int128 remainder = unscaled % ScaleFactor;

        if (remainder >= ScaleFactor / 2)
            quotient++;
        else if (remainder <= -(ScaleFactor / 2))
            quotient--;

        if (quotient > long.MaxValue || quotient < long.MinValue)
        {
            value = 0;
            return false;
        }

        value = (long)quotient;
        return true;
    }

    // ── Arithmetic ───────────────────────────────────────────────────────────

    /// <summary>Exact sum. Throws <see cref="CamusDBErrorCodes.NumericValueOutOfRange"/> outside the range.</summary>
    public static Int128 Add(Int128 left, Int128 right)
    {
        // Both operands are inside ±(10³⁸ − 1), so these bounds never overflow Int128 themselves.
        if (right > 0 ? left > MaxUnscaled - right : left < MinUnscaled - right)
            throw OutOfRange("+");

        return left + right;
    }

    /// <summary>Exact difference. Throws <see cref="CamusDBErrorCodes.NumericValueOutOfRange"/> outside the range.</summary>
    public static Int128 Subtract(Int128 left, Int128 right) => Add(left, -right);

    /// <summary>Exact negation. Always in range: the range is symmetric.</summary>
    public static Int128 Negate(Int128 value) => -value;

    /// <summary>
    /// The product, rounded half away from zero to nine fractional digits. Throws
    /// <see cref="CamusDBErrorCodes.NumericValueOutOfRange"/> outside the range.
    /// </summary>
    public static Int128 Multiply(Int128 left, Int128 right)
    {
        if (left == 0 || right == 0)
            return 0;

        bool negative = (left < 0) != (right < 0);
        UInt128 a = Magnitude(left);
        UInt128 b = Magnitude(right);
        UInt128 magnitude;

        // The exact product fits in 128 bits when both magnitudes fit in 64 bits (no division needed
        // to know it), or when one division proves it. Only a product wider than 128 bits takes the
        // allocating BigInteger path.
        if ((a <= ulong.MaxValue && b <= ulong.MaxValue) || a <= UInt128.MaxValue / b)
        {
            UInt128 product = a * b;
            magnitude = product / ScaleFactor;

            if (product % ScaleFactor >= ScaleFactor / 2)
                magnitude++;
        }
        else
        {
            BigInteger product = (BigInteger)a * b;
            BigInteger quotient = BigInteger.DivRem(product, ScaleFactor, out BigInteger remainder);

            if (remainder >= ScaleFactor / 2)
                quotient++;

            if (quotient > (BigInteger)MaxMagnitude)
                throw OutOfRange("*");

            magnitude = (UInt128)quotient;
        }

        if (magnitude > MaxMagnitude)
            throw OutOfRange("*");

        return negative ? -(Int128)magnitude : (Int128)magnitude;
    }

    /// <summary>
    /// The quotient, rounded half away from zero to nine fractional digits. A zero divisor is an
    /// <see cref="CamusDBErrorCodes.InvalidInput"/> "Division by zero" error, the message every other
    /// numeric type gives. Throws <see cref="CamusDBErrorCodes.NumericValueOutOfRange"/> outside the range.
    /// </summary>
    public static Int128 Divide(Int128 left, Int128 right)
    {
        if (right == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Division by zero");

        bool negative = (left < 0) != (right < 0);
        UInt128 a = Magnitude(left);
        UInt128 b = Magnitude(right);
        UInt128 magnitude;

        if (a <= MaxScalableMagnitude)
        {
            UInt128 numerator = a * ScaleFactor;
            magnitude = numerator / b;
            UInt128 remainder = numerator % b;

            // remainder < b ≤ 10³⁸, so doubling it stays below UInt128.MaxValue (≈ 3.4 × 10³⁸).
            if (remainder * 2 >= b)
                magnitude++;
        }
        else
        {
            BigInteger quotient = BigInteger.DivRem((BigInteger)a * ScaleFactor, b, out BigInteger remainder);

            if (remainder * 2 >= b)
                quotient++;

            if (quotient > (BigInteger)MaxMagnitude)
                throw OutOfRange("/");

            magnitude = (UInt128)quotient;
        }

        if (magnitude > MaxMagnitude)
            throw OutOfRange("/");

        return negative ? -(Int128)magnitude : (Int128)magnitude;
    }

    /// <summary>
    /// The remainder of truncated division; the result has the sign of the dividend, as Spanner
    /// <c>MOD</c>. Exact, so always in range. A zero divisor is a "Division by zero" error.
    /// </summary>
    public static Int128 Remainder(Int128 left, Int128 right)
    {
        if (right == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Division by zero");

        // Both operands share the scale, so the remainder of the unscaled values is the unscaled remainder.
        return left % right;
    }

    /// <summary>
    /// Rounds to <paramref name="digits"/> digits after the decimal point; a negative count rounds to
    /// the left of it (<c>round(1234.5, -2)</c> is 1200). Nine or more digits leave the value unchanged,
    /// because the type holds nine. A result past the range (<c>ceil</c> of the maximum) throws
    /// <see cref="CamusDBErrorCodes.NumericValueOutOfRange"/>.
    /// </summary>
    public static Int128 Round(Int128 unscaled, long digits, NumericRounding mode, string operation)
    {
        if (digits >= Scale)
            return unscaled;

        long drop = Scale - digits;

        if (drop > Precision)
        {
            // The unit is past every value: the result is zero, or one unit away from zero, which is
            // past the range.
            bool awayFromZero = mode switch
            {
                NumericRounding.Floor => unscaled < 0,
                NumericRounding.Ceiling => unscaled > 0,
                _ => false,
            };

            return awayFromZero ? throw OutOfRange(operation) : 0;
        }

        Int128 unit = PowersOfTen[drop];
        Int128 quotient = unscaled / unit;   // toward zero
        Int128 remainder = unscaled % unit;  // sign of the value

        switch (mode)
        {
            case NumericRounding.HalfAwayFromZero:
                if (remainder >= unit - remainder && remainder > 0)
                    quotient++;
                else if (-remainder >= unit + remainder && remainder < 0)
                    quotient--;
                break;

            case NumericRounding.Floor:
                if (remainder < 0)
                    quotient--;
                break;

            case NumericRounding.Ceiling:
                if (remainder > 0)
                    quotient++;
                break;
        }

        // The product cannot overflow Int128: |result| ≤ |unscaled| + unit, which is at most
        // 1.1 × 10³⁸ for unit ≤ 10³⁷; for unit = 10³⁸ the quotient is −1, 0 or 1, so |result| ≤ 10³⁸.
        // Both are below Int128.MaxValue (about 1.7 × 10³⁸). The range check below still applies.
        Int128 result = quotient * unit;

        if (!IsInRange(result))
            throw OutOfRange(operation);

        return result;
    }

    private static UInt128 Magnitude(Int128 value) => value < 0 ? (UInt128)(-value) : (UInt128)value;

    /// <summary>The error for a result outside the NUMERIC range.</summary>
    public static CamusDBException OutOfRange(string operation) =>
        new(CamusDBErrorCodes.NumericValueOutOfRange,
            $"NUMERIC overflow in {operation}: the result is outside the range ±99999999999999999999999999999.999999999");
}
