/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Numerics;
using System.Text;

namespace CamusDB.Core.CommandsExecutor.Models.Jsonb;

/// <summary>
/// An exact JSON number: <c>Mantissa / 10^Scale</c>, the model of PostgreSQL <c>numeric</c> inside
/// <c>jsonb</c>. The scale is never negative and keeps the source scale, so <c>1.50</c> reads back as
/// <c>1.50</c>, and exponent notation is made plain (<c>1.5e1</c> is 15, <c>1.50e1</c> is 15.0).
///
/// <para>Two numbers with different scales can be equal (<c>1.0 = 1</c>). <see cref="Compare"/> and
/// <see cref="AddHash"/> agree on that: the hash strips trailing fraction zeros first.</para>
/// </summary>
internal readonly struct JsonbNumber
{
    public BigInteger Mantissa { get; }

    public int Scale { get; }

    public JsonbNumber(BigInteger mantissa, int scale)
    {
        Mantissa = mantissa;
        Scale = scale;
    }

    /// <summary>
    /// Parses the UTF-8 text of a number that a JSON reader already accepted, so the grammar is
    /// <c>-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?</c>. Refuses a number past the PostgreSQL
    /// <c>numeric</c> limits (<see cref="JsonbLimits.MaxIntegerDigits"/>,
    /// <see cref="JsonbLimits.MaxScale"/>) with <see cref="CamusDBErrorCodes.InvalidJsonText"/>.
    /// </summary>
    public static JsonbNumber ParseJsonText(ReadOnlySpan<byte> text)
    {
        int i = 0;
        bool negative = false;
        if (i < text.Length && text[i] == (byte)'-')
        {
            negative = true;
            i++;
        }

        int intStart = i;
        while (i < text.Length && IsDigit(text[i]))
            i++;
        int intLength = i - intStart;

        int fracStart = i;
        int fracLength = 0;
        if (i < text.Length && text[i] == (byte)'.')
        {
            i++;
            fracStart = i;
            while (i < text.Length && IsDigit(text[i]))
                i++;
            fracLength = i - fracStart;
        }

        long exponent = 0;
        if (i < text.Length && (text[i] == (byte)'e' || text[i] == (byte)'E'))
        {
            i++;
            bool negativeExponent = false;
            if (i < text.Length && (text[i] == (byte)'+' || text[i] == (byte)'-'))
            {
                negativeExponent = text[i] == (byte)'-';
                i++;
            }

            while (i < text.Length && IsDigit(text[i]))
            {
                // Saturate: a value this large is past every limit below, whatever its exact value.
                if (exponent < 1_000_000_000)
                    exponent = exponent * 10 + (text[i] - (byte)'0');
                i++;
            }

            if (negativeExponent)
                exponent = -exponent;
        }

        if (intLength == 0 || i != text.Length)
            throw new CamusDBException(CamusDBErrorCodes.InvalidJsonText, "invalid input syntax for type json: malformed number");

        ReadOnlySpan<byte> integerPart = text.Slice(intStart, intLength);
        ReadOnlySpan<byte> fractionPart = fracLength > 0 ? text.Slice(fracStart, fracLength) : default;

        // PostgreSQL checks the digits that carry the value: leading zeros do not count, and a zero
        // takes any positive exponent (0e999999 is 0).
        int significant = SignificantDigits(integerPart, fractionPart);
        long scale = fracLength - exponent;
        if (scale > JsonbLimits.MaxScale || (significant > 0 && significant - scale > JsonbLimits.MaxIntegerDigits))
            throw new CamusDBException(CamusDBErrorCodes.InvalidJsonText, "value overflows numeric format");

        BigInteger mantissa = ParseDigits(integerPart, fractionPart);

        if (scale < 0)
        {
            if (!mantissa.IsZero)
                mantissa *= BigInteger.Pow(10, (int)-scale);
            scale = 0;
        }

        // PostgreSQL numeric has no negative zero: -0 and -0.0 read back as 0 and 0.0.
        if (negative && !mantissa.IsZero)
            mantissa = -mantissa;

        return new JsonbNumber(mantissa, (int)scale);
    }

    private static bool IsDigit(byte b) => b is >= (byte)'0' and <= (byte)'9';

    private static int SignificantDigits(ReadOnlySpan<byte> integerPart, ReadOnlySpan<byte> fractionPart)
    {
        int total = integerPart.Length + fractionPart.Length;
        for (int k = 0; k < total; k++)
        {
            byte digit = k < integerPart.Length ? integerPart[k] : fractionPart[k - integerPart.Length];
            if (digit != (byte)'0')
                return total - k;
        }

        return 0;
    }

    private static BigInteger ParseDigits(ReadOnlySpan<byte> integerPart, ReadOnlySpan<byte> fractionPart)
    {
        int total = integerPart.Length + fractionPart.Length;

        // The common case fits a long: 18 digits never overflow.
        if (total <= 18)
        {
            long value = 0;
            foreach (byte b in integerPart)
                value = value * 10 + (b - (byte)'0');
            foreach (byte b in fractionPart)
                value = value * 10 + (b - (byte)'0');
            return value;
        }

        char[] digits = new char[total];
        for (int k = 0; k < integerPart.Length; k++)
            digits[k] = (char)integerPart[k];
        for (int k = 0; k < fractionPart.Length; k++)
            digits[integerPart.Length + k] = (char)fractionPart[k];

        return BigInteger.Parse(digits, NumberStyles.None, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// The number for a FLOAT64 value: its shortest round-trip decimal text, as PostgreSQL
    /// <c>to_jsonb(float8)</c> gives. NaN and infinity have no JSON form and are refused.
    /// </summary>
    public static JsonbNumber FromDouble(double value)
    {
        if (!double.IsFinite(value))
            throw new CamusDBException(CamusDBErrorCodes.InvalidJsonText, "cannot convert NaN or infinity to jsonb");

        string text = value.ToString("R", CultureInfo.InvariantCulture);
        return ParseJsonText(Encoding.ASCII.GetBytes(text));
    }

    /// <summary>
    /// Compares two numbers by value: <c>1.0</c> and <c>1</c> are equal. The smaller scale is scaled
    /// up to the larger one before the mantissas are compared.
    /// </summary>
    public static int Compare(in JsonbNumber a, in JsonbNumber b)
    {
        if (a.Scale == b.Scale)
            return a.Mantissa.CompareTo(b.Mantissa);

        int signA = a.Mantissa.Sign;
        int signB = b.Mantissa.Sign;
        if (signA != signB)
            return signA.CompareTo(signB);

        return a.Scale < b.Scale
            ? (a.Mantissa * BigInteger.Pow(10, b.Scale - a.Scale)).CompareTo(b.Mantissa)
            : a.Mantissa.CompareTo(b.Mantissa * BigInteger.Pow(10, a.Scale - b.Scale));
    }

    /// <summary>
    /// Adds the value, not the spelling, to <paramref name="hash"/>: trailing fraction zeros are
    /// stripped first, so equal numbers with different scales hash the same.
    /// </summary>
    public void AddHash(ref HashCode hash)
    {
        BigInteger mantissa = Mantissa;
        int scale = Scale;
        while (scale > 0 && !mantissa.IsZero)
        {
            BigInteger quotient = BigInteger.DivRem(mantissa, 10, out BigInteger remainder);
            if (!remainder.IsZero)
                break;
            mantissa = quotient;
            scale--;
        }

        if (mantissa.IsZero)
            scale = 0;

        AddNormalizedHash(ref hash, mantissa, scale);
    }

    /// <summary>The hash of an integer read straight from a CBOR integer item: scale 0 by definition.</summary>
    public static void AddIntegerHash(ref HashCode hash, long value) => AddNormalizedHash(ref hash, value, 0);

    private static void AddNormalizedHash(ref HashCode hash, BigInteger mantissa, int scale)
    {
        if (mantissa >= long.MinValue && mantissa <= long.MaxValue)
            hash.Add((long)mantissa);
        else
            hash.AddBytes(mantissa.ToByteArray());

        hash.Add(scale);
    }

    /// <summary>The PostgreSQL text form: plain digits, a point when the scale is positive.</summary>
    public void AppendText(StringBuilder output)
    {
        if (Scale == 0)
        {
            output.Append(Mantissa.ToString(CultureInfo.InvariantCulture));
            return;
        }

        string digits = BigInteger.Abs(Mantissa).ToString(CultureInfo.InvariantCulture);
        if (digits.Length <= Scale)
            digits = new string('0', Scale - digits.Length + 1) + digits;

        if (Mantissa.Sign < 0)
            output.Append('-');

        output.Append(digits, 0, digits.Length - Scale);
        output.Append('.');
        output.Append(digits, digits.Length - Scale, Scale);
    }

    public override string ToString()
    {
        StringBuilder builder = new();
        AppendText(builder);
        return builder.ToString();
    }

    /// <summary>The value as a double, for a cast of a JSON number to FLOAT64.</summary>
    public double ToDouble() =>
        double.Parse(ToString(), NumberStyles.Float, CultureInfo.InvariantCulture);
}
