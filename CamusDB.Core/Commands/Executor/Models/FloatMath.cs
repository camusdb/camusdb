/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// The one rule for Float64 and Float32 values: the PostgreSQL rule. Every SQL path that orders,
/// computes, converts or parses a float goes through this class, so a table scan, an index seek, a
/// sort, a hash group and a cluster fragment all agree.
///
/// <list type="bullet">
///   <item><b>Non-finite values are legal.</b> <c>'Infinity'</c>, <c>'-Infinity'</c> and
///   <c>'NaN'</c> are values that a CAST, a parameter or a typed row writes on purpose. They are
///   stored, returned, indexed and compared like any other value.</item>
///   <item><b>Overflow and underflow from finite inputs are errors.</b> A result that IEEE arithmetic
///   would make ±infinity (<c>1e308 * 10</c>) or zero (<c>1e-300 * 1e-300</c>) from finite, nonzero
///   inputs is <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/>, never a stored value. A
///   subnormal result is not an underflow: it is a nonzero value that the type holds. A literal or a
///   string past the range is the same error.</item>
///   <item><b>Order.</b> <see cref="Compare(double, double)"/> is a total order: NaN equals NaN and
///   sorts above every other value, +infinity included, and -0 equals 0. <c>double.CompareTo</c>
///   puts NaN below every value, so it must not order SQL values.</item>
///   <item><b>One SQL value, one key.</b> <see cref="Canonical(double)"/> maps -0 to 0 and every NaN
///   to one bit pattern, so an index holds one key for one SQL value. The stored row keeps the
///   value as written, so a SELECT of a stored -0 returns -0.</item>
/// </list>
///
/// <para><b>Cost.</b> A check is one or two predictable compares on the result, and the methods
/// inline into the caller. Only an error pays for the exception, through an out-of-line throw
/// helper, because a method with a throw or a try block does not inline.</para>
/// </summary>
internal static class FloatMath
{
    /// <summary>The NaN that an index key holds for every NaN: the positive quiet NaN.</summary>
    private static readonly double CanonicalNaN = BitConverter.Int64BitsToDouble(0x7FF8_0000_0000_0000);

    /// <summary>The single-precision counterpart of <see cref="CanonicalNaN"/>.</summary>
    private static readonly float CanonicalSingleNaN = BitConverter.Int32BitsToSingle(0x7FC0_0000);

    /// <summary>
    /// The SQL order of two doubles: -infinity &lt; finite values &lt; +infinity &lt; NaN, with
    /// NaN = NaN and -0 = 0. This matches the order of <see cref="Canonical(double)"/> keys.
    /// </summary>
    public static int Compare(double a, double b)
    {
        if (a < b)
            return -1;

        if (a > b)
            return 1;

        if (a == b)
            return 0;

        // At least one operand is NaN.
        return double.IsNaN(a) ? (double.IsNaN(b) ? 0 : 1) : -1;
    }

    /// <summary>The single-precision counterpart of <see cref="Compare(double, double)"/>.</summary>
    public static int Compare(float a, float b)
    {
        if (a < b)
            return -1;

        if (a > b)
            return 1;

        if (a == b)
            return 0;

        return float.IsNaN(a) ? (float.IsNaN(b) ? 0 : 1) : -1;
    }

    /// <summary>SQL equality of two doubles: <see cref="Compare(double, double)"/> is 0.</summary>
    public static bool SqlEquals(double a, double b) => a == b || (double.IsNaN(a) && double.IsNaN(b));

    /// <summary>
    /// The value an index key encodes for <paramref name="value"/>: 0 for -0, and one NaN for every
    /// NaN, whatever its sign or payload. Two values that compare equal get one key.
    /// </summary>
    public static double Canonical(double value)
    {
        if (value == 0)
            return 0.0;

        return double.IsNaN(value) ? CanonicalNaN : value;
    }

    /// <summary>The single-precision counterpart of <see cref="Canonical(double)"/>.</summary>
    public static float Canonical(float value)
    {
        if (value == 0)
            return 0.0f;

        return float.IsNaN(value) ? CanonicalSingleNaN : value;
    }

    /// <summary><c>l + r</c>, or <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/> on an overflow.</summary>
    public static double Add(double l, double r)
    {
        double sum = l + r;

        // A sum of two finite values is never zero by underflow: it is exact near zero.
        if (double.IsInfinity(sum) && double.IsFinite(l) && double.IsFinite(r))
            ThrowBinary(l, "+", r, overflow: true);

        return sum;
    }

    /// <summary><c>l - r</c>, or <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/> on an overflow.</summary>
    public static double Subtract(double l, double r)
    {
        double difference = l - r;

        if (double.IsInfinity(difference) && double.IsFinite(l) && double.IsFinite(r))
            ThrowBinary(l, "-", r, overflow: true);

        return difference;
    }

    /// <summary><c>l * r</c>, or <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/> on an overflow or an underflow.</summary>
    public static double Multiply(double l, double r)
    {
        double product = l * r;

        if (double.IsInfinity(product) && double.IsFinite(l) && double.IsFinite(r))
            ThrowBinary(l, "*", r, overflow: true);

        // Zero from two nonzero operands. A NaN operand gives NaN, and an infinite one gives an
        // infinity or NaN, so neither reaches here.
        if (product == 0 && l != 0 && r != 0)
            ThrowBinary(l, "*", r, overflow: false);

        return product;
    }

    /// <summary>
    /// <c>l / r</c>, or <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/> on an overflow or an
    /// underflow. The caller refuses a zero divisor first, so <c>x / 0</c> is a division-by-zero
    /// error and never an infinity.
    /// </summary>
    public static double Divide(double l, double r)
    {
        double quotient = l / r;

        if (double.IsInfinity(quotient) && double.IsFinite(l))
            ThrowBinary(l, "/", r, overflow: true);

        // A finite value over an infinity is zero by definition, not by underflow.
        if (quotient == 0 && l != 0 && !double.IsInfinity(r))
            ThrowBinary(l, "/", r, overflow: false);

        return quotient;
    }

    /// <summary>
    /// Narrows a Float64 value to Float32: <c>CAST</c>, a write into a <c>float32</c> column, and a
    /// Float32 arithmetic result computed in double. A finite value past the single-precision range
    /// is an overflow and a nonzero value that rounds to zero is an underflow; both are
    /// <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/>. ±infinity and NaN narrow to themselves.
    /// </summary>
    public static float NarrowToSingle(double value)
    {
        float narrowed = (float)value;

        if (float.IsInfinity(narrowed) && double.IsFinite(value))
            ThrowNarrowing(value, overflow: true);

        if (narrowed == 0 && value != 0)
            ThrowNarrowing(value, overflow: false);

        return narrowed;
    }

    /// <summary>
    /// Checks the result of a function such as <c>pow</c> against its finite arguments: an infinite
    /// result is an overflow, and a zero result from a nonzero <paramref name="nonzeroInput"/> is an
    /// underflow, when <paramref name="finiteInputs"/> is true.
    /// </summary>
    public static double CheckFunctionResult(string functionName, double result, bool finiteInputs, bool nonzeroInput)
    {
        if (finiteInputs)
        {
            if (double.IsInfinity(result))
                throw OutOfRange($"Function '{functionName}' float64 overflow: the result is outside the float64 range");

            if (result == 0 && nonzeroInput)
                throw OutOfRange($"Function '{functionName}' float64 underflow: the result is a nonzero value too small for float64");
        }

        return result;
    }

    /// <summary>The outcome of parsing the text form of a float.</summary>
    public enum ParseStatus
    {
        /// <summary>The text is a number in range, or a named value (<c>Infinity</c>, <c>NaN</c>).</summary>
        Ok,

        /// <summary>The text is not a number.</summary>
        Invalid,

        /// <summary>The text is a number too large for the type.</summary>
        Overflow,

        /// <summary>The text is a nonzero number too small for the type.</summary>
        Underflow,
    }

    /// <summary>
    /// Parses the PostgreSQL text form of a <c>float8</c>: optional white space, then a decimal
    /// number (<c>[+-]digits[.digits][e[+-]digits]</c>, a leading or trailing point allowed), or a
    /// named value, case-insensitive: <c>NaN</c>, <c>Infinity</c>, <c>+Infinity</c>,
    /// <c>-Infinity</c>, <c>inf</c>, <c>+inf</c> or <c>-inf</c>. A number past the range is
    /// <see cref="ParseStatus.Overflow"/> and a nonzero number that rounds to zero is
    /// <see cref="ParseStatus.Underflow"/>; neither becomes ±infinity or zero. The .NET parser alone
    /// accepts other spellings (<c>∞</c>) and silently maps a number past the range to infinity, so it
    /// runs only on text that passes the grammar check.
    /// </summary>
    public static ParseStatus TryParseFloat64(ReadOnlySpan<char> text, out double value)
    {
        text = text.Trim();

        if (TryParseNamed(text, out value))
            return ParseStatus.Ok;

        if (!IsDecimalNumberText(text, out bool nonzeroMantissa)
            || !double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value))
        {
            value = 0;
            return ParseStatus.Invalid;
        }

        if (double.IsInfinity(value))
            return ParseStatus.Overflow;

        if (value == 0 && nonzeroMantissa)
            return ParseStatus.Underflow;

        return ParseStatus.Ok;
    }

    /// <summary>
    /// The single-precision counterpart of <see cref="TryParseFloat64"/>, the PostgreSQL text form
    /// of a <c>float4</c>. The text is parsed to the nearest float directly, not through a double,
    /// so the value is rounded once.
    /// </summary>
    public static ParseStatus TryParseFloat32(ReadOnlySpan<char> text, out float value)
    {
        text = text.Trim();

        if (TryParseNamed(text, out double named))
        {
            value = (float)named;
            return ParseStatus.Ok;
        }

        if (!IsDecimalNumberText(text, out bool nonzeroMantissa)
            || !float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value))
        {
            value = 0;
            return ParseStatus.Invalid;
        }

        if (float.IsInfinity(value))
            return ParseStatus.Overflow;

        if (value == 0 && nonzeroMantissa)
            return ParseStatus.Underflow;

        return ParseStatus.Ok;
    }

    /// <summary>
    /// The text that the HTTP JSON encoders and the catalog write for a non-finite value, which JSON
    /// has no number for: <c>Infinity</c>, <c>-Infinity</c> or <c>NaN</c>. The same spelling
    /// <see cref="TryParseFloat64"/> and <c>System.Text.Json</c> named-literal handling read back.
    /// </summary>
    public static string NonFiniteText(double value)
    {
        if (double.IsNaN(value))
            return "NaN";

        return value > 0 ? "Infinity" : "-Infinity";
    }

    /// <summary>The error for a float value outside its type's range.</summary>
    public static CamusDBException OutOfRange(string message) =>
        new(CamusDBErrorCodes.FloatValueOutOfRange, message);

    private static bool TryParseNamed(ReadOnlySpan<char> text, out double value)
    {
        value = 0;

        if (text.Length is < 3 or > 9)
            return false;

        bool negative = false;
        ReadOnlySpan<char> name = text;

        if (name[0] is '+' or '-')
        {
            negative = name[0] == '-';
            name = name[1..];
        }
        else if (name.Equals("nan", StringComparison.OrdinalIgnoreCase))
        {
            value = double.NaN;
            return true;
        }

        if (name.Equals("infinity", StringComparison.OrdinalIgnoreCase) || name.Equals("inf", StringComparison.OrdinalIgnoreCase))
        {
            value = negative ? double.NegativeInfinity : double.PositiveInfinity;
            return true;
        }

        return false;
    }

    /// <summary>
    /// True when <paramref name="text"/> is <c>[+-]digits[.digits][(e|E)[+-]digits]</c> with at least
    /// one mantissa digit (<c>.5</c> and <c>5.</c> are numbers). <paramref name="nonzeroMantissa"/>
    /// reports a mantissa digit other than 0, which tells an underflow (<c>1e-400</c>) from a zero
    /// (<c>0e-400</c>).
    /// </summary>
    private static bool IsDecimalNumberText(ReadOnlySpan<char> text, out bool nonzeroMantissa)
    {
        nonzeroMantissa = false;
        int i = 0;

        if (i < text.Length && text[i] is '+' or '-')
            i++;

        int mantissaDigits = 0;
        bool point = false;

        for (; i < text.Length; i++)
        {
            char c = text[i];

            if (c is >= '0' and <= '9')
            {
                mantissaDigits++;
                if (c != '0')
                    nonzeroMantissa = true;
            }
            else if (c == '.' && !point)
            {
                point = true;
            }
            else
            {
                break;
            }
        }

        if (mantissaDigits == 0)
            return false;

        if (i == text.Length)
            return true;

        if (text[i] is not ('e' or 'E'))
            return false;

        i++;

        if (i < text.Length && text[i] is '+' or '-')
            i++;

        int exponentDigits = 0;

        for (; i < text.Length && text[i] is >= '0' and <= '9'; i++)
            exponentDigits++;

        return exponentDigits > 0 && i == text.Length;
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowBinary(double l, string symbol, double r, bool overflow)
    {
        string left = l.ToString("R", CultureInfo.InvariantCulture);
        string right = r.ToString("R", CultureInfo.InvariantCulture);

        throw OutOfRange(overflow
            ? $"Float64 overflow: {left} {symbol} {right} is outside the float64 range"
            : $"Float64 underflow: {left} {symbol} {right} is a nonzero value too small for float64");
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowNarrowing(double value, bool overflow)
    {
        string text = value.ToString("R", CultureInfo.InvariantCulture);

        throw OutOfRange(overflow
            ? $"Float32 overflow: {text} is outside the float32 range"
            : $"Float32 underflow: {text} is a nonzero value too small for float32");
    }
}
