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
/// Checked Integer64 arithmetic for the SQL operators, and the one error that every Integer64
/// overflow raises: <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/>.
///
/// <para><b>Why it exists.</b> C# <see langword="long"/> arithmetic wraps by default, so
/// <c>9223372036854775807 + 1</c> silently became <c>-9223372036854775808</c>, and an UPDATE stored
/// the wrapped value. <c>long.MinValue / -1</c> did not wrap: it threw a raw
/// <see cref="OverflowException"/>, which reached the client as an unclassified server error. Every
/// SQL path that combines two Integer64 values goes through this class or calls
/// <see cref="OutOfRange"/>, so an overflow is always the same classified error and never a stored
/// value. PostgreSQL raises <c>bigint out of range</c> and Spanner raises an overflow error for the
/// same inputs.</para>
///
/// <para><b>Cost.</b> The common path is one add, subtract or multiply plus one predictable branch,
/// and the checks inline into the caller. Only an overflow pays for the exception.</para>
/// </summary>
internal static class Int64Math
{
    /// <summary><c>l + r</c>, or <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/>.</summary>
    public static long Add(long l, long r)
    {
        long sum = unchecked(l + r);

        // Signed overflow happened when both operands have the sign the sum does not have.
        if (((l ^ sum) & (r ^ sum)) < 0)
            ThrowBinaryOverflow(l, "+", r);

        return sum;
    }

    /// <summary><c>l - r</c>, or <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/>.</summary>
    public static long Subtract(long l, long r)
    {
        long difference = unchecked(l - r);

        // Signed overflow happened when the operands differ in sign and the result lost l's sign.
        if (((l ^ r) & (l ^ difference)) < 0)
            ThrowBinaryOverflow(l, "-", r);

        return difference;
    }

    /// <summary><c>l * r</c>, or <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/>.</summary>
    public static long Multiply(long l, long r)
    {
        long high = Math.BigMul(l, r, out long low);

        // The 128-bit product fits in 64 bits only when the high half is the sign extension of the low half.
        if (high != low >> 63)
            ThrowBinaryOverflow(l, "*", r);

        return low;
    }

    /// <summary>
    /// <c>l / r</c>, truncated toward zero. The caller checks for a zero divisor first, because
    /// division by zero has its own error. The one quotient that does not fit is
    /// <c>long.MinValue / -1</c>: the true result is <c>long.MaxValue + 1</c>.
    /// </summary>
    public static long Divide(long l, long r)
    {
        if (r == -1 && l == long.MinValue)
            ThrowBinaryOverflow(l, "/", r);

        return l / r;
    }

    /// <summary><c>-value</c>, or <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/> for <c>long.MinValue</c>.</summary>
    public static long Negate(long value)
    {
        if (value == long.MinValue)
            throw OutOfRange($"Integer overflow in unary minus for value {Format(value)}");

        return -value;
    }

    /// <summary>
    /// The error for an Integer64 result outside the 64-bit range. <paramref name="message"/> names
    /// the operation and its input, so the client can see which expression overflowed.
    /// </summary>
    public static CamusDBException OutOfRange(string message) =>
        new(CamusDBErrorCodes.IntegerValueOutOfRange, message);

    /// <summary>
    /// True when <paramref name="text"/> is an optional sign followed by one or more decimal digits.
    /// A text of this shape that <see cref="long.TryParse(string?, out long)"/> refuses can only be a
    /// value past the int64 range, so the caller reports <see cref="OutOfRange"/> instead of a syntax
    /// error.
    /// </summary>
    public static bool IsDecimalIntegerText(ReadOnlySpan<char> text)
    {
        text = text.Trim();

        if (text.Length > 0 && (text[0] == '-' || text[0] == '+'))
            text = text[1..];

        if (text.Length == 0)
            return false;

        foreach (char c in text)
        {
            if (c is < '0' or > '9')
                return false;
        }

        return true;
    }

    /// <summary>
    /// Kept out of line, so the checks above stay small enough for the JIT to inline them into the
    /// evaluator: a <see langword="throw"/> in the method body would stop that.
    /// </summary>
    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowBinaryOverflow(long l, string symbol, long r) =>
        throw OutOfRange($"Integer overflow: {Format(l)} {symbol} {Format(r)} is outside the int64 range");

    private static string Format(long value) => value.ToString(CultureInfo.InvariantCulture);
}
