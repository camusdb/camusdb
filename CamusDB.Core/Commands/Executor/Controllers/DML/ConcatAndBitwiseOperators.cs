/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// Evaluates <c>||</c> and the bitwise operators <c>&amp; | # ~ &lt;&lt; &gt;&gt;</c>, with the
/// PostgreSQL semantics, and gives the static result type of each one.
///
/// <para>Every operator gives NULL for a NULL operand. The NULL check runs before the type check, so
/// a NULL row value never turns into a type error. For <c>||</c> this is the difference from
/// <c>concat()</c>, which skips a NULL argument.</para>
///
/// <para><see cref="ResultType"/> and the evaluators share one type table, so the column type that a
/// derived table or a client sees is the type that the value has.</para>
/// </summary>
internal static class ConcatAndBitwiseOperators
{
    /// <summary>
    /// <c>a || b</c>. Two strings or two byte strings join. When one side is a string, the other
    /// scalar converts through <see cref="ScalarTextForm"/>, the same text form that the string cast
    /// and <c>concat()</c> use, and the result is a string. Thus <c>'v=' || X'01'</c> is
    /// <c>'v=\x01'</c>, as in PostgreSQL. Any other pair is an error, the same as PostgreSQL, which
    /// has no <c>||</c> for two numbers. An array operand is an error with its own message.
    /// </summary>
    public static ColumnValue Concat(ColumnValue left, ColumnValue right)
    {
        if (left.Type == ColumnType.Null || right.Type == ColumnType.Null)
            return ColumnValue.Null;

        if (left.Type == ColumnType.Array || right.Type == ColumnType.Array)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Operator || does not support array operands");

        if (left.Type == ColumnType.Bytes && right.Type == ColumnType.Bytes)
        {
            byte[] l = left.BytesValue ?? [];
            byte[] r = right.BytesValue ?? [];
            byte[] joined = new byte[l.Length + r.Length];
            l.CopyTo(joined, 0);
            r.CopyTo(joined, l.Length);
            return new ColumnValue(joined);
        }

        if ((left.Type == ColumnType.String || right.Type == ColumnType.String)
            && ScalarTextForm.TryFormat(left, out string leftText)
            && ScalarTextForm.TryFormat(right, out string rightText))
            return new ColumnValue(ColumnType.String, string.Concat(leftText, rightText));

        throw NoMatchingSignature(NodeType.ExprConcat, left.Type, right.Type);
    }

    /// <summary>
    /// <c>a &amp; b</c>, <c>a | b</c>, <c>a # b</c>, <c>a &lt;&lt; n</c> and <c>a &gt;&gt; n</c>.
    /// <list type="bullet">
    ///   <item><description>AND, OR and XOR take two Integer64 values, or two byte strings of equal
    ///   length. Byte strings of different length are an error, as PostgreSQL does for
    ///   <c>bit</c> strings.</description></item>
    ///   <item><description>A shift takes two Integer64 values. <c>&gt;&gt;</c> is arithmetic, so it
    ///   keeps the sign. Only the low six bits of the count are used: the count is taken modulo 64.
    ///   PostgreSQL does not check the count and compiles a plain C shift, which gives that result on
    ///   x86-64 and ARM64; the .NET shift on a long applies the same mask.</description></item>
    /// </list>
    /// A float, NUMERIC or Bool operand is an error, as in PostgreSQL. The operator is not defined
    /// for them, and a silent conversion would hide a mistake.
    /// </summary>
    public static ColumnValue Bitwise(NodeType op, ColumnValue left, ColumnValue right)
    {
        if (left.Type == ColumnType.Null || right.Type == ColumnType.Null)
            return ColumnValue.Null;

        if (left.Type == ColumnType.Integer64 && right.Type == ColumnType.Integer64)
        {
            long l = left.LongValue, r = right.LongValue;

            long result = op switch
            {
                NodeType.ExprBitAnd => l & r,
                NodeType.ExprBitOr => l | r,
                NodeType.ExprBitXor => l ^ r,
                NodeType.ExprShiftLeft => l << (int)(r & 63),
                _ => l >> (int)(r & 63),
            };

            return new ColumnValue(ColumnType.Integer64, result);
        }

        if (left.Type == ColumnType.Bytes && right.Type == ColumnType.Bytes
            && op is NodeType.ExprBitAnd or NodeType.ExprBitOr or NodeType.ExprBitXor)
        {
            byte[] l = left.BytesValue ?? [];
            byte[] r = right.BytesValue ?? [];

            if (l.Length != r.Length)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Operator {Symbol(op)} needs byte strings of equal length, got {l.Length} and {r.Length} bytes");

            byte[] result = new byte[l.Length];

            for (int i = 0; i < result.Length; i++)
            {
                result[i] = op switch
                {
                    NodeType.ExprBitAnd => (byte)(l[i] & r[i]),
                    NodeType.ExprBitOr => (byte)(l[i] | r[i]),
                    _ => (byte)(l[i] ^ r[i]),
                };
            }

            return new ColumnValue(result);
        }

        throw NoMatchingSignature(op, left.Type, right.Type);
    }

    /// <summary>
    /// Prefix <c>~a</c>: the complement of an Integer64 value, or of each byte of a byte string.
    /// </summary>
    public static ColumnValue BitNot(ColumnValue value)
    {
        switch (value.Type)
        {
            case ColumnType.Null:
                return ColumnValue.Null;

            case ColumnType.Integer64:
                return new ColumnValue(ColumnType.Integer64, ~value.LongValue);

            case ColumnType.Bytes:
            {
                byte[] source = value.BytesValue ?? [];
                byte[] result = new byte[source.Length];

                for (int i = 0; i < source.Length; i++)
                    result[i] = (byte)~source[i];

                return new ColumnValue(result);
            }

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"No matching signature for unary operator ~ for argument type: {value.Type}");
        }
    }

    /// <summary>
    /// The static result type of one of these operators for the given operand types, or null when the
    /// evaluator would refuse the pair or an operand type is not known. A NULL operand type is
    /// compatible with every form, because the value is then NULL. For <see cref="NodeType.ExprBitNot"/>
    /// pass the operand type as <paramref name="left"/>; <paramref name="right"/> is ignored.
    /// </summary>
    public static ColumnType? ResultType(NodeType op, ColumnType left, ColumnType right)
    {
        switch (op)
        {
            case NodeType.ExprConcat:
                if (IsOrNull(left, ColumnType.Bytes) && IsOrNull(right, ColumnType.Bytes)
                    && (left == ColumnType.Bytes || right == ColumnType.Bytes))
                    return ColumnType.Bytes;

                return left == ColumnType.String || right == ColumnType.String ? ColumnType.String : null;

            case NodeType.ExprBitAnd:
            case NodeType.ExprBitOr:
            case NodeType.ExprBitXor:
                if (IsOrNull(left, ColumnType.Integer64) && IsOrNull(right, ColumnType.Integer64))
                    return ColumnType.Integer64;

                return IsOrNull(left, ColumnType.Bytes) && IsOrNull(right, ColumnType.Bytes) ? ColumnType.Bytes : null;

            case NodeType.ExprShiftLeft:
            case NodeType.ExprShiftRight:
                return IsOrNull(left, ColumnType.Integer64) && IsOrNull(right, ColumnType.Integer64)
                    ? ColumnType.Integer64
                    : null;

            case NodeType.ExprBitNot:
                return left is ColumnType.Integer64 or ColumnType.Bytes ? left : null;

            default:
                return null;
        }
    }

    /// <summary>The SQL spelling of a binary operator this class evaluates, for error messages.</summary>
    public static string Symbol(NodeType op) => op switch
    {
        NodeType.ExprConcat => "||",
        NodeType.ExprBitAnd => "&",
        NodeType.ExprBitOr => "|",
        NodeType.ExprBitXor => "#",
        NodeType.ExprShiftLeft => "<<",
        _ => ">>",
    };

    private static bool IsOrNull(ColumnType type, ColumnType expected) =>
        type == expected || type == ColumnType.Null;

    private static CamusDBException NoMatchingSignature(NodeType op, ColumnType left, ColumnType right) =>
        new(CamusDBErrorCodes.InvalidInput,
            $"No matching signature for operator {Symbol(op)} for argument types: {left}, {right}");
}
