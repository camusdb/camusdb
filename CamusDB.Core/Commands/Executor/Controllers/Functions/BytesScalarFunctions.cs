/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Unicode;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The functions that convert between <c>bytes</c> and text, read and change single bytes and bits, and
/// make random bytes. The names and the rules follow PostgreSQL, plus the MySQL names <c>hex</c>,
/// <c>unhex</c>, <c>to_base64</c> and <c>from_base64</c>. The user-facing list is in
/// <c>docs/bytes-functions.md</c>.
///
/// <para><b>Text forms.</b> <c>encode</c> / <c>decode</c> know the formats <c>hex</c> (lowercase out,
/// either case in), <c>base64</c> (RFC 4648) and <c>escape</c>. Unlike PostgreSQL, <c>encode(x,
/// 'base64')</c> adds no line break every 76 characters: a line break in a value is a trap for every
/// client that compares or stores the text. <c>decode</c> still ignores white space in base64 and
/// between hex pairs, so PostgreSQL output decodes. Bad text is <c>InvalidInput</c>, never a partial
/// result.</para>
///
/// <para><b>UTF-8 only.</b> <c>convert_to</c> / <c>convert_from</c> accept the encoding name
/// <c>UTF8</c> (also spelled <c>UTF-8</c> or <c>utf8</c>) and nothing else, because a CamusDB string is
/// stored as UTF-8. <c>convert_from</c> rejects a byte sequence that is not valid UTF-8 instead of
/// replacing it with U+FFFD, so a round trip never loses data without an error.</para>
///
/// <para><b>Offsets.</b> <c>get_byte</c>, <c>set_byte</c>, <c>get_bit</c> and <c>set_bit</c> take
/// 0-based offsets, as in PostgreSQL. Bit <c>n</c> is bit <c>n % 8</c> of byte <c>n / 8</c>, counted
/// from the least significant bit. An offset out of range is an error. The <c>set_</c> functions
/// return a changed copy; a <c>bytes</c> value is never changed in place, because other cells,
/// cached rows and cached query results can share its array.</para>
///
/// <para><b>Volatility.</b> Only <c>gen_random_bytes</c> is volatile. It takes an argument, so it
/// cannot be a column <c>DEFAULT</c>: the per-row default path accepts zero-argument calls only.</para>
/// </summary>
internal static class BytesScalarFunctions
{
    /// <summary>
    /// The largest <c>gen_random_bytes</c> request, the same limit as PostgreSQL's pgcrypto.
    /// </summary>
    public const int MaxRandomBytes = 1024;

    /// <summary>
    /// The largest base64 input decoded into a stack buffer. Longer input rents a pooled buffer.
    /// </summary>
    private const int MaxStackDecodeBytes = 512;

    private enum TextFormat
    {
        Hex,
        Base64,
        Escape,
    }

    public static void Register(ScalarFunctionRegistry registry)
    {
        Register(registry, "encode", 2, EvaluateEncode, ColumnType.String);
        Register(registry, "decode", 2, EvaluateDecode, ColumnType.Bytes);
        Register(registry, "hex", 1, EvaluateHex, ColumnType.String);
        Register(registry, "unhex", 1, EvaluateUnhex, ColumnType.Bytes);
        Register(registry, "to_base64", 1, EvaluateToBase64, ColumnType.String);
        Register(registry, "from_base64", 1, EvaluateFromBase64, ColumnType.Bytes);
        Register(registry, "convert_to", 2, EvaluateConvertTo, ColumnType.Bytes);
        Register(registry, "convert_from", 2, EvaluateConvertFrom, ColumnType.String);

        Register(registry, "get_byte", 2, EvaluateGetByte, ColumnType.Integer64);
        Register(registry, "set_byte", 3, EvaluateSetByte, ColumnType.Bytes);
        Register(registry, "get_bit", 2, EvaluateGetBit, ColumnType.Integer64);
        Register(registry, "set_bit", 3, EvaluateSetBit, ColumnType.Bytes);
        Register(registry, "bit_length", 1, EvaluateBitLength, ColumnType.Integer64);
        Register(registry, "bit_count", 1, EvaluateBitCount, ColumnType.Integer64);

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "gen_random_bytes",
            MinArity = 1,
            MaxArity = 1,
            Evaluator = EvaluateGenRandomBytes,
            InferReturnType = _ => ColumnType.Bytes,
            IsVolatile = true,
        });
    }

    private static void Register(
        ScalarFunctionRegistry registry,
        string name,
        int arity,
        ScalarFunctionEvaluatorDelegate evaluator,
        ColumnType returnType)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = arity,
            MaxArity = arity,
            Evaluator = evaluator,
            InferReturnType = _ => returnType,
        });
    }

    // ── Text forms ──────────────────────────────────────────────────────────

    private static ColumnValue EvaluateEncode(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireType(calledName, 0, arguments[0], ColumnType.Bytes);
        ReadOnlySpan<byte> value = arguments[0].BytesValue;

        string text = ParseFormat(calledName, arguments[1]) switch
        {
            TextFormat.Hex => Convert.ToHexStringLower(value),
            TextFormat.Base64 => Convert.ToBase64String(value),
            _ => EncodeEscape(value),
        };

        return new ColumnValue(ColumnType.String, text);
    }

    private static ColumnValue EvaluateDecode(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireString(calledName, 0, arguments[0]);
        string text = arguments[0].StrValue!;

        byte[] value = ParseFormat(calledName, arguments[1]) switch
        {
            TextFormat.Hex => DecodeHex(calledName, text, skipWhiteSpace: true),
            TextFormat.Base64 => DecodeBase64(calledName, text),
            _ => DecodeEscape(calledName, text),
        };

        return new ColumnValue(value);
    }

    /// <summary>
    /// MySQL <c>hex(x)</c>: uppercase hex of a <c>bytes</c> value, of the UTF-8 form of a
    /// <c>string</c>, or of an <c>int64</c> number. A number is printed without leading zeros, and a
    /// negative number as its 64-bit two's complement, as in MySQL (<c>hex(-1)</c> is sixteen F).
    /// </summary>
    private static ColumnValue EvaluateHex(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        ColumnValue argument = arguments[0];

        if (argument.Type == ColumnType.Null)
            return ColumnValue.Null;

        string text = argument.Type switch
        {
            ColumnType.Bytes => Convert.ToHexString(argument.BytesValue ?? []),
            ColumnType.String => Convert.ToHexString(Encoding.UTF8.GetBytes(argument.StrValue ?? "")),
            ColumnType.Integer64 => ((ulong)argument.LongValue).ToString("X", System.Globalization.CultureInfo.InvariantCulture),
            _ => throw TypeError(calledName, 0, argument, "Bytes, String or Integer64"),
        };

        return new ColumnValue(ColumnType.String, text);
    }

    /// <summary>
    /// MySQL <c>unhex(text)</c>. Unlike MySQL, which returns NULL, text that is not an even number of
    /// hex digits is an error, the same as <c>decode(text, 'hex')</c>. White space is not skipped.
    /// </summary>
    private static ColumnValue EvaluateUnhex(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (arguments[0].Type == ColumnType.Null)
            return ColumnValue.Null;

        ScalarFunctionArguments.RequireString(calledName, 0, arguments[0]);
        return new ColumnValue(DecodeHex(calledName, arguments[0].StrValue!, skipWhiteSpace: false));
    }

    private static ColumnValue EvaluateToBase64(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        ColumnValue argument = arguments[0];

        return argument.Type switch
        {
            ColumnType.Null => ColumnValue.Null,
            ColumnType.Bytes => new ColumnValue(ColumnType.String, Convert.ToBase64String(argument.BytesValue ?? [])),
            ColumnType.String => new ColumnValue(ColumnType.String, Convert.ToBase64String(Encoding.UTF8.GetBytes(argument.StrValue ?? ""))),
            _ => throw TypeError(calledName, 0, argument, "Bytes or String"),
        };
    }

    private static ColumnValue EvaluateFromBase64(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (arguments[0].Type == ColumnType.Null)
            return ColumnValue.Null;

        ScalarFunctionArguments.RequireString(calledName, 0, arguments[0]);
        return new ColumnValue(DecodeBase64(calledName, arguments[0].StrValue!));
    }

    private static ColumnValue EvaluateConvertTo(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireString(calledName, 0, arguments[0]);
        RequireUtf8(calledName, arguments[1]);

        return new ColumnValue(Encoding.UTF8.GetBytes(arguments[0].StrValue!));
    }

    private static ColumnValue EvaluateConvertFrom(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ScalarFunctionArguments.RequireType(calledName, 0, arguments[0], ColumnType.Bytes);
        RequireUtf8(calledName, arguments[1]);

        ReadOnlySpan<byte> value = arguments[0].BytesValue;

        if (!Utf8.IsValid(value))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' received bytes that are not valid UTF-8");

        return new ColumnValue(ColumnType.String, Encoding.UTF8.GetString(value));
    }

    private static TextFormat ParseFormat(string calledName, ColumnValue argument)
    {
        ScalarFunctionArguments.RequireString(calledName, 1, argument);
        string name = argument.StrValue!;

        if (name.Equals("hex", StringComparison.OrdinalIgnoreCase))
            return TextFormat.Hex;

        if (name.Equals("base64", StringComparison.OrdinalIgnoreCase))
            return TextFormat.Base64;

        if (name.Equals("escape", StringComparison.OrdinalIgnoreCase))
            return TextFormat.Escape;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' does not support the format '{name}'. Use hex, base64 or escape");
    }

    private static void RequireUtf8(string calledName, ColumnValue argument)
    {
        ScalarFunctionArguments.RequireString(calledName, 1, argument);
        string name = argument.StrValue!;

        if (name.Equals("UTF8", StringComparison.OrdinalIgnoreCase)
            || name.Equals("UTF-8", StringComparison.OrdinalIgnoreCase))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' does not support the encoding '{name}'. Only UTF8 is supported");
    }

    /// <summary>
    /// Reads hex digit pairs in either case. With <paramref name="skipWhiteSpace"/>, white space between
    /// two pairs is ignored, as PostgreSQL's <c>decode(x, 'hex')</c> does; white space inside a pair is
    /// still an error.
    /// </summary>
    private static byte[] DecodeHex(string calledName, string text, bool skipWhiteSpace)
    {
        byte[] buffer = new byte[text.Length / 2];
        int count = 0;
        int i = 0;

        while (i < text.Length)
        {
            char c = text[i];

            if (skipWhiteSpace && IsSkippedWhiteSpace(c))
            {
                i++;
                continue;
            }

            if (i + 1 >= text.Length)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Function '{calledName}' expects an even number of hex digits");

            int high = HexDigitValue(c);
            int low = HexDigitValue(text[i + 1]);

            if (high < 0 || low < 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Function '{calledName}' found the invalid hex digit '{(high < 0 ? c : text[i + 1])}'");

            buffer[count++] = (byte)((high << 4) | low);
            i += 2;
        }

        return count == buffer.Length ? buffer : buffer.AsSpan(0, count).ToArray();
    }

    private static bool IsSkippedWhiteSpace(char c) => c is ' ' or '\t' or '\n' or '\r';

    private static int HexDigitValue(char c) => c switch
    {
        >= '0' and <= '9' => c - '0',
        >= 'a' and <= 'f' => c - 'a' + 10,
        >= 'A' and <= 'F' => c - 'A' + 10,
        _ => -1,
    };

    /// <summary>
    /// Reads RFC 4648 base64 with its <c>=</c> padding. White space anywhere is ignored, so text that
    /// PostgreSQL wrote with line breaks decodes.
    /// </summary>
    private static byte[] DecodeBase64(string calledName, string text)
    {
        // Three bytes for each four characters is the most a valid text can hold.
        int maxLength = (text.Length / 4 + 1) * 3;

        byte[]? rented = null;
        Span<byte> buffer = maxLength <= MaxStackDecodeBytes
            ? stackalloc byte[MaxStackDecodeBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(maxLength));

        try
        {
            if (!Convert.TryFromBase64String(text, buffer, out int written))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Function '{calledName}' received text that is not valid base64");

            return buffer[..written].ToArray();
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// PostgreSQL's <c>escape</c> format: a zero byte and each byte with the high bit set become a
    /// three-digit octal escape (<c>\000</c>, <c>\377</c>), a backslash becomes two backslashes, and
    /// every other byte is its ASCII character.
    /// </summary>
    private static string EncodeEscape(ReadOnlySpan<byte> value)
    {
        StringBuilder builder = new(value.Length);

        foreach (byte b in value)
        {
            if (b == (byte)'\\')
            {
                builder.Append("\\\\");
            }
            else if (b == 0 || b >= 0x80)
            {
                builder.Append('\\')
                    .Append((char)('0' + (b >> 6)))
                    .Append((char)('0' + ((b >> 3) & 7)))
                    .Append((char)('0' + (b & 7)));
            }
            else
            {
                builder.Append((char)b);
            }
        }

        return builder.ToString();
    }

    /// <summary>
    /// The reverse of <see cref="EncodeEscape"/>, with PostgreSQL's rules: <c>\\</c> is one backslash,
    /// <c>\nnn</c> (three octal digits, at most <c>\377</c>) is one byte, and any other backslash is an
    /// error. Every other character is written as its UTF-8 bytes, so <c>decode('é', 'escape')</c> gives
    /// the two bytes of <c>é</c>, as PostgreSQL does in a UTF-8 database.
    /// </summary>
    private static byte[] DecodeEscape(string calledName, string text)
    {
        byte[] buffer = new byte[Encoding.UTF8.GetMaxByteCount(text.Length)];
        int count = 0;
        int i = 0;

        while (i < text.Length)
        {
            int backslash = text.IndexOf('\\', i);
            int runEnd = backslash < 0 ? text.Length : backslash;

            if (runEnd > i)
            {
                count += Encoding.UTF8.GetBytes(text.AsSpan(i, runEnd - i), buffer.AsSpan(count));
                i = runEnd;
                continue;
            }

            if (i + 1 < text.Length && text[i + 1] == '\\')
            {
                buffer[count++] = (byte)'\\';
                i += 2;
                continue;
            }

            if (i + 3 < text.Length
                && text[i + 1] is >= '0' and <= '3'
                && text[i + 2] is >= '0' and <= '7'
                && text[i + 3] is >= '0' and <= '7')
            {
                buffer[count++] = (byte)(((text[i + 1] - '0') << 6) | ((text[i + 2] - '0') << 3) | (text[i + 3] - '0'));
                i += 4;
                continue;
            }

            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' found an invalid escape at offset {i}. Use \\\\ for a backslash or \\nnn (octal) for a byte");
        }

        return buffer.AsSpan(0, count).ToArray();
    }

    // ── Byte and bit access ─────────────────────────────────────────────────

    private static ColumnValue EvaluateGetByte(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ReadOnlySpan<byte> value = RequireBytes(calledName, arguments[0]);
        int offset = (int)RequireOffset(calledName, arguments[1], value.Length, "byte");

        return new ColumnValue(ColumnType.Integer64, value[offset]);
    }

    private static ColumnValue EvaluateSetByte(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ReadOnlySpan<byte> value = RequireBytes(calledName, arguments[0]);
        int offset = (int)RequireOffset(calledName, arguments[1], value.Length, "byte");

        ScalarFunctionArguments.RequireType(calledName, 2, arguments[2], ColumnType.Integer64);
        long newValue = arguments[2].LongValue;

        if (newValue is < 0 or > 255)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' expects a byte value from 0 to 255 but received {newValue}");

        byte[] result = value.ToArray();
        result[offset] = (byte)newValue;
        return new ColumnValue(result);
    }

    private static ColumnValue EvaluateGetBit(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ReadOnlySpan<byte> value = RequireBytes(calledName, arguments[0]);
        long bit = RequireOffset(calledName, arguments[1], value.Length * 8L, "bit");

        return new ColumnValue(ColumnType.Integer64, (value[(int)(bit >> 3)] >> (int)(bit & 7)) & 1);
    }

    private static ColumnValue EvaluateSetBit(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        ReadOnlySpan<byte> value = RequireBytes(calledName, arguments[0]);
        long bit = RequireOffset(calledName, arguments[1], value.Length * 8L, "bit");

        ScalarFunctionArguments.RequireType(calledName, 2, arguments[2], ColumnType.Integer64);
        long newBit = arguments[2].LongValue;

        if (newBit is not (0 or 1))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' expects a bit value of 0 or 1 but received {newBit}");

        byte[] result = value.ToArray();
        int index = (int)(bit >> 3);
        byte mask = (byte)(1 << (int)(bit & 7));
        result[index] = newBit == 1 ? (byte)(result[index] | mask) : (byte)(result[index] & ~mask);
        return new ColumnValue(result);
    }

    /// <summary>
    /// <c>bit_length(x)</c>: eight times the byte count of a <c>bytes</c> value or of the UTF-8 form of a
    /// <c>string</c>, as in PostgreSQL. The string form counts bytes without making them.
    /// </summary>
    private static ColumnValue EvaluateBitLength(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        ColumnValue argument = arguments[0];

        return argument.Type switch
        {
            ColumnType.Null => ColumnValue.Null,
            ColumnType.Bytes => new ColumnValue(ColumnType.Integer64, (argument.BytesValue?.Length ?? 0) * 8L),
            ColumnType.String => new ColumnValue(ColumnType.Integer64, Encoding.UTF8.GetByteCount(argument.StrValue ?? "") * 8L),
            _ => throw TypeError(calledName, 0, argument, "Bytes or String"),
        };
    }

    /// <summary>
    /// <c>bit_count(x)</c>: the number of bits set in a <c>bytes</c> value. It reads eight bytes at a
    /// time and does not copy the value.
    /// </summary>
    private static ColumnValue EvaluateBitCount(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (arguments[0].Type == ColumnType.Null)
            return ColumnValue.Null;

        ReadOnlySpan<byte> value = RequireBytes(calledName, arguments[0]);
        long count = 0;
        int i = 0;

        for (; i + 8 <= value.Length; i += 8)
            count += BitOperations.PopCount(BinaryPrimitives.ReadUInt64LittleEndian(value[i..]));

        for (; i < value.Length; i++)
            count += BitOperations.PopCount(value[i]);

        return new ColumnValue(ColumnType.Integer64, count);
    }

    // ── Random ──────────────────────────────────────────────────────────────

    /// <summary>
    /// <c>gen_random_bytes(n)</c>: <c>n</c> bytes from the cryptographic random number generator, for
    /// 1 to <see cref="MaxRandomBytes"/>, as in PostgreSQL's pgcrypto. It is volatile, so each call
    /// gives new bytes and a query that names it bypasses the result cache.
    /// </summary>
    private static ColumnValue EvaluateGenRandomBytes(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (arguments[0].Type == ColumnType.Null)
            return ColumnValue.Null;

        ScalarFunctionArguments.RequireType(calledName, 0, arguments[0], ColumnType.Integer64);
        long count = arguments[0].LongValue;

        if (count is < 1 or > MaxRandomBytes)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Function '{calledName}' expects a length from 1 to {MaxRandomBytes} but received {count}");

        return new ColumnValue(RandomNumberGenerator.GetBytes((int)count));
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static ReadOnlySpan<byte> RequireBytes(string calledName, ColumnValue argument)
    {
        ScalarFunctionArguments.RequireType(calledName, 0, argument, ColumnType.Bytes);
        return argument.BytesValue;
    }

    /// <summary>
    /// Checks a 0-based offset against <paramref name="limit"/> (the byte count, or the bit count) and
    /// returns it. The bit count is a <c>long</c> because eight times the length of a large value does
    /// not fit in an <c>int</c>.
    /// </summary>
    private static long RequireOffset(string calledName, ColumnValue argument, long limit, string unit)
    {
        ScalarFunctionArguments.RequireType(calledName, 1, argument, ColumnType.Integer64);
        long offset = argument.LongValue;

        if (offset < 0 || offset >= limit)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                limit == 0
                    ? $"Function '{calledName}' cannot read {unit} {offset} of an empty value"
                    : $"Function '{calledName}' expects a {unit} offset from 0 to {limit - 1} but received {offset}");

        return offset;
    }

    private static CamusDBException TypeError(string calledName, int argumentIndex, ColumnValue argument, string expected)
        => new(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' expects argument {argumentIndex + 1} of type {expected} but received {argument.Type}");
}
