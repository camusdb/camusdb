/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Security.Cryptography;
using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The digest functions <c>md5</c>, <c>sha1</c>, <c>sha256</c> and <c>sha512</c>. Each one takes one
/// <c>string</c> or <c>bytes</c> argument and returns the digest as lowercase hex text, the same as
/// PostgreSQL's <c>md5(text)</c> and <c>encode(sha256(x), 'hex')</c>.
///
/// <para>A <c>string</c> is hashed as its UTF-8 bytes, so the digest of a given text matches PostgreSQL
/// and every other UTF-8 tool. A <c>bytes</c> value is hashed as its raw bytes. A NULL argument gives
/// NULL, and any other type is an error: there is no implicit text form of a number here, because the
/// digest would then depend on a float-to-text format that other systems do not share.</para>
///
/// <para>The functions are deterministic, so they are not volatile: constant folding, the query-result
/// cache, a <c>CHECK</c> constraint and a column <c>DEFAULT</c> can all use them.</para>
///
/// <para><b>Not for security.</b> MD5 and SHA-1 have practical collision attacks, and none of these
/// functions is a password hash (there is no salt and no work factor). Use them for checksums, change
/// detection, bucketing and test data.</para>
///
/// <para>Allocation: the digest goes into a stack buffer and the only heap allocation is the result
/// string. A short string encodes to UTF-8 on the stack; a long one rents a pooled buffer.</para>
/// </summary>
internal static class HashScalarFunctions
{
    /// <summary>
    /// The largest string, in UTF-16 characters, whose UTF-8 form is encoded on the stack. Three UTF-8
    /// bytes per character is the worst case for one UTF-16 unit, so the buffer is at most 768 bytes.
    /// </summary>
    private const int MaxStackChars = 256;

    private enum HashKind
    {
        Md5,
        Sha1,
        Sha256,
        Sha512,
    }

    public static void Register(ScalarFunctionRegistry registry)
    {
        RegisterHash(registry, "md5", EvaluateMd5);
        RegisterHash(registry, "sha1", EvaluateSha1);
        RegisterHash(registry, "sha256", EvaluateSha256);
        RegisterHash(registry, "sha512", EvaluateSha512);
    }

    private static void RegisterHash(
        ScalarFunctionRegistry registry,
        string name,
        ScalarFunctionEvaluatorDelegate evaluator)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = name,
            MinArity = 1,
            MaxArity = 1,
            Evaluator = evaluator,
            InferReturnType = _ => ColumnType.String,
            IsVolatile = false,
        });
    }

    private static ColumnValue EvaluateMd5(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateHash(calledName, arguments, HashKind.Md5);

    private static ColumnValue EvaluateSha1(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateHash(calledName, arguments, HashKind.Sha1);

    private static ColumnValue EvaluateSha256(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateHash(calledName, arguments, HashKind.Sha256);

    private static ColumnValue EvaluateSha512(string calledName, IReadOnlyList<ColumnValue> arguments)
        => EvaluateHash(calledName, arguments, HashKind.Sha512);

    private static ColumnValue EvaluateHash(string calledName, IReadOnlyList<ColumnValue> arguments, HashKind kind)
    {
        ColumnValue argument = arguments[0];

        switch (argument.Type)
        {
            case ColumnType.Null:
                return ColumnValue.Null;

            case ColumnType.Bytes:
                return new ColumnValue(ColumnType.String, HashToHex(argument.BytesValue ?? [], kind));

            case ColumnType.String:
                return new ColumnValue(ColumnType.String, HashStringToHex(argument.StrValue ?? "", kind));

            default:
                ScalarFunctionArguments.RequireType(calledName, 0, argument, ColumnType.String, ColumnType.Bytes);
                return ColumnValue.Null; // unreachable: RequireType throws for every other type
        }
    }

    private static string HashStringToHex(string value, HashKind kind)
    {
        if (value.Length <= MaxStackChars)
        {
            Span<byte> utf8 = stackalloc byte[MaxStackChars * 3];
            int written = Encoding.UTF8.GetBytes(value, utf8);
            return HashToHex(utf8[..written], kind);
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(Encoding.UTF8.GetByteCount(value));
        try
        {
            int written = Encoding.UTF8.GetBytes(value, rented);
            return HashToHex(rented.AsSpan(0, written), kind);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static string HashToHex(ReadOnlySpan<byte> input, HashKind kind)
    {
        Span<byte> digest = stackalloc byte[SHA512.HashSizeInBytes];

        int length = kind switch
        {
            HashKind.Md5 => MD5.HashData(input, digest),
            HashKind.Sha1 => SHA1.HashData(input, digest),
            HashKind.Sha256 => SHA256.HashData(input, digest),
            HashKind.Sha512 => SHA512.HashData(input, digest),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };

        return Convert.ToHexStringLower(digest[..length]);
    }
}
