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
/// <para>The browser build computes <c>md5</c> with <see cref="Util.Hashes.ManagedMd5"/>, because .NET
/// on WebAssembly has no MD5. The SHA functions use the platform implementation on every target.</para>
///
/// <para>Allocation: the digest goes into a stack buffer and the only heap allocation is the result
/// string. A short string encodes to UTF-8 on the stack; a long one rents a pooled buffer.</para>
///
/// <para><b>Binary forms.</b> <c>digest(data, algorithm)</c> and <c>hmac(data, key, algorithm)</c>
/// (from PostgreSQL's pgcrypto) return the digest as <c>bytes</c>, so
/// <c>digest(x, 'sha256') = decode(sha256(x), 'hex')</c>. <c>crc32(data)</c> (from MySQL) returns the
/// CRC-32 checksum as an unsigned <c>int64</c>. Each one takes <c>string</c> (as UTF-8) or <c>bytes</c>
/// data, the same as the hex functions. The algorithm names are <c>md5</c>, <c>sha1</c>, <c>sha256</c>,
/// <c>sha384</c> and <c>sha512</c>, in any case.</para>
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
        Sha384,
        Sha512,
    }

    public static void Register(ScalarFunctionRegistry registry)
    {
        RegisterHash(registry, "md5", EvaluateMd5);
        RegisterHash(registry, "sha1", EvaluateSha1);
        RegisterHash(registry, "sha256", EvaluateSha256);
        RegisterHash(registry, "sha512", EvaluateSha512);

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "digest",
            MinArity = 2,
            MaxArity = 2,
            Evaluator = EvaluateDigest,
            InferReturnType = _ => ColumnType.Bytes,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "hmac",
            MinArity = 3,
            MaxArity = 3,
            Evaluator = EvaluateHmac,
            InferReturnType = _ => ColumnType.Bytes,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "crc32",
            MinArity = 1,
            MaxArity = 1,
            Evaluator = EvaluateCrc32,
            InferReturnType = _ => ColumnType.Integer64,
        });
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

    /// <summary>
    /// <c>digest(data, algorithm)</c>: the digest of <c>data</c> as <c>bytes</c>.
    /// </summary>
    private static ColumnValue EvaluateDigest(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        byte[] data = DataBytes(calledName, 0, arguments[0]);
        HashKind kind = ParseAlgorithm(calledName, 1, arguments[1]);

        Span<byte> digest = stackalloc byte[SHA512.HashSizeInBytes];
        int length = ComputeHash(data, kind, digest);
        return new ColumnValue(digest[..length].ToArray());
    }

    /// <summary>
    /// <c>hmac(data, key, algorithm)</c>: the HMAC (RFC 2104) of <c>data</c> under <c>key</c>, as
    /// <c>bytes</c>. A key longer than the block size is hashed first, as the RFC requires; the
    /// platform implementations do that, and so does the managed MD5 form of the browser build.
    /// </summary>
    private static ColumnValue EvaluateHmac(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        byte[] data = DataBytes(calledName, 0, arguments[0]);
        byte[] key = DataBytes(calledName, 1, arguments[1]);
        HashKind kind = ParseAlgorithm(calledName, 2, arguments[2]);

        Span<byte> mac = stackalloc byte[SHA512.HashSizeInBytes];

        int length = kind switch
        {
#if CAMUSDB_BROWSER
            // .NET on WebAssembly has no MD5, so it has no HMAC-MD5 either; see ManagedHmacMd5.
            HashKind.Md5 => ManagedHmacMd5(key, data, mac),
#else
            HashKind.Md5 => HMACMD5.HashData(key, data, mac),
#endif
            HashKind.Sha1 => HMACSHA1.HashData(key, data, mac),
            HashKind.Sha256 => HMACSHA256.HashData(key, data, mac),
            HashKind.Sha384 => HMACSHA384.HashData(key, data, mac),
            HashKind.Sha512 => HMACSHA512.HashData(key, data, mac),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };

        return new ColumnValue(mac[..length].ToArray());
    }

    /// <summary>
    /// <c>crc32(data)</c>: the CRC-32 (IEEE 802.3) checksum, the same value as MySQL's <c>CRC32</c> and
    /// zlib's <c>crc32</c>, returned as a non-negative <c>int64</c>.
    /// </summary>
    private static ColumnValue EvaluateCrc32(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (ScalarFunctionArguments.PropagateNull(arguments) is ColumnValue nullResult)
            return nullResult;

        uint checksum = System.IO.Hashing.Crc32.HashToUInt32(DataBytes(calledName, 0, arguments[0]));

        return new ColumnValue(ColumnType.Integer64, checksum);
    }

    /// <summary>
    /// The bytes a binary digest function reads: a <c>bytes</c> value as it is, a <c>string</c> as its
    /// UTF-8 form. Any other type is an error, for the reason the class summary gives.
    /// </summary>
    private static byte[] DataBytes(string calledName, int argumentIndex, ColumnValue argument)
    {
        if (argument.Type == ColumnType.Bytes)
            return argument.BytesValue ?? [];

        ScalarFunctionArguments.RequireType(calledName, argumentIndex, argument, ColumnType.String, ColumnType.Bytes);
        return Encoding.UTF8.GetBytes(argument.StrValue ?? "");
    }

    private static HashKind ParseAlgorithm(string calledName, int argumentIndex, ColumnValue argument)
    {
        ScalarFunctionArguments.RequireString(calledName, argumentIndex, argument);

        string name = argument.StrValue ?? "";

        if (name.Equals("md5", StringComparison.OrdinalIgnoreCase))
            return HashKind.Md5;

        if (name.Equals("sha1", StringComparison.OrdinalIgnoreCase))
            return HashKind.Sha1;

        if (name.Equals("sha256", StringComparison.OrdinalIgnoreCase))
            return HashKind.Sha256;

        if (name.Equals("sha384", StringComparison.OrdinalIgnoreCase))
            return HashKind.Sha384;

        if (name.Equals("sha512", StringComparison.OrdinalIgnoreCase))
            return HashKind.Sha512;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' does not support the algorithm '{name}'. Use md5, sha1, sha256, sha384 or sha512");
    }

#if CAMUSDB_BROWSER
    /// <summary>
    /// The HMAC block size of MD5, in bytes.
    /// </summary>
    private const int Md5BlockSize = 64;

    /// <summary>
    /// HMAC-MD5 for the browser build, which has no platform MD5. It follows RFC 2104 over
    /// <see cref="Util.Hashes.ManagedMd5"/>: a key longer than the block is replaced by its digest, the
    /// key is padded with zeros to the block, and the result is
    /// <c>MD5((key ^ opad) || MD5((key ^ ipad) || data))</c>.
    /// </summary>
    private static int ManagedHmacMd5(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data, Span<byte> destination)
    {
        Span<byte> block = stackalloc byte[Md5BlockSize];

        if (key.Length > Md5BlockSize)
            Util.Hashes.ManagedMd5.HashData(key, block);
        else
            key.CopyTo(block);

        byte[] inner = new byte[Md5BlockSize + data.Length];
        for (int i = 0; i < Md5BlockSize; i++)
            inner[i] = (byte)(block[i] ^ 0x36);
        data.CopyTo(inner.AsSpan(Md5BlockSize));

        Span<byte> outer = stackalloc byte[Md5BlockSize + Util.Hashes.ManagedMd5.HashSizeInBytes];
        for (int i = 0; i < Md5BlockSize; i++)
            outer[i] = (byte)(block[i] ^ 0x5C);

        Util.Hashes.ManagedMd5.HashData(inner, outer[Md5BlockSize..]);
        return Util.Hashes.ManagedMd5.HashData(outer, destination);
    }
#endif

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
        int length = ComputeHash(input, kind, digest);
        return Convert.ToHexStringLower(digest[..length]);
    }

    /// <summary>
    /// Writes the digest of <paramref name="input"/> to <paramref name="digest"/>, which must hold
    /// <see cref="SHA512.HashSizeInBytes"/> bytes, and returns the digest length.
    /// </summary>
    private static int ComputeHash(ReadOnlySpan<byte> input, HashKind kind, Span<byte> digest)
    {
        return kind switch
        {
#if CAMUSDB_BROWSER
            // .NET on WebAssembly has no MD5; see ManagedMd5.
            HashKind.Md5 => Util.Hashes.ManagedMd5.HashData(input, digest),
#else
            HashKind.Md5 => MD5.HashData(input, digest),
#endif
            HashKind.Sha1 => SHA1.HashData(input, digest),
            HashKind.Sha256 => SHA256.HashData(input, digest),
            HashKind.Sha384 => SHA384.HashData(input, digest),
            HashKind.Sha512 => SHA512.HashData(input, digest),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };
    }
}
