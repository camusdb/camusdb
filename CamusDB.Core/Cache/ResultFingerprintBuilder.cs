
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Globalization;
using System.IO.Hashing;
using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Cache;

/// <summary>
/// Builds a stable, collision-resistant cache-key fingerprint for a cacheable SELECT result.
///
/// <para>The fingerprint encodes all inputs that distinguish one result from another:
/// database identity, cache family name, query shape, typed parameter values, schema
/// versions, and cache options. Two queries that differ in any of these produce different
/// fingerprints; two that are identical in all of them map to the same cache slot.</para>
///
/// <para><b>Important invariants:</b></para>
/// <list type="bullet">
///   <item><description>
///     Every variable-length user string (parameter keys, string/id column values, table
///     names, cache names, database ids) is written as <c>{len}:{content}</c> where
///     <c>{len}</c> is the <c>string.Length</c> (UTF-16 code unit count). This prevents
///     delimiter injection: a string containing the separator characters <c>|</c>, <c>=</c>,
///     <c>:</c>, or <c>\n</c> cannot produce the same canonical bytes as two distinct
///     shorter strings, because the length prefix uniquely bounds where the content ends.
///   </description></item>
///   <item><description>
///     Parameter values are serialized with their <see cref="ColumnType"/> tag so integer
///     <c>1</c> and string <c>"1"</c> cannot collide (<c>i:1</c> vs <c>s:3:1</c>).
///   </description></item>
///   <item><description>
///     Parameter dictionaries are serialized in sorted key order for determinism.
///   </description></item>
///   <item><description>
///     Immutable table IDs (not mutable table names) and schema versions are both included so a
///     drop-and-recreate of a table with the same name produces a different fingerprint: the new
///     table receives a fresh ID regardless of whether its schema version happens to match the
///     dropped table.
///   </description></item>
///   <item><description>
///     The fingerprint is a lowercase 128-bit XxHash128 hex digest of an injective,
///     length-prefixed canonical UTF-8 byte sequence, making it suitable as a dictionary key
///     without further escaping. XxHash is non-cryptographic — appropriate here because the
///     key defends against accidental collisions in a local cache, not against an adversary.
///   </description></item>
/// </list>
///
/// <para>The plan cache uses shape IDs that intentionally ignore literal values; this builder
/// is a separate path and must always include literal/parameter values.</para>
/// </summary>
public static class ResultFingerprintBuilder
{
    // Canonical fingerprint inputs are typically well under 1 KiB; keep those off the heap
    // entirely and only rent a pooled buffer for the rare oversized statement.
    private const int StackEncodeThreshold = 1024;

    /// <summary>
    /// Builds and returns the fingerprint string for the given query context.
    ///
    /// <para><paramref name="queryShapeId"/> may be null when the planner has not yet set it
    /// (e.g. legacy non-SQL paths). The fingerprint still distinguishes by all other inputs;
    /// the shape slot is left empty rather than omitted so the canonical form is stable.</para>
    /// </summary>
    public static string Build(
        string databaseId,
        string cacheName,
        string? queryShapeId,
        Dictionary<string, ColumnValue>? parameters,
        IReadOnlyList<(string TableId, int SchemaVersion)>? schemaDeps,
        CacheHintOptions hint)
    {
        var sb = new StringBuilder(256);

        // Each user-supplied string is length-prefixed to prevent delimiter injection.
        sb.Append("db:");
        AppendLP(sb, databaseId);
        sb.Append('\n');

        sb.Append("name:");
        AppendLP(sb, cacheName);
        sb.Append('\n');

        sb.Append("shape:");
        AppendLP(sb, queryShapeId ?? "");
        sb.Append('\n');

        // Parameters — sorted by key for determinism; both key and value are injection-safe.
        if (parameters is { Count: > 0 })
        {
            List<string> keys = new(parameters.Count);
            foreach (string k in parameters.Keys)
                keys.Add(k);
            keys.Sort(StringComparer.Ordinal);

            sb.Append("params:");
            for (int i = 0; i < keys.Count; i++)
            {
                if (i > 0) sb.Append('|');
                AppendLP(sb, keys[i]);
                sb.Append('=');
                AppendTypedValue(sb, parameters[keys[i]]);
            }
            sb.Append('\n');
        }
        else
        {
            sb.Append("params:\n");
        }

        // Schema deps — sorted by table name for determinism.
        if (schemaDeps is { Count: > 0 })
        {
            List<(string, int)> sorted = new(schemaDeps);
            sorted.Sort((a, b) => string.CompareOrdinal(a.Item1, b.Item1));

            sb.Append("schema:");
            for (int i = 0; i < sorted.Count; i++)
            {
                if (i > 0) sb.Append('|');
                AppendLP(sb, sorted[i].Item1);
                sb.Append('=').Append(sorted[i].Item2);
            }
            sb.Append('\n');
        }
        else
        {
            sb.Append("schema:\n");
        }

        // Cache options (both are scalars — no injection risk).
        sb.Append("strict:").Append(hint.IsStrict ? '1' : '0').Append('\n');
        sb.Append("ttl:").Append(hint.TtlMs?.ToString() ?? "").Append('\n');

        string canonical = sb.ToString();
        int byteCount = Encoding.UTF8.GetByteCount(canonical);

        byte[]? rented = byteCount > StackEncodeThreshold ? ArrayPool<byte>.Shared.Rent(byteCount) : null;
        try
        {
            Span<byte> input = rented is not null ? rented.AsSpan(0, byteCount) : stackalloc byte[byteCount];
            Encoding.UTF8.GetBytes(canonical, input);

            // Non-cryptographic 128-bit hash: this fingerprint is a collision-avoidance key for a
            // local cache, not a security boundary, so we do not need SHA-256's adversarial
            // resistance — only a very low accidental-collision probability. XxHash128 gives a
            // 128-bit digest (birthday bound ~2^64), which is far below any realistic cache
            // population, while being roughly an order of magnitude faster than SHA-256. The
            // injective, length-prefixed canonical form above is what actually prevents *logical*
            // collisions; the hash only guards against digest collisions.
            Span<byte> hash = stackalloc byte[16];
            XxHash128.Hash(input, hash);

            return Convert.ToHexStringLower(hash);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Appends <paramref name="value"/> in length-prefixed form: <c>{len}:{value}</c> where
    /// <c>{len}</c> is <c>value.Length</c> (UTF-16 code units). The reader can recover the
    /// exact boundary of the value by reading <c>{len}</c> characters after the colon, so
    /// any character — including the structural delimiters <c>|</c>, <c>=</c>, <c>:</c>,
    /// <c>\n</c> — is safe to embed in <paramref name="value"/>.
    /// </summary>
    private static void AppendLP(StringBuilder sb, string value)
    {
        sb.Append(value.Length).Append(':').Append(value);
    }

    private static void AppendTypedValue(StringBuilder sb, ColumnValue v)
    {
        switch (v.Type)
        {
            case ColumnType.Null:
                sb.Append("null");
                break;
            case ColumnType.Integer64:
                sb.Append("i:").Append(v.LongValue);
                break;
            case ColumnType.Float64:
                sb.Append("f:").Append(v.FloatValue.ToString("G17", CultureInfo.InvariantCulture));
                break;
            case ColumnType.Float32:
                sb.Append("r:").Append(((float)v.FloatValue).ToString("G9", CultureInfo.InvariantCulture));
                break;
            case ColumnType.Bool:
                sb.Append("b:").Append(v.BoolValue ? '1' : '0');
                break;
            case ColumnType.String:
                sb.Append("s:");
                AppendLP(sb, v.StrValue ?? "");
                break;
            case ColumnType.Id:
                sb.Append("id:");
                AppendLP(sb, v.StrValue ?? "");
                break;
            case ColumnType.Date:
                sb.Append("d:").Append(v.LongValue);
                break;
            case ColumnType.DateTime:
                sb.Append("dt:").Append(v.LongValue);
                break;
            case ColumnType.Bytes:
                // Hex-encoding uses [0-9a-f] only — no structural characters, no injection risk.
                sb.Append("by:");
                if (v.BytesValue is { Length: > 0 })
                    sb.Append(Convert.ToHexStringLower(v.BytesValue));
                break;
            case ColumnType.Uuid:
                // Both halves must be hashed — the low half alone would collide two UUIDs that
                // share their low 64 bits (e.g. adjacent v7 values), fingerprinting them equal.
                sb.Append("uu:").Append(((ulong)v.UuidHigh).ToString("x16", CultureInfo.InvariantCulture))
                  .Append(((ulong)v.LongValue).ToString("x16", CultureInfo.InvariantCulture));
                break;
            case ColumnType.Array:
                // Each element is recursively injection-safe; the element count removes the
                // ambiguity that would otherwise arise from comma-separated variable-length elements.
                sb.Append("a").Append(v.ArrayValues?.Count ?? 0).Append('[');
                if (v.ArrayValues is { Count: > 0 })
                {
                    for (int i = 0; i < v.ArrayValues.Count; i++)
                    {
                        if (i > 0) sb.Append(',');
                        AppendTypedValue(sb, v.ArrayValues[i]);
                    }
                }
                sb.Append(']');
                break;
            default:
                // Unknown type: use type-tag + length-prefixed string representation so any
                // future type that happens to produce a string cannot collide with a known type.
                sb.Append("?:").Append((int)v.Type).Append(':');
                AppendLP(sb, v.StrValue ?? v.LongValue.ToString());
                break;
        }
    }
}
