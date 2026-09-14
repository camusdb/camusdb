/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Value-based equality comparer for <see cref="CompositeColumnValue"/> hash table keys.
///
/// Neither <see cref="CompositeColumnValue"/> nor <see cref="ColumnValue"/> overrides
/// <c>Equals</c>/<c>GetHashCode</c> (both only implement <see cref="IComparable"/>), so
/// the default dictionary comparer uses reference equality and would match nothing. This
/// comparer hashes and compares by <c>Type + payload</c>, consistent with
/// <see cref="ColumnValue.CompareTo"/> for non-NULL values. NULL keys are excluded before
/// they reach the table so the comparer does not need null-equality semantics.
///
/// Implements the span alternate (<see cref="IAlternateEqualityComparer{TAlternate,T}"/>) so
/// build and probe loops can look a just-extracted key up as a <see cref="ReadOnlySpan{T}"/>
/// over a reused scratch array — no per-row key array or <see cref="CompositeColumnValue"/>
/// wrapper is allocated. An owned key (a defensive copy of the span, via <see cref="Create"/>)
/// is materialized only when a new bucket is inserted, so a stored key never aliases a
/// caller's scratch array. Span and object forms share one hash and one equality
/// implementation, so a span probe and a stored key always agree.
///
/// Shared by every join strategy that keys rows — the hash build and probe, the broadcast probe
/// and its remote fragment half, and the Grace partitioner — so all of them agree on what makes
/// two join keys equal. It is internal rather than private so tests can differentially exercise
/// span-versus-object lookup parity for every key type.
/// </summary>
internal sealed class CompositeColumnValueComparer
    : IEqualityComparer<CompositeColumnValue>,
      IAlternateEqualityComparer<ReadOnlySpan<ColumnValue>, CompositeColumnValue>
{
    public static readonly CompositeColumnValueComparer Instance = new();

    public bool Equals(CompositeColumnValue? x, CompositeColumnValue? y)
    {
        if (x is null && y is null) return true;
        if (x is null || y is null) return false;
        return Equals(x.Values, y.Values);
    }

    /// <summary>
    /// Materializes an owned, immutable key from a probe span. Called by the dictionary only
    /// when a new bucket is inserted through the alternate lookup; the copy guarantees the
    /// stored key does not alias the caller's reusable scratch array.
    /// </summary>
    public CompositeColumnValue Create(ReadOnlySpan<ColumnValue> alternate) =>
        new(alternate.ToArray());

    /// <summary>
    /// Alternate-lookup equality between a probe span and a stored key. Delegates to the
    /// span-span overload so the semantics (type match plus <see cref="ColumnValue.CompareTo"/>
    /// per position) cannot drift from the object form.
    /// </summary>
    public bool Equals(ReadOnlySpan<ColumnValue> alternate, CompositeColumnValue other) =>
        Equals(alternate, other.Values);

    /// <summary>
    /// Span-based key equality — lets callers compare join keys without wrapping either operand in a
    /// throwaway <see cref="CompositeColumnValue"/>. Identical semantics to the object overload
    /// (type match plus <see cref="ColumnValue.CompareTo"/> == 0 per position).
    /// </summary>
    public bool Equals(ReadOnlySpan<ColumnValue> x, ReadOnlySpan<ColumnValue> y)
    {
        if (x.Length != y.Length) return false;

        for (int i = 0; i < x.Length; i++)
        {
            ColumnValue a = x[i];
            ColumnValue b = y[i];

            if (a.Type != b.Type) return false;
            if (a.CompareTo(b) != 0) return false;
        }

        return true;
    }

    public int GetHashCode(CompositeColumnValue obj) => GetHashCode(obj.Values);

    /// <summary>
    /// Span-based key hash — lets callers (partition routing, probe) hash a just-extracted key array
    /// without allocating a <see cref="CompositeColumnValue"/> wrapper. Byte-identical mixing to the
    /// object overload, so a span-hashed row routes/probes the same as a materialized key.
    /// </summary>
    public int GetHashCode(ReadOnlySpan<ColumnValue> values)
    {
        HashCode h = new();

        foreach (ColumnValue v in values)
        {
            h.Add((int)v.Type);

            switch (v.Type)
            {
                case ColumnType.String:
                case ColumnType.Id:
                    h.Add(v.StrValue, StringComparer.Ordinal);
                    break;

                case ColumnType.Integer64:
                case ColumnType.Date:
                case ColumnType.DateTime:
                    h.Add(v.LongValue);
                    break;

                case ColumnType.Float64:
                    h.Add(v.FloatValue);
                    break;

                case ColumnType.Float32:
                    h.Add((float)v.FloatValue);
                    break;

                case ColumnType.Bool:
                    h.Add(v.BoolValue);
                    break;

                case ColumnType.Bytes:
                    if (v.BytesValue is not null)
                        foreach (byte b in v.BytesValue)
                            h.Add(b);
                    break;

                case ColumnType.Uuid:
                    // Hash both halves; the low half alone would collide all UUIDs sharing it.
                    h.Add(v.UuidHigh);
                    h.Add(v.LongValue);
                    break;
            }
        }

        return h.ToHashCode();
    }
}
