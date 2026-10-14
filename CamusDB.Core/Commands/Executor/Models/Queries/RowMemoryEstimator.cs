/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.InteropServices;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Estimates the heap bytes a retaining operator keeps alive when it buffers a row, so the operator
/// can spill on <c>CamusDBOptions.SpillThresholdBytes</c> as well as on its row cap.
///
/// <para><b>Cheap, and from the representation.</b> The estimate reads what the row already holds:
/// its cell count, the length of each string, bytes and array payload, and, for a borrowed row, the
/// length of the KV buffer it keeps alive. It never decodes a cell and never materializes a
/// <see cref="ColumnValue"/>, so charging a row costs one pass over its cells and allocates nothing.
/// The constants are object sizes on a 64-bit runtime, rounded up.</para>
///
/// <para><b>It errs high.</b> A borrowed row is charged the whole buffer it references, even when the
/// query reads one small cell of it, because the buffer stays alive while the row does. A pair-backed
/// join row is charged its own state plus both input rows, although many pair rows can share one
/// input row. A dictionary row is charged its key strings, although keys are often shared. An
/// estimate that is too high spills earlier than needed, which costs time but never changes a result;
/// an estimate that is too low lets the heap pass the cap, which is what the cap exists to stop.</para>
///
/// <para>Stateless and thread-safe.</para>
/// </summary>
internal static class RowMemoryEstimator
{
    /// <summary>Object header plus method-table pointer of any heap object.</summary>
    internal const int ObjectHeaderBytes = 16;

    /// <summary>Header plus length field of an array or string, before its elements.</summary>
    internal const int ArrayHeaderBytes = 24;

    /// <summary>One reference in an array or a field.</summary>
    internal const int ReferenceBytes = 8;

    /// <summary>One <see cref="ColumnValue"/> object: header plus its nine fields, rounded up.</summary>
    internal const int ColumnValueBytes = 80;

    /// <summary>One <see cref="ValueSlot"/> stored inline in a slot array.</summary>
    internal const int ValueSlotBytes = 32;

    /// <summary>One <see cref="QueryResultRow"/> stored inline in a buffer list (row id plus row reference).</summary>
    internal const int ResultRowSlotBytes = 32;

    /// <summary>A <see cref="QueryRow"/> object with its backing fields and the inline borrowed view.</summary>
    internal const int QueryRowBytes = 160;

    /// <summary>A <see cref="Dictionary{TKey,TValue}"/> object with its bucket and entry arrays, before entries.</summary>
    private const int DictionaryBytes = 80 + 2 * ArrayHeaderBytes;

    /// <summary>One dictionary entry (hash, next, key, value) plus its bucket slot.</summary>
    private const int DictionaryEntryBytes = 32;

    /// <summary>
    /// Bytes one buffered <see cref="QueryResultRow"/> keeps alive: its slot in the buffer plus
    /// everything its row references (<see cref="EstimateRow"/>).
    /// </summary>
    public static long Estimate(in QueryResultRow row) => ResultRowSlotBytes + EstimateRow(row.Row);

    /// <summary>
    /// Bytes a row object keeps alive. A <see cref="QueryRow"/> is charged by its backing
    /// (<see cref="QueryRow.EstimateRetainedBytes"/>); any other dictionary is charged per entry,
    /// key string included.
    /// </summary>
    public static long EstimateRow(IReadOnlyDictionary<string, ColumnValue> row)
    {
        if (row is QueryRow queryRow)
            return queryRow.EstimateRetainedBytes();

        long total = DictionaryBytes;

        foreach (KeyValuePair<string, ColumnValue> cell in row)
            total += DictionaryEntryBytes + StringBytes(cell.Key) + EstimateValue(cell.Value);

        return total;
    }

    /// <summary>Bytes one <see cref="ColumnValue"/> keeps alive: the object plus its payload.</summary>
    public static long EstimateValue(ColumnValue? value)
    {
        if (value is null)
            return 0;

        return ColumnValueBytes + PayloadBytes(value);
    }

    /// <summary>
    /// Bytes a group or hash key keeps alive: the key object, its value array, and each value.
    /// </summary>
    public static long EstimateKey(ReadOnlySpan<ColumnValue> key)
    {
        long total = ObjectHeaderBytes + ReferenceBytes + ArrayHeaderBytes + (long)ReferenceBytes * key.Length;

        for (int i = 0; i < key.Length; i++)
            total += EstimateValue(key[i]);

        return total;
    }

    /// <summary>
    /// The heap bytes a <see cref="ColumnValue"/> references beyond its own object: a string, a byte
    /// array, or an array of element values. Fixed-width values reference nothing.
    /// </summary>
    internal static long PayloadBytes(ColumnValue value)
    {
        switch (value.Type)
        {
            case ColumnType.String:
            case ColumnType.Id:
                return StringBytes(value.StrValue);

            case ColumnType.Bytes:
                return value.BytesValue is { } bytes ? ArrayHeaderBytes + bytes.Length : 0;

            case ColumnType.Array:
                if (value.ArrayValues is not { } elements)
                    return 0;

                long total = ObjectHeaderBytes + ArrayHeaderBytes + (long)ReferenceBytes * elements.Count;

                if (!HasVariablePayload(value.ArrayElementType))
                    return total + (long)ColumnValueBytes * elements.Count;

                for (int i = 0; i < elements.Count; i++)
                    total += EstimateValue(elements[i]);

                return total;

            default:
                return 0;
        }
    }

    /// <summary>
    /// The heap bytes a <see cref="ValueSlot"/> references beyond its inline 32 bytes. Same rules as
    /// <see cref="PayloadBytes(ColumnValue)"/>: only strings, byte arrays and element arrays count.
    /// </summary>
    internal static long SlotPayloadBytes(in ValueSlot slot)
    {
        switch (slot.Type)
        {
            case ColumnType.String:
            case ColumnType.Id:
                return StringBytes(slot.AsString);

            case ColumnType.Bytes:
                return slot.AsBytes is { } bytes ? ArrayHeaderBytes + bytes.Length : 0;

            case ColumnType.Array:
                ValueSlot[] elements = slot.ArrayElements;
                long total = ArrayHeaderBytes + (long)ValueSlotBytes * elements.Length;

                if (!HasVariablePayload(slot.ArrayElementType))
                    return total;

                for (int i = 0; i < elements.Length; i++)
                    total += SlotPayloadBytes(elements[i]);

                return total;

            default:
                return 0;
        }
    }

    /// <summary>
    /// Bytes of the array that backs <paramref name="storage"/>. A borrowed row keeps the whole array
    /// alive, not only the slice it reads, so the whole array is charged.
    /// </summary>
    internal static long BackingBufferBytes(ReadOnlyMemory<byte> storage)
    {
        if (MemoryMarshal.TryGetArray(storage, out ArraySegment<byte> segment) && segment.Array is { } array)
            return ArrayHeaderBytes + array.Length;

        return ArrayHeaderBytes + storage.Length;
    }

    /// <summary>Bytes of a managed string: header, length and two bytes per UTF-16 unit.</summary>
    internal static long StringBytes(string? value) =>
        value is null ? 0 : ArrayHeaderBytes + 2L * value.Length;

    private static bool HasVariablePayload(ColumnType type) =>
        type is ColumnType.String or ColumnType.Id or ColumnType.Bytes or ColumnType.Array;
}
