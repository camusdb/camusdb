/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer.Models;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Serializes the stored/payload (INCLUDE) columns of a covering secondary index into the trailing
/// portion of the index entry's value — the "include tuple" that follows the fixed 24-byte rowId
/// (see <see cref="BranchKvCodec.EncodeIndexRowId(Util.ObjectIds.ObjectIdValue, System.ReadOnlySpan{byte})"/>).
///
/// The tuple reuses the row serializer's self-describing per-column format via
/// <see cref="RowEncoder.WriteColumnValue"/> / <see cref="RowEncoder.ReadColumnValue"/>, so there is
/// exactly one on-disk value format across rows and index payloads. Columns are written positionally
/// in the index's include order; a missing/absent value writes a single <c>TypeNull</c> marker.
///
/// Decode is schema-driven and <b>tolerant of a short or absent tuple</b>: an entry written before
/// the index gained a given include column simply runs out of bytes, and every remaining include
/// column decodes to <see cref="ColumnValue.Null"/>. This keeps a partially-widened index readable
/// and makes a zero-include index's payload byte-identical to the historical rowId-only value.
/// </summary>
public static class IndexIncludeValueCodec
{
    /// <summary>An empty tuple — the value of an index with no INCLUDE columns.</summary>
    public static readonly byte[] Empty = [];

    /// <summary>
    /// Encodes the include tuple for <paramref name="includeColumns"/> by reading each column's value
    /// from <paramref name="row"/> (absent → NULL) in include order. Returns <see cref="Empty"/> when
    /// there are no include columns.
    /// </summary>
    public static byte[] EncodeTuple(string[] includeColumns, IReadOnlyDictionary<string, ColumnValue> row)
    {
        if (includeColumns.Length == 0)
            return Empty;

        int length = 0;
        for (int i = 0; i < includeColumns.Length; i++)
            length += RowEncoder.ColumnValueSize(GetValueOrNull(row, includeColumns[i]));

        byte[] tuple = new byte[length];
        int pointer = 0;
        for (int i = 0; i < includeColumns.Length; i++)
            RowEncoder.WriteColumnValue(tuple, GetValueOrNull(row, includeColumns[i]), ref pointer);

        return tuple;
    }

    /// <summary>
    /// Encodes the include tuple (as <see cref="EncodeTuple"/>) and enforces the per-entry byte ceiling
    /// <see cref="CamusDBConfig.MaxIndexIncludeTupleBytes"/>: a row whose encoded payload exceeds the
    /// limit is rejected with <see cref="CamusDBErrorCodes.SchemaLimitExceeded"/> before any KV mutation,
    /// so a single oversized String/Bytes/Array value cannot bloat every index entry, its replication,
    /// and the enclosing transaction batch. Every write path (INSERT/UPDATE/backfill) that materializes
    /// a covering-index tuple goes through this. Limit disabled when the config value is <c>&lt;= 0</c>.
    /// </summary>
    public static byte[] EncodeTupleChecked(string[] includeColumns, IReadOnlyDictionary<string, ColumnValue> row, string indexName)
    {
        byte[] tuple = EncodeTuple(includeColumns, row);

        int max = CamusDBConfig.MaxIndexIncludeTupleBytes;
        if (max > 0 && tuple.Length > max)
            throw new CamusDBException(
                CamusDBErrorCodes.SchemaLimitExceeded,
                $"INCLUDE payload for index '{indexName}' is {tuple.Length} bytes, exceeding the maximum of {max}");

        return tuple;
    }

    /// <summary>
    /// Decodes an include tuple into a positional <see cref="CompositeColumnValue"/> aligned with
    /// <paramref name="includeTypes"/>. Reading stops early and fills the remaining slots with
    /// <see cref="ColumnValue.Null"/> once <paramref name="tuple"/> is exhausted (short/absent tuple
    /// tolerance).
    /// </summary>
    public static CompositeColumnValue DecodeTuple(ColumnType[] includeTypes, ReadOnlySpan<byte> tuple)
    {
        ColumnValue[] values = new ColumnValue[includeTypes.Length];
        int pointer = 0;

        for (int i = 0; i < includeTypes.Length; i++)
        {
            if (pointer >= tuple.Length)
            {
                values[i] = ColumnValue.Null;
                continue;
            }

            values[i] = RowEncoder.ReadColumnValue(includeTypes[i], tuple, ref pointer);
        }

        return new CompositeColumnValue(values);
    }

    /// <summary>
    /// Decodes only the <b>projected</b> included columns of a covering scan directly into
    /// <paramref name="output"/>, in a single left-to-right pass over the positional tuple. For each
    /// include position <c>p</c>, <paramref name="outputForPosition"/><c>[p]</c> is the destination
    /// index in <paramref name="output"/>, or <c>-1</c> when that column is not projected — in which
    /// case the value is <b>skipped</b> (advanced past, not materialized) via
    /// <see cref="RowEncoder.SkipColumnValue"/>. This avoids allocating a <see cref="ColumnValue"/> for
    /// unused columns and avoids an intermediate <see cref="CompositeColumnValue"/>, so scan cost tracks
    /// the <i>projected</i> include width, not the total include width. A short/absent tuple fills the
    /// remaining projected positions with <see cref="ColumnValue.Null"/>. The pass stops once the last
    /// projected position has been consumed.
    /// </summary>
    public static void DecodeTupleInto(
        ColumnType[] includeTypes,
        int[] outputForPosition,
        ColumnValue[] output,
        ReadOnlySpan<byte> tuple)
    {
        int pointer = 0;

        for (int p = 0; p < includeTypes.Length; p++)
        {
            int outIdx = outputForPosition[p];

            if (pointer >= tuple.Length)
            {
                // Tuple exhausted (written before this include column existed): remaining
                // projected columns decode as NULL; unprojected ones need nothing.
                if (outIdx >= 0)
                    output[outIdx] = ColumnValue.Null;
                continue;
            }

            if (outIdx >= 0)
                output[outIdx] = RowEncoder.ReadColumnValue(includeTypes[p], tuple, ref pointer);
            else
                RowEncoder.SkipColumnValue(includeTypes[p], tuple, ref pointer);
        }
    }

    private static ColumnValue GetValueOrNull(IReadOnlyDictionary<string, ColumnValue> row, string name)
        => row.TryGetValue(name, out ColumnValue? value) ? value : ColumnValue.Null;
}
