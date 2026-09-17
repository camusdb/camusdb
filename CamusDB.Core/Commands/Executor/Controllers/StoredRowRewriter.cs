/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Rewrites whole rows under the table's current schema version and current storage rules, with no
/// index change. The DDL paths that re-encode every row (ADD COLUMN and DROP COLUMN) use it.
///
/// <para><b>Why a plain re-encode is not enough.</b> A stored row may point at out-of-line values.
/// Writing the re-encoded row alone would leave those keys behind with nothing pointing at them, and
/// would store a value inline that the current rules move out of line. So each batch first reads the
/// old rows raw to learn which keys their pointers name, then writes the new row, its new out-of-line
/// values and the removal of the old keys in one <see cref="KvTableStore.UpdateRowsBatch"/>. A key the
/// new row writes again at the same ordinal is overwritten in place rather than removed.</para>
/// </summary>
internal static class StoredRowRewriter
{
    /// <summary>Rows buffered per batch by the streaming callers.</summary>
    internal const int BatchRows = 256;

    /// <summary>
    /// Rewrites <paramref name="rows"/> (row id plus the full decoded value set) in one batch inside
    /// <paramref name="tx"/>. The values must already be resolved, as every default store read returns
    /// them.
    /// </summary>
    internal static async Task RewriteAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        KvTransaction tx,
        IReadOnlyList<(ObjectIdValue RowId, IReadOnlyDictionary<string, ColumnValue> Values)> rows)
    {
        if (rows.Count == 0)
            return;

        List<ObjectIdValue> rowIds = new(rows.Count);
        for (int i = 0; i < rows.Count; i++)
            rowIds.Add(rows[i].RowId);

        ReadOnlyMemory<byte>?[] stored = await table.Store.GetRowsBatch(tx, rowIds, default, LargeValueFetch.Raw).ConfigureAwait(false);

        CompiledRowCodec codec = await table.GetRowCodecAsync(tx.TransactionId, table.Schema.Version).ConfigureAwait(false);
        List<TableColumnSchema> columns = table.Schema.Columns!;
        LargeValuePolicy policy = LargeValuePolicy.For(columns, database.Options);

        List<KvTableStore.RowUpdate> batch = new(rows.Count);
        for (int i = 0; i < rows.Count; i++)
        {
            EncodedRow encoded = codec.EncodeStorageValue(RowSlotAdapter.FromRow(columns, rows[i].Values), policy);
            List<int>? oldOrdinals = stored[i] is { } old ? RowStorageForms.OutOfLineOrdinals(old.Span) : null;

            batch.Add(new KvTableStore.RowUpdate
            {
                RowId = rows[i].RowId,
                NewRowData = encoded.StorageValue,
                LargeValues = encoded.OutOfLine,
                LargeValueDeletes = OrdinalsNotRewritten(oldOrdinals, encoded),
            });
        }

        await table.Store.UpdateRowsBatch(tx, batch).ConfigureAwait(false);
    }

    /// <summary>
    /// The old out-of-line ordinals that <paramref name="encoded"/> does not write again, or null when
    /// there are none. Those keys must be removed in the same batch as the new row.
    /// </summary>
    internal static List<int>? OrdinalsNotRewritten(List<int>? oldOrdinals, EncodedRow encoded)
    {
        if (oldOrdinals is null)
            return null;

        List<int>? deletes = null;
        foreach (int ordinal in oldOrdinals)
        {
            bool rewritten = false;
            for (int w = 0; encoded.OutOfLine is { } writes && w < writes.Count; w++)
            {
                if (writes[w].VariableOrdinal == ordinal)
                {
                    rewritten = true;
                    break;
                }
            }

            if (!rewritten)
                (deletes ??= []).Add(ordinal);
        }

        return deletes;
    }
}
