
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Executes a <see cref="SemiJoinNode"/> at runtime: for each outer row from
/// <paramref name="outerCursor"/> probes the inner table and emits or discards
/// the row based on <see cref="SemiJoinMode"/>.
/// </summary>
internal sealed class SemiJoinExecutor
{
    public async IAsyncEnumerable<QueryResultRow> ExecuteAsync(
        IAsyncEnumerable<QueryResultRow> outerCursor,
        SemiJoinNode node,
        QueryTicket outerTicket)
    {
        // NullAwareAnti: pre-scan inner for any NULL values; if found, all outer rows
        // are excluded (SQL three-valued-logic: NOT IN with NULL inner → UNKNOWN → false).
        if (node.Mode == SemiJoinMode.NullAwareAnti)
        {
            bool innerHasNull = await InnerHasNullAsync(node, outerTicket).ConfigureAwait(false);
            if (innerHasNull)
                yield break;
        }

        string outerCol = node.OuterColumn;
        int dot = outerCol.IndexOf('.');
        if (dot >= 0) outerCol = outerCol[(dot + 1)..];

        await foreach (QueryResultRow outerRow in outerCursor.ConfigureAwait(false))
        {
            ColumnValue? outerValue = ResolveColumnValue(outerRow.Row, outerCol);
            if (outerValue is null)
                continue;

            // NULL outer value → UNKNOWN semantics for both IN and NOT IN: row not emitted
            if (outerValue.Type == ColumnType.Null)
                continue;

            bool hasMatch = await ProbeInnerAsync(node, outerTicket, outerValue!).ConfigureAwait(false);

            bool emit = node.Mode switch
            {
                SemiJoinMode.Semi => hasMatch,
                SemiJoinMode.Anti or SemiJoinMode.NullAwareAnti => !hasMatch,
                _ => false,
            };

            if (emit)
                yield return outerRow;
        }
    }

    private static ColumnValue? ResolveColumnValue(Dictionary<string, ColumnValue> row, string columnName)
    {
        if (row.TryGetValue(columnName, out ColumnValue? value))
            return value;

        foreach ((string key, ColumnValue val) in row)
        {
            if (key.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                return val;
        }

        return null;
    }

    private async Task<bool> ProbeInnerAsync(
        SemiJoinNode node,
        QueryTicket outerTicket,
        ColumnValue probeValue)
    {
        if (probeValue.Type == ColumnType.Null)
            return false;

        if (node.InnerIndex is not null)
            return await ProbeViaIndexAsync(node, outerTicket.TxnState.TransactionId, probeValue).ConfigureAwait(false);

        return await ProbeViaScanAsync(node, outerTicket, probeValue).ConfigureAwait(false);
    }

    private static async Task<bool> ProbeViaIndexAsync(
        SemiJoinNode node,
        HLCTimestamp txId,
        ColumnValue probeValue)
    {
        TableDescriptor inner = node.InnerTable;
        TableIndexSchema index = node.InnerIndex!;
        CompositeColumnValue key = new(new[] { probeValue });

        if (index.Type == IndexType.Unique)
        {
            ObjectIdValue? rowId = await inner.Store.LookupUnique(txId, index.Name, key).ConfigureAwait(false);
            if (rowId is null)
                return false;

            if (node.InnerFilter is null)
                return true;

            byte[]? data = await inner.Store.GetRow(txId, rowId.Value).ConfigureAwait(false);
            if (data is null || data.Length == 0)
                return false;

            Dictionary<string, ColumnValue> innerRow = await RowEncoder.DecodeAsync(
                inner.Schema, txId, rowId.Value, data, null, inner.Schema.Version).ConfigureAwait(false);

            return EvalFilter(node.InnerFilter, innerRow, null);
        }

        // Non-unique: equality range scan using NextSortValue upper bound (exclusive),
        // matching the pattern used by QueryExecutor for non-unique index equality lookups.
        ColumnType keyType = GetColumnType(inner, index.Columns[0]);

        ColumnValue? nextValue = IndexScanSelector.NextSortValue(keyType, probeValue);
        CompositeColumnValue? upperBound = nextValue is not null
            ? new CompositeColumnValue(new[] { nextValue })
            : null;

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId) in inner.Store.ScanIndex(
            txId, index.Name, new[] { keyType },
            key, upperBound,
            false, true, false,
            maxRows: null))
        {
            byte[]? data = await inner.Store.GetRow(txId, rowId).ConfigureAwait(false);
            if (data is null || data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> innerRow = await RowEncoder.DecodeAsync(
                inner.Schema, txId, rowId, data, null, inner.Schema.Version).ConfigureAwait(false);

            // For types where NextSortValue returns null (String, Id), upperBound is null and the
            // scan may run past the equality range. Always verify the actual key matches.
            ColumnValue? innerVal = ResolveColumnValue(innerRow, node.InnerColumn);
            if (!ColumnValuesEqual(innerVal, probeValue))
                continue;

            if (node.InnerFilter is null || EvalFilter(node.InnerFilter, innerRow, null))
                return true;
        }

        return false;
    }

    private static async Task<bool> ProbeViaScanAsync(
        SemiJoinNode node,
        QueryTicket outerTicket,
        ColumnValue probeValue)
    {
        TableDescriptor inner = node.InnerTable;
        HLCTimestamp txId = outerTicket.TxnState.TransactionId;

        await foreach ((ObjectIdValue rowId, byte[] data) in inner.Store.ScanRows(txId, maxRows: null))
        {
            if (data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> innerRow = await RowEncoder.DecodeAsync(
                inner.Schema, txId, rowId, data, null, inner.Schema.Version).ConfigureAwait(false);

            ColumnValue? innerVal = ResolveColumnValue(innerRow, node.InnerColumn);
            if (innerVal is null)
                continue;

            if (!ColumnValuesEqual(innerVal, probeValue))
                continue;

            if (node.InnerFilter is not null && !EvalFilter(node.InnerFilter, innerRow, outerTicket.Parameters))
                continue;

            return true;
        }

        return false;
    }

    private static async Task<bool> InnerHasNullAsync(SemiJoinNode node, QueryTicket outerTicket)
    {
        TableDescriptor inner = node.InnerTable;
        HLCTimestamp txId = outerTicket.TxnState.TransactionId;

        await foreach ((ObjectIdValue rowId, byte[] data) in inner.Store.ScanRows(txId, maxRows: null))
        {
            if (data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> innerRow = await RowEncoder.DecodeAsync(
                inner.Schema, txId, rowId, data, null, inner.Schema.Version).ConfigureAwait(false);

            ColumnValue? innerVal2 = ResolveColumnValue(innerRow, node.InnerColumn);
            if (innerVal2 is not null && innerVal2.Type == ColumnType.Null)
                return true;
        }

        return false;
    }

    private static bool EvalFilter(
        NodeAst filter,
        Dictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters)
    {
        ColumnValue result = SqlExecutor.EvalExpr(filter, row, parameters);
        return result.Type == ColumnType.Bool && result.BoolValue;
    }

    private static bool ColumnValuesEqual(ColumnValue? a, ColumnValue b)
    {
        if (a is null) return false;
        if (a.Type != b.Type) return false;

        return a.Type switch
        {
            ColumnType.Integer64 => a.LongValue == b.LongValue,
            ColumnType.String => string.Equals(a.StrValue, b.StrValue, StringComparison.Ordinal),
            ColumnType.Id => string.Equals(a.StrValue, b.StrValue, StringComparison.Ordinal),
            ColumnType.Float64 => a.FloatValue == b.FloatValue,
            ColumnType.Bool => a.BoolValue == b.BoolValue,
            ColumnType.Null => false,
            _ => false,
        };
    }

    private static ColumnType GetColumnType(TableDescriptor table, string columnName)
    {
        if (table.Schema.Columns is null)
            return ColumnType.String;

        TableColumnSchema? col = table.Schema.Columns.Find(c => c.Name == columnName);
        return col?.Type ?? ColumnType.String;
    }
}
