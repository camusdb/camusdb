
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
        // NullAwareAnti: pre-scan inner for NULLs and record emptiness in one pass.
        // If inner has any NULL → all outer rows excluded (NOT IN with NULL inner → UNKNOWN).
        bool innerIsEmpty = false;
        if (node.Mode == SemiJoinMode.NullAwareAnti)
        {
            (bool innerHasNull, innerIsEmpty) = await InnerScanForNullsAsync(node, outerTicket).ConfigureAwait(false);
            if (innerHasNull)
                yield break;
        }

        string outerCol = node.OuterColumn;
        int dot = outerCol.IndexOf('.');
        if (dot >= 0) outerCol = outerCol[(dot + 1)..];

        // For Anti/NullAwareAnti, a NULL outer value emits iff the inner is empty:
        //   NULL NOT IN ()          → TRUE  (emit)
        //   NULL NOT IN (non-empty) → UNKNOWN → FALSE (skip)
        // We check emptiness lazily (once) for Anti; NullAwareAnti already has it from the pre-scan.
        bool? innerIsEmptyCache = node.Mode == SemiJoinMode.NullAwareAnti ? innerIsEmpty : null;

        await foreach (QueryResultRow outerRow in outerCursor.ConfigureAwait(false))
        {
            ColumnValue? outerValue = ResolveColumnValue(outerRow.Row, outerCol);
            if (outerValue is null)
                continue;

            if (outerValue.Type == ColumnType.Null)
            {
                // Semi: NULL IN anything → UNKNOWN → false; never emit.
                if (node.Mode == SemiJoinMode.Semi)
                    continue;

                // Anti/NullAwareAnti: emit only when inner is empty.
                innerIsEmptyCache ??= !await InnerHasAnyRowAsync(node, outerTicket).ConfigureAwait(false);
                if (innerIsEmptyCache.Value)
                    yield return outerRow;
                continue;
            }

            bool hasMatch = await ProbeInnerAsync(node, outerTicket, outerValue).ConfigureAwait(false);

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
            return await ProbeViaIndexAsync(node, outerTicket, probeValue).ConfigureAwait(false);

        return await ProbeViaScanAsync(node, outerTicket, probeValue).ConfigureAwait(false);
    }

    private static async Task<bool> ProbeViaIndexAsync(
        SemiJoinNode node,
        QueryTicket outerTicket,
        ColumnValue probeValue)
    {
        TableDescriptor inner = node.InnerTable;
        TableIndexSchema index = node.InnerIndex!;
        HLCTimestamp txId = outerTicket.TxnState.TransactionId;
        Dictionary<string, ColumnValue>? parameters = outerTicket.Parameters;
        CompositeColumnValue key = new(new[] { probeValue });
        IReadOnlySet<string> required = BuildRequiredColumns(node.InnerColumn, node.InnerFilter);

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
                inner.Schema, txId, rowId.Value, data, required, inner.Schema.Version).ConfigureAwait(false);

            return EvalFilter(node.InnerFilter, innerRow, parameters);
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
                inner.Schema, txId, rowId, data, required, inner.Schema.Version).ConfigureAwait(false);

            // For types where NextSortValue returns null (String, Id), upperBound is null and the
            // scan may run past the equality range. Always verify the actual key matches.
            ColumnValue? innerVal = ResolveColumnValue(innerRow, node.InnerColumn);
            if (!ColumnValuesEqual(innerVal, probeValue))
                continue;

            if (node.InnerFilter is null || EvalFilter(node.InnerFilter, innerRow, parameters))
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
        IReadOnlySet<string> required = BuildRequiredColumns(node.InnerColumn, node.InnerFilter);

        await foreach ((ObjectIdValue rowId, byte[] data) in inner.Store.ScanRows(txId, maxRows: null))
        {
            if (data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> innerRow = await RowEncoder.DecodeAsync(
                inner.Schema, txId, rowId, data, required, inner.Schema.Version).ConfigureAwait(false);

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

    /// <summary>
    /// Single pass over the inner table: returns whether any row has a NULL in the inner
    /// column, and whether the table is empty. Used by NullAwareAnti pre-scan.
    /// </summary>
    private static async Task<(bool hasNull, bool isEmpty)> InnerScanForNullsAsync(
        SemiJoinNode node, QueryTicket outerTicket)
    {
        TableDescriptor inner = node.InnerTable;
        HLCTimestamp txId = outerTicket.TxnState.TransactionId;
        bool sawAnyRow = false;

        // Must include filter columns: a NULL in a row excluded by the inner WHERE
        // is not part of the subquery result set and must not poison the anti-join.
        IReadOnlySet<string> required = BuildRequiredColumns(node.InnerColumn, node.InnerFilter);

        await foreach ((ObjectIdValue rowId, byte[] data) in inner.Store.ScanRows(txId, maxRows: null))
        {
            if (data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> innerRow = await RowEncoder.DecodeAsync(
                inner.Schema, txId, rowId, data, required, inner.Schema.Version).ConfigureAwait(false);

            // Only rows that pass the inner filter belong to the subquery result set.
            if (node.InnerFilter is not null && !EvalFilter(node.InnerFilter, innerRow, outerTicket.Parameters))
                continue;

            sawAnyRow = true;

            ColumnValue? innerVal = ResolveColumnValue(innerRow, node.InnerColumn);
            if (innerVal is not null && innerVal.Type == ColumnType.Null)
                return (hasNull: true, isEmpty: false);
        }

        return (hasNull: false, isEmpty: !sawAnyRow);
    }

    /// <summary>
    /// Returns true if the inner table contains at least one non-empty row.
    /// Used to resolve NULL-outer semantics for Anti mode lazily (once per execution).
    /// </summary>
    private static async Task<bool> InnerHasAnyRowAsync(SemiJoinNode node, QueryTicket outerTicket)
    {
        TableDescriptor inner = node.InnerTable;
        HLCTimestamp txId = outerTicket.TxnState.TransactionId;

        await foreach ((ObjectIdValue _, byte[] data) in inner.Store.ScanRows(txId, maxRows: 1))
        {
            if (data.Length > 0)
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

    /// <summary>
    /// Builds the minimal set of inner-table columns needed to probe a row:
    /// the probe column itself plus every bare identifier referenced by the inner filter.
    /// Passing this to DecodeAsync avoids deserialising unneeded columns.
    /// </summary>
    private static IReadOnlySet<string> BuildRequiredColumns(string innerColumn, NodeAst? filter)
    {
        HashSet<string> cols = new(StringComparer.Ordinal) { innerColumn };
        if (filter is not null)
            CollectIdentifiers(filter, cols);
        return cols;
    }

    private static void CollectIdentifiers(NodeAst node, HashSet<string> cols)
    {
        if (node.nodeType == NodeType.Identifier && node.yytext is not null)
            cols.Add(node.yytext);

        if (node.leftAst     is not null) CollectIdentifiers(node.leftAst,     cols);
        if (node.rightAst    is not null) CollectIdentifiers(node.rightAst,    cols);
        if (node.extendedOne is not null) CollectIdentifiers(node.extendedOne, cols);
        if (node.extendedTwo is not null) CollectIdentifiers(node.extendedTwo, cols);
        if (node.extendedThree is not null) CollectIdentifiers(node.extendedThree, cols);
        if (node.extendedFour  is not null) CollectIdentifiers(node.extendedFour,  cols);
        if (node.extendedFive  is not null) CollectIdentifiers(node.extendedFive,  cols);
        if (node.extendedSix   is not null) CollectIdentifiers(node.extendedSix,   cols);
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
