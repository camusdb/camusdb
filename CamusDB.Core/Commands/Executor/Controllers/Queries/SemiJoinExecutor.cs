
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

    private static ColumnValue? ResolveColumnValue(IReadOnlyDictionary<string, ColumnValue> row, string columnName)
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
            Diagnostics.IndexUsageCounters? usage = inner.Store.IndexUsage?.BeginRead(index.KvId);

            ObjectIdValue? rowId = await inner.Store.LookupUnique(outerTicket.TxnState, index.KvId, key, outerTicket.CancellationToken).ConfigureAwait(false);
            if (rowId is null)
                return false;

            usage?.AddEntriesRead(1);

            if (node.InnerFilter is null)
                return true;

            ReadOnlyMemory<byte>? data = await inner.Store.GetRow(outerTicket.TxnState, rowId.Value, outerTicket.CancellationToken).ConfigureAwait(false);
            if (data is null || data.Value.Length == 0)
                return false;

            Dictionary<string, ColumnValue> innerRow = await RowEncoder.DecodeAsync(
                inner.Schema, txId, rowId.Value, data.Value, required, inner.Schema.Version).ConfigureAwait(false);

            return EvalFilter(node.InnerFilter, innerRow, parameters);
        }

        // Non-unique: equality range scan over the probe value, with the same direction-aware
        // bounds as every other single-value index seek.
        ColumnType keyType = GetColumnType(inner, index.Columns[0]);
        (CompositeColumnValue upperBound, bool toInclusive) = IndexScanSelector.EqualitySeekUpperBound(inner, index, key);

        // The predicate lock of the probed value under Serializable read-write: an entry another
        // transaction inserts for it is held back until this one ends, so the existence answer holds
        // for the whole transaction. The unique probe above gets the same from LookupUnique's point lock.
        await inner.Store.AcquireIndexEqualityLockAsync(
            outerTicket.TxnState, index.KvId, key, unique: false, index.Columns.Length, outerTicket.CancellationToken).ConfigureAwait(false);

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in Diagnostics.TableIndexUsage.Count(inner.Store.IndexUsage, index.KvId, inner.Store.ScanIndex(
            outerTicket.TxnState, index.KvId, new[] { keyType },
            key, upperBound,
            unique: false, fromInclusive: true, toInclusive: toInclusive,
            maxRows: null, cancellationToken: outerTicket.CancellationToken)))
        {
            ReadOnlyMemory<byte>? data = await inner.Store.GetRow(outerTicket.TxnState, rowId, outerTicket.CancellationToken).ConfigureAwait(false);
            if (data is null || data.Value.Length == 0)
                continue;

            Dictionary<string, ColumnValue> innerRow = await RowEncoder.DecodeAsync(
                inner.Schema, txId, rowId, data.Value, required, inner.Schema.Version).ConfigureAwait(false);

            // The decoded-key filter already trims the scan to the probe value; the comparison here
            // is a second guard that keeps the existence answer exact.
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

        // Phantom fence over the whole inner table under Serializable read-write, as a single-table
        // scan takes it. The probe runs once per outer row; the lock is taken once and held to commit.
        await AcquireInnerRowRangeLockAsync(inner, outerTicket).ConfigureAwait(false);

        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in inner.Store.ScanRows(
            outerTicket.TxnState, maxRows: null, cancellationToken: outerTicket.CancellationToken))
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

        await AcquireInnerRowRangeLockAsync(inner, outerTicket).ConfigureAwait(false);

        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in inner.Store.ScanRows(
            outerTicket.TxnState, maxRows: null, cancellationToken: outerTicket.CancellationToken))
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

        await AcquireInnerRowRangeLockAsync(inner, outerTicket).ConfigureAwait(false);

        await foreach ((ObjectIdValue _, ReadOnlyMemory<byte> data) in inner.Store.ScanRows(
            outerTicket.TxnState, maxRows: 1, cancellationToken: outerTicket.CancellationToken))
        {
            if (data.Length > 0)
                return true;
        }

        return false;
    }

    /// <summary>
    /// The phantom fence of a scan of the inner table: a shared lock over its whole row range, taken
    /// only by a Serializable read-write transaction, exactly as a single-table scan takes it. Every
    /// inner scan of this executor calls it; a lock the transaction already holds costs no round trip.
    /// </summary>
    private static Task AcquireInnerRowRangeLockAsync(TableDescriptor inner, QueryTicket outerTicket)
        => inner.Store.AcquireRowRangeLockAsync(
            outerTicket.TxnState,
            exclusive: outerTicket.ExclusivePredicateLocks,
            cancellationToken: outerTicket.CancellationToken);

    private static bool EvalFilter(
        NodeAst filter,
        IReadOnlyDictionary<string, ColumnValue> row,
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
        HashSet<string> cols = new(StringComparer.OrdinalIgnoreCase) { innerColumn };
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

    /// <summary>
    /// SQL equality of an inner value read through the index and the outer probe value, the second
    /// guard of <see cref="ProbeViaIndexAsync"/>. Every indexable type must have an arm: a type that
    /// falls through to "not equal" makes every probe of an index on that type miss, so an IN
    /// subquery loses its matches and a NOT IN subquery keeps rows it must remove. The types without a
    /// rule of their own use <see cref="ColumnValue.CompareTo"/>, the order of the index itself.
    /// </summary>
    private static bool ColumnValuesEqual(ColumnValue? a, ColumnValue b)
    {
        if (a is null) return false;
        if (a.Type != b.Type) return false;

        return a.Type switch
        {
            ColumnType.Integer64 => a.LongValue == b.LongValue,
            ColumnType.String => string.Equals(a.StrValue, b.StrValue, StringComparison.Ordinal),
            ColumnType.Id => string.Equals(a.StrValue, b.StrValue, StringComparison.Ordinal),
            // SQL equality, the rule of `=`: NaN equals NaN (see FloatMath).
            ColumnType.Float64 => FloatMath.SqlEquals(a.FloatValue, b.FloatValue),
            ColumnType.Bool => a.BoolValue == b.BoolValue,
            ColumnType.Null => false,
            _ => a.CompareTo(b) == 0,
        };
    }

    private static ColumnType GetColumnType(TableDescriptor table, string columnName)
    {
        if (table.Schema.Columns is null)
            return ColumnType.String;

        TableColumnSchema? col = table.Schema.Columns.Find(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
        return col?.Type ?? ColumnType.String;
    }
}
