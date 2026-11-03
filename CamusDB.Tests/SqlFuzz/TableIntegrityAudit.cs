/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.SqlFuzz;

/// <summary>
/// Checks that the stored forms of one table agree: the rows a full scan returns, every ordered index,
/// and the out-of-line value cells. It returns the problems as text and never throws for a problem, so
/// a caller can report all of them at once. The SQL fuzzer (<c>CamusDB.SqlFuzz.Tests</c>, which links
/// this file) runs it after every generated write batch; a hand-written test asserts that it returns
/// nothing. It extends <see cref="CommandsExecutor.TableIntegrityChecker"/>, which checks the primary
/// key only.
///
/// <para><b>What it checks.</b></para>
/// <list type="number">
/// <item>No two live rows share a primary-key value (a twin).</item>
/// <item>Every entry of every unique or non-unique index names a live row, and its key equals that
/// row's values for the index columns.</item>
/// <item>Every live row has exactly one entry in each index, with one exception: a unique index holds
/// no entry for a row with a NULL in a key column, because NULL never equals NULL. A non-unique index
/// holds rows with NULL keys.</item>
/// <item>The payload of a covering index (its INCLUDE columns) equals the row's values for those
/// columns. An UPDATE that changes an INCLUDE column must rewrite the payload, also when the key stays
/// the same; a stale payload makes a covering read return old values.</item>
/// <item>The out-of-line value keys (<c>|v</c>) of the table are exactly the cells that the stored rows
/// point to: for each live row, one key per out-of-line cell, with the cell's ordinal. A key that no
/// stored row points to is an orphan, also when its row is still live (an UPDATE from a large value to
/// a short one or to NULL must remove the old cell). A pointer with no key is a lost cell.</item>
/// </list>
/// <para>A table can permit equal rows: two rows with the same values and different keys are not a
/// problem. Only the key identifies a row. GIN indexes and indexes that are not yet public are not
/// checked.</para>
///
/// <para>The scan, the index reads and the large-value scan run in one transaction, so a concurrent
/// commit cannot separate them. A failure to run the check (a table that does not exist) is an
/// exception, not a problem: a caller must not read it as an inconsistency.</para>
/// </summary>
internal static class TableIntegrityAudit
{
    public static async Task<IReadOnlyList<string>> CheckAsync(CommandExecutor executor, DatabaseDescriptor database, string databaseName, string tableName)
    {
        List<string> problems = new();
        TableDescriptor table = await executor.OpenTableWithDescriptor(database, new OpenTableTicket(databaseName, tableName)).ConfigureAwait(false);

        KvTransaction tx = await database.Transactions.BeginAsync().ConfigureAwait(false);
        try
        {
            // SELECT * needs every column, so the planner reads the rows themselves, not an index.
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, databaseName, $"SELECT * FROM {tableName}", null)).ConfigureAwait(false);

            Dictionary<string, IReadOnlyDictionary<string, ColumnValue>> rows = new(StringComparer.Ordinal);
            await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
            {
                string rowId = row.RowId.ToString();
                if (!rows.TryAdd(rowId, Materialize(row.Row)))
                    problems.Add($"{tableName}: the scan returned row id {rowId} twice");
            }

            TableIndexSchema? primaryKey = table.AllIndexes.Values.FirstOrDefault(i => i.Name == CamusDBConstants.PrimaryKeyInternalName);
            if (primaryKey is not null)
            {
                Dictionary<string, string> keys = new(StringComparer.Ordinal);
                foreach ((string rowId, IReadOnlyDictionary<string, ColumnValue> values) in rows)
                {
                    string key = KeyText(values, primaryKey.Columns);
                    if (keys.TryGetValue(key, out string? other))
                        problems.Add($"{tableName}: rows {other} and {rowId} share the primary key {key}; a key has a twin");
                    else
                        keys[key] = rowId;
                }
            }

            foreach (TableIndexSchema index in table.AllIndexes.Values)
            {
                if (index.Type is not (IndexType.Unique or IndexType.Multi) || index.State != SchemaElementState.Public)
                    continue;
                await CheckIndexAsync(table, tx, index, rows, problems).ConfigureAwait(false);
            }

            await CheckLargeValuesAsync(database, table, tx, rows, problems).ConfigureAwait(false);
        }
        finally
        {
            try { await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false); } catch { }
        }

        return problems;
    }

    private static async Task CheckIndexAsync(
        TableDescriptor table,
        KvTransaction tx,
        TableIndexSchema index,
        Dictionary<string, IReadOnlyDictionary<string, ColumnValue>> rows,
        List<string> problems)
    {
        ColumnType[] keyTypes = new ColumnType[index.Columns.Length];
        for (int i = 0; i < index.Columns.Length; i++)
        {
            TableColumnSchema? column = table.Schema.Columns!.FirstOrDefault(c => string.Equals(c.Name, index.Columns[i], StringComparison.OrdinalIgnoreCase));
            if (column is null)
            {
                problems.Add($"{table.Name}.{index.Name}: key column {index.Columns[i]} is not a column of the table");
                return;
            }
            keyTypes[i] = column.Type;
        }

        bool unique = index.Type == IndexType.Unique;
        Dictionary<string, int> entriesPerRow = new(StringComparer.Ordinal);

        if (index.HasIncludeColumns && index.IncludeColumns.Length == 0)
        {
            problems.Add($"{table.Name}.{index.Name}: the index has INCLUDE columns, but their names are not resolved");
            return;
        }
        ColumnType[] includeTypes = new ColumnType[index.IncludeColumns.Length];
        for (int i = 0; i < index.IncludeColumns.Length; i++)
        {
            TableColumnSchema? column = table.Schema.Columns!.FirstOrDefault(c => string.Equals(c.Name, index.IncludeColumns[i], StringComparison.OrdinalIgnoreCase));
            if (column is null)
            {
                problems.Add($"{table.Name}.{index.Name}: INCLUDE column {index.IncludeColumns[i]} is not a column of the table");
                return;
            }
            includeTypes[i] = column.Type;
        }

        await foreach ((CompositeColumnValue key, ObjectIdValue rowIdValue, ReadOnlyMemory<byte> includeTuple) in table.Store.ScanIndex(
            tx, index.KvId, keyTypes, from: null, to: null, unique: unique).ConfigureAwait(false))
        {
            string rowId = rowIdValue.ToString();
            entriesPerRow[rowId] = entriesPerRow.GetValueOrDefault(rowId) + 1;

            if (!rows.TryGetValue(rowId, out IReadOnlyDictionary<string, ColumnValue>? row))
            {
                problems.Add($"{table.Name}.{index.Name}: an entry names row {rowId}, which the scan does not return (an orphan entry)");
                continue;
            }

            string entryKey = SqlFuzzValues.RenderRow(key.Values);
            string rowKey = KeyText(row, index.Columns);
            if (entryKey != rowKey)
                problems.Add($"{table.Name}.{index.Name}: the entry for row {rowId} has key {entryKey}, the row has {rowKey}");

            if (includeTypes.Length > 0)
            {
                CompositeColumnValue payload = IndexIncludeValueCodec.DecodeTuple(includeTypes, includeTuple.Span);
                string entryPayload = SqlFuzzValues.RenderRow(payload.Values);
                string rowPayload = KeyText(row, index.IncludeColumns);
                if (entryPayload != rowPayload)
                    problems.Add($"{table.Name}.{index.Name}: the entry for row {rowId} has INCLUDE payload {entryPayload}, the row has {rowPayload} (a stale covering payload)");
            }
        }

        foreach ((string rowId, IReadOnlyDictionary<string, ColumnValue> row) in rows)
        {
            bool hasNullKey = index.Columns.Any(c => Cell(row, c).Type == ColumnType.Null);
            int expected = unique && hasNullKey ? 0 : 1;
            int actual = entriesPerRow.GetValueOrDefault(rowId);
            if (actual != expected)
                problems.Add($"{table.Name}.{index.Name}: row {rowId} (key {KeyText(row, index.Columns)}) has {actual} entries, expected {expected}");
        }
    }

    /// <summary>
    /// The keys of the large-value space of the table (<c>{prefix}/{rowIdHex24}{ordinalHex4}</c>) must be
    /// exactly the out-of-line cells the stored rows point to. The pointers come from each live row's raw
    /// stored bytes (<see cref="LargeValueFetch.Raw"/>, <see cref="RowStorageForms.OutOfLineOrdinals"/>).
    /// The space comes from <see cref="KvTableStore.LargeValueKeySpace"/>, never from a spelled-out key
    /// shape.
    /// </summary>
    private static async Task CheckLargeValuesAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        KvTransaction tx,
        Dictionary<string, IReadOnlyDictionary<string, ColumnValue>> rows,
        List<string> problems)
    {
        string bucket = table.Store.LargeValueKeySpace;
        string keyPrefix = bucket + "/";

        // Cell names are the row id and the ordinal as lower-case hex; compare them case-insensitively.
        HashSet<string> pointed = new(StringComparer.OrdinalIgnoreCase);
        foreach (string rowId in rows.Keys)
        {
            ReadOnlyMemory<byte>? raw = await table.Store.GetRow(tx, ObjectId.ToValue(rowId), largeValues: LargeValueFetch.Raw).ConfigureAwait(false);
            if (raw is not { } payload)
            {
                problems.Add($"{table.Name}: the scan returned row {rowId}, but a point read of its stored bytes found nothing");
                continue;
            }
            foreach (int ordinal in RowStorageForms.OutOfLineOrdinals(payload.Span) ?? [])
                pointed.Add(rowId + ordinal.ToString("x4", System.Globalization.CultureInfo.InvariantCulture));
        }

        HashSet<string> present = new(StringComparer.OrdinalIgnoreCase);

        await foreach ((string kvKey, _) in database.Kahuna.Kahuna.LocateAndScanRange(
            tx.TransactionId,
            bucket,
            null, true,
            null, true,
            KvStoreConstants.DefaultPageSize,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            CancellationToken.None).ConfigureAwait(false))
        {
            if (!kvKey.StartsWith(keyPrefix, StringComparison.Ordinal) || kvKey.Length < keyPrefix.Length + 24)
                continue;
            string cell = kvKey[keyPrefix.Length..];
            present.Add(cell);
            string rowId = cell[..24];
            if (!rows.ContainsKey(rowId))
                problems.Add($"{table.Name}: large-value key {kvKey} belongs to row {rowId}, which the scan does not return (an orphan cell)");
            else if (!pointed.Contains(cell))
                problems.Add($"{table.Name}: large-value key {kvKey} belongs to live row {rowId}, but the stored row does not point to it (a stale cell an UPDATE left)");
        }

        foreach (string cell in pointed)
            if (!present.Contains(cell))
                problems.Add($"{table.Name}: row {cell[..24]} points to out-of-line cell {cell[24..]}, but the large-value space has no key for it (a lost cell)");
    }

    private static IReadOnlyDictionary<string, ColumnValue> Materialize(IReadOnlyDictionary<string, ColumnValue> row)
    {
        Dictionary<string, ColumnValue> copy = new(row.Count, StringComparer.OrdinalIgnoreCase);
        foreach ((string name, ColumnValue value) in row)
            copy[name] = value;
        return copy;
    }

    private static ColumnValue Cell(IReadOnlyDictionary<string, ColumnValue> row, string column) =>
        row.TryGetValue(column, out ColumnValue? value) ? value : ColumnValue.Null;

    private static string KeyText(IReadOnlyDictionary<string, ColumnValue> row, IReadOnlyList<string> columns) =>
        SqlFuzzValues.RenderRow(columns.Select(c => Cell(row, c)).ToList());
}
