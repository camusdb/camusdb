/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Checks a row fetched through a secondary-index entry against the entry's key before the row is
/// returned. An index read answers a predicate from the entry and then fetches the row by the id the
/// entry names; the planner drops the predicate the entry answered from the residual filter, so a
/// row whose indexed value no longer matches its entry would be returned as if it matched. Such an
/// entry is a defect somewhere else (an index maintained out of order, a backfill that indexed a
/// value a concurrent writer replaced), and the writers are built not to leave one. This check is
/// the last line: a row that disagrees with its entry is skipped, counted
/// (<c>camus.index_read.stale_entries</c>) and logged, so a stale entry costs a missing row in one
/// result instead of a wrong row in every result until the index is rebuilt.
///
/// <para>The check compares the key columns of the entry with the same columns of the fetched row,
/// so every path that fetches the row must decode them: <see cref="RequiredColumns"/> adds them to
/// the columns a scan decodes. A column the row did not decode is not judged. A covering (index-only)
/// read never fetches the row and cannot be checked; it answers from the entry alone.</para>
///
/// <para>NULLs compare equal to NULLs only; a non-unique index keeps an entry for a row with a NULL
/// key, and that entry is right as long as the row's value is still NULL.</para>
/// </summary>
internal static class IndexEntryCheck
{
    /// <summary>Minimum spacing of the warning for one index, so a stale index does not flood the log.</summary>
    private static readonly TimeSpan WarningInterval = TimeSpan.FromMinutes(1);

    private static readonly ConcurrentDictionary<string, long> lastWarningTicks = new(StringComparer.Ordinal);

    /// <summary>
    /// The columns a row fetched through <paramref name="index"/> must decode: <paramref name="required"/>
    /// plus the index's key columns. Null (every column) stays null; a set that already holds every key
    /// column is returned as is.
    /// </summary>
    internal static IReadOnlySet<string>? RequiredColumns(IReadOnlySet<string>? required, TableIndexSchema index)
    {
        if (required is null)
            return null;

        HashSet<string>? extended = null;

        foreach (string column in index.Columns)
        {
            if (required.Contains(column) || extended is { } ext && ext.Contains(column))
                continue;

            extended ??= new HashSet<string>(required, StringComparer.OrdinalIgnoreCase);
            extended.Add(column);
        }

        return extended ?? required;
    }

    /// <summary>
    /// True when <paramref name="row"/> agrees with <paramref name="entryKey"/> on every key column of
    /// <paramref name="index"/> that the row decoded, so the row may be returned. On a disagreement the
    /// stale entry is counted and logged and the method returns false; the caller skips the row.
    /// <paramref name="entryKey"/> may be shorter than the index (a prefix lookup) and, on a non-unique
    /// index, never carries the row-id tie-breaker.
    /// </summary>
    internal static bool Confirms(TableDescriptor table, TableIndexSchema index, CompositeColumnValue entryKey, QueryRow row, ILogger logger)
    {
        ColumnValue[] keyValues = entryKey.Values;
        int columns = Math.Min(index.Columns.Length, keyValues.Length);

        for (int i = 0; i < columns; i++)
        {
            int ordinal = row.Layout.IndexOf(index.Columns[i]);
            if (ordinal < 0)
                continue;

            if (SameValue(keyValues[i], row.GetColumnValue(ordinal)))
                continue;

            RecordStaleEntry(table, index, row.RowId, logger);
            return false;
        }

        return true;
    }

    /// <summary>
    /// Whether an entry's key value and a row's cell hold the same value. Values of different types are
    /// not judged (true): an entry key decodes to the column's schema type, so a difference there is a
    /// decoding shape, not a stale entry.
    /// </summary>
    private static bool SameValue(ColumnValue keyValue, ColumnValue rowValue)
    {
        bool keyNull = keyValue.Type == ColumnType.Null;
        bool rowNull = rowValue.Type == ColumnType.Null;

        if (keyNull || rowNull)
            return keyNull && rowNull;

        if (keyValue.Type != rowValue.Type)
            return true;

        return keyValue.CompareTo(rowValue) == 0;
    }

    private static void RecordStaleEntry(TableDescriptor table, TableIndexSchema index, ObjectIdValue rowId, ILogger logger)
    {
        ServerDiagnostics.RecordStaleIndexEntry(table.Name, index.Name);

        string key = table.Id + "|" + index.KvId;
        long now = Environment.TickCount64;
        long last = lastWarningTicks.GetValueOrDefault(key, long.MinValue);

        if (now - last < WarningInterval.TotalMilliseconds)
            return;

        lastWarningTicks[key] = now;
        Log.LogStaleIndexEntrySkipped(logger, index.Name, table.Name, rowId);
    }
}
