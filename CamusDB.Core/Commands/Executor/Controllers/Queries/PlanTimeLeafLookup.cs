/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The rows a small dimension leaf keeps after its pushed filter, read at plan time: the number of
/// rows and, for each single-column unique key of the table, the key values of those rows
/// (<see cref="PlanTimeLeafLookup"/>). The join search prices a probe through one of those keys at
/// the key's own share of the fact column, read from the column's histogram, instead of the
/// average key.
/// </summary>
/// <param name="RowCount">The rows the filter keeps.</param>
/// <param name="ValuesByColumn">The values of each single-column unique key column, in row order.</param>
public sealed record PlanTimeLeafKeys(int RowCount, IReadOnlyDictionary<string, IReadOnlyList<ColumnValue>> ValuesByColumn)
{
    /// <summary>The kept values of <paramref name="column"/>, or null when it is not a unique key of the leaf.</summary>
    public IReadOnlyList<ColumnValue>? ValuesOf(string column) =>
        ValuesByColumn.TryGetValue(column, out IReadOnlyList<ColumnValue>? values) ? values : null;
}

/// <summary>
/// Reads, before the join search runs, the rows that a filter on a small dimension table keeps,
/// so the search knows which keys a probe into a fact table will carry.
///
/// <para>A filter such as <c>it.info = 'release dates'</c> keeps one row of <c>info_type</c>, and
/// the statistics price the probe into <c>movie_info</c> by that row's key at the average key:
/// 394 rows of 44,507 over 113 keys, where the named key holds 3,208. No statistic can say which
/// key a filter names. Two priors that guessed the named key is a popular one were measured and
/// rejected: on the Join Order Benchmark the named keys are the popular ones, on the retail data
/// set they are not, and the guess cost the retail tier a fifth of its work. This class reads the
/// key instead: one scan of the small table through the query's own transaction and snapshot,
/// with the leaf's filter evaluated as the leaf scan evaluates it.</para>
///
/// <para>A leaf qualifies when its table has a single-column unique key, its row count is at most
/// <see cref="CamusDBOptions.StatsPlanTimeLookupMaxRows"/> (0 turns the lookup off), and its filter
/// has an equality or an <c>IN</c> list on constants, so it names rows. A scan that meets more rows
/// than twice the cap (the count was stale), or keeps more than <see cref="MaxKeys"/> rows, yields
/// nothing for that leaf, and the search keeps its statistics. The reads count on the statement's
/// probe as rows read, since the statement reads them.</para>
/// </summary>
internal static class PlanTimeLeafLookup
{
    /// <summary>
    /// The most rows a leaf may keep for its keys to be used: above it the keys are many enough
    /// that the average key prices the probe well, and the per-key shares would cost more to sum
    /// than they are worth.
    /// </summary>
    internal const int MaxKeys = 64;

    /// <summary>
    /// The keys of every qualifying leaf of <paramref name="bound"/>, by alias; null when no leaf
    /// qualifies or the lookup is off. <paramref name="filterer"/> evaluates the leaf filters;
    /// <paramref name="stats"/> gives the row counts that bound the scan.
    /// </summary>
    internal static async Task<IReadOnlyDictionary<string, PlanTimeLeafKeys>?> ResolveAsync(
        DatabaseDescriptor database,
        BoundSelectQuery bound,
        QueryTicket ticket,
        JoinPredicatePushdown.Result pushdown,
        StatisticsManager? stats,
        QueryFilterer filterer,
        CamusDBOptions options)
    {
        int maxRows = options.StatsPlanTimeLookupMaxRows;
        if (maxRows <= 0 || stats is null || !bound.IsMultiSource || bound.Sources.Count < 2)
            return null;

        Dictionary<string, PlanTimeLeafKeys>? keys = null;

        foreach (BoundTableSource source in bound.Sources)
        {
            if (!pushdown.ScanFiltersByAlias.TryGetValue(source.Alias, out NodeAst? filter) || filter is null)
                continue;

            long? rows = stats.GetRowCountEstimate(database, source.Table);
            if (rows is null || rows.Value > maxRows)
                continue;

            List<string> keyColumns = SingleColumnUniqueKeys(source.Table);
            if (keyColumns.Count == 0 || !NamesRows(filter, ticket.Parameters))
                continue;

            PlanTimeLeafKeys? leaf = await ReadAsync(database, source, filter, keyColumns, maxRows, ticket, filterer, options).ConfigureAwait(false);
            if (leaf is null)
                continue;

            keys ??= new(StringComparer.Ordinal);
            keys[source.Alias] = leaf;
        }

        return keys;
    }

    /// <summary>
    /// True when the filter has an equality or an <c>IN</c> list on constants: a range alone
    /// (<c>t.production_year > 2010</c>) keeps a share of the table, not named rows, and the
    /// statistics price that share.
    /// </summary>
    private static bool NamesRows(NodeAst filter, Dictionary<string, ColumnValue>? parameters)
    {
        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(filter, parameters);

        if (analysis.InListComparisons.Count > 0)
            return true;

        foreach (AnalyzedComparison comparison in analysis.IndexableComparisons)
        {
            if (comparison.Operator == "=")
                return true;
        }

        return false;
    }

    private static List<string> SingleColumnUniqueKeys(TableDescriptor table)
    {
        List<string> columns = [];

        if (table.Indexes is null)
            return columns;

        foreach (TableIndexSchema index in table.Indexes.Values)
        {
            if (index.Type != IndexType.Unique || index.Columns is not { Length: 1 } || !SchemaElementStateRules.IsReadable(index))
                continue;

            if (!columns.Contains(index.Columns[0], StringComparer.OrdinalIgnoreCase))
                columns.Add(index.Columns[0]);
        }

        return columns;
    }

    private static async Task<PlanTimeLeafKeys?> ReadAsync(
        DatabaseDescriptor database,
        BoundTableSource source,
        NodeAst filter,
        List<string> keyColumns,
        int maxRows,
        QueryTicket ticket,
        QueryFilterer filterer,
        CamusDBOptions options)
    {
        TableDescriptor table = source.Table;
        HLCTimestamp txId = ticket.TxnState.TransactionId;
        RowEncoder.RowDecodeState decodeState = new();
        RowLayout? qualifiedLayout = null;
        long scanLimit = 2L * maxRows;
        long scanned = 0;

        List<ColumnValue>[] values = new List<ColumnValue>[keyColumns.Count];
        for (int i = 0; i < values.Length; i++)
            values[i] = [];

        string[] qualifiedKeys = new string[keyColumns.Count];
        for (int i = 0; i < keyColumns.Count; i++)
            qualifiedKeys[i] = source.Alias + "." + keyColumns[i];

        int kept = 0;

        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(
            ticket.TxnState, cancellationToken: ticket.CancellationToken).ConfigureAwait(false))
        {
            if (data.Length == 0)
                continue;

            // The row count was stale: the table is not the small dimension the statistics said.
            if (++scanned > scanLimit)
                return null;

            QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                table.Schema, txId, rowId, data, options,
                requiredColumns: null, visibilitySchemaVersion: table.Schema.Version, decodeState).ConfigureAwait(false);

            ticket.Probe?.AddRowRead();

            qualifiedLayout ??= QueryRowMerger.BuildQualifiedLayout(row.Layout, source.Alias);
            IReadOnlyDictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRowAsQueryRow(row, qualifiedLayout);

            if (!await filterer.MeetWhereAsync(filter, qualified, ticket, database).ConfigureAwait(false))
                continue;

            if (++kept > MaxKeys)
                return null;

            for (int i = 0; i < keyColumns.Count; i++)
            {
                if (qualified.TryGetValue(qualifiedKeys[i], out ColumnValue? value) && value.Type != ColumnType.Null)
                    values[i].Add(value);
            }
        }

        Dictionary<string, IReadOnlyList<ColumnValue>> byColumn = new(keyColumns.Count, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < keyColumns.Count; i++)
            byColumn[keyColumns[i]] = values[i];

        return new PlanTimeLeafKeys(kept, byColumn);
    }
}
