
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Statistics;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Implements <c>ANALYZE TABLE</c>: full-scan (or sampled) statistics collection.
///
/// For tables with row count ≤ <see cref="CamusDBConfig.StatsAnalyzeSampleRows"/> the entire
/// table is scanned; for larger tables the first N rows in storage order are used as the
/// sample. v1 uses exact scan-based counting; HyperLogLog/reservoir sampling are deferred.
///
/// One pass builds all statistics simultaneously:
///   • row count
///   • per-column min/max (all indexed columns)
///   • per-column NDV (approximate — exact when within sample limit)
///   • per-column equi-depth histograms
///   • per-composite-index-prefix key-tuple NDV
///   • per-index entry counts
/// </summary>
internal sealed class TableAnalyzer
{
    private readonly StatisticsManager statistics;

    public TableAnalyzer(StatisticsManager statistics)
    {
        this.statistics = statistics;
    }

    internal async Task<QueryResultRow> AnalyzeAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        KvTransaction tx)
    {
        int sampleLimit = CamusDBConfig.StatsAnalyzeSampleRows;
        int bucketCount = CamusDBConfig.StatsHistogramBuckets;
        if (bucketCount < 1) bucketCount = 1;

        // Collect indexed columns and composite index key-column groups.
        List<string> indexedColumns = GetIndexedColumns(table);
        List<(string indexName, string[] keyColumns)> readableIndexes = GetReadableIndexes(table);

        // Per-column accumulators.
        var distinctSets   = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        var minMax         = new Dictionary<string, ColumnMinMax>(StringComparer.Ordinal);
        var valueLists     = new Dictionary<string, List<ScalarBound>>(StringComparer.Ordinal);
        // Per-index entry counts (non-null rows).
        var indexCounts    = new Dictionary<string, long>(StringComparer.Ordinal);
        // Per-composite-key-tuple distinct sets.
        var keyDistinct    = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);

        foreach (string col in indexedColumns)
        {
            distinctSets[col] = new HashSet<string>(StringComparer.Ordinal);
            valueLists[col]   = [];
        }

        foreach ((string indexName, string[] keyCols) in readableIndexes)
        {
            // Initialize a distinct-set for every composite prefix of length 2..N.
            // A WHERE col1=… AND col2=… query needs KeyNdv["col1,col2"] even when the index
            // has a third column — ANALYZE must emit all prefix lengths so the optimizer can look them up.
            for (int len = 2; len <= keyCols.Length; len++)
            {
                string sig = StatisticsManager.KeyTupleSignature(keyCols[..len]);
                keyDistinct.TryAdd(sig, new HashSet<string>(StringComparer.Ordinal));
            }

            indexCounts[indexName] = 0;
        }

        long rowCount   = 0;
        bool isSampled  = false;
        long? limit     = sampleLimit > 0 ? sampleLimit : null;

        // Request one row past the sample limit so the sentinel can set isSampled=true;
        // the sentinel is detected below and not counted toward rowCount.
        long? scanLimit = limit.HasValue ? limit.Value + 1 : null;
        await foreach ((ObjectIdValue rowId, byte[] data) in table.Store.ScanRows(tx, maxRows: scanLimit))
        {
            if (limit.HasValue && rowCount >= limit.Value)
            {
                isSampled = true;
                break;
            }

            Dictionary<string, ColumnValue> row = RowEncoder.Decode(table.Schema, rowId, data);
            rowCount++;

            foreach (string col in indexedColumns)
            {
                if (!row.TryGetValue(col, out ColumnValue? val) || val.Type is ColumnType.Null or ColumnType.Bool)
                    continue;

                ScalarBound bound = ScalarBound.FromColumnValue(val);
                UpdateMinMax(minMax, col, bound);
                distinctSets[col].Add(BoundKey(bound));

                if (IsOrderable(val.Type))
                    valueLists[col].Add(bound);
            }

            // Index entry counts and composite-key NDV.
            foreach ((string indexName, string[] keyCols) in readableIndexes)
            {
                // An index entry exists only when the first key column is non-null.
                if (!row.TryGetValue(keyCols[0], out ColumnValue? firstVal) || firstVal.Type == ColumnType.Null)
                    continue;

                indexCounts[indexName]++;

                // Accumulate distinct tuple keys for every prefix length 2..N,
                // mirroring the initialization above.
                for (int len = 2; len <= keyCols.Length; len++)
                {
                    string sig = StatisticsManager.KeyTupleSignature(keyCols[..len]);
                    string tupleKey = BuildTupleKey(row, keyCols[..len]);
                    keyDistinct[sig].Add(tupleKey);
                }
            }
        }

        // --- Build result dicts ---

        var columnNdv = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach ((string col, HashSet<string> set) in distinctSets)
            columnNdv[col] = set.Count;

        var keyNdv = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach ((string sig, HashSet<string> set) in keyDistinct)
            keyNdv[sig] = set.Count;

        var histograms = new Dictionary<string, ColumnHistogram>(StringComparer.Ordinal);
        foreach ((string col, List<ScalarBound> values) in valueLists)
        {
            if (values.Count == 0) continue;
            histograms[col] = BuildEquiDepthHistogram(values, bucketCount);
        }

        // --- Persist ---

        // Seed all base stats (row count, min/max, index counts) in one call before the first
        // flush so every SetXxxAsync call captures the complete snapshot. The actual Kahuna write
        // happens inside SetHistogramsAsync / SetNdvAsync (each calls FlushAsync internally).
        // Running SeedColumnStats after those flushes would leave min/max and index counts
        // in-memory only and they would never reach Kahuna.
        statistics.SeedColumnStats(database, table, rowCount, minMax, indexCounts);

        // These two calls each load the entry (if evicted) then flush everything to Kahuna,
        // including the RowCount and ColumnStats seeded above.
        await statistics.SetHistogramsAsync(database, table, histograms).ConfigureAwait(false);
        await statistics.SetNdvAsync(database, table, columnNdv, keyNdv.Count > 0 ? keyNdv : null).ConfigureAwait(false);

        string status = isSampled
            ? $"sampled {rowCount} rows (table larger than {CamusDBConfig.StatsAnalyzeSampleRows})"
            : $"analyzed {rowCount} rows";

        return new QueryResultRow(default, new()
        {
            { "table",   new ColumnValue(ColumnType.String, table.Name) },
            { "status",  new ColumnValue(ColumnType.String, status) },
            { "rows",    new ColumnValue(ColumnType.Integer64, rowCount) },
            { "columns", new ColumnValue(ColumnType.Integer64, (long)columnNdv.Count) },
        });
    }

    // --- Private helpers ---

    private static List<string> GetIndexedColumns(TableDescriptor table)
    {
        var cols = new HashSet<string>(StringComparer.Ordinal);
        if (table.Indexes is null) return [];
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            if (!SchemaElementStateRules.IsReadable(kv.Value)) continue;
            if (kv.Value.Columns is { Length: > 0 })
                cols.Add(kv.Value.Columns[0]);
        }
        return [.. cols];
    }

    private static List<(string indexName, string[] keyColumns)> GetReadableIndexes(TableDescriptor table)
    {
        var result = new List<(string, string[])>();
        if (table.Indexes is null) return result;
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            if (!SchemaElementStateRules.IsReadable(kv.Value)) continue;
            if (kv.Value.Columns is { Length: > 0 })
                result.Add((kv.Key, kv.Value.Columns));
        }
        return result;
    }

    private static void UpdateMinMax(Dictionary<string, ColumnMinMax> minMax, string col, ScalarBound bound)
    {
        if (!minMax.TryGetValue(col, out ColumnMinMax? mm))
        {
            minMax[col] = new ColumnMinMax { Min = bound, Max = bound };
            return;
        }
        if (mm.Min is null || bound.CompareTo(mm.Min) < 0) mm.Min = bound;
        if (mm.Max is null || bound.CompareTo(mm.Max) > 0) mm.Max = bound;
    }

    private static string BoundKey(ScalarBound b) => b.Type switch
    {
        ColumnType.Integer64 => b.LongValue.ToString(),
        ColumnType.Float64   => b.FloatValue.ToString("R"),
        ColumnType.Float32   => b.FloatValue.ToString("R"),
        ColumnType.String    => b.StrValue ?? "",
        ColumnType.Id        => b.StrValue ?? "",
        ColumnType.Date      => b.LongValue.ToString(),
        ColumnType.DateTime  => b.LongValue.ToString(),
        _                    => b.Type + ":" + b.LongValue,
    };

    private static bool IsOrderable(ColumnType t) => t is
        ColumnType.Integer64 or ColumnType.Float64 or ColumnType.Float32 or
        ColumnType.String or ColumnType.Id or ColumnType.Date or ColumnType.DateTime;

    private static string BuildTupleKey(Dictionary<string, ColumnValue> row, string[] keyCols)
    {
        var sb = new System.Text.StringBuilder();
        for (int i = 0; i < keyCols.Length; i++)
        {
            if (i > 0) sb.Append('\x1F'); // unit-separator, not in SQL identifiers
            if (row.TryGetValue(keyCols[i], out ColumnValue? v) && v.Type != ColumnType.Null)
                sb.Append(BoundKey(ScalarBound.FromColumnValue(v)));
        }
        return sb.ToString();
    }

    private static ColumnHistogram BuildEquiDepthHistogram(List<ScalarBound> values, int bucketCount)
    {
        values.Sort((a, b) => a.CompareTo(b));

        long total = values.Count;
        int bucketsActual = Math.Min(bucketCount, (int)total);
        int bucketSize = (int)Math.Ceiling((double)total / bucketsActual);

        var buckets = new List<ColumnHistogramBucket>(bucketsActual);

        // Iterate by bucket index so the last bucket always ends at total-1, even when total
        // is not a multiple of bucketSize. The previous index-stepping loop stopped short of
        // the tail, leaving a partial run unrepresented and the last UpperBound below the max.
        for (int k = 0; k < bucketsActual; k++)
        {
            int start = k * bucketSize;
            if (start > (int)total - 1) break;
            int end   = Math.Min((k + 1) * bucketSize - 1, (int)total - 1);

            var distinct = new HashSet<string>(StringComparer.Ordinal);
            for (int j = start; j <= end; j++)
                distinct.Add(BoundKey(values[j]));

            buckets.Add(new ColumnHistogramBucket
            {
                UpperBound       = values[end],
                CumulativeRows   = end + 1,
                DistinctInBucket = distinct.Count,
            });
        }

        // Guarantee the last bucket's CumulativeRows equals TotalRows in case rounding
        // caused end+1 to be one short of total.
        if (buckets.Count > 0)
            buckets[^1].CumulativeRows = total;

        return new ColumnHistogram { Buckets = buckets, TotalRows = total };
    }
}
