/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The worker side of a broadcast join. Serves a peer coordinator's probe fragment: it rebuilds
/// the buckets from the shipped build rows, scans its own span of the probe table at the
/// fragment's snapshot, and ships one frame per probe row that matched — the row's raw bytes plus
/// the indices of the build rows it matched.
///
/// <para>The buckets are rebuilt in shipped order, so equal-key rows keep the coordinator's
/// bucket order and the returned indices reproduce the coordinator's merge order exactly. This
/// runs through the join engine rather than the scan path because match semantics must be the
/// local hash probe's: the same key comparer, the same NULL-key exclusion, and the same merged-row
/// ON evaluation through <see cref="QueryRowMerger"/>. The coordinator half is
/// <see cref="BroadcastHashProbeExecutor"/>.</para>
/// </summary>
internal sealed class JoinFragmentProbeExecutor
{
    private readonly JoinExecutionServices services;

    public JoinFragmentProbeExecutor(JoinExecutionServices services)
    {
        this.services = services;
    }

    /// <summary>
    /// Serves a peer coordinator's broadcast-join probe fragment: rebuilds the buckets from
    /// the shipped build rows (each keeping its index into the shipped array), scans the probe
    /// span at the fragment's snapshot, and for each probe row that passes the probe filter
    /// and has at least one candidate whose merged pair satisfies the full ON predicate,
    /// ships one frame — the probe row's raw bytes plus its match indices. Runs through this
    /// class rather than the scan path because match semantics must be exactly the local hash
    /// probe's: same key comparer, same NULL-key exclusion, same merged-row ON evaluation via
    /// <see cref="QueryRowMerger"/>.
    /// </summary>
    internal async IAsyncEnumerable<QueryFragmentRow> ExecuteFragmentJoinProbe(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryFragmentJoinSpec join,
        ObjectIdValue? fromRowId,
        ObjectIdValue? untilRowId,
        int schemaVersion,
        IReadOnlySet<string>? requiredColumns,
        KvTransaction snapshotTx,
        bool wantStats = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Which columns key which side depends on the shipped build side: a right-side build
        // ships bare rows keyed by BuildKeyColumns and probes qualified left rows via
        // ProbeKeyColumns; a left-side build ships qualified rows keyed by ProbeKeyColumns
        // and probes bare right rows via BuildKeyColumns — exactly the coordinator's own
        // key-extraction rules for each shape.
        string[] bucketKeyColumns = join.BuildIsLeft ? join.ProbeKeyColumns : join.BuildKeyColumns;
        string[] probeKeyColumns = join.BuildIsLeft ? join.BuildKeyColumns : join.ProbeKeyColumns;

        // Rebuild the buckets in shipped order so equal-key rows keep the coordinator's
        // bucket order — match indices must reproduce the coordinator's merge order exactly.
        Dictionary<CompositeColumnValue, List<(int Index, Dictionary<string, ColumnValue> Row)>> buckets =
            new(CompositeColumnValueComparer.Instance);
        var bucketLookup = buckets.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();

        // Build and probe each get their own scratch array: the two key-column sets can differ,
        // and a shared buffer would let a probe silently read a stale build key on a length match.
        ColumnValue[] buildKeyScratch = new ColumnValue[bucketKeyColumns.Length];

        for (int i = 0; i < join.BuildRows.Length; i++)
        {
            Dictionary<string, ColumnValue> buildRow = QueryExecutor.ParseCells(join.BuildRows[i]);

            if (!JoinKeyExtractor.TryExtractKeyInto(buildRow, bucketKeyColumns, buildKeyScratch))
                continue;

            if (!bucketLookup.TryGetValue(buildKeyScratch.AsSpan(), out List<(int Index, Dictionary<string, ColumnValue> Row)>? bucket))
            { bucket = []; bucketLookup[buildKeyScratch.AsSpan()] = bucket; }

            bucket.Add((i, buildRow));
        }

        NodeAst onPredicate = NodeAstWireCodec.Deserialize(join.OnPredicateJson);
        NodeAst? probeFilter = join.ProbeFilterJson is null ? null : NodeAstWireCodec.Deserialize(join.ProbeFilterJson);

        QueryTicket ticket = new(
            txnState: snapshotTx,
            databaseName: database.Name,
            tableName: table.Name,
            index: null,
            projection: null,
            where: null,
            filters: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null,
            cancellationToken: cancellationToken);

        RowEncoder.RowDecodeState decodeState = new();
        RowLayout? qualifiedLayout = null;
        RowLayout? joinLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;
        ColumnValue[] probeKeyScratch = new ColumnValue[probeKeyColumns.Length];
        List<int> matches = [];
        long scanned = 0, shipped = 0;

        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(
            snapshotTx,
            cancellationToken: cancellationToken,
            untilRowId: untilRowId,
            fromRowId: fromRowId).ConfigureAwait(false))
        {
            if (data.Length == 0)
                continue;

            scanned++;

            QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                table.Schema,
                snapshotTx.TransactionId,
                rowId,
                data,
                database.Options,
                requiredColumns,
                schemaVersion,
                decodeState).ConfigureAwait(false);

            qualifiedLayout ??= QueryRowMerger.BuildQualifiedLayout(row.Layout, join.ProbeAlias);
            IReadOnlyDictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRowAsQueryRow(row, qualifiedLayout);

            if (probeFilter is not null
                && !await services.Filterer.MeetWhereAsync(probeFilter, qualified, ticket, database).ConfigureAwait(false))
                continue;

            // Same key-source rule as the coordinator: qualified row for a left-side probe,
            // bare row for a right-side one.
            IReadOnlyDictionary<string, ColumnValue> keySource = join.BuildIsLeft ? row : qualified;

            if (!JoinKeyExtractor.TryExtractKeyInto(keySource, probeKeyColumns, probeKeyScratch))
                continue;

            if (!bucketLookup.TryGetValue(probeKeyScratch.AsSpan(), out List<(int Index, Dictionary<string, ColumnValue> Row)>? bucket))
                continue;

            matches.Clear();

            foreach ((int index, Dictionary<string, ColumnValue> buildRow) in bucket)
            {
                QueryRow merged;

                if (join.BuildIsLeft)
                {
                    joinLayout      ??= QueryRowMerger.BuildJoinLayout(buildRow, row, join.BuildAlias);
                    rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(row, join.BuildAlias, joinLayout);
                    merged = QueryRowMerger.MergeRowsAsQueryRow(buildRow, row, joinLayout, rightOrdinalMap);
                }
                else
                {
                    joinLayout      ??= QueryRowMerger.BuildJoinLayout(qualified, buildRow, join.BuildAlias);
                    rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(buildRow, join.BuildAlias, joinLayout);
                    merged = QueryRowMerger.MergeRowsAsQueryRow(qualified, buildRow, joinLayout, rightOrdinalMap);
                }

                if (await services.Filterer.MeetWhereAsync(onPredicate, merged, ticket, database).ConfigureAwait(false))
                    matches.Add(index);
            }

            if (matches.Count == 0)
                continue;

            shipped++;
            yield return new QueryFragmentRow(rowId.ToString(), data.ToArray(), MatchIndices: matches.ToArray());
        }

        if (wantStats)
            yield return new QueryFragmentRow(null, null, null, new QueryFragmentScanStats(scanned, shipped));
    }
}
