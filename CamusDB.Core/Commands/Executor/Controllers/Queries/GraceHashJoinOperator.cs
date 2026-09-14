/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The spill-to-disk hash join, used when a build side does not fit in memory. Both inputs are
/// partitioned to files by a hash of the join key, then each partition pair is joined on its own.
/// Because equal keys hash to the same partition, joining the pairs independently produces
/// exactly the in-memory join's rows.
///
/// <para>A partition whose build half is still too large is re-partitioned with a fresh seed, so
/// keys that collided at one level separate at the next. Keys that are actually equal cannot be
/// separated at any depth, so past a depth limit the pair falls back to a nested-loop join over
/// the two files: many more reads, but constant memory, and it always terminates.</para>
///
/// <para>Every partition file is written into one scope that is disposed in a <c>finally</c>,
/// which covers normal completion, an abandoned enumeration, cancellation, and a fault. Spill
/// files are real files; a dropped scope would leak them.</para>
/// </summary>
internal sealed class GraceHashJoinOperator
{
    private readonly JoinExecutionServices services;

    private readonly IJoinNodeExecutor tree;

    private readonly JoinLeafScanner scanner;

    public GraceHashJoinOperator(JoinExecutionServices services, IJoinNodeExecutor tree, JoinLeafScanner scanner)
    {
        this.services = services;
        this.tree = tree;
        this.scanner = scanner;
    }

    /// <summary>
    /// Qualifies every row produced by <paramref name="inputNode"/> so column names carry the
    /// source alias (e.g. "order_id" → "o.order_id"). Used when the left subtree must be
    /// partitioned before the join phase, where it would normally be qualified inline.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> QualifyStreamAsync(
        PhysicalPlanNode inputNode,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        RowLayout? qualifiedLayout = null;

        await foreach (QueryResultRow row in tree.ExecuteNode(inputNode, plan).WithCancellation(ct).ConfigureAwait(false))
        {
            string alias = JoinAliasMetadata.ResolveLeftAlias(inputNode, row);
            IReadOnlyDictionary<string, ColumnValue> qualified;
            if (row.Row is QueryRow qr)
            {
                qualifiedLayout ??= QueryRowMerger.BuildQualifiedLayout(qr.Layout, alias);
                qualified = QueryRowMerger.QualifyRowAsQueryRow(qr, qualifiedLayout);
            }
            else
            {
                qualified = QueryRowMerger.QualifyRow(row.Row, alias);
            }
            yield return new QueryResultRow(row.RowId, qualified);
        }
    }

    /// <summary>Maximum recursion depth for Grace hash-join repartitioning before forcing an
    /// in-memory load regardless of partition size (last resort for extreme single-key skew).</summary>
    private const int MaxGraceHashDepth = 2;

    /// <summary>
    /// Reads all rows from a spill file lazily via a <see cref="SpillRunReader"/>.
    /// Returns an empty sequence when the file is empty.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> ReadSpillFileAsync(
        string path,
        int maxFrameBytes,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        SpillRunReader? reader = await SpillRunReader.OpenAsync(path, maxFrameBytes, ct: ct).ConfigureAwait(false);
        if (reader is null) yield break;
        await using (reader)
        {
            while (!reader.IsExhausted)
            {
                yield return reader.Current;
                if (!await reader.AdvanceAsync(ct).ConfigureAwait(false)) break;
            }
        }
    }

    /// <summary>
    /// Partitions every row in <paramref name="input"/> into <paramref name="K"/> spill files
    /// using a seed-mixed hash of the join-key columns (see <see cref="PartitionIndex"/>).
    /// Rows with any NULL key column are silently dropped — consistent with SQL inner-join
    /// NULL exclusion. Returns the <paramref name="K"/> file paths in partition order.
    /// </summary>
    private static async Task<string[]> PartitionStreamToFilesAsync(
        SpillScope scope,
        int K,
        int seed,
        IAsyncEnumerable<QueryResultRow> input,
        IReadOnlyList<string> keyColumns,
        CancellationToken ct)
    {
        FileStream[] writers = new FileStream[K];
        string[] paths = new string[K];

        for (int i = 0; i < K; i++)
            paths[i] = scope.OpenWriter(out writers[i]);

        try
        {
            ColumnValue[] keyScratch = new ColumnValue[keyColumns.Count];

            await foreach (QueryResultRow row in input.WithCancellation(ct).ConfigureAwait(false))
            {
                if (!JoinKeyExtractor.TryExtractKeyInto(row.Row, keyColumns, keyScratch)) continue;

                int p = PartitionIndex(keyScratch, K, seed);
                SpillRowCodec.EncodeToStream(writers[p], row);
            }

            for (int i = 0; i < K; i++)
            {
                await writers[i].FlushAsync(ct).ConfigureAwait(false);
                writers[i].Close();
                writers[i] = null!;
            }

            return paths;
        }
        catch
        {
            for (int i = 0; i < K; i++)
                try { writers[i]?.Close(); } catch { }
            throw;
        }
    }

    /// <summary>
    /// Maps a composite join key to a partition bucket in [0, <paramref name="partitionCount"/>).
    /// <paramref name="seed"/> lets recursive calls use a statistically independent bucketing so
    /// that keys colliding at depth N are redistributed at depth N+1.
    ///
    /// The seed is folded in with a multiplicative mix and a murmur-style finalizer before the
    /// modulo so that each level's bucketing is independent across all bits — not just the high
    /// bits as XOR-then-mod would be for power-of-two bucket counts. This means that two keys
    /// sharing a bucket at depth 0 will land in different buckets at depth 1 (unless they are
    /// actually equal, in which case they are inseparable and the depth-limit load-all backstop
    /// in <see cref="JoinPartitionAsync"/> is the correct resolution).
    /// </summary>
    internal static int PartitionIndex(ColumnValue[] keyValues, int partitionCount, int seed)
    {
        uint h = (uint)CompositeColumnValueComparer.Instance.GetHashCode(keyValues.AsSpan());
        h ^= (uint)seed * 0x9E3779B1u;   // fold level seed in with a multiplier
        h *= 0x85EBCA6Bu; h ^= h >> 13;  // murmur-style bit-mixing finalizer
        h *= 0xC2B2AE35u; h ^= h >> 16;
        return (int)(h % (uint)partitionCount);
    }

    /// <summary>
    /// Grace/hybrid hash join entry point. Partitions both build and probe sides into
    /// <see cref="CamusDBOptions.SpillMergeFanIn"/> spill files each (keyed by join-key hash),
    /// then joins each partition pair. Partitions whose build side exceeds
    /// <see cref="CamusDBOptions.SpillEffectiveThreshold"/> are recursively re-partitioned up to
    /// <see cref="MaxGraceHashDepth"/> levels deep; beyond that depth the entire partition is
    /// loaded into the hash table regardless (correctness backstop for extreme single-key skew).
    ///
    /// <para>
    /// All temporary partition files are written into a <see cref="SpillScope"/> that is
    /// disposed (and all files deleted) in the <c>finally</c> block, covering normal
    /// completion, cancellation, and exception paths.
    /// </para>
    ///
    /// <para>
    /// Side normalization:
    /// <list type="bullet">
    ///   <item><see cref="HashJoinBuildSide.Right"/>: build = unqualified right scan; probe = qualified left scan.</item>
    ///   <item><see cref="HashJoinBuildSide.Left"/>: build = qualified left scan; probe = unqualified right scan.</item>
    /// </list>
    /// In both cases the probe-to-build match at emit time is <c>MergeRows(leftQualified, rightUnqualified, alias)</c>.
    /// </para>
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> GraceHashJoinAsync(
        HashJoinNode joinNode,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        int K = plan.Database.Options.SpillMergeFanIn;

        if (services.Statistics is not null)
            services.Statistics.HashJoinGracePathCount++;

        IAsyncEnumerable<QueryResultRow> buildStream;
        IReadOnlyList<string> buildKeyColumns;
        IAsyncEnumerable<QueryResultRow> probeStream;
        IReadOnlyList<string> probeKeyColumns;

        if (joinNode.BuildSide == HashJoinBuildSide.Right)
        {
            buildStream     = scanner.ScanJoinRightSource(joinNode.BuildSource, joinNode.BuildExecutionFilter, plan);
            buildKeyColumns = joinNode.BuildKeyColumns;
            probeStream     = QualifyStreamAsync(joinNode.Input!, plan, ct);
            probeKeyColumns = joinNode.ProbeKeyColumns;
        }
        else
        {
            buildStream     = QualifyStreamAsync(joinNode.Input!, plan, ct);
            buildKeyColumns = joinNode.ProbeKeyColumns;
            probeStream     = scanner.ScanJoinRightSource(joinNode.BuildSource, joinNode.BuildExecutionFilter, plan);
            probeKeyColumns = joinNode.BuildKeyColumns;
        }

        plan.Ticket.Probe?.NoteSpill();
        SpillScope scope = SpillFileManager.CreateScope(QueryExecutionContext.For(plan.Database, plan.Ticket).SpillDirectory);

        try
        {
            string[] buildFiles = await PartitionStreamToFilesAsync(scope, K, seed: 0, buildStream, buildKeyColumns, ct).ConfigureAwait(false);
            string[] probeFiles = await PartitionStreamToFilesAsync(scope, K, seed: 0, probeStream, probeKeyColumns, ct).ConfigureAwait(false);

            for (int p = 0; p < K; p++)
            {
                await foreach (QueryResultRow row in JoinPartitionAsync(
                    joinNode, plan, scope, buildFiles[p], probeFiles[p],
                    buildKeyColumns, probeKeyColumns, seed: 0, depth: 0, ct).ConfigureAwait(false))
                    yield return row;
            }
        }
        finally
        {
            await scope.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Joins a single partition pair by loading the build partition into an in-memory hash table
    /// and streaming the probe partition against it.
    ///
    /// <para>
    /// When the build partition count exceeds <see cref="CamusDBOptions.SpillEffectiveThreshold"/>
    /// and the recursion depth allows it, both files are re-read with a perturbed hash seed to
    /// split into sub-partitions. When the depth limit is reached (extreme skew where a single
    /// key dominates and cannot be split further), the method falls back to a nested-loop join
    /// over the partition files — O(|probe|×|build|) reads but O(1) memory — instead of
    /// materialising the entire skewed build partition into a hash table.
    /// </para>
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> JoinPartitionAsync(
        HashJoinNode joinNode,
        QueryPlan plan,
        SpillScope scope,
        string buildFile,
        string probeFile,
        IReadOnlyList<string> buildKeyColumns,
        IReadOnlyList<string> probeKeyColumns,
        int seed,
        int depth,
        [EnumeratorCancellation] CancellationToken ct)
    {
        int threshold = plan.Database.Options.SpillEffectiveThreshold;
        int K = plan.Database.Options.SpillMergeFanIn;
        QueryTicket ticket = plan.Ticket;
        string rightAlias = joinNode.BuildSource.Alias;

        // Load build partition into an in-memory hash table, stopping early whenever
        // the threshold is exceeded regardless of depth — the depth determines the
        // overflow strategy, not whether we detect the overflow.
        Dictionary<CompositeColumnValue, List<IReadOnlyDictionary<string, ColumnValue>>> hashTable =
            new(CompositeColumnValueComparer.Instance);
        var hashLookup = hashTable.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();
        // Separate scratch arrays for the build and probe loops of this partition: reusing one
        // buffer across the two phases would risk a probe reading leftover build-key values.
        ColumnValue[] buildKeyScratch = new ColumnValue[buildKeyColumns.Count];
        int buildCount = 0;
        bool overflow = false;

        await using IAsyncEnumerator<QueryResultRow> buildEnum =
            ReadSpillFileAsync(buildFile, services.Options.SpillMaxFrameBytes, ct).GetAsyncEnumerator(ct);

        while (await buildEnum.MoveNextAsync().ConfigureAwait(false))
        {
            QueryResultRow buildRow = buildEnum.Current;
            // NULL-key rows are skipped before the threshold check, exactly as before: they
            // never count toward the overflow decision.
            if (!JoinKeyExtractor.TryExtractKeyInto(buildRow.Row, buildKeyColumns, buildKeyScratch)) continue;

            if (buildCount >= threshold)
            {
                overflow = true;
                break;
            }

            if (!hashLookup.TryGetValue(buildKeyScratch.AsSpan(), out List<IReadOnlyDictionary<string, ColumnValue>>? bucket))
            { bucket = []; hashLookup[buildKeyScratch.AsSpan()] = bucket; }
            bucket.Add(buildRow.Row);
            buildCount++;
        }

        if (overflow)
        {
            if (depth < MaxGraceHashDepth)
            {
                // Re-read both files with a new hash seed to distribute rows across K sub-partitions.
                int newSeed = seed + 1;
                string[] subBuildFiles = await PartitionStreamToFilesAsync(
                    scope, K, newSeed, ReadSpillFileAsync(buildFile, services.Options.SpillMaxFrameBytes, ct), buildKeyColumns, ct).ConfigureAwait(false);
                string[] subProbeFiles = await PartitionStreamToFilesAsync(
                    scope, K, newSeed, ReadSpillFileAsync(probeFile, services.Options.SpillMaxFrameBytes, ct), probeKeyColumns, ct).ConfigureAwait(false);

                for (int p = 0; p < K; p++)
                {
                    await foreach (QueryResultRow row in JoinPartitionAsync(
                        joinNode, plan, scope, subBuildFiles[p], subProbeFiles[p],
                        buildKeyColumns, probeKeyColumns, newSeed, depth + 1, ct).ConfigureAwait(false))
                        yield return row;
                }
            }
            else
            {
                // Recursion depth limit reached and the partition still exceeds the threshold
                // (extreme single-key skew — the key cannot be split further). Fall back to a
                // nested-loop join over the partition files: O(|probe|×|build|) reads, O(1) memory.
                if (services.Statistics is not null)
                    services.Statistics.HashJoinNljPartitionFallbackCount++;

                await foreach (QueryResultRow row in NestedLoopPartitionJoinAsync(
                    joinNode, buildFile, probeFile, buildKeyColumns, probeKeyColumns, ticket, plan, ct)
                    .ConfigureAwait(false))
                    yield return row;
            }
            yield break;
        }

        if (hashTable.Count == 0) yield break;

        // Probe phase: stream probe partition against the loaded hash table.
        RowLayout? joinLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;
        ColumnValue[] probeKeyScratch = new ColumnValue[probeKeyColumns.Count];
        await foreach (QueryResultRow probeRow in ReadSpillFileAsync(probeFile, services.Options.SpillMaxFrameBytes, ct).ConfigureAwait(false))
        {
            if (!JoinKeyExtractor.TryExtractKeyInto(probeRow.Row, probeKeyColumns, probeKeyScratch)) continue;

            if (!hashLookup.TryGetValue(probeKeyScratch.AsSpan(), out List<IReadOnlyDictionary<string, ColumnValue>>? bucket)) continue;

            foreach (IReadOnlyDictionary<string, ColumnValue> buildRow in bucket)
            {
                // Probe rows are the qualified left-side; build rows are the unqualified right-side
                // (and vice-versa for BuildSide.Left) — see GraceHashJoinAsync side normalization.
                IReadOnlyDictionary<string, ColumnValue> leftQ  = joinNode.BuildSide == HashJoinBuildSide.Right ? probeRow.Row : buildRow;
                IReadOnlyDictionary<string, ColumnValue> rightR = joinNode.BuildSide == HashJoinBuildSide.Right ? buildRow : probeRow.Row;
                joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftQ, rightR, rightAlias);
                rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightR, rightAlias, joinLayout);
                QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(leftQ, rightR, joinLayout, rightOrdinalMap);

                if (!await services.Filterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                    continue;

                yield return new QueryResultRow(default(ObjectIdValue), merged);
            }
        }
    }

    /// <summary>
    /// Nested-loop join over two partition files, used as the bounded-memory backstop when
    /// the recursion depth limit is reached and a skewed partition still exceeds the threshold.
    /// For each probe row, the build file is streamed from the beginning and matching rows are
    /// emitted. Memory is O(1): neither file is materialised.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> NestedLoopPartitionJoinAsync(
        HashJoinNode joinNode,
        string buildFile,
        string probeFile,
        IReadOnlyList<string> buildKeyColumns,
        IReadOnlyList<string> probeKeyColumns,
        QueryTicket ticket,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct)
    {
        string rightAlias = joinNode.BuildSource.Alias;
        RowLayout? joinLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;

        // The outer scratch must stay stable while the inner loop compares against it,
        // so the two loops each own a buffer.
        ColumnValue[] probeKeyScratch = new ColumnValue[probeKeyColumns.Count];
        ColumnValue[] buildKeyScratch = new ColumnValue[buildKeyColumns.Count];

        await foreach (QueryResultRow probeRow in ReadSpillFileAsync(probeFile, services.Options.SpillMaxFrameBytes, ct).ConfigureAwait(false))
        {
            if (!JoinKeyExtractor.TryExtractKeyInto(probeRow.Row, probeKeyColumns, probeKeyScratch)) continue;

            await foreach (QueryResultRow buildRow in ReadSpillFileAsync(buildFile, services.Options.SpillMaxFrameBytes, ct).ConfigureAwait(false))
            {
                if (!JoinKeyExtractor.TryExtractKeyInto(buildRow.Row, buildKeyColumns, buildKeyScratch)) continue;

                if (!CompositeColumnValueComparer.Instance.Equals(probeKeyScratch.AsSpan(), buildKeyScratch.AsSpan()))
                    continue;

                IReadOnlyDictionary<string, ColumnValue> leftQ  = joinNode.BuildSide == HashJoinBuildSide.Right ? probeRow.Row : buildRow.Row;
                IReadOnlyDictionary<string, ColumnValue> rightR = joinNode.BuildSide == HashJoinBuildSide.Right ? buildRow.Row : probeRow.Row;
                joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftQ, rightR, rightAlias);
                rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightR, rightAlias, joinLayout);
                QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(leftQ, rightR, joinLayout, rightOrdinalMap);

                if (!await services.Filterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                    continue;

                yield return new QueryResultRow(default(ObjectIdValue), merged);
            }
        }
    }
}
