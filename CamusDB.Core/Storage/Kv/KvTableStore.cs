/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Per-table data access layer built on top of <see cref="IKahuna"/>. This type is the entry point
/// callers use; the work is split across focused collaborators it composes:
///
/// <list type="bullet">
///   <item><see cref="KvKeyBuilder"/> — composes every key and holds the per-index metadata.</item>
///   <item><see cref="KvRangeLockManager"/> — range, point and exclusive key locks.</item>
///   <item><see cref="KvBranchReader"/> — lineage-aware probes and raw iterators.</item>
///   <item><see cref="KvRowAccessor"/> / <see cref="KvIndexAccessor"/> — primary rows and secondary indexes.</item>
///   <item><see cref="KvBatchWriter"/> — the set-based insert/update/delete and bulk purges.</item>
///   <item><see cref="KahunaRetryPolicy"/> and <see cref="KvConflictMessageBuilder"/> — retry rules and conflict diagnostics.</item>
/// </list>
///
/// <para>Key layout (all keys share the leading <c>{dbId}:{tableId}</c> segment so databases are
/// isolated in the shared keyspace and every key of one table routes together):</para>
///
/// <code>
///   Primary rows:      {dbId}:{tableId}:r/{rowIdHex24}                         -> serialized row bytes
///   Unique index:      {dbId}:{tableId}:i:{indexId}/{encodedKey}               -> rowIdHex24 (UTF-8)
///   Non-unique index:  {dbId}:{tableId}:i:{indexId}/{encodedKey}{rowIdHex24}   -> rowIdHex24 (UTF-8)
///     (rowId appended without separator; it is always exactly 24 lowercase hex chars)
/// </code>
///
/// <para><b>Table id format:</b> newly created tables get a <em>short base-62</em> table id allocated
/// from a per-store persistent monotonic sequence (<c>_system/tableseq</c>) via
/// <see cref="CamusDB.Core.CommandsExecutor.Controllers.DatabaseRegistry.AllocateTableIdAsync"/>.
/// The id is typically 1–4 characters (e.g. <c>"1"</c>, <c>"A0"</c>) and contains none of the key
/// separators (<c>/</c>, <c>:</c>, <c>~</c>). Tables created before this change keep their original
/// 24-character lowercase-hex ObjectId (e.g. <c>"6849f3a1c2e7d50b4f8a91d3"</c>); the two forms
/// coexist safely because their lengths and character sets never overlap.</para>
///
/// <para><b>Hash-routing constraint (the default mode).</b> Under hash routing a key space must hash
/// to exactly one partition whether it is reached by a scan or by a point write, or a scan would miss
/// rows a write placed elsewhere:
/// <c>LocateAndScanRange</c> routes via <c>SimpleHash(prefix)</c> while individual TrySet/Delete route
/// via <c>InversePrefixedStaticHash(key, '/') = SimpleHash(key[..lastSlash])</c>.
/// For rows: bucket prefix <c>{dbId}:{tableId}:r</c> hashes the same as every row key's prefix.
/// For indexes: bucket prefix <c>{dbId}:{tableId}:i:{indexId}</c> matches writes whose key is
/// <c>{dbId}:{tableId}:i:{indexId}/{...}</c> (the last slash sits before the suffix). Non-unique keys
/// append the rowId with no extra slash, so the hash agrees for both unique and non-unique.
/// Do not add slashes to a key or a bucket prefix without re-deriving both sides of that equality.</para>
///
/// <para><b>This is a property of hash routing only, not a layout invariant.</b> When
/// <see cref="CamusDBOptions.KeyRangeShardingEnabled"/> is on, this store's row space — and every
/// eligible index space — is registered for key-range routing instead, and Kahuna may split a
/// space into child ranges owned by different partitions, so one table's keys are no longer
/// confined to a single partition. Correctness there comes from the range map rather than from the
/// hash agreement above: reads resolve every intersecting range descriptor and merge the results in
/// key order, and writes that arrive while a range boundary is moving are refused with
/// <c>MustRetry</c> and retried by <see cref="KahunaRetryPolicy"/>. Nothing in the key layout changes.</para>
///
/// <para>All write methods take a <see cref="KvTransaction"/> so they can accumulate acquired locks
/// and modified keys for the 2-phase commit.</para>
/// </summary>
public sealed partial class KvTableStore
{
    private readonly IKahuna kahuna;
    private readonly ILogger logger;

    private readonly KvKeyBuilder keys;
    private readonly KvConflictMessageBuilder messages;
    private readonly KahunaRetryPolicy retry;
    private readonly KvRangeLockManager locks;
    private readonly KvBranchReader branch;
    private readonly KvRowAccessor rows;
    private readonly KvIndexAccessor indexes;
    private readonly KvBatchWriter batch;

    // Branch lineage stores, in nearest-parent-first order. Retained (rather than only their key
    // builders and readers) because RegisterIndexDirections has to propagate into each ancestor.
    // Empty for root databases.
    private readonly (KvTableStore store, HLCTimestamp forkTimestamp)[] ancestorStores;

    /// <summary>
    /// Creates a table store for the given <paramref name="dbId"/> and <paramref name="tableId"/>.
    /// Pass <paramref name="ancestorStores"/> (nearest parent first) when the database is a branch;
    /// read methods will walk the lineage on a miss so inherited rows and index entries are visible
    /// without having been physically copied into the branch namespace.
    /// <paramref name="dbName"/> and <paramref name="tableName"/> are carried purely for
    /// diagnostics (lock-conflict and deadline messages name the object a user recognizes);
    /// they are never part of a KV key, so an empty value only degrades an error message.
    /// </summary>
    public KvTableStore(
        IKahuna kahuna,
        CamusDBOptions options,
        string dbId,
        string tableId,
        string tableName = "",
        ILogger<ICamusDB>? logger = null,
        (KvTableStore store, HLCTimestamp forkTimestamp)[]? ancestorStores = null,
        string dbName = "")
    {
        ArgumentNullException.ThrowIfNull(kahuna);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrEmpty(dbId);
        ArgumentException.ThrowIfNullOrEmpty(tableId);

        this.kahuna = kahuna;
        this.logger = logger ?? NullLogger<ICamusDB>.Instance;
        this.ancestorStores = ancestorStores ?? [];

        keys = new KvKeyBuilder(dbId, dbName, tableId, tableName);
        messages = new KvConflictMessageBuilder(keys, options);
        retry = new KahunaRetryPolicy(messages, options);
        locks = new KvRangeLockManager(kahuna, this.logger, keys, messages, retry, options);

        BranchLevel[] levels = new BranchLevel[this.ancestorStores.Length];
        for (int i = 0; i < this.ancestorStores.Length; i++)
        {
            (KvTableStore ancestorStore, HLCTimestamp forkTimestamp) = this.ancestorStores[i];
            levels[i] = new BranchLevel(ancestorStore.keys, ancestorStore.branch, forkTimestamp);
        }

        branch = new KvBranchReader(kahuna, keys, levels);
        rows = new KvRowAccessor(kahuna, keys, locks, branch, retry);
        indexes = new KvIndexAccessor(kahuna, keys, locks, branch, retry);
        batch = new KvBatchWriter(kahuna, this.logger, keys, branch, retry, messages, options);

        if (this.ancestorStores.Length >= BranchMetrics.LineageWarningThreshold)
        {
            BranchMetrics.RecordDeepLineageWarning();
            this.logger.LogWarning(
                "Table '{Table}' opened on a branch with lineage depth {Depth}. " +
                "Point reads probe every ancestor level on a miss and scans open one iterator per level. " +
                "Consider compacting the branch chain to reduce read amplification.",
                tableName, this.ancestorStores.Length);
        }
    }

    /// <summary>
    /// Swaps in a newly published configuration snapshot, forwarding it to every collaborator that
    /// reads configuration. Reference assignment is atomic and the record itself stays immutable;
    /// readers pin the field once at the top of an operation, so an in-flight operation keeps the
    /// snapshot it started with and a change takes effect at the next operation boundary.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next)
    {
        messages.ApplyOptions(next);
        retry.ApplyOptions(next);
        locks.ApplyOptions(next);
        batch.ApplyOptions(next);
    }

    /// <summary>Key composition and per-index metadata for this table. Read by a descendant's lineage.</summary>
    internal KvKeyBuilder Keys => keys;

    /// <summary>Lineage-aware raw read layer for this table. Read by a descendant's lineage.</summary>
    internal KvBranchReader BranchReader => branch;

    /// <summary>
    /// The Kahuna key space for this table's rows (<c>{dbId}:{tableId}:r</c>) — the prefix before the last
    /// <c>'/'</c> of every row key. This is the exact string to pass to
    /// <see cref="IKahuna.RegisterKeyRange"/> when opting the row space into key-range routing.
    /// </summary>
    public string RowKeySpace => keys.RowBucketPrefix;

    /// <summary>
    /// The Kahuna key space for a secondary index (<c>{dbId}:{tableId}:i:{indexId}</c>). Pass to
    /// <see cref="IKahuna.RegisterKeyRange"/> when opting an index into key-range routing. All
    /// column types are order-safe for range routing (String included, via its ordered ASCII encoding).
    /// </summary>
    public string IndexKeySpace(string indexId) => keys.BuildIndexBucketPrefix(indexId);

    /// <summary>
    /// Returns the full KV key for the given row: <c>{dbId}:{tableId}:r/{rowIdHex24}</c>.
    /// Used by the dependency collector to record per-row point dependencies without exposing
    /// the internal key-prefix fields.
    /// </summary>
    public string RowPointKey(ObjectIdValue rowId) => keys.BuildRowKey(rowId);

    /// <summary>
    /// Exposes the underlying <see cref="IKahuna"/> instance for use by the strict-validation
    /// path, which needs direct key probes with <c>LastModified</c> timestamps. Not for general
    /// DML/DDL use — those must go through <see cref="KvTransaction"/> to track lock and key sets.
    /// </summary>
    internal IKahuna Kahuna => kahuna;

    /// <summary>
    /// Number of ancestor levels this store is configured to walk on a read miss.
    /// Zero for root databases (no ancestry). This is the per-read amplification factor:
    /// a point read may probe up to <c>LineageDepth</c> extra levels, and a range scan
    /// opens <c>LineageDepth</c> extra iterators. Exposed for observability and tests;
    /// see <see cref="BranchMetrics"/> for process-wide counters.
    /// </summary>
    public int LineageDepth => branch.Depth;

    // -----------------------------------------------------------------------
    // Index registration (table-open time)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Marks <paramref name="indexId"/> as key-range routed on this node. Called by
    /// <c>TableOpener</c> after successfully registering the index space. Once marked,
    /// <see cref="AcquireIndexRangeLockAsync"/> uses a Kahuna range lock instead of a prefix lock.
    /// </summary>
    public void MarkIndexAsRanged(string indexId) => locks.MarkIndexAsRanged(indexId);

    /// <summary>
    /// Registers the human-readable display name for an index KvId so that duplicate-key
    /// errors show the mutable index name (e.g., <c>robots.name_idx</c>) rather than the
    /// immutable KvId stored in KV keys. Called by <c>TableOpener</c> for every index entry
    /// when loading a table, before any DML can reference the index.
    /// </summary>
    public void RegisterIndexName(string indexId, string displayName) => keys.RegisterIndexName(indexId, displayName);

    /// <summary>
    /// Registers the per-column sort directions for an index KvId so that key encoding and index
    /// scans invert the ordinal order of descending columns. A null or all-ascending vector keeps the
    /// encoder on its ascending fast path, leaving every existing index byte-identical. Called by
    /// <c>TableOpener</c> for each loaded index and by the index-add path, before any DML can
    /// reference the index.
    ///
    /// <para>The registration propagates into the branch lineage: an ancestor namespace stores this
    /// index with the same descending encoding, so a lineage lookup must encode identically.</para>
    /// </summary>
    public void RegisterIndexDirections(string indexId, OrderType[]? directions)
    {
        keys.RegisterIndexDirections(indexId, directions);

        foreach ((KvTableStore ancestorStore, HLCTimestamp _) in ancestorStores)
            ancestorStore.RegisterIndexDirections(indexId, directions);
    }

    // -----------------------------------------------------------------------
    // Range (prefix) locking — opt-in serializable scans
    // -----------------------------------------------------------------------

    /// <inheritdoc cref="KvRangeLockManager.AcquireRowRangeLockAsync"/>
    public Task AcquireRowRangeLockAsync(KvTransaction tx, bool exclusive = false, CancellationToken cancellationToken = default)
        => locks.AcquireRowRangeLockAsync(tx, exclusive, cancellationToken);

    /// <inheritdoc cref="KvRangeLockManager.AcquireExclusiveRowSpaceFenceAsync"/>
    public Task AcquireExclusiveRowSpaceFenceAsync(KvTransaction tx, CancellationToken cancellationToken = default)
        => locks.AcquireExclusiveRowSpaceFenceAsync(tx, cancellationToken);

    /// <inheritdoc cref="KvRangeLockManager.AcquireIndexRangeLockAsync"/>
    public Task AcquireIndexRangeLockAsync(KvTransaction tx, string indexId, bool exclusive = false, CancellationToken cancellationToken = default)
        => locks.AcquireIndexRangeLockAsync(tx, indexId, exclusive, cancellationToken);

    /// <inheritdoc cref="KvRangeLockManager.AcquireBoundedIndexRangeLockAsync"/>
    public Task AcquireBoundedIndexRangeLockAsync(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue? fromBound, bool fromInclusive,
        CompositeColumnValue? toBound,   bool toInclusive,
        bool unique,
        bool exclusive = false,
        CancellationToken cancellationToken = default,
        int keyColumnCount = 0)
        => locks.AcquireBoundedIndexRangeLockAsync(tx, indexId, fromBound, fromInclusive, toBound, toInclusive, unique, exclusive, cancellationToken, keyColumnCount);

    // -----------------------------------------------------------------------
    // Primary row operations
    // -----------------------------------------------------------------------

    /// <inheritdoc cref="KvRowAccessor.GetRow"/>
    public Task<ReadOnlyMemory<byte>?> GetRow(KvTransaction tx, ObjectIdValue rowId, CancellationToken cancellationToken = default)
        => rows.GetRow(tx, rowId, cancellationToken);

    /// <inheritdoc cref="KvRowAccessor.GetRowsBatch"/>
    public Task<ReadOnlyMemory<byte>?[]> GetRowsBatch(KvTransaction tx, IReadOnlyList<ObjectIdValue> rowIds, CancellationToken cancellationToken = default)
        => rows.GetRowsBatch(tx, rowIds, cancellationToken);

    /// <inheritdoc cref="KvRowAccessor.GetRowsBatchLockedForMutation"/>
    public Task<ReadOnlyMemory<byte>?[]> GetRowsBatchLockedForMutation(KvTransaction tx, IReadOnlyList<ObjectIdValue> rowIds, CancellationToken cancellationToken = default)
        => rows.GetRowsBatchLockedForMutation(tx, rowIds, cancellationToken);

    /// <inheritdoc cref="KvRowAccessor.ScanRows"/>
    public IAsyncEnumerable<(ObjectIdValue rowId, ReadOnlyMemory<byte> data)> ScanRows(
        KvTransaction tx,
        long? maxRows = null,
        ObjectIdValue? afterRowId = null,
        CancellationToken cancellationToken = default,
        ObjectIdValue? untilRowId = null,
        ObjectIdValue? fromRowId = null)
        => rows.ScanRows(tx, maxRows, afterRowId, cancellationToken, untilRowId, fromRowId);

    /// <summary>
    /// Inserts a new row. Acquires a pessimistic exclusive lock then writes the key.
    /// Throws <see cref="CamusDBException"/> if the lock or set fails.
    /// </summary>
    public Task InsertRow(KvTransaction tx, ObjectIdValue rowId, byte[] data, CancellationToken cancellationToken = default)
        => rows.WriteRow(tx, rowId, data, cancellationToken);

    /// <summary>
    /// Updates an existing row. Same mechanics as insert — the KV store overwrites the value.
    /// </summary>
    public Task UpdateRow(KvTransaction tx, ObjectIdValue rowId, byte[] data, CancellationToken cancellationToken = default)
        => rows.WriteRow(tx, rowId, data, cancellationToken);

    /// <inheritdoc cref="KvRowAccessor.DeleteRow"/>
    public Task DeleteRow(KvTransaction tx, ObjectIdValue rowId, CancellationToken cancellationToken = default)
        => rows.DeleteRow(tx, rowId, cancellationToken);

    // -----------------------------------------------------------------------
    // Secondary index operations
    // -----------------------------------------------------------------------

    /// <inheritdoc cref="KvIndexAccessor.LookupUnique"/>
    public Task<ObjectIdValue?> LookupUnique(KvTransaction tx, string indexId, CompositeColumnValue key, CancellationToken cancellationToken = default)
        => indexes.LookupUnique(tx, indexId, key, cancellationToken);

    /// <inheritdoc cref="KvIndexAccessor.LookupUniqueUntracked"/>
    internal Task<ObjectIdValue?> LookupUniqueUntracked(string indexId, CompositeColumnValue key, CancellationToken cancellationToken = default)
        => indexes.LookupUniqueUntracked(indexId, key, cancellationToken);

    /// <inheritdoc cref="KvIndexAccessor.LookupUniqueCovering"/>
    public Task<(ObjectIdValue rowId, ReadOnlyMemory<byte> includeTuple)?> LookupUniqueCovering(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue key,
        CancellationToken cancellationToken = default)
        => indexes.LookupUniqueCovering(tx, indexId, key, cancellationToken);

    /// <inheritdoc cref="KvIndexAccessor.ScanIndex"/>
    public IAsyncEnumerable<(CompositeColumnValue key, ObjectIdValue rowId, ReadOnlyMemory<byte> includeTuple)> ScanIndex(
        KvTransaction tx,
        string indexId,
        ColumnType[] keyTypes,
        CompositeColumnValue? from,
        CompositeColumnValue? to,
        bool unique,
        bool fromInclusive = true,
        bool toInclusive = true,
        long? maxRows = null,
        CancellationToken cancellationToken = default)
        => indexes.ScanIndex(tx, indexId, keyTypes, from, to, unique, fromInclusive, toInclusive, maxRows, cancellationToken);

    /// <inheritdoc cref="KvIndexAccessor.PutIndexEntry"/>
    public Task PutIndexEntry(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue key,
        ObjectIdValue rowId,
        bool unique,
        bool backfillMode = false,
        byte[]? includeTuple = null,
        CancellationToken cancellationToken = default)
        => indexes.PutIndexEntry(tx, indexId, key, rowId, unique, backfillMode, includeTuple, cancellationToken);

    /// <inheritdoc cref="KvIndexAccessor.DeleteIndexEntry"/>
    public Task DeleteIndexEntry(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue key,
        ObjectIdValue rowId,
        bool unique,
        CancellationToken cancellationToken = default)
        => indexes.DeleteIndexEntry(tx, indexId, key, rowId, unique, cancellationToken);

    // -----------------------------------------------------------------------
    // Batched write paths
    // -----------------------------------------------------------------------

    /// <inheritdoc cref="KvBatchWriter.WriteRowsBatch"/>
    public Task WriteRowsBatch(KvTransaction tx, IReadOnlyList<RowWrite> rows, CancellationToken cancellationToken = default)
        => batch.WriteRowsBatch(tx, rows, cancellationToken);

    /// <inheritdoc cref="KvBatchWriter.UpdateRowsBatch"/>
    public Task UpdateRowsBatch(KvTransaction tx, IReadOnlyList<RowUpdate> rows, CancellationToken cancellationToken = default)
        => batch.UpdateRowsBatch(tx, rows, cancellationToken);

    /// <inheritdoc cref="KvBatchWriter.DeleteRowsBatch"/>
    public Task DeleteRowsBatch(KvTransaction tx, IReadOnlyList<RowDelete> rows, CancellationToken cancellationToken = default)
        => batch.DeleteRowsBatch(tx, rows, cancellationToken);

    /// <inheritdoc cref="KvBatchWriter.DropIndexEntries"/>
    public Task<int> DropIndexEntries(KvTransaction tx, string indexName, CancellationToken cancellationToken = default)
        => batch.DropIndexEntries(tx, indexName, cancellationToken);

    /// <inheritdoc cref="KvBatchWriter.PurgeLocalRowOverlayAsync"/>
    public Task<int> PurgeLocalRowOverlayAsync(KvTransaction tx, CancellationToken cancellationToken = default)
        => batch.PurgeLocalRowOverlayAsync(tx, cancellationToken);

    // -----------------------------------------------------------------------
    // Test-only seams
    // -----------------------------------------------------------------------

    /// <summary>
    /// Test-only: overwrites the row entry for <paramref name="rowId"/> with a
    /// <see cref="BranchKvKind.Tombstone"/> envelope, mirroring the locked write path of a normal row
    /// write. Useful for storage-layer tests that need to inject a tombstone directly without going
    /// through a DELETE DML statement.
    /// </summary>
    internal async Task WriteRowTombstoneForTesting(KvTransaction tx, ObjectIdValue rowId, CancellationToken cancellationToken = default)
    {
        tx.ReserveMutations(1);
        string key = keys.BuildRowKey(rowId);
        await locks.AcquireExclusiveKeyLockAsync(tx, key, cancellationToken).ConfigureAwait(false);
        await SetTombstoneForTesting(tx, key, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Test-only: overwrites the unique-index entry for <paramref name="key"/> with a
    /// <see cref="BranchKvKind.Tombstone"/> envelope. See <see cref="WriteRowTombstoneForTesting"/>.
    /// </summary>
    internal async Task WriteUniqueIndexTombstoneForTesting(KvTransaction tx, string indexId, CompositeColumnValue key, CancellationToken cancellationToken = default)
    {
        tx.ReserveMutations(1);
        string kvKey = keys.BuildUniqueIndexKey(indexId, key);
        await locks.AcquireExclusiveKeyLockAsync(tx, kvKey, cancellationToken).ConfigureAwait(false);
        await SetTombstoneForTesting(tx, kvKey, cancellationToken).ConfigureAwait(false);
    }

    private async Task SetTombstoneForTesting(KvTransaction tx, string key, CancellationToken cancellationToken)
    {
        (KeyValueResponseType type, _, _) = await retry.RetryOnMustRetryRegistered(tx, "row tombstone write", key,
            (coordinatorKey, operationId) => kahuna.LocateAndTrySetKeyValue(tx.TransactionId, key, BranchKvCodec.EncodeTombstone(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
            cancellationToken
        ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Set)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"WriteRowTombstoneForTesting failed for key {key}: {type}");

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }
}
