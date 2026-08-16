
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

using System.Text.Json.Serialization;
using Kommander.Time;

/// <summary>
/// Represents the current version of the table schema.
/// </summary>
public sealed class TableSchema
{
    /// <summary>
    /// Unique identifier of the table. It remains immutable throughout the life of the table.
    /// </summary>
    public string? Id { get; set; }

    /// <summary>
    /// Column-layout version, used for row MVCC decoding. Incremented only on column
    /// adds, drops, and state transitions — operations that change how stored bytes are
    /// interpreted. Index DDL deliberately does NOT bump this: indexes are not part of
    /// the row encoding, so changing them requires no re-decoding of existing rows.
    /// Index changes ride the table blob (via <c>PersistSchemaTableAsync</c> /
    /// <c>TableSchema.Indexes</c>) but stay invisible to other cluster nodes until routed
    /// through <c>SchemaChangeLogEntry</c>.
    /// </summary>
    public int Version { get; set; }

    /// <summary>
    /// The name of the table. It can be changed.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// The key-space this relation's rows and index entries actually live under, when that is not
    /// <see cref="Id"/>. Null — the case for every ordinary table — means the two are the same.
    ///
    /// <para>They come apart for exactly one reason: a materialized-view refresh builds its new
    /// contents in a fresh key-space and then adopts it. <see cref="Id"/> has to stay put across that,
    /// because it is the relation's <b>identity</b> — privilege grants, the views that depend on it,
    /// the result cache and the statistics are all keyed by it, and changing it would silently revoke
    /// every grant on the materialized view and orphan everything else, as though a refresh had
    /// dropped and recreated the object. It did not: the object is continuous, only its contents were
    /// replaced.</para>
    ///
    /// <para>So: use <see cref="Id"/> to talk <em>about</em> the relation, and
    /// <see cref="EffectiveStorageId"/> to read or write its rows.</para>
    /// </summary>
    public string? StorageId { get; set; }

    /// <summary>The key-space to read and write this relation's rows and index entries under.</summary>
    [JsonIgnore]
    public string EffectiveStorageId => string.IsNullOrEmpty(StorageId) ? Id ?? "" : StorageId;

    /// <summary>
    /// Advances every time a materialized view's contents are replaced. Zero on an ordinary table.
    ///
    /// <para>Identity cannot double as a contents version. <see cref="Id"/> deliberately survives a
    /// refresh so grants and dependencies survive with it, and <see cref="Version"/> describes the row
    /// <em>encoding</em>, which a refresh does not change — so after a swap every field a cache or a
    /// plan would key on looks exactly as it did before, while the rows underneath are entirely
    /// different ones in a different key-space. This is the field that differs, and it is what lets
    /// anything holding a result computed from the old contents notice.</para>
    /// </summary>
    public long ContentsGeneration { get; set; }

    /// <summary>
    /// Advances on <b>every</b> replicated change to this relation's metadata — columns, indexes,
    /// constraints, settings, comment, name, contents.
    ///
    /// <para>It exists so an operation that reads a relation's definition, works for a while, and then
    /// writes a definition derived from what it read can tell whether anything moved underneath it.
    /// A materialized-view refresh is exactly that shape: it copies the column and index layout when
    /// it starts staging, rebuilds for as long as that takes, and then publishes. Without a generation
    /// to compare, an index created during the rebuild is silently erased by the publish — the copy it
    /// writes back predates the index.</para>
    ///
    /// <para>Bumped in one place (the schema-delta dispatcher) rather than in each apply arm, because
    /// the value of this field is that it cannot be forgotten: an arm that failed to bump it would
    /// reintroduce precisely the lost-update it exists to prevent.</para>
    /// </summary>
    public long MetadataGeneration { get; set; }

    /// <summary>
    /// The list of columns that make up the table
    /// </summary>
    public List<TableColumnSchema>? Columns { get; set; }

    /// <summary>
    /// Index definitions replicated alongside this table schema. Each entry carries an
    /// immutable <c>Id</c> and <c>ColumnIds</c>; column names are resolved at table-open
    /// time. Null for tables that have not yet been migrated from the legacy
    /// <c>SystemSchema.Indexes</c> storage; <c>LoadMetaAsync</c> populates this in-memory
    /// on load and persists it on the next DDL write.
    /// </summary>
    public List<TableIndexSchema>? Indexes { get; set; }

    /// <summary>
    /// CHECK constraints defined on this table (both column-level — desugared to table-level
    /// at create time — and explicit table-level constraints). Null for tables that have no
    /// check constraints. Does not bump <c>Version</c> when constraints change (checks do not
    /// affect row encoding), matching the precedent set by <c>Indexes</c>.
    /// </summary>
    public List<CheckConstraintSchema>? CheckConstraints { get; set; }

    /// <summary>
    /// Table storage parameters (<c>ALTER TABLE t SET (key = value)</c>). A free-form string→string bag
    /// so new settings need no schema-model change. Like <c>Indexes</c> / <c>CheckConstraints</c> it
    /// rides the table blob and does <b>not</b> bump <c>Version</c> — settings do not affect row
    /// encoding. Null for tables that have never set one (all defaults). Currently the only recognized
    /// key is <c>sql_stats_automatic_collection_enabled</c>.
    /// </summary>
    public Dictionary<string, string>? Settings { get; set; }

    /// <summary>
    /// Whether the automatic-analyze scheduler may collect statistics for this table
    /// (<c>sql_stats_automatic_collection_enabled</c>). Default <c>true</c> (absent ⇒ enabled). Gates
    /// only the background scheduler; a manual <c>ANALYZE TABLE</c> always runs regardless.
    /// </summary>
    [JsonIgnore]
    public bool AutoStatsCollectionEnabled =>
        Settings is null ||
        !Settings.TryGetValue(SqlStatsAutomaticCollectionEnabledKey, out string? value) ||
        !string.Equals(value, "false", StringComparison.OrdinalIgnoreCase);

    /// <summary>The one recognized storage-parameter key: per-table auto-analyze opt-out.</summary>
    public const string SqlStatsAutomaticCollectionEnabledKey = "sql_stats_automatic_collection_enabled";

    /// <summary>
    /// Free-text description attached to the table via <c>COMMENT ON TABLE</c> or an inline
    /// <c>) COMMENT '…'</c> on <c>CREATE TABLE</c>. Null means no comment; an empty string is a
    /// comment that is present but empty (<c>IS ''</c>), and the two are deliberately
    /// distinguishable — <c>SHOW CREATE TABLE</c> omits the clause entirely for null. Like
    /// <c>Indexes</c> / <c>CheckConstraints</c> / <c>Settings</c> it rides the table blob and does
    /// <b>not</b> bump <c>Version</c>: comments do not affect row encoding.
    /// </summary>
    public string? Comment { get; set; }

    /// <summary>
    /// Whether this relation is an ordinary table or a materialized view. Absent/<c>Table</c> for
    /// every relation created before this field existed. Like <c>Indexes</c> / <c>CheckConstraints</c>
    /// / <c>Settings</c> / <c>Comment</c> it rides the table blob and does <b>not</b> bump
    /// <see cref="Version"/>: what a relation is called does not change how its rows are encoded.
    /// </summary>
    public RelationKind Kind { get; set; }

    /// <summary>
    /// The query that populates this relation, for a materialized view; null for an ordinary table.
    /// Does not bump <see cref="Version"/>, for the same reason as <see cref="Kind"/>.
    /// </summary>
    public ViewDefinition? ViewDefinition { get; set; }

    /// <summary>
    /// Whether a materialized view holds data. False for one created <c>WITH NO DATA</c> and never
    /// refreshed. Always true (and meaningless) for an ordinary table.
    ///
    /// <para>Reading an unpopulated materialized view is an <i>error</i>, not an empty result. An
    /// empty result would make a forgotten <c>REFRESH</c> indistinguishable from a correct empty
    /// answer, which is precisely the failure PostgreSQL added its own error for.</para>
    /// </summary>
    public bool IsPopulated { get; set; }

    /// <summary>
    /// HLC of the snapshot the last successful <c>REFRESH</c> read its source at — not the wall time
    /// the refresh finished. This is the timestamp the contents are consistent as of, so it is the
    /// only value that answers "how stale is this materialized view", and it is reported by
    /// <c>SHOW MATERIALIZED VIEWS</c>. Null when never refreshed.
    /// </summary>
    public HLCTimestamp? RefreshedAt { get; set; }

    /// <summary>Convenience predicate for the many call sites that only care whether DML and
    /// <c>REFRESH</c> apply.</summary>
    [JsonIgnore]
    public bool IsMaterializedView => Kind == RelationKind.MaterializedView;

    /// <summary>
    /// A list of all the previous versions of the table schema.
    /// </summary>
    public List<TableSchemaHistory>? SchemaHistory { get; set; }

    [JsonIgnore]
    public Func<HLCTimestamp, int, ValueTask<TableSchemaHistory?>>? SchemaHistoryLoader { get; set; }

    /// <summary>
    /// Memoized wrapper for the current-version case of <see cref="GetSchemaHistory"/> /
    /// <see cref="GetSchemaHistoryAsync"/>: both used to allocate a fresh <see cref="TableSchemaHistory"/>
    /// per call, and mutation decode paths call them once (or twice) per row. The cache is validated by
    /// value (<see cref="Version"/>) and by list identity (<see cref="Columns"/>) on every read, so a DDL
    /// that bumps the version or swaps the column list naturally invalidates it. The wrapper shares the
    /// live <see cref="Columns"/> list exactly as the per-call allocation did — no ownership change.
    /// A benign race may build two identical instances; the field write is an atomic reference store.
    /// </summary>
    private TableSchemaHistory? currentVersionHistory;

    /// <summary>
    /// Serializes lazy schema-history loads so concurrent decoders (parallel scan workers)
    /// requesting the same missing version perform one KV read, and so the published
    /// <see cref="SchemaHistory"/> list is replaced by copy-on-write swap — never mutated in
    /// place — keeping lock-free readers safe. DDL writers mutate the list under the schema
    /// semaphore and are unaffected.
    /// </summary>
    private readonly SemaphoreSlim historyLoadSemaphore = new(1, 1);

    private TableSchemaHistory GetCurrentVersionHistory()
    {
        TableSchemaHistory? cached = currentVersionHistory;
        if (cached is not null && cached.Version == Version && ReferenceEquals(cached.Columns, Columns))
            return cached;

        cached = new() { Version = Version, Columns = Columns };
        currentVersionHistory = cached;
        return cached;
    }

    public TableSchemaHistory GetSchemaHistory(int version)
    {
        if (version == Version && Columns is not null)
            return GetCurrentVersionHistory();

        if (SchemaHistory is not null)
        {
            foreach (TableSchemaHistory history in SchemaHistory)
            {
                if (history.Version == version)
                    return history;
            }
        }

        if (SchemaHistoryLoader is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Schema history for table '{Name}' version {version} requires asynchronous loading"
            );

        throw new CamusDBException(
            CamusDBErrorCodes.SystemSpaceCorrupt,
            $"Missing schema history for table '{Name}' version {version}"
        );
    }

    public async ValueTask<TableSchemaHistory> GetSchemaHistoryAsync(HLCTimestamp txId, int version)
    {
        if (version == Version && Columns is not null)
            return GetCurrentVersionHistory();

        // Read a local snapshot of the list reference: the load path below never mutates a
        // published list in place (copy-on-write swap), so iterating a snapshot is safe even
        // while another decoder is loading a different version concurrently.
        List<TableSchemaHistory>? snapshot = SchemaHistory;
        if (snapshot is not null)
        {
            foreach (TableSchemaHistory history in snapshot)
            {
                if (history.Version == version)
                    return history;
            }
        }

        // Parallel scan decoders can request the same missing version at once; serialize the
        // load so the KV read happens once and the list update is a single atomic swap.
        await historyLoadSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            snapshot = SchemaHistory;
            if (snapshot is not null)
            {
                foreach (TableSchemaHistory history in snapshot)
                {
                    if (history.Version == version)
                        return history;
                }
            }

            TableSchemaHistory? loaded = SchemaHistoryLoader is not null
                ? await SchemaHistoryLoader(txId, version).ConfigureAwait(false)
                : null;

            if (loaded is not null)
            {
                // Copy-on-write: readers outside the semaphore may be iterating the current
                // list, so publish a new sorted list instead of mutating in place.
                List<TableSchemaHistory> next = snapshot is null ? new(1) : new(snapshot);
                next.Add(loaded);
                next.Sort(static (a, b) => a.Version.CompareTo(b.Version));
                SchemaHistory = next;
                return loaded;
            }
        }
        finally
        {
            historyLoadSemaphore.Release();
        }

        throw new CamusDBException(
            CamusDBErrorCodes.SystemSpaceCorrupt,
            $"Missing schema history for table '{Name}' version {version}"
        );
    }
}
