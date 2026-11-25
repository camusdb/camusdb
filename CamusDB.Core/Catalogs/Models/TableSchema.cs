
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

using System.Text.Json.Serialization;
using CamusDB.Core.Transactions;
using Kommander.Time;

/// <summary>
/// Represents the current version of the table schema.
/// </summary>
public sealed class TableSchema : IWriteShapeSource
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
    ///
    /// <para>A reader that needs the version and <see cref="Columns"/> of the same layout calls
    /// <see cref="ReadLayout"/>: two reads of the two properties can straddle a schema change.</para>
    /// </summary>
    public int Version
    {
        get => layout.Version;
        set => layout = new(value, layout.Columns);
    }

    /// <summary>
    /// The layout version and its columns. They are published as one reference, so a statement that
    /// runs without the schema lock reads a version together with the columns of that version.
    /// </summary>
    private volatile LayoutPair layout = new(0, null);

    /// <summary>A column-layout version and the column list of that version. Never changed after it is built.</summary>
    private sealed class LayoutPair(int version, List<TableColumnSchema>? columns)
    {
        public readonly int Version = version;

        public readonly List<TableColumnSchema>? Columns = columns;
    }

    /// <summary>
    /// Returns <see cref="Version"/> and the <see cref="Columns"/> of that version, read together. A row
    /// is encoded by the codec of one version from values laid out by one column list, and the two must
    /// belong to the same layout: a pair mixed from two layouts encodes the wrong number of cells.
    /// </summary>
    internal (int Version, List<TableColumnSchema>? Columns) ReadLayout()
    {
        LayoutPair pair = layout;
        return (pair.Version, pair.Columns);
    }

    /// <summary>
    /// Publishes a layout version and its columns in one step. Every schema change that moves both
    /// calls this: setting the two properties one after the other shows readers the new version with
    /// the old columns in between. Called under the schema lock.
    /// </summary>
    internal void PublishLayout(int version, List<TableColumnSchema>? columns) => layout = new(version, columns);

    /// <summary>
    /// The name of the table. It can be changed.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// The key-space this relation's rows and index entries actually live under, when that is not
    /// <see cref="Id"/>. Null — the case for every ordinary table — means the two are the same.
    ///
    /// <para>They come apart when a relation's <em>contents</em> are replaced while its
    /// <em>identity</em> must survive: a materialized-view refresh builds its new contents in a fresh
    /// key-space and then adopts it, and a <c>TRUNCATE</c> adopts a fresh empty one.
    /// <see cref="Id"/> has to stay put across that,
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
    /// Advances every time this relation's contents are replaced wholesale — a materialized-view
    /// refresh, or a <c>TRUNCATE</c>. Zero on a relation whose contents were never replaced.
    ///
    /// <para>Identity cannot double as a contents version. <see cref="Id"/> deliberately survives a
    /// refresh so grants and dependencies survive with it, and <see cref="Version"/> describes the row
    /// <em>encoding</em>, which a contents swap does not change — so after a swap every field a cache
    /// or a plan would key on looks exactly as it did before, while the rows underneath are entirely
    /// different ones in a different key-space. This is the field that differs, and it is what lets
    /// anything holding a result computed from the old contents notice.</para>
    /// </summary>
    public long ContentsGeneration { get; set; }

    /// <summary>
    /// The HLC timestamp at which the current <see cref="StorageId"/> generation became valid, or
    /// null on a relation whose contents were never replaced.
    ///
    /// <para>It is minted only after the exclusive fence over the whole old row key-space is held,
    /// so no write into the previous generation can commit after it. That makes it a true
    /// linearization cut for the contents, which is what lets a time-travel read decide honestly
    /// whether it can be answered: a snapshot at or after the cut reads the current generation, and
    /// a snapshot before it names rows that live in a key-space the live schema can no longer
    /// locate. The latter is rejected rather than answered with zero rows.</para>
    /// </summary>
    public HLCTimestamp? ContentsValidFrom { get; set; }

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

    private long writeShapeChangedAt;

    /// <summary>
    /// The write-shape epoch of the last change that added a write obligation to this table on this
    /// node: an index that started to take entries, a foreign key that became enforced on either
    /// side, a CHECK or NOT NULL constraint. A transaction that wrote the table under an earlier
    /// epoch is refused at commit (<see cref="KvTransaction.TryEnterCommit"/>).
    ///
    /// <para><b>Node-local and in memory only.</b> It orders this node's statements against this
    /// node's applies, so it is neither persisted nor replicated, and it restarts at zero on load.
    /// That is safe: no transaction of this process can hold a pin from before the load.</para>
    ///
    /// <para>Stamped only by <see cref="WriteShapeClock"/>, which also defines the order of the
    /// stamp and the published epoch. <c>long.MaxValue</c> marks an instance that the node replaced
    /// with a reloaded one; a pin on it can never be valid again.</para>
    /// </summary>
    [JsonIgnore]
    public long WriteShapeChangedAt => Volatile.Read(ref writeShapeChangedAt);

    /// <summary>Name shown by a commit that the write-shape fence refused.</summary>
    string? IWriteShapeSource.WriteShapeName => Name;

    /// <summary>Sets <see cref="WriteShapeChangedAt"/>. Only <see cref="WriteShapeClock"/> calls it.</summary>
    internal void StampWriteShapeChange(long epoch) => Volatile.Write(ref writeShapeChangedAt, epoch);

    /// <summary>
    /// The list of columns that make up the table
    /// </summary>
    public List<TableColumnSchema>? Columns
    {
        get => layout.Columns;
        set => layout = new(layout.Version, value);
    }

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
    /// Foreign keys that this table owns — the constraints in which it is the referencing (child)
    /// table. Null for a table with none. The referenced (parent) table records nothing, so a
    /// constraint change touches only this table; the reverse direction is answered in memory by
    /// <see cref="ForeignKeyGraph"/>. Like <c>Indexes</c> and <c>CheckConstraints</c> it rides the table
    /// blob and does <b>not</b> bump <c>Version</c>: a constraint does not affect row encoding.
    /// </summary>
    public List<ForeignKeySchema>? ForeignKeys { get; set; }

    /// <summary>
    /// Row triggers on this table (<c>CREATE TRIGGER</c>), in creation order, which is also the firing
    /// order. Null for a table with none, so a DML statement on such a table pays one null check. Like
    /// the constraint lists it rides the table blob and does <b>not</b> bump <c>Version</c>: a trigger
    /// does not affect row encoding. The list is replaced, never edited in place, so a statement that
    /// read it keeps a consistent list. See <see cref="TriggerDefinition"/>.
    /// </summary>
    public List<TriggerDefinition>? Triggers { get; set; }

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
    /// The column layouts this table has had, the current one included, as far as they are held in
    /// memory. Null on a table loaded from storage until a layout is fetched through
    /// <see cref="SchemaHistoryLoader"/>.
    ///
    /// <para>Statements iterate this list without the schema lock, so a list that a shared table
    /// schema holds is never changed in place. A layout is added with
    /// <see cref="AppendSchemaHistory"/>, which publishes a new list.</para>
    /// </summary>
    public List<TableSchemaHistory>? SchemaHistory
    {
        get => Volatile.Read(ref schemaHistory);
        set => Volatile.Write(ref schemaHistory, value);
    }

    private List<TableSchemaHistory>? schemaHistory;

    /// <summary>
    /// Adds a layout to <see cref="SchemaHistory"/> by publishing a copy of the list with the layout
    /// at its end. A statement that iterates the list it read keeps a list that no longer changes.
    ///
    /// <para>A schema change calls this under the schema lock while a lazy load in
    /// <see cref="GetSchemaHistoryAsync"/> may publish under its own semaphore. Both publish by
    /// compare-and-swap and build again from the newer list when they lose, so neither drops the
    /// layout the other added.</para>
    /// </summary>
    internal void AppendSchemaHistory(TableSchemaHistory layout)
    {
        while (true)
        {
            List<TableSchemaHistory>? seen = Volatile.Read(ref schemaHistory);

            List<TableSchemaHistory> next = new((seen?.Count ?? 0) + 1);
            if (seen is not null)
                next.AddRange(seen);
            next.Add(layout);

            if (ReferenceEquals(Interlocked.CompareExchange(ref schemaHistory, next, seen), seen))
                return;
        }
    }

    /// <summary>
    /// Fetches one past layout from storage by version. Null on a table whose every layout is in
    /// <see cref="SchemaHistory"/>.
    /// </summary>
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
    /// requesting the same missing version perform one KV read. A schema change does not take this
    /// semaphore: it adds its layout through <see cref="AppendSchemaHistory"/>, and the load publishes
    /// by compare-and-swap for that reason.
    /// </summary>
    private readonly SemaphoreSlim historyLoadSemaphore = new(1, 1);

    private TableSchemaHistory GetCurrentVersionHistory(LayoutPair current)
    {
        TableSchemaHistory? cached = currentVersionHistory;
        if (cached is not null && cached.Version == current.Version && ReferenceEquals(cached.Columns, current.Columns))
            return cached;

        cached = new() { Version = current.Version, Columns = current.Columns };
        currentVersionHistory = cached;
        return cached;
    }

    public TableSchemaHistory GetSchemaHistory(int version)
    {
        // One read of the pair: a version taken with the columns of another layout would be handed out,
        // and cached by callers, as the layout of that version.
        LayoutPair current = layout;
        if (version == current.Version && current.Columns is not null)
            return GetCurrentVersionHistory(current);

        // One read of the list reference: a published list is never changed in place.
        List<TableSchemaHistory>? snapshot = SchemaHistory;
        if (snapshot is not null)
        {
            foreach (TableSchemaHistory history in snapshot)
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
        LayoutPair current = layout;
        if (version == current.Version && current.Columns is not null)
            return GetCurrentVersionHistory(current);

        // Read a local snapshot of the list reference: a published list is never changed in place,
        // so iterating a snapshot is safe while a load or a schema change publishes another list.
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
                // The stored layout keeps the names its columns had when the version was recorded;
                // the decoder needs the names they have now (see TableSchemaHistory.TakeCurrentNames).
                List<TableColumnSchema>? labeledWith = Columns;
                loaded.TakeCurrentNames(labeledWith);

                // Readers outside the semaphore may be iterating the current list, so publish a new
                // sorted list. A schema change can publish its own layout while this one loads; the
                // swap then fails and the list is built again from the newer one.
                while (true)
                {
                    List<TableSchemaHistory> next = snapshot is null ? new(1) : new(snapshot);
                    next.Add(loaded);
                    next.Sort(static (a, b) => a.Version.CompareTo(b.Version));

                    if (ReferenceEquals(Interlocked.CompareExchange(ref schemaHistory, next, snapshot), snapshot))
                        break;

                    snapshot = Volatile.Read(ref schemaHistory);

                    // A list set whole by a contents swap may already hold this version.
                    if (snapshot is not null && snapshot.Exists(history => history.Version == version))
                        break;
                }

                // A rename relabels the layouts already in the list under the schema semaphore,
                // not under this one, so one that ran while this layout was loading missed it. Every
                // DDL replaces Columns rather than editing it, so a changed reference is the sign
                // that the names may have moved again; relabeling is idempotent.
                while (!ReferenceEquals(labeledWith, Columns))
                {
                    labeledWith = Columns;
                    loaded.TakeCurrentNames(labeledWith);
                }

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
