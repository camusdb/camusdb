
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

    public TableSchemaHistory GetSchemaHistory(int version)
    {
        if (version == Version && Columns is not null)
            return new() { Version = Version, Columns = Columns };

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
            return new() { Version = Version, Columns = Columns };

        if (SchemaHistory is not null)
        {
            foreach (TableSchemaHistory history in SchemaHistory)
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
            SchemaHistory ??= [];
            SchemaHistory.Add(loaded);
            SchemaHistory.Sort(static (a, b) => a.Version.CompareTo(b.Version));
            return loaded;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.SystemSpaceCorrupt,
            $"Missing schema history for table '{Name}' version {version}"
        );
    }
}
