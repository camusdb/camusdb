
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using Kommander.Time;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Discriminator for <see cref="SchemaElementStatePayload"/>: whether the state
/// transition targets a column or an index element.
/// </summary>
public enum SchemaElementKind
{
    Column,
    Index,
}

public sealed class SchemaCreateTablePayload
{
    /// <summary>
    /// Storage parameters supplied inline on <c>CREATE TABLE ... WITH (...)</c>, or null when none.
    /// Replicated with the rest of the table definition so every node's schema agrees on them from the
    /// moment the table exists, rather than only after a later ALTER.
    /// </summary>
    public Dictionary<string, string>? Settings { get; set; }

    public string? TableId { get; set; }

    public string TableName { get; set; } = "";

    public SchemaColumnPayload[] Columns { get; set; } = [];

    // Inline constraints (PRIMARY KEY / UNIQUE / INDEX declared in CREATE TABLE) are folded into
    // this single CreateTable delta so creating a table is exactly one schema version. A new table
    // is empty, so its indexes are born at Public with nothing to backfill. Null/empty on tables
    // declared without inline constraints, and on log entries written before this field existed.
    public TableIndexSchema[]? Indexes { get; set; }

    /// <summary>
    /// CHECK constraints declared in the CREATE TABLE statement (both column-level, desugared to
    /// named constraints, and explicit table-level forms). Null/empty when no checks were declared;
    /// absent in log entries written before this field existed (backward-compatible: null → no checks).
    /// The <c>ParsedCondition</c> field on each entry is not persisted (<c>[JsonIgnore]</c>); it is
    /// rebuilt from <c>Expression</c> at table-open time.
    /// </summary>
    public CheckConstraintSchema[]? CheckConstraints { get; set; }

    /// <summary>
    /// Table-level comment declared with a trailing <c>) COMMENT '…'</c> on the CREATE TABLE
    /// statement. Null when none was declared, and absent in log entries written before this field
    /// existed (backward-compatible: null ⇒ no comment).
    /// </summary>
    public string? Comment { get; set; }

    /// <summary>
    /// Whether the relation being created is an ordinary table or a materialized view. Absent in
    /// entries written before this field existed, which decode to <c>Table</c> — the correct answer
    /// for every relation that predates materialized views.
    /// </summary>
    public RelationKind Kind { get; set; }

    /// <summary>The query that populates a materialized view. Null for an ordinary table.</summary>
    public ViewDefinition? ViewDefinition { get; set; }

    /// <summary>
    /// Whether the materialized view is created already populated. Always false at
    /// <c>CREATE</c> time — even <c>WITH DATA</c> creates the relation empty and then refreshes it,
    /// so that a refresh failure leaves an unpopulated view rather than a view that claims data it
    /// does not have.
    /// </summary>
    public bool IsPopulated { get; set; }
}

/// <summary>
/// Payload for <see cref="SchemaOp.CreateView"/> and <see cref="SchemaOp.ReplaceView"/>: the whole
/// view, including the id the proposer allocated. The id is carried in the payload rather than
/// generated during apply so every node assigns the same one — the same rule table ids follow.
/// </summary>
public sealed class SchemaViewPayload
{
    public string? ViewId { get; set; }

    public string ViewName { get; set; } = "";

    public ViewDefinition? Definition { get; set; }

    /// <summary>The view's comment, carried so a replace does not silently drop it.</summary>
    public string? Comment { get; set; }
}

/// <summary>
/// Payload for <see cref="SchemaOp.DropView"/>. Carries only the name: a view owns no keyspace, so
/// unlike a table drop there is nothing to detach, retain, or reclaim — the definition is the whole
/// object.
/// </summary>
public sealed class SchemaDropViewPayload
{
    public string ViewName { get; set; } = "";
}

/// <summary>
/// Payload for <see cref="SchemaOp.SetViewDefinition"/>: the rewritten body for one dependent view
/// after a base table or column was renamed.
/// </summary>
public sealed class SchemaSetViewDefinitionPayload
{
    public string ViewName { get; set; } = "";

    public ViewDefinition? Definition { get; set; }
}

/// <summary>
/// Payload for <see cref="SchemaOp.SetMaterializedViewState"/>.
/// </summary>
/// <remarks>
/// Keyed by <see cref="TableId"/> rather than by name because a refresh can outlive a concurrent
/// rename: the job that started against <c>mv</c> must still mark the relation it actually built,
/// not whatever now answers to that name.
/// </remarks>
public sealed class SchemaSetMatViewStatePayload
{
    public string TableId { get; set; } = "";

    public bool IsPopulated { get; set; }

    /// <summary>The snapshot the refresh read its source at, in HLC form, or null when the state
    /// change is an invalidation rather than a completion.</summary>
    public HLCTimestamp? RefreshedAt { get; set; }

    /// <summary>
    /// The table id of the freshly built relation the live name should now point at, when this state
    /// change is the swap half of a build-and-swap refresh. Null for a plain flag update.
    /// </summary>
    public string? SwapToTableId { get; set; }

    /// <summary>
    /// The materialized view's metadata generation when the rebuild began, for a swap to compare
    /// against before publishing. Null skips the check — a plain flag update replaces no definition
    /// and so cannot lose one.
    /// </summary>
    /// <remarks>
    /// Carried in the payload rather than checked only at the proposer so the comparison happens
    /// inside the apply: it is then evaluated during the dry-run under the schema lock (which is what
    /// aborts the refresh) and again on every node, so a swap can never be applied somewhere its
    /// precondition does not hold.
    /// </remarks>
    public long? ExpectedMetadataGeneration { get; set; }
}

public sealed class SchemaAlterColumnPayload
{
    public string TableName { get; set; } = "";

    public SchemaColumnPayload Column { get; set; } = new();
}

public sealed class SchemaColumnPayload
{
    public string? Id { get; set; }

    public string Name { get; set; } = "";

    public ColumnType Type { get; set; }

    public bool NotNull { get; set; }

    public ColumnValue? DefaultValue { get; set; }

    /// <summary>
    /// Name of a nullary volatile scalar function evaluated per inserted row for this column's
    /// default (e.g. <c>gen_uuid_v7</c>). Null for constant or absent defaults.
    /// </summary>
    public string? DefaultFunction { get; set; }

    public SchemaElementState State { get; set; } = SchemaElementState.Public;

    /// <summary>
    /// Maximum length in characters (String) or bytes (Bytes). Null = unbounded-but-default-capped.
    /// </summary>
    public int? MaxLength { get; set; }

    /// <summary>
    /// Element type for Array columns. Null for non-Array types.
    /// </summary>
    public ColumnType? ArrayElementType { get; set; }

    /// <summary>
    /// Name of the NOT NULL constraint for this column, when named. Null for unnamed NOT NULL.
    /// </summary>
    public string? NotNullConstraintName { get; set; }

    /// <summary>
    /// Free-text column comment. Null when absent; an empty string is a present-but-empty comment.
    /// </summary>
    public string? Comment { get; set; }

    public static SchemaColumnPayload FromColumnInfo(ColumnInfo column)
    {
        return new()
        {
            Name = column.Name,
            Type = column.Type,
            NotNull = column.NotNull,
            DefaultValue = column.Default,
            DefaultFunction = column.DefaultFunction,
            MaxLength = column.MaxLength,
            ArrayElementType = column.ArrayElementType,
            NotNullConstraintName = column.NotNullConstraintName,
            Comment = column.Comment,
        };
    }
}

public sealed class SchemaDropTablePayload
{
    public string TableName { get; set; } = "";

    /// <summary>
    /// When <c>true</c> the table is being dropped deferred (recoverable): the schema-checkpoint that
    /// deletes the per-table meta key also writes the table's orphan record in the <em>same</em>
    /// transaction, so the detach and the recovery record commit atomically. When <c>false</c> (default,
    /// and for old replayed entries) the drop is immediate and no orphan record is written.
    /// </summary>
    public bool Deferred { get; set; }
}

/// <summary>
/// Payload for <see cref="SchemaOp.RelinkTable"/>: enough to rebuild an orphaned table identically
/// under a new name — its preserved id, real schema <see cref="Version"/>, column ids, index
/// definitions, and check constraints. Distinct from <see cref="SchemaCreateTablePayload"/> because
/// the apply preserves the version and configures a lazy history loader rather than starting at
/// version 0 with a synthetic history.
/// </summary>
public sealed class SchemaRelinkTablePayload
{
    /// <summary>
    /// The relation's own key-space when it differs from its id — a refreshed materialized view. Null
    /// for every ordinary table. Carried so a relink reattaches to the storage that actually holds the
    /// retained rows rather than to an empty prefix named after the identity.
    /// </summary>
    public string? StorageId { get; set; }

    /// <summary>Whether the reattached relation is a table or a materialized view.</summary>
    public RelationKind Kind { get; set; }

    /// <summary>The defining query, so a relinked materialized view can still be refreshed.</summary>
    public ViewDefinition? ViewDefinition { get; set; }

    /// <summary>Whether the retained contents are a populated materialization.</summary>
    public bool IsPopulated { get; set; }

    /// <summary>The snapshot the retained contents are consistent as of.</summary>
    public HLCTimestamp? RefreshedAt { get; set; }

    public string? TableId { get; set; }

    public string TableName { get; set; } = "";

    /// <summary>The table's schema version at drop time, preserved so post-<c>ALTER</c> rows decode.</summary>
    public int Version { get; set; }

    public SchemaColumnPayload[] Columns { get; set; } = [];

    public TableIndexSchema[]? Indexes { get; set; }

    public CheckConstraintSchema[]? CheckConstraints { get; set; }

    /// <summary>
    /// Table storage parameters captured at drop time, preserved so a deferred-dropped table that is
    /// relinked keeps its opt-out (e.g. <c>sql_stats_automatic_collection_enabled</c>) rather than
    /// silently reverting to defaults. Null/empty when the table had no settings.
    /// </summary>
    public Dictionary<string, string>? Settings { get; set; }

    /// <summary>
    /// Table comment captured at drop time, preserved so a relinked table keeps its description
    /// rather than coming back undocumented. Null when the table had no comment.
    /// </summary>
    public string? Comment { get; set; }
}

public sealed class SchemaIndexPayload
{
    public string TableName { get; set; } = "";

    public TableIndexSchema? Index { get; set; }

    public string IndexName { get; set; } = "";
}

/// <summary>
/// Discriminator for <see cref="SchemaRenamePayload"/>: whether the rename targets a table,
/// a column, or an index.
/// </summary>
public enum SchemaRenameKind
{
    Table,
    Column,
    Index,

    /// <summary>A non-materialized view. <c>TableName</c> carries the view's current name; a
    /// materialized-view rename is a <see cref="Table"/> rename, because a materialized view is a
    /// relation.</summary>
    View,
}

/// <summary>
/// Payload for <see cref="SchemaOp.RenameTable"/>, <see cref="SchemaOp.RenameColumn"/>,
/// and <see cref="SchemaOp.RenameIndex"/>. Rename ops are single-delta, metadata-only;
/// no row or index bytes move.
/// </summary>
public sealed class SchemaRenamePayload
{
    /// <summary>
    /// Converted bodies of views that read the relation being renamed and still name their relations
    /// in text, keyed by view name, applied in the <b>same</b> delta as the rename. Null — the
    /// steady state — when every dependent already refers to its relations by immutable id and the
    /// rename is therefore metadata-only.
    /// </summary>
    /// <remarks>
    /// The conversion rides the rename rather than following it, because the gap between two deltas
    /// is not merely a window of unavailability. A body that still names the old relation does not
    /// fail during that gap — it <em>resolves</em>, and if anything has since created a new relation
    /// under the freed name it resolves to that one and returns its rows. One delta removes the gap:
    /// every node observes the complete old graph or the complete new one. It also removes the second
    /// failure mode, where a crash or leadership change between deltas left the change permanently
    /// half-applied.
    /// </remarks>
    public Dictionary<string, ViewDefinition>? DependentViewDefinitions { get; set; }

    /// <summary>Current (pre-rename) table name. Always the table being touched.</summary>
    public string TableName { get; set; } = "";

    public SchemaRenameKind Kind { get; set; }

    /// <summary>Old column or index name. Null for <see cref="SchemaRenameKind.Table"/>.</summary>
    public string? ElementName { get; set; }

    public string NewName { get; set; } = "";
}

/// <summary>
/// Payload for <see cref="SchemaOp.AddCheckConstraint"/> and <see cref="SchemaOp.DropCheckConstraint"/>.
/// Carries the table name, constraint name, and (for Add) the condition SQL text and referenced columns.
/// For Drop, only <see cref="TableName"/> and <see cref="ConstraintName"/> are consulted.
/// </summary>
public sealed class SchemaCheckConstraintPayload
{
    /// <summary>Table that owns the constraint.</summary>
    public string TableName { get; set; } = "";

    /// <summary>Unique name of the constraint within the table.</summary>
    public string ConstraintName { get; set; } = "";

    /// <summary>
    /// SQL text of the condition, rendered by <c>CheckConditionRenderer</c>. Empty string for
    /// Drop operations (the constraint is identified by name alone).
    /// </summary>
    public string Expression { get; set; } = "";

    /// <summary>
    /// Column identifiers referenced by the condition. Empty for Drop operations.
    /// </summary>
    public string[] ReferencedColumns { get; set; } = [];
}

/// <summary>
/// Payload for <see cref="SchemaOp.SetColumnNotNull"/>. Carries the target column name, the new
/// <c>NotNull</c> value (true = SET NOT NULL, false = DROP NOT NULL), and the optional constraint
/// name. Used both by <c>ALTER TABLE … ALTER COLUMN … SET/DROP NOT NULL</c> and by
/// <c>DROP CONSTRAINT name</c> when the name resolves to a column's NOT NULL constraint.
/// </summary>
public sealed class SchemaSetColumnNotNullPayload
{
    public string TableName { get; set; } = "";

    public string ColumnName { get; set; } = "";

    /// <summary>True = SET NOT NULL; false = DROP NOT NULL.</summary>
    public bool NotNull { get; set; }

    /// <summary>
    /// Name assigned to this NOT NULL constraint. Set by SET NOT NULL (auto-named
    /// <c>{table}_{col}_not_null</c>), or preserved from the original column declaration.
    /// Null for DROP NOT NULL (constraint is being removed).
    /// </summary>
    public string? ConstraintName { get; set; }
}

public sealed class SchemaElementStatePayload
{
    public string TableName { get; set; } = "";

    public string ElementName { get; set; } = "";

    public SchemaElementState State { get; set; } = SchemaElementState.Public;

    /// <summary>
    /// Identifies whether this transition targets a column (default) or an index.
    /// Absent in legacy log entries — deserialized as <see cref="SchemaElementKind.Column"/>.
    /// </summary>
    public SchemaElementKind ElementKind { get; set; } = SchemaElementKind.Column;
}

/// <summary>
/// Payload for <see cref="SchemaOp.SetTableSettings"/>: table storage parameters merged into
/// <see cref="TableSchema.Settings"/> (e.g. <c>sql_stats_automatic_collection_enabled</c>, the
/// <c>ttl_*</c> family).
///
/// <para>One payload carries both directions of the change. <c>ALTER TABLE … SET</c> populates
/// <see cref="Settings"/>; <c>ALTER TABLE … RESET</c> populates <see cref="RemovedKeys"/>. Both are a
/// version-neutral rewrite of the same bag, so they share an op rather than splitting the apply path in
/// two — a second op would have to duplicate the merge, the validation, and the ordering guarantees.</para>
/// </summary>
public sealed class SchemaSetTableSettingsPayload
{
    public string TableName { get; set; } = "";

    /// <summary>Keys to set or overwrite. Empty for a pure RESET.</summary>
    public Dictionary<string, string> Settings { get; set; } = new();

    /// <summary>
    /// Keys to remove, already expanded (a <c>RESET (ttl)</c> arrives as every TTL key, never as the
    /// group name) so apply never has to know what a group means. Empty for a pure SET.
    /// </summary>
    public List<string> RemovedKeys { get; set; } = new();
}

/// <summary>
/// What a <c>COMMENT ON</c> statement targets. Shared by the command ticket and by
/// <see cref="SchemaSetCommentPayload"/>, whose value is persisted in the schema log — so members
/// must never be renumbered. <see cref="Database"/> is valid on a ticket but never reaches the
/// payload: a database comment lives on the cross-database registry entry, not in a per-database
/// schema log.
/// </summary>
public enum CommentTarget
{
    Table = 0,
    Column = 1,
    Index = 2,
    Database = 3,
}

/// <summary>
/// Payload for <see cref="SchemaOp.SetComment"/>: attach or remove a free-text description on a
/// table, one of its columns, or one of its indexes. <see cref="Comment"/> being <c>null</c> means
/// <b>remove</b> — an empty string is a present-but-empty comment, and the two must stay distinct.
/// </summary>
public sealed class SchemaSetCommentPayload
{
    /// <summary>Table that owns the commented element (the table itself when Target is Table).</summary>
    public string TableName { get; set; } = "";

    /// <summary>Never <see cref="CommentTarget.Database"/> — that path bypasses the schema log.</summary>
    public CommentTarget Target { get; set; }

    /// <summary>Column or index name. Null (and ignored) when <see cref="Target"/> is Table.</summary>
    public string? ElementName { get; set; }

    /// <summary>The comment text, or null to remove an existing comment.</summary>
    public string? Comment { get; set; }
}
