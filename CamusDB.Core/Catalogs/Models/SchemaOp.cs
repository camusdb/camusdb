
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// The kind of change carried by a <see cref="SchemaChangeLogEntry"/>. The numeric values
/// are persisted/serialized, so they must remain stable. <c>SchemaDeltaApplier.ApplySchemaDelta</c>
/// dispatches on this enum.
/// </summary>
public enum SchemaOp
{
    /// <summary>Create a new table (payload: <c>SchemaCreateTablePayload</c>).</summary>
    CreateTable = 0,

    /// <summary>Remove a table (payload: <c>SchemaDropTablePayload</c>).</summary>
    DropTable = 1,

    /// <summary>Add a column to a table (payload: <c>SchemaAlterColumnPayload</c>).</summary>
    AddColumn = 2,

    /// <summary>Remove a column from a table (payload: <c>SchemaAlterColumnPayload</c>).</summary>
    DropColumn = 3,

    /// <summary>
    /// Add or rebuild an index (payload: <c>SchemaIndexPayload</c>). The proposer runs the
    /// full backfill locally first (shared KV means the index data is immediately visible to
    /// all nodes), then replicates the completed <c>TableIndexSchema</c> so every node
    /// updates its in-memory <c>TableSchema.Indexes</c> and evicts its cached
    /// <c>TableDescriptor</c>. Does not bump <c>TableSchema.Version</c>.
    /// </summary>
    AddIndex = 4,

    /// <summary>
    /// Remove an index (payload: <c>SchemaIndexPayload</c>). Idempotent on apply.
    /// Does not bump <c>TableSchema.Version</c>.
    /// </summary>
    DropIndex = 5,

    /// <summary>
    /// Advance a column/index across one adjacent online-schema state
    /// (payload: <c>SchemaElementStatePayload</c>). The building block of staged
    /// add/drop sequences — see <see cref="SchemaElementState"/>.
    /// </summary>
    SetElementState = 6,

    /// <summary>Rename a table (payload: <c>SchemaRenamePayload</c>). Metadata-only; no row rewrite.</summary>
    RenameTable = 7,

    /// <summary>Rename a column (payload: <c>SchemaRenamePayload</c>). Metadata-only; bumps <c>TableSchema.Version</c>.</summary>
    RenameColumn = 8,

    /// <summary>Rename an index (payload: <c>SchemaRenamePayload</c>). Metadata-only; does not bump <c>TableSchema.Version</c>.</summary>
    RenameIndex = 9,

    /// <summary>
    /// Add a CHECK constraint (payload: <c>SchemaCheckConstraintPayload</c>). Idempotent on apply
    /// (an existing constraint with the same name is replaced). Does not bump <c>TableSchema.Version</c>
    /// because check constraints do not affect row encoding.
    /// </summary>
    AddCheckConstraint = 10,

    /// <summary>
    /// Remove a CHECK constraint (payload: <c>SchemaCheckConstraintPayload</c>). Idempotent on apply —
    /// if the constraint is already absent the operation is a no-op. Does not bump
    /// <c>TableSchema.Version</c>.
    /// </summary>
    DropCheckConstraint = 11,

    /// <summary>
    /// Set or clear the NOT NULL flag and its optional constraint name on a single column
    /// (payload: <c>SchemaSetColumnNotNullPayload</c>). Used for both SET NOT NULL and
    /// DROP NOT NULL. Does not bump <c>TableSchema.Version</c> — the NOT NULL flag is
    /// enforced at write time, not encoded in row bytes.
    /// </summary>
    SetColumnNotNull = 12,

    /// <summary>
    /// Reattach a deferred-dropped (orphaned) table to the live schema under a new name
    /// (payload: <c>SchemaRelinkTablePayload</c>). Unlike <see cref="CreateTable"/>, it preserves the
    /// table's original id, column ids, index definitions, check constraints, <b>and schema version</b>,
    /// and sets a lazy schema-history loader instead of synthesizing a version-0 history — so rows
    /// written under the table's real (post-<c>ALTER</c>) versions still decode against the retained
    /// on-disk history keys. The reattached table reads its retained rows/indexes.
    /// </summary>
    RelinkTable = 13,

    /// <summary>
    /// Set table storage parameters (payload: <c>SchemaSetTableSettingsPayload</c>), e.g.
    /// <c>sql_stats_automatic_collection_enabled</c>. Merged into <c>TableSchema.Settings</c> on apply.
    /// Does not bump <c>TableSchema.Version</c> — settings do not affect row encoding.
    /// </summary>
    SetTableSettings = 14,

    /// <summary>
    /// Attach or remove a free-text comment on a table, column, or index
    /// (payload: <c>SchemaSetCommentPayload</c>). Idempotent on apply — a replay simply overwrites
    /// with the same value, and a target that no longer exists is a no-op rather than a failure, so
    /// a re-delivered entry cannot wedge apply after a later DROP COLUMN. Does not bump
    /// <c>TableSchema.Version</c>: comments do not affect row encoding.
    /// </summary>
    SetComment = 15,

    /// <summary>
    /// Create a non-materialized view (payload: <c>SchemaViewPayload</c>). Materialized views do
    /// <b>not</b> use this op — they are relations and ride <see cref="CreateTable"/> with
    /// <c>Kind = MaterializedView</c>, which is what gives them deferred drop, relink and orphan
    /// reclaim for free.
    /// </summary>
    CreateView = 16,

    /// <summary>
    /// Replace an existing view's definition in place (payload: <c>SchemaViewPayload</c>), preserving
    /// its id so dependents keep resolving. Distinct from <see cref="CreateView"/> because apply must
    /// overwrite rather than reject an existing name.
    /// </summary>
    ReplaceView = 17,

    /// <summary>
    /// Remove a view (payload: <c>SchemaDropViewPayload</c>). Idempotent on apply — a view that is
    /// already gone is a no-op, so a re-delivered entry cannot wedge the apply pipeline.
    /// </summary>
    DropView = 18,

    /// <summary>Rename a view (payload: <c>SchemaRenamePayload</c> with
    /// <c>Kind = SchemaRenameKind.View</c>). Metadata-only; the view's id is unchanged.</summary>
    RenameView = 19,

    /// <summary>
    /// Overwrite a view's stored definition without the user having issued a
    /// <c>CREATE OR REPLACE</c> (payload: <c>SchemaSetViewDefinitionPayload</c>).
    ///
    /// <para>This exists because renaming a base table or column must rewrite every dependent view's
    /// body — CamusDB stores a view as text, so unlike PostgreSQL's OID-keyed parse tree the text
    /// would otherwise go stale and the view would stop resolving. Keeping the rewrite a distinct op
    /// makes it visible in the replicated log and idempotent on replay, rather than hiding it inside
    /// the rename's apply.</para>
    /// </summary>
    SetViewDefinition = 20,

    /// <summary>
    /// Record the outcome of a materialized-view <c>REFRESH</c>: its populated flag and the snapshot
    /// its contents are consistent as of (payload: <c>SchemaSetMatViewStatePayload</c>).
    ///
    /// <para>Must be idempotent on apply. Raft re-delivers, and a replayed completion that flipped a
    /// since-invalidated flag back to populated would leave the cluster claiming data that is not
    /// there. Does not bump <c>TableSchema.Version</c> — neither flag affects row encoding.</para>
    /// </summary>
    SetMaterializedViewState = 21,

    /// <summary>
    /// Replace a base table's physical contents generation, emptying it
    /// (payload: <c>SchemaTruncateTablePayload</c>).
    ///
    /// <para>Apply is a compare-and-swap on <c>StorageId</c> and <c>ContentsGeneration</c>, so a
    /// re-delivered entry performs one transition rather than retiring a second key-space, and an
    /// entry that lost a race against another schema change is refused rather than applied to the
    /// wrong generation. Does <b>not</b> bump <c>TableSchema.Version</c>: emptying a table does not
    /// change how a row is encoded, which is also why nothing may test "did this apply?" by looking
    /// at the version.</para>
    /// </summary>
    TruncateTable = 22,

    /// <summary>
    /// Set the storage strategy of one column (payload: <c>SchemaSetColumnStoragePayload</c>), from
    /// <c>ALTER TABLE ... ALTER COLUMN ... SET STORAGE</c>.
    ///
    /// <para>Idempotent on apply: a replay sets the same value again. Does not bump
    /// <c>TableSchema.Version</c> — a strategy decides only how future rows are written, and a reader
    /// follows the marks each stored cell carries — and it rewrites no stored row.</para>
    /// </summary>
    SetColumnStorage = 23,

    /// <summary>
    /// Create a user sequence (payload: <c>SchemaSequencePayload</c>). The delta adds the catalog
    /// record only; the proposer creates the Kahuna counter around the replication, because apply
    /// runs inside the commit pipeline and must never do storage work.
    ///
    /// <para>Idempotent on apply: a re-delivered create whose payload carries the id already in the
    /// schema is a no-op rather than a name conflict.</para>
    /// </summary>
    CreateSequence = 24,

    /// <summary>
    /// Remove a user sequence (payload: <c>SchemaDropSequencePayload</c>). Idempotent on apply — a
    /// sequence that is already gone is a no-op, so a re-delivered entry cannot wedge the pipeline.
    /// The counter is deleted by the proposer afterwards; a counter left behind by a crash is
    /// unreachable rather than harmful, since nothing in the catalog names its id any more.
    /// </summary>
    DropSequence = 25,

    /// <summary>
    /// Rename a user sequence (payload: <c>SchemaRenamePayload</c> with
    /// <c>Kind = SchemaRenameKind.Sequence</c>). Metadata-only: the counter is named after the
    /// sequence's immutable id, so it does not move, and a column default bound to that id keeps
    /// resolving.
    /// </summary>
    RenameSequence = 26,

    /// <summary>
    /// Change a sequence's parameters or ownership (payload: <c>SchemaAlterSequencePayload</c>).
    /// Carries only the fields the statement named; every other field keeps its recorded value.
    ///
    /// <para>The counter itself is moved by the proposer through Kahuna's sequence update, which is
    /// what makes <c>RESTART</c> and <c>setval</c> safe. This delta records the catalog side so
    /// every node agrees on the parameters.</para>
    /// </summary>
    AlterSequence = 27
}
