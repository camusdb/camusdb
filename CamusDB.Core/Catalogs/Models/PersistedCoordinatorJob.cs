
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Durable record of an in-flight coordinator job. Written to the KV store before
/// the first step and deleted after the last. If the coordinator node crashes or
/// loses leadership mid-sequence the new leader reads the persisted jobs and resumes
/// from where the previous leader left off (leader-change resume).
/// </summary>
public sealed class PersistedCoordinatorJob
{
    public string TableName { get; set; } = "";

    /// <summary>
    /// Immutable table id. Resume resolution matches this against the live schema to
    /// detect drop/recreate aliasing: if the current table with <see cref="TableName"/> has a
    /// different id, the job belongs to a stale table and must be deleted without driving.
    /// </summary>
    public string TableId { get; set; } = "";

    public string ElementName { get; set; } = "";

    public SchemaElementState TargetState { get; set; }

    /// <summary>
    /// Set when the column must still be created (add sequence that may not yet
    /// have reached the first <c>DeleteOnly</c> step). Null when the column already
    /// exists and only state transitions remain.
    /// </summary>
    public ColumnType? ColumnType { get; set; }

    public bool ColumnNotNull { get; set; }

    public ColumnValue? ColumnDefault { get; set; }

    // The rest of the column definition. A resume that finds the column still Absent re-creates it
    // from these fields, so each one the statement declared must be here: a field left out comes
    // back as its default, and the column is then created without, for example, its sequence
    // default, and its existing rows are filled with NULL. All are absent in records written before
    // they existed, which read as "not declared".

    public int? ColumnMaxLength { get; set; }

    public ColumnType? ColumnArrayElementType { get; set; }

    public string? ColumnDefaultFunction { get; set; }

    public string? ColumnDefaultSequenceId { get; set; }

    public bool ColumnIdentityAlways { get; set; }

    public string? ColumnNotNullConstraintName { get; set; }

    public string? ColumnComment { get; set; }

    public ColumnStorageStrategy? ColumnStorage { get; set; }

    /// <summary>
    /// Identifies whether this job drives a column (default) or an index element.
    /// Absent in legacy persisted entries — deserialized as <see cref="SchemaElementKind.Column"/>.
    /// </summary>
    public SchemaElementKind ElementKind { get; set; } = SchemaElementKind.Column;

    // ── Index-specific fields (null for column jobs) ──────────────────────────

    /// <summary>Immutable index ID, set when ElementKind is Index.</summary>
    public string? IndexId { get; set; }

    /// <summary>Column IDs the index covers, used to resolve column names on resume.</summary>
    public string[]? IndexColumnIds { get; set; }

    /// <summary>Index type (Unique / Multi), needed to rebuild IndexBuildInfo on resume.</summary>
    public IndexType? IndexType { get; set; }

    /// <summary>
    /// Column IDs of the index's stored/payload (INCLUDE) columns, if any. Persisted so a
    /// leader-change resume rebuilds the same covering index (value tuple) it started. Null for
    /// column jobs and for indexes without INCLUDE columns.
    /// </summary>
    public string[]? IndexIncludeColumnIds { get; set; }

    /// <summary>
    /// Per-key-column sort directions, positionally aligned with <see cref="IndexColumnIds"/>. Persisted
    /// so a leader-change resume rebuilds the index with the same directions it started — otherwise a
    /// resumed DESCENDING index would be replicated/registered as ascending, disagreeing with any
    /// descending key encoding the interrupted run already wrote. Null means all-ascending (and is the
    /// value for column jobs and for indexes created before mixed-direction support).
    /// </summary>
    public OrderType[]? IndexColumnDirections { get; set; }

    /// <summary>
    /// Last row-id checkpoint written by the backfill pass. Non-null only when the
    /// backfill has committed at least one checkpoint batch. On resume the backfill
    /// starts scanning from this offset instead of the beginning of the table.
    /// </summary>
    public string? StartOffset { get; set; }

    /// <summary>
    /// Number of leader-change resume attempts already spent on this job. The initial
    /// (user-driven) run records 0; each subsequent <c>ResumeJobsAsync</c> pickup bumps it
    /// durably before driving. Once it reaches the coordinator's max, the job is abandoned
    /// (deleted + logged) instead of being retried on every future election — a doomed job
    /// must not poison leader changes forever.
    /// </summary>
    public int Attempts { get; set; }

    /// <summary>
    /// The schema-leader term of the node that wrote this record, read when it took the job over
    /// (the statement's run, or a resume). A record is never overwritten or deleted by a writer whose
    /// term is older than the term stored here (<c>CoordinatorJobStore</c>): a driver that lost the
    /// schema leadership, and kept running until it noticed, finds the job owned by its successor at
    /// its next write and stops. The schema log refuses a stale term's proposals on its own; this
    /// extends the fence to the job record, which is an ordinary KV write. Zero in records written
    /// before the field existed, and by a writer that states no term; such a write is not fenced.
    /// </summary>
    public long LeaderTerm { get; set; }

    /// <summary>
    /// The error that made the driver take the element back, recorded with the removal intent
    /// (<see cref="TargetState"/> = <c>Absent</c>). A driver that handed the job over to the next
    /// schema leader reads it while it waits for the outcome, and answers its client with the actual
    /// reason rather than a generic one. Null for a job that is not a removal.
    /// </summary>
    public string? FailureCode { get; set; }

    /// <summary>The message of the error recorded in <see cref="FailureCode"/>.</summary>
    public string? FailureMessage { get; set; }
}
