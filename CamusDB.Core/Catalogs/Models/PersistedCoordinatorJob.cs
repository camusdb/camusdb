
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
/// from where the previous leader left off (D2 leader-change resume).
/// </summary>
public sealed class PersistedCoordinatorJob
{
    public string TableName { get; set; } = "";

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
}
