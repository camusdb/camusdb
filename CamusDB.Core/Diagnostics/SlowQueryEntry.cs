/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// How a recorded statement ended. The duration alone does not say this, and it changes what the
/// number means: a statement that ran for four seconds and returned every row is a different
/// problem from one that ran for four seconds and then failed.
/// </summary>
public enum SlowQueryOutcome
{
    /// <summary>The statement ran to completion and its caller consumed every row it produced.</summary>
    Completed,

    /// <summary>
    /// The caller stopped reading before the cursor was exhausted — a client that disconnected, or
    /// one that took the rows it wanted and walked away. Rows returned counts what the caller
    /// actually consumed, so it is a floor on what the statement would have produced, not a total.
    /// </summary>
    Abandoned,

    /// <summary>
    /// The statement raised. Recorded like any other, because a slow failure is exactly what an
    /// operator is looking for and dropping it would hide the worst cases.
    /// </summary>
    Failed,
}

/// <summary>
/// One recorded statement: what ran, how long it took, and the execution facts that explain the
/// duration.
///
/// <para>The explanation half is the point. An entry that carried only a duration would send the
/// reader straight back to <c>EXPLAIN</c> to find out why, and by then the conditions that made the
/// statement slow are gone. <see cref="FullScan"/>, <see cref="Spilled"/> and the ratio of
/// <see cref="RowsRead"/> to <see cref="RowsReturned"/> answer the common causes without a second
/// run.</para>
///
/// <para>Immutable once recorded, so a reader enumerating a snapshot cannot observe an entry being
/// filled in. It is also detached from the statement that produced it — it holds no transaction, no
/// descriptor and no cursor — so keeping one alive in the ring pins nothing.</para>
/// </summary>
public sealed record SlowQueryEntry
{
    /// <summary>
    /// Position in the recording order on this node, starting at 1. It keeps increasing after the
    /// ring wraps, so a gap between two snapshots tells a reader how many entries were overwritten
    /// between them — which a timestamp alone cannot, and which is the signal that the ring is too
    /// small for the threshold in force.
    /// </summary>
    public required long Sequence { get; init; }

    /// <summary>
    /// When the statement started, in UTC wall-clock time.
    ///
    /// <para>Wall clock, not HLC, on purpose. Nothing orders these entries against events on another
    /// node — the log is node-local and never gathered from peers — and the reader is a person
    /// correlating a slow statement against an incident timeline held in ordinary time.</para>
    /// </summary>
    public required DateTime StartedAt { get; init; }

    /// <summary>
    /// Wall-clock milliseconds from the start of engine work to the end of the statement.
    ///
    /// <para>For a row-returning statement this includes draining the cursor, so it spans parsing,
    /// authorization, opening the database, planning, scanning and every row handed to the caller.
    /// Time the caller spent not reading is inside it too: a client that pauses between pages makes
    /// its own statement slow, and hiding that would misattribute the delay to the engine.</para>
    /// </summary>
    public required double DurationMs { get; init; }

    /// <summary>Database the statement named. Empty for a statement that needs no database context.</summary>
    public required string Database { get; init; }

    /// <summary>
    /// Authenticated caller, or null when authentication is disabled and there is no principal to
    /// name. Null is not "unknown user" — it means the node was not authenticating at all.
    /// </summary>
    public string? User { get; init; }

    /// <summary>
    /// The statement's kind, spelled the way SQL does: <c>select</c>, <c>insert</c>, <c>update</c>.
    /// Present so a reader can narrow by kind without pattern-matching the text.
    /// </summary>
    public required string Kind { get; init; }

    /// <summary>
    /// The statement text, truncated to the configured length. Parameters are not substituted in:
    /// the text is what the client sent.
    /// </summary>
    public required string Sql { get; init; }

    /// <summary>
    /// True when <see cref="Sql"/> is shorter than what ran. Reported rather than left implicit,
    /// because a reader who cannot tell a complete statement from a cut one will read a truncated
    /// <c>WHERE</c> clause as the whole predicate.
    /// </summary>
    public required bool SqlTruncated { get; init; }

    /// <summary>
    /// Rows the caller consumed. For a statement that returns no rows this carries the affected-row
    /// count instead, so the column means "rows this statement was about" in both cases.
    /// </summary>
    public required long RowsReturned { get; init; }

    /// <summary>
    /// Rows fetched from storage before filtering. Far above <see cref="RowsReturned"/> is the
    /// signature of a predicate no index serves.
    /// </summary>
    public required long RowsRead { get; init; }

    /// <summary>True when the plan scanned a whole relation rather than seeking through an index.</summary>
    public required bool FullScan { get; init; }

    /// <summary>
    /// True when a blocking operator — a sort, a hash join, a grouping, a distinct — ran out of its
    /// in-memory budget and wrote to disk. A spilled statement is doing file work the operator was
    /// meant to avoid, so it separates "slow because it read too much" from "slow because it did not
    /// fit in memory".
    /// </summary>
    public required bool Spilled { get; init; }

    /// <summary>How the statement ended.</summary>
    public required SlowQueryOutcome Outcome { get; init; }

    /// <summary>
    /// The engine error code when <see cref="Outcome"/> is <see cref="SlowQueryOutcome.Failed"/>,
    /// null otherwise.
    /// </summary>
    public string? ErrorCode { get; init; }
}
