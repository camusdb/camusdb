
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.Ttl;

/// <summary>
/// The durable description of one row-level TTL sweep over one table: which table, at what expiry
/// horizon, divided into how many spans.
///
/// <para><b>The horizon is captured once per run, not per span.</b> Every worker in the run tests rows
/// against the same instant, so two workers can never disagree about whether a row on their shared
/// boundary is expired. It is an HLC timestamp rather than a wall clock because it is a cluster-wide
/// ordering decision made concurrently on several nodes — the one thing wall clocks cannot do.</para>
///
/// <para><b>Keyed by table id, never by name.</b> A run that outlives a <c>DROP</c>/<c>CREATE</c> of the
/// same table name would otherwise start deleting rows from the new table under the old table's
/// configuration. A manifest whose <see cref="TableId"/> no longer matches the live schema is discarded
/// without being driven, which is the same aliasing guard the deferred-drop coordinator jobs use.</para>
///
/// <para><b>The manifest is what makes a run adoptable.</b> When the planner's node loses leadership
/// mid-run, the next leader finds this record and continues the existing run rather than minting a
/// second one against a fresh horizon — two concurrent runs over one table would double the scan work
/// and race each other's spans for no benefit.</para>
/// </summary>
internal sealed class TtlRunManifest
{
    /// <summary>Identifies this run, so a span claim can be tied to the run that created it.</summary>
    public string RunId { get; set; } = "";

    /// <summary>The table this run sweeps, by immutable id (see the aliasing note above).</summary>
    public string TableId { get; set; } = "";

    /// <summary>Table name at the time the run was minted — for diagnostics only, never for routing.</summary>
    public string TableName { get; set; } = "";

    /// <summary>
    /// The physical key-space the run was planned against (<c>EffectiveStorageId</c>). Empty on a
    /// manifest written before this field existed.
    /// </summary>
    public string StorageId { get; set; } = "";

    /// <summary>
    /// The contents generation the run was planned against.
    /// </summary>
    /// <remarks>
    /// <para>A run is keyed by the relation's identity, and a <c>TRUNCATE</c> keeps that identity while
    /// replacing every row. Without this the run would survive the swap and go on deleting — from the
    /// new generation under a horizon and a span plan computed for a different set of rows, and,
    /// worse, from whatever it still holds a descriptor for, which is the retired generation a
    /// recovery is entitled to get back. A mismatch makes the run inert immediately; the deletes stop
    /// before the manifest is cleaned up, so a crash that prevents the cleanup is harmless.</para>
    /// </remarks>
    public long ContentsGeneration { get; set; }

    /// <summary>Node component of the run's expiry horizon (<see cref="HLCTimestamp.N"/>).</summary>
    public int HorizonNode { get; set; }

    /// <summary>Physical component of the run's expiry horizon (<see cref="HLCTimestamp.L"/>).</summary>
    public long HorizonPhysical { get; set; }

    /// <summary>Logical component of the run's expiry horizon (<see cref="HLCTimestamp.C"/>).</summary>
    public uint HorizonCounter { get; set; }

    /// <summary>How many spans the keyspace was divided into for this run.</summary>
    public int SpanCount { get; set; }

    /// <summary>
    /// The column this run expires on, captured when the run was minted.
    ///
    /// <para><b>The predicate is frozen with the horizon, for the same reason.</b> Resolving the current
    /// configuration on every tick lets one run apply two different predicates: spans finished before an
    /// <c>ALTER</c> were judged on the old column, spans after it on the new one, and rows already
    /// checkpointed past are never reconsidered under either. Freezing means a run always means one
    /// thing; a configuration change ends the run rather than quietly changing what it was doing.</para>
    /// </summary>
    public string ExpirationColumn { get; set; } = "";

    /// <summary>Grace period this run applies, captured with the expiration column.</summary>
    public long GraceMs { get; set; }

    /// <summary>
    /// Fingerprint of the predicate-defining settings at mint time. Cheap to compare each tick, and the
    /// thing that tells the planner a run is describing a configuration that no longer exists.
    /// </summary>
    public string PredicateFingerprint { get; set; } = "";

    /// <summary>
    /// Builds the fingerprint for a table's current predicate configuration. Only the settings that
    /// change <em>which rows a run would delete</em> take part: batch sizes and rate limits are pacing,
    /// and re-planning a run because someone tuned a rate limit would discard real progress for nothing.
    /// </summary>
    public static string BuildPredicateFingerprint(string? expirationColumn, long graceMs) =>
        $"{expirationColumn ?? ""}|{graceMs}";

    /// <summary>Physical component of when the run was minted, for staleness reporting.</summary>
    public long StartedPhysical { get; set; }

    /// <summary>
    /// Physical time at which every span finished, or <c>0</c> while the run is still open.
    ///
    /// <para>A finished run is retained rather than deleted, for two reasons. It is what enforces the
    /// table's <c>ttl_job_cron</c> cadence — without a record of when the last run ended, the next tick
    /// would immediately mint another and the table would sweep continuously regardless of its
    /// configuration. And it is what lets an operator see when a table was last swept and against which
    /// horizon, which is otherwise invisible.</para>
    /// </summary>
    public long CompletedPhysical { get; set; }

    /// <summary>Whether every span of this run has finished.</summary>
    public bool IsComplete => CompletedPhysical > 0;

    /// <summary>The run's expiry horizon, reassembled from its persisted components.</summary>
    public HLCTimestamp Horizon => new(HorizonNode, HorizonPhysical, HorizonCounter);
}

/// <summary>
/// Per-span progress for a TTL run: how far through its span a worker has processed, and whether the
/// span is finished.
///
/// <para><b>Deliberately a separate key from the span's claim lease.</b> The lease carries a native
/// expiry so a crashed owner stops blocking the span — but progress must <em>outlive</em> that expiry,
/// or the reclaiming worker would restart the span from the beginning and re-scan everything the dead
/// worker already did. Owner identity and work progress have opposite correct lifetimes, so they are
/// stored separately rather than packed into one value.</para>
/// </summary>
internal sealed class TtlSpanCheckpoint
{
    /// <summary>The run this checkpoint belongs to; a checkpoint from an older run is ignored.</summary>
    public string RunId { get; set; } = "";

    /// <summary>
    /// Kahuna's fencing token for the span claim that wrote this record.
    ///
    /// <para>Because the token increases with every grant of the lock, a later owner's token is always
    /// greater than any earlier one's. That turns "is this writer still the owner?" — a question local
    /// state can only guess at — into an ordering comparison the stored record answers by itself: a
    /// write carrying a token below the one already recorded is definitively stale, whoever sent it and
    /// whatever that sender still believes.</para>
    /// </summary>
    public long OwnerFencingToken { get; set; }

    /// <summary>
    /// Hex row id of the last row processed, exclusive resume point for the next batch. Empty means the
    /// span has not started, so the scan resumes from the span's lower bound.
    /// </summary>
    public string LastRowIdHex { get; set; } = "";

    /// <summary>Whether the span is fully processed and needs no further work in this run.</summary>
    public bool Done { get; set; }

    /// <summary>Rows deleted in this span so far, for run-level reporting.</summary>
    public long RowsDeleted { get; set; }

    /// <summary>
    /// Rows that looked expired during the scan but failed the re-check at delete time — almost always
    /// because another transaction extended their expiry in between. Counted rather than retried so the
    /// number is visible instead of silently absorbed.
    /// </summary>
    public long RowsSkipped { get; set; }

    /// <summary>
    /// Rows whose delete transaction failed or whose outcome could not be resolved.
    ///
    /// <para>Deliberately separate from <see cref="RowsSkipped"/>. A skip is the system working: the row
    /// was renewed and correctly spared. A failure is the system not working, and the two must not be
    /// summed into one number that looks healthy at a glance. These rows are left before the checkpoint,
    /// so a later attempt retries them rather than stepping over them.</para>
    /// </summary>
    public long RowsFailed { get; set; }
}
