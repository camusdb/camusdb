
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Controllers.Ttl;

/// <summary>
/// What a TTL run is currently doing for one table, as an operator would describe it.
///
/// <para>Cumulative counters answer "is TTL working at all". They cannot answer "which table stopped",
/// because one healthy table's numbers hide another's silence. These states exist so the two questions
/// stay separable — and so <em>idle</em> and <em>stalled</em> are never reported as the same thing,
/// which is the failure mode where a jammed sweep looks like a sweep with nothing to do.</para>
/// </summary>
public enum TtlRunState
{
    /// <summary>No open run: the table's cadence has not elapsed since its last completed sweep.</summary>
    Idle,

    /// <summary>Configured but <c>ttl_pause</c> is set. Not broken — switched off on purpose.</summary>
    Paused,

    /// <summary>A run is open and this node is completing spans of it.</summary>
    Progressing,

    /// <summary>A run is open but this node completed no spans this tick, and none are outstanding here.</summary>
    Waiting,

    /// <summary>A run is open, spans remain, and deletes are failing or unresolved.</summary>
    Failing,

    /// <summary>A run is open and past its cadence, but no span has advanced since the last tick.</summary>
    Stalled,
}

/// <summary>
/// A snapshot of one table's TTL run for introspection. Written by the scheduler each tick and read by
/// <c>SHOW ENGINE STATS</c>; never used to make sweep decisions, which read the durable manifest.
/// </summary>
public sealed class TtlTableStatus
{
    public string DatabaseName { get; set; } = "";

    public string TableName { get; set; } = "";

    public string RunId { get; set; } = "";

    public TtlRunState State { get; set; }

    /// <summary>Physical component of the run's horizon — the instant rows are judged against.</summary>
    public long HorizonPhysical { get; set; }

    /// <summary>Spans finished, out of the run's total, so a stuck run shows how far it got.</summary>
    public int SpansDone { get; set; }

    public int SpanCount { get; set; }

    /// <summary>Rows this run has deleted, spared by the delete-time re-check, and failed to delete.</summary>
    public long RowsDeleted { get; set; }

    public long RowsSkipped { get; set; }

    public long RowsFailed { get; set; }

    /// <summary>Physical time of the last tick that observed this table, so staleness is visible.</summary>
    public long LastObservedPhysical { get; set; }
}
