
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Durable record of an in-flight materialized-view refresh, and — more importantly — the only thing
/// that <b>owns</b> the relation that refresh is building into.
///
/// <para>Staging storage is a registered relation with real rows and index entries, and until this
/// record existed nothing durable said whose it was. A process that died mid-rebuild left it in the
/// schema, reachable only by a later refresh of the same materialized view: if the view were dropped
/// first, or simply never refreshed again, that storage was never handed to orphan collection and
/// leaked for the lifetime of the database. Written before the staging relation is created and deleted
/// after the swap, this record is what makes the storage findable by anything that sweeps.</para>
///
/// <para>It is deliberately <b>not</b> the fence. The fence is a leased, cluster-visible key held for
/// the duration of the refresh; it answers "is someone working on this right now". This record answers
/// "what did the last run leave behind", which has to survive the fence lapsing and the process
/// exiting — otherwise nothing could tell an abandoned build from one still in progress.</para>
/// </summary>
public sealed class MaterializedViewRefreshJob
{
    /// <summary>Identifies one refresh attempt. Present so a log line can be tied to a specific run.</summary>
    public string JobId { get; set; } = "";

    /// <summary>The materialized view's immutable relation id — the fence key, and the record key.</summary>
    public string ViewTableId { get; set; } = "";

    /// <summary>Name at the time the refresh started, for messages only; a rename may have moved it.</summary>
    public string ViewName { get; set; } = "";

    /// <summary>
    /// The relation this attempt is building into. Whatever sweeps an abandoned job drops exactly this
    /// storage, rather than guessing from a name pattern.
    /// </summary>
    public string StagingTableId { get; set; } = "";

    /// <summary>The name the staging relation is registered under, so a sweep can drop it by name.</summary>
    public string StagingName { get; set; } = "";

    /// <summary>The snapshot the rebuild reads its source at, for the whole run.</summary>
    public HLCTimestamp SourceSnapshot { get; set; }

    /// <summary>
    /// The view's metadata generation when the rebuild began. The swap refuses to publish if it has
    /// moved; carrying it here means a resumed or inspected job can tell what it was validated against.
    /// </summary>
    public long ExpectedMetadataGeneration { get; set; }

    /// <summary>The node that started this attempt, for diagnostics — never for authorization.</summary>
    public string Owner { get; set; } = "";

    /// <summary>When the attempt began, in cluster time.</summary>
    public HLCTimestamp StartedAt { get; set; }

    /// <summary>
    /// Whether the interrupted statement was <c>REFRESH … WITH NO DATA</c>, so a run restarted from
    /// this record empties the materialized view instead of repopulating it.
    ///
    /// <para>It cannot be inferred from anything else: an empty staging relation looks identical to
    /// one whose rebuild had not written a row yet, and guessing wrong either resurrects contents the
    /// user asked to discard or discards contents they asked to rebuild.</para>
    /// </summary>
    public bool WithNoData { get; set; }

    /// <summary>
    /// How many times a background sweep has already restarted this refresh after finding it
    /// abandoned. Bounded by <see cref="CamusDBOptions.MaterializedViewRefreshTakeoverAttempts"/>.
    ///
    /// <para>Durable, and incremented on the record that still owns the dead run's storage
    /// <b>before</b> that storage is touched — a count kept only in memory, or written after the
    /// destructive step, would reset every time a takeover itself died and turn a rebuild that fails
    /// deterministically into an endless one.</para>
    /// </summary>
    public int TakeoverAttempts { get; set; }
}
