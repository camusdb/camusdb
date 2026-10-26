/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// The node's count of how statements used each index, read by <c>SHOW INDEX USAGE</c>. Its job is
/// to make an unused index visible: an index that no statement reads still costs a write for every
/// row mutation, and nothing else in the engine says that it is never read.
///
/// <para><b>Node-local and volatile.</b> The counts describe the statements this process ran, they
/// start at zero when the process starts, and they are never gathered from peers. A reader that wants
/// a window subtracts two readings; a reader that wants history keeps the readings itself.</para>
///
/// <para><b>Keyed by the relation, not by the descriptor.</b> A table descriptor is rebuilt after every
/// schema change and after an idle eviction. The counts must survive that, so they live here, keyed by
/// database id and table id, and each new descriptor's store is handed the same
/// <see cref="TableIndexUsage"/> (see <c>TableOpener.LoadTable</c>). The per-index key is the index's
/// immutable kv id, so a rename keeps the count.</para>
///
/// <para><b>When counting starts.</b> <see cref="IndexUsageCounters.TrackedSince"/> is the process
/// start for an index this node saw when it first opened the table, and the moment the node first saw
/// the index for one that appeared on a table it already tracked — the case of a <c>CREATE INDEX</c>
/// after start. Without the second rule a new index would claim a window longer than its life, and a
/// reader looking for indexes unread for a week would flag one created an hour ago.</para>
///
/// <para><b>Enabled flag.</b> <see cref="CamusDBOptions.IndexUsageEnabled"/> is re-read through
/// <see cref="ApplyOptions"/>, so turning the counts off stops them on the next statement and keeps
/// what was counted.</para>
/// </summary>
public sealed class IndexUsageRegistry
{
    private readonly ConcurrentDictionary<(string DatabaseId, string TableId), TableIndexUsage> tables = new();

    private volatile bool enabled;

    /// <summary>When this registry, and therefore this node's counting, started.</summary>
    public DateTime StartedAtUtc { get; }

    /// <summary>True while statements count their index use.</summary>
    public bool Enabled => enabled;

    public IndexUsageRegistry(CamusDBOptions options)
    {
        enabled = options.IndexUsageEnabled;
        StartedAtUtc = DateTime.UtcNow;
    }

    /// <summary>Adopts the enabled flag of a newly published configuration snapshot.</summary>
    internal void ApplyOptions(CamusDBOptions next) => enabled = next.IndexUsageEnabled;

    /// <summary>
    /// Returns the usage object of a table and registers the indexes a fresh descriptor carries, so
    /// that each index has a counter, and a start time, from the moment the descriptor exists.
    ///
    /// <para>Called once per descriptor build, never per statement. An index id seen for the first time
    /// on a table this registry already knew starts counting now; every index of a table seen for the
    /// first time starts at the process start — see the class summary.</para>
    /// </summary>
    public TableIndexUsage Track(string databaseId, string tableId, IEnumerable<string> indexKvIds)
    {
        bool known = tables.TryGetValue((databaseId, tableId), out TableIndexUsage? usage);

        if (!known)
            usage = tables.GetOrAdd((databaseId, tableId), _ => new TableIndexUsage(this));

        DateTime since = known ? DateTime.UtcNow : StartedAtUtc;

        foreach (string kvId in indexKvIds)
            usage!.Register(kvId, since);

        return usage!;
    }

    /// <summary>
    /// The usage object of a table, or null when this node has never opened it. A reader treats null
    /// as "no reads on this node since it started": a read opens the table, so a table never opened
    /// was never read.
    /// </summary>
    public TableIndexUsage? Find(string databaseId, string tableId)
        => tables.TryGetValue((databaseId, tableId), out TableIndexUsage? usage) ? usage : null;
}

/// <summary>
/// The index counters of one table. Held by the table's <see cref="KvTableStore"/> so a read site
/// reaches its counter through the table it already holds, with one dictionary lookup per index access
/// path and never one per row.
/// </summary>
public sealed class TableIndexUsage
{
    private readonly IndexUsageRegistry owner;

    private readonly ConcurrentDictionary<string, IndexUsageCounters> counters = new(StringComparer.Ordinal);

    internal TableIndexUsage(IndexUsageRegistry owner) => this.owner = owner;

    internal void Register(string kvId, DateTime since)
        => counters.GetOrAdd(kvId, static (_, start) => new IndexUsageCounters(start), since);

    /// <summary>
    /// The counter of an index, or null when it was never registered. An index can be read before its
    /// descriptor registered it only in a test that builds a store by hand; such a read is not counted.
    /// </summary>
    public IndexUsageCounters? Find(string kvId) => counters.TryGetValue(kvId, out IndexUsageCounters? c) ? c : null;

    /// <summary>
    /// Counts one statement read opened through an index and returns its counter, so the caller can
    /// add the entries the read returns. Null when counting is off or the index is unknown, in which
    /// case the caller adds nothing.
    /// </summary>
    public IndexUsageCounters? BeginRead(string kvId)
    {
        if (!owner.Enabled || Find(kvId) is not { } c)
            return null;

        c.NoteRead();
        return c;
    }

    /// <summary>
    /// Counts reads the engine made through an index to enforce a unique key or a foreign key. Kept
    /// apart from <see cref="BeginRead"/> because those reads keep an index in use even when no query
    /// chooses it: a unique index that only checks inserts cannot be dropped.
    /// </summary>
    public void NoteConstraintReads(string kvId, long reads, long entries)
    {
        if (reads <= 0 || !owner.Enabled || Find(kvId) is not { } c)
            return;

        c.NoteConstraintReads(reads, entries);
    }

    /// <summary>
    /// Wraps an index scan so the entries it yields are counted on the index, or returns the scan
    /// unchanged when counting is off. One read is counted when the scan is opened; the entries are
    /// added once, when the consumer stops reading, so the per-entry cost is a local increment.
    /// </summary>
    public static IAsyncEnumerable<T> Count<T>(TableIndexUsage? usage, string kvId, IAsyncEnumerable<T> scan)
    {
        if (usage?.BeginRead(kvId) is not { } counter)
            return scan;

        return CountEntries(counter, scan);
    }

    /// <summary>
    /// Wraps a scan so its entries are added to a read the caller already counted with
    /// <see cref="BeginRead"/> — the case of an access path that runs several seeks for one read, such
    /// as an <c>IN</c> list. Returns the scan unchanged when <paramref name="counter"/> is null.
    /// </summary>
    public static IAsyncEnumerable<T> CountInto<T>(IndexUsageCounters? counter, IAsyncEnumerable<T> scan)
        => counter is null ? scan : CountEntries(counter, scan);

    private static async IAsyncEnumerable<T> CountEntries<T>(
        IndexUsageCounters counter,
        IAsyncEnumerable<T> scan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        long entries = 0;

        try
        {
            await foreach (T item in scan.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                entries++;
                yield return item;
            }
        }
        finally
        {
            counter.AddEntriesRead(entries);
        }
    }

    /// <summary>
    /// Counts the index entries a batch of inserted rows writes, and the unique-key checks those
    /// writes make. Called once per successful batch, after the batch committed its staged writes, so
    /// a failed statement counts nothing.
    /// </summary>
    public void CountInserts(IReadOnlyList<KvTableStore.RowWrite> rows)
    {
        if (!owner.Enabled)
            return;

        IndexWriteTally tally = new();

        for (int r = 0; r < rows.Count; r++)
        {
            IReadOnlyList<KvTableStore.IndexWrite>? entries = rows[r].IndexEntries;
            for (int e = 0; entries is not null && e < entries.Count; e++)
                tally.Add(entries[e].IndexId, writes: 1, constraintReads: entries[e].Unique && !entries[e].Overwrite ? 1 : 0);
        }

        tally.FlushTo(this);
    }

    /// <summary>
    /// Counts the index entries a batch of updated rows removes and writes. An update that leaves an
    /// index key unchanged carries no entry for that index, so it counts nothing there; one that changes
    /// the key counts a delete and a write.
    /// </summary>
    public void CountUpdates(IReadOnlyList<KvTableStore.RowUpdate> rows)
    {
        if (!owner.Enabled)
            return;

        IndexWriteTally tally = new();

        for (int r = 0; r < rows.Count; r++)
        {
            IReadOnlyList<KvTableStore.IndexDelete>? removed = rows[r].OldIndexEntries;
            for (int e = 0; removed is not null && e < removed.Count; e++)
                tally.Add(removed[e].IndexId, writes: 1, constraintReads: 0);

            IReadOnlyList<KvTableStore.IndexWrite>? written = rows[r].NewIndexEntries;
            for (int e = 0; written is not null && e < written.Count; e++)
                tally.Add(written[e].IndexId, writes: 1, constraintReads: written[e].Unique && !written[e].Overwrite ? 1 : 0);
        }

        tally.FlushTo(this);
    }

    /// <summary>Counts the index entries a batch of deleted rows removes.</summary>
    public void CountDeletes(IReadOnlyList<KvTableStore.RowDelete> rows)
    {
        if (!owner.Enabled)
            return;

        IndexWriteTally tally = new();

        for (int r = 0; r < rows.Count; r++)
        {
            IReadOnlyList<KvTableStore.IndexDelete>? entries = rows[r].IndexEntries;
            for (int e = 0; entries is not null && e < entries.Count; e++)
                tally.Add(entries[e].IndexId, writes: 1, constraintReads: 0);
        }

        tally.FlushTo(this);
    }

    internal void AddWrites(string kvId, long writes, long constraintReads)
    {
        if (Find(kvId) is not { } c)
            return;

        if (writes > 0)
            c.AddWrites(writes);

        if (constraintReads > 0)
            c.NoteConstraintReads(constraintReads, constraintReads);
    }

    /// <summary>
    /// Sums a batch's index entries per index before they reach the shared counters, so a batch of a
    /// thousand rows costs one interlocked add per index rather than one per row. A table has few
    /// indexes, so a linear search by reference — index ids are the schema's own string instances —
    /// beats a dictionary here.
    /// </summary>
    private struct IndexWriteTally
    {
        private (string Id, long Writes, long ConstraintReads)[]? slots;

        private int count;

        public void Add(string id, long writes, long constraintReads)
        {
            slots ??= new (string, long, long)[8];

            for (int i = 0; i < count; i++)
            {
                if (ReferenceEquals(slots[i].Id, id) || string.Equals(slots[i].Id, id, StringComparison.Ordinal))
                {
                    slots[i].Writes += writes;
                    slots[i].ConstraintReads += constraintReads;
                    return;
                }
            }

            if (count == slots.Length)
                Array.Resize(ref slots, count * 2);

            slots[count++] = (id, writes, constraintReads);
        }

        public readonly void FlushTo(TableIndexUsage usage)
        {
            for (int i = 0; i < count; i++)
                usage.AddWrites(slots![i].Id, slots[i].Writes, slots[i].ConstraintReads);
        }
    }
}

/// <summary>
/// The counters of one index. Every field is updated with an interlocked operation, so readers and
/// concurrent statements never need a lock; a reading is a set of independent counters, not an atomic
/// snapshot, which is what a diagnostic that is read by subtracting two readings needs.
/// </summary>
public sealed class IndexUsageCounters
{
    private long reads;

    private long constraintReads;

    private long entriesRead;

    private long writes;

    private long lastReadTicks;

    /// <summary>When this node started to count this index. See <see cref="IndexUsageRegistry"/>.</summary>
    public DateTime TrackedSince { get; }

    internal IndexUsageCounters(DateTime trackedSince) => TrackedSince = trackedSince;

    /// <summary>Statement reads opened through this index.</summary>
    public long Reads => Interlocked.Read(ref reads);

    /// <summary>Reads made to enforce a unique key or a foreign key.</summary>
    public long ConstraintReads => Interlocked.Read(ref constraintReads);

    /// <summary>Index entries returned by all reads.</summary>
    public long EntriesRead => Interlocked.Read(ref entriesRead);

    /// <summary>Index entries written or removed by DML.</summary>
    public long Writes => Interlocked.Read(ref writes);

    /// <summary>The newest read or constraint read, or null when there was none.</summary>
    public DateTime? LastReadAt
    {
        get
        {
            long ticks = Interlocked.Read(ref lastReadTicks);
            return ticks == 0 ? null : new DateTime(ticks, DateTimeKind.Utc);
        }
    }

    internal void NoteRead()
    {
        Interlocked.Increment(ref reads);
        Volatile.Write(ref lastReadTicks, DateTime.UtcNow.Ticks);
    }

    internal void NoteConstraintReads(long count, long entries)
    {
        Interlocked.Add(ref constraintReads, count);
        Interlocked.Add(ref entriesRead, entries);
        Volatile.Write(ref lastReadTicks, DateTime.UtcNow.Ticks);
    }

    /// <summary>Adds the entries one read returned.</summary>
    public void AddEntriesRead(long count)
    {
        if (count > 0)
            Interlocked.Add(ref entriesRead, count);
    }

    internal void AddWrites(long count) => Interlocked.Add(ref writes, count);
}
