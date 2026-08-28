/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// The node's bounded record of statements that ran longer than the configured threshold, read by
/// <c>SHOW SLOW QUERIES</c>.
///
/// <para><b>Fixed capacity is the whole design.</b> The ring overwrites its oldest entry once it is
/// full, so the log has a hard memory ceiling that no workload can push past and no eviction policy
/// to tune or get wrong. A growing list guarded by a timer would have neither property: a burst of
/// slow statements is exactly the moment a node is already under pressure, and that is the worst
/// moment to let a diagnostic allocate without a bound.</para>
///
/// <para><b>Truncation happens here, not at the caller.</b> An entry count bounds memory only if an
/// entry is itself bounded, and a single statement can carry megabytes of literal text. Doing it in
/// <see cref="Record"/> means no recording path can bypass it, however many recording paths are
/// added later.</para>
///
/// <para><b>Node-local and volatile.</b> It describes the statements this process served, it is
/// never gathered from peers, and it does not survive a restart. That is a deliberate limit rather
/// than an unfinished edge: durable slow-query history is a different feature with different
/// storage, and pretending this one provides it would mislead an operator into not building it.</para>
///
/// <para>Thread-safe for concurrent recording and reading.</para>
/// </summary>
public sealed class SlowQueryLog
{
    private readonly object writeLock = new();

    private readonly SlowQueryEntry?[] ring;

    /// <summary>Index of the next slot to write; also the count of entries ever recorded.</summary>
    private long written;

    /// <summary>
    /// Truncation length, re-read on every record. Volatile rather than readonly because the length
    /// is a runtime-mutable setting: <see cref="ApplyOptions"/> publishes a new one, and the next
    /// statement recorded obeys it. The ring's capacity cannot follow the same pattern — the array
    /// is allocated once — which is why the two settings are classified differently.
    /// </summary>
    private volatile int maxSqlLength;

    /// <summary>Entries this log can hold before it starts overwriting the oldest.</summary>
    public int Capacity => ring.Length;

    /// <summary>Statements recorded since this node started, including entries already overwritten.</summary>
    public long TotalRecorded => Interlocked.Read(ref written);

    public SlowQueryLog(int capacity, int maxSqlLength)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxSqlLength, 1);

        ring = new SlowQueryEntry?[capacity];
        this.maxSqlLength = maxSqlLength;
    }

    /// <summary>
    /// Adopts the SQL truncation length from a newly published configuration snapshot. The ring's
    /// capacity is not adopted: reallocating the array under concurrent readers would either drop
    /// entries or need a lock around every read, and the setting is documented as restart-class for
    /// exactly that reason.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next)
    {
        if (next.SlowQueryLogMaxSqlLength >= 1)
            maxSqlLength = next.SlowQueryLogMaxSqlLength;
    }

    /// <summary>
    /// Records one statement and returns the entry as it was stored, so a caller that wants to log
    /// or assert on it does not have to go looking for it in the ring.
    ///
    /// <para>The caller has already decided the statement is slow enough to keep. This method does
    /// not re-check the threshold, because the threshold belongs to the recording site — which knows
    /// which configuration snapshot the statement ran under — and re-checking here against a
    /// possibly newer snapshot would silently drop entries the caller believed it had stored.</para>
    /// </summary>
    public SlowQueryEntry Record(
        DateTime startedAtUtc,
        double durationMs,
        string database,
        string? user,
        string kind,
        string sql,
        long rowsReturned,
        long rowsRead,
        bool fullScan,
        bool spilled,
        SlowQueryOutcome outcome,
        string? errorCode)
    {
        int limit = maxSqlLength;
        bool truncated = sql.Length > limit;

        SlowQueryEntry entry;

        // The sequence number is assigned under the same lock that claims the slot, so two concurrent
        // recorders cannot end up with a sequence order that disagrees with their ring order.
        lock (writeLock)
        {
            long sequence = written + 1;

            entry = new SlowQueryEntry
            {
                Sequence = sequence,
                StartedAt = startedAtUtc,
                DurationMs = durationMs,
                Database = database,
                User = user,
                Kind = kind,
                Sql = truncated ? sql[..limit] : sql,
                SqlTruncated = truncated,
                RowsReturned = rowsReturned,
                RowsRead = rowsRead,
                FullScan = fullScan,
                Spilled = spilled,
                Outcome = outcome,
                ErrorCode = errorCode,
            };

            ring[(int)(written % ring.Length)] = entry;
            written = sequence;
        }

        return entry;
    }

    /// <summary>
    /// The entries currently held, newest first. That order is what a reader wants without an
    /// <c>ORDER BY</c>: the question asked of a slow query log is almost always "what just happened",
    /// and a bare <c>SHOW SLOW QUERIES</c> should answer it.
    ///
    /// <para>The result is a copy taken under the write lock, so it is a consistent point-in-time
    /// view and enumerating it cannot race a recorder. A statement that finishes during the copy
    /// appears in the next snapshot, not half in this one.</para>
    /// </summary>
    public IReadOnlyList<SlowQueryEntry> Snapshot()
    {
        lock (writeLock)
        {
            int held = (int)Math.Min(written, ring.Length);
            List<SlowQueryEntry> entries = new(held);

            // Walk backwards from the most recently written slot so the newest entry comes first.
            for (int back = 1; back <= held; back++)
            {
                SlowQueryEntry? entry = ring[(int)((written - back) % ring.Length)];
                if (entry is not null)
                    entries.Add(entry);
            }

            return entries;
        }
    }

    /// <summary>
    /// Drops every held entry. The sequence counter keeps going: restarting it would make an entry
    /// recorded after a clear indistinguishable from one recorded before it, and the numbering is
    /// what tells a reader that entries went missing.
    /// </summary>
    public void Clear()
    {
        lock (writeLock)
            Array.Clear(ring);
    }
}
