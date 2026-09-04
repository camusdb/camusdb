/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// How the schema-ack gate terminated.
/// </summary>
internal enum SchemaAckOutcome
{
    /// <summary>Every live node acked before the quorum backstop fired.</summary>
    FullConvergence,

    /// <summary>
    /// A quorum of live nodes acked within <c>quorumBackstopDelay</c>; one or more minority
    /// followers were still lagging but DDL is safe to proceed (the committed log is durable on
    /// the majority). This is the liveness path.
    /// </summary>
    QuorumBackstop,

    /// <summary>Neither full convergence nor quorum was reached before the gate timeout.</summary>
    Timeout,
}

/// <summary>
/// Tracks, per database and per node endpoint, the highest schema version each node has applied
/// ("acked"). This is the data behind the <b>two-version invariant</b>: a DDL proposer waits
/// via <see cref="WaitForAllLiveAsync"/> until every live node has acked the previous version
/// before proposing the next, so the cluster never spans more than two adjacent schema versions.
/// See the architecture documentation.
///
/// Live membership is sourced from Raft (<c>GetNodes()</c> + local endpoint) at query time,
/// not from a manual register set. This closes the gap where a deregistered-but-up node still
/// counted as live. A finite <c>liveNodeLease</c> additionally drops a live member from the gate
/// once its last ack is older than the lease (interim apply-derived liveness); the default lease
/// is infinite, so production blocks on every member until a real heartbeat is supplied.
///
/// <para>
/// <b>Quorum backstop:</b> when <c>quorumBackstopDelay</c> is finite, the gate also accepts
/// once a majority (⌊N/2⌋+1) of live members have acked and the backstop delay has elapsed.
/// This bounds DDL latency to <c>quorumBackstopDelay</c> even when minority followers are slow
/// or unreachable — the committed Raft log already guarantees durability on the majority, so DDL
/// is safe to proceed. See <see cref="SchemaAckOutcome"/> and
///
/// </para>
/// </summary>
internal sealed class SchemaAckTracker
{
    // One entry per database, each carrying its own lock. A single tracker-wide lock would make DDL on
    // one database contend with DDL on every other — and the gate below is held for as long as a
    // cluster takes to converge, so that contention is measured in round-trips, not instructions.
    private readonly ConcurrentDictionary<string, DatabaseAcks> databases = new(StringComparer.Ordinal);

    public void RecordApplied(string database, string node, long schemaVersion)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(node);

        DatabaseAcks acks = databases.GetOrAdd(database, _ => new DatabaseAcks());

        lock (acks.Sync)
        {
            long version = acks.Nodes.TryGetValue(node, out NodeAck existing)
                ? Math.Max(existing.Version, schemaVersion)
                : schemaVersion;

            // Always refresh LastSeen — every RecordApplied (even an idempotent re-apply of the
            // same version) is a liveness signal for the interim lease. A real heartbeat replaces
            // this apply-derived signal when one is available.
            acks.Nodes[node] = new NodeAck(version, DateTime.UtcNow);
        }

        // Outside the lock: waking a waiter must not run continuations while this database's lock is
        // held. Signalled on every record, not only on a version advance — a refreshed LastSeen can
        // change a lease-expiry verdict too, and a waiter that wakes needlessly simply re-evaluates.
        acks.SignalAck();
    }

    /// <summary>
    /// How long the gate may sleep without being woken by an ack.
    ///
    /// <para>Acks wake it immediately, so this is <b>not</b> the convergence latency — it is the
    /// resolution at which the two conditions that change with no ack behind them are re-examined:
    /// a live member's lease expiring, and cluster membership changing. Both are states rather than
    /// events, so nothing can signal them; they have to be looked at. The fixed deadlines
    /// (the quorum backstop, the overall timeout) are not polled — the sleep is shortened to land
    /// exactly on whichever comes first.</para>
    /// </summary>
    private static readonly TimeSpan LivenessRecheckInterval = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Waits until every endpoint returned by <paramref name="getLiveMembers"/> has acked
    /// <paramref name="schemaVersion"/> (full convergence), OR — when
    /// <paramref name="quorumBackstopDelay"/> is finite — until a majority (⌊N/2⌋+1) of those
    /// members has acked and the backstop delay has elapsed (quorum backstop).
    ///
    /// <para><b>Woken by acks, not by polling.</b> Each iteration captures the database's ack signal
    /// <em>before</em> evaluating the condition, so an ack that lands during evaluation completes the
    /// captured signal and the next wait returns immediately — the ordering is what makes a lost
    /// wake-up impossible. A coarse timer still runs alongside it for the conditions no ack announces
    /// (see <see cref="LivenessRecheckInterval"/>); replacing the poll with a pure signal would hang a
    /// gate whose only remaining blocker is a silent node whose lease is about to expire.</para>
    ///
    /// <para>The entry is re-resolved every iteration rather than captured once, so a database whose
    /// state is forgotten and rebuilt underneath an in-flight gate is picked up rather than waited on
    /// forever.</para>
    /// </summary>
    /// <param name="quorumBackstopDelay">
    /// How long to wait for full convergence before accepting quorum. Pass
    /// <see cref="Timeout.InfiniteTimeSpan"/> to keep the original strict behaviour (every node
    /// must ack; no quorum shortcut).
    /// </param>
    public async Task<SchemaAckOutcome> WaitForAllLiveAsync(
        string database,
        long schemaVersion,
        TimeSpan timeout,
        Func<IReadOnlyCollection<string>> getLiveMembers,
        TimeSpan liveNodeLease,
        TimeSpan quorumBackstopDelay,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(database);
        ArgumentNullException.ThrowIfNull(getLiveMembers);

        DateTime deadline = DateTime.UtcNow.Add(timeout);

        bool backstopEnabled = quorumBackstopDelay != Timeout.InfiniteTimeSpan;
        DateTime backstopDeadline = backstopEnabled
            ? DateTime.UtcNow.Add(quorumBackstopDelay)
            : DateTime.MaxValue;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Captured BEFORE the condition is evaluated. An ack recorded after the evaluation but
            // before the await completes this task, so the wait returns at once and re-evaluates.
            // Capturing it afterwards would drop exactly that ack and sleep through it.
            databases.TryGetValue(database, out DatabaseAcks? acks);
            Task ackArrived = acks?.AckArrived ?? Task.CompletedTask;

            IReadOnlyCollection<string> liveMembers = getLiveMembers();
            DateTime now = DateTime.UtcNow;

            if (acks is null)
            {
                if (HasEveryLiveNodeAcked(null, schemaVersion, liveMembers, liveNodeLease, now))
                    return SchemaAckOutcome.FullConvergence;

                if (backstopEnabled && now >= backstopDeadline &&
                    HasQuorumAcked(null, schemaVersion, liveMembers, liveNodeLease, now))
                    return SchemaAckOutcome.QuorumBackstop;
            }
            else
            {
                lock (acks.Sync)
                {
                    if (HasEveryLiveNodeAcked(acks, schemaVersion, liveMembers, liveNodeLease, now))
                        return SchemaAckOutcome.FullConvergence;

                    if (backstopEnabled && now >= backstopDeadline &&
                        HasQuorumAcked(acks, schemaVersion, liveMembers, liveNodeLease, now))
                        return SchemaAckOutcome.QuorumBackstop;
                }
            }

            if (now >= deadline)
                return SchemaAckOutcome.Timeout;

            // Sleep until the next thing that could change the answer: an ack (signalled), the
            // liveness re-check, or whichever fixed deadline comes first.
            TimeSpan delay = deadline - now;

            if (backstopEnabled && now < backstopDeadline && backstopDeadline - now < delay)
                delay = backstopDeadline - now;

            if (LivenessRecheckInterval < delay)
                delay = LivenessRecheckInterval;

            if (delay > TimeSpan.Zero)
            {
                // A no-op when the ack signal wins the race; the delay task is abandoned to the
                // timer queue either way, which is why the interval is coarse rather than 25 ms.
                await Task.WhenAny(ackArrived, Task.Delay(delay, cancellationToken)).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Drops all ack state for a database this node is no longer holding open.
    ///
    /// <para>The map is keyed by database and only ever grows: every database that has had a single
    /// DDL applied here keeps an entry, with one node-ack record per cluster member, for the life of
    /// the process. On a node that has served thousands of databases that is pure residue, so the
    /// entry is released when the descriptor is.</para>
    ///
    /// <para>Safe to forget because the state is a cache of progress, not a source of truth: reopening
    /// the database re-records this node's applied version (see
    /// <c>EmbeddedKahuna.RecordAndPublishSchemaApplied</c>, called from schema-replicator
    /// registration), and a DDL gate consults live Raft membership for who must ack. Forgetting a
    /// database with a gate in flight would only make that gate wait for acks to be re-reported, which
    /// is why eviction never runs while DDL is in flight.</para>
    /// </summary>
    public void Forget(string database)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(database);

        if (databases.TryRemove(database, out DatabaseAcks? removed))
        {
            // Wake anything waiting on the entry that was just detached. Its waiters hold a signal
            // belonging to an object no future ack will ever touch, so without this they would sit
            // until the liveness re-check rather than immediately re-resolving the live entry.
            removed.SignalAck();
        }
    }

    private static bool HasEveryLiveNodeAcked(
        DatabaseAcks? acks,
        long schemaVersion,
        IReadOnlyCollection<string> liveMembers,
        TimeSpan liveNodeLease,
        DateTime now)
    {
        // A finite lease lets a live Raft member that has gone silent (no ack within the lease)
        // be treated as down, so it stops blocking the gate. The default lease is infinite, so
        // production keeps the strict "wait for every member" behaviour until a real heartbeat is
        // supplied; only an explicit finite lease (tests / operators accepting the limitation)
        // activates apply-derived expiry.
        bool leaseFinite = liveNodeLease != Timeout.InfiniteTimeSpan;

        foreach (string node in liveMembers)
        {
            if (acks is null || !acks.Nodes.TryGetValue(node, out NodeAck ack))
            {
                // A node with no ack record for this database hasn't opened it yet and isn't
                // serving it. When it does open, it loads the durable checkpoint the proposer
                // persisted, and the freshness probes (open-time, miss-triggered, periodic sweep)
                // repair the case where the checkpoint had not yet landed at load time — the
                // committed delta itself is delivered only to registered subscribers, so an
                // unopened node never receives it. See SchemaFreshnessReconciler.
                // - schemaVersion == 0: the cluster is at version 0 (pre-first-DDL); all nodes
                //   are implicitly at 0 even without a record — gate must not block.
                // - acks != null: at least one node has opened the database and recorded an ack,
                //   so the node in question simply hasn't opened it yet — skip it.
                // - acks == null && schemaVersion > 0: shouldn't happen in normal operation (the
                //   proposing leader always acks before calling the gate), but return false as a
                //   conservative fallback so we don't silently pass a corrupted state.
                if (schemaVersion == 0 || acks is not null)
                    continue;
                return false;
            }

            if (ack.Version >= schemaVersion)
                continue; // acked the target version.

            // Behind: keep blocking while the member still looks alive (acked within the lease).
            // If it has been silent longer than the lease, presume it down and don't block on it.
            if (leaseFinite && now - ack.LastSeen > liveNodeLease)
                continue;

            return false;
        }

        return true;
    }

    /// <summary>
    /// Returns true when a majority (⌊N/2⌋+1) of <paramref name="liveMembers"/> have explicitly
    /// acked <paramref name="schemaVersion"/>. A node with no ack record at all is not counted as
    /// having acked — the backstop requires positive evidence of apply, not just absence of a
    /// blocking record. Nodes whose lease has expired are treated as not-acked so the backstop
    /// cannot be gamed by a disconnected node's stale record.
    /// </summary>
    private static bool HasQuorumAcked(
        DatabaseAcks? acks,
        long schemaVersion,
        IReadOnlyCollection<string> liveMembers,
        TimeSpan liveNodeLease,
        DateTime now)
    {
        int quorum = liveMembers.Count / 2 + 1;

        if (acks is null)
        {
            // No records at all. Only satisfied if schemaVersion == 0 and every node implicitly
            // counts — but schemaVersion 0 is handled by HasEveryLiveNodeAcked before the backstop
            // fires, so this case should not arise in practice.
            return schemaVersion == 0 && liveMembers.Count >= quorum;
        }

        bool leaseFinite = liveNodeLease != Timeout.InfiniteTimeSpan;
        int ackedCount = 0;

        foreach (string node in liveMembers)
        {
            if (!acks.Nodes.TryGetValue(node, out NodeAck ack))
                continue; // no record → not acked

            if (leaseFinite && now - ack.LastSeen > liveNodeLease)
                continue; // lease expired → treat as not acked for quorum

            if (ack.Version >= schemaVersion)
                ackedCount++;
        }

        return ackedCount >= quorum;
    }

    /// <summary>
    /// Returns the endpoints in <paramref name="liveMembers"/> that have <b>not</b> acked
    /// <paramref name="schemaVersion"/> — i.e. no ack record at all, or a recorded version still
    /// behind the target. Used to name the laggards in the quorum-backstop / timeout warnings.
    /// Lease expiry is intentionally ignored here: this answers "who is behind?", which
    /// is exactly the set an operator wants named, independent of whether they are also presumed
    /// dead. A best-effort snapshot — a node may catch up between this call and the log line.
    /// </summary>
    public IReadOnlyList<string> GetLaggingNodes(
        string database,
        long schemaVersion,
        IReadOnlyCollection<string> liveMembers)
    {
        List<string> lagging = [];

        if (!databases.TryGetValue(database, out DatabaseAcks? acks))
            return [.. liveMembers]; // nothing recorded: every member is behind

        lock (acks.Sync)
        {
            foreach (string node in liveMembers)
            {
                if (acks.Nodes.TryGetValue(node, out NodeAck ack) && ack.Version >= schemaVersion)
                    continue; // acked the target version or newer

                lagging.Add(node);
            }
        }

        return lagging;
    }

    private readonly record struct NodeAck(long Version, DateTime LastSeen);

    /// <summary>
    /// One database's ack records, the lock that guards them, and the signal that wakes its waiters.
    ///
    /// <para>The lock is per database precisely so that a DDL gate on one database — which is held
    /// open for as long as the cluster takes to converge — cannot delay an unrelated database's
    /// DDL.</para>
    /// </summary>
    private sealed class DatabaseAcks
    {
        public object Sync { get; } = new();

        public Dictionary<string, NodeAck> Nodes { get; } = new(StringComparer.Ordinal);

        // Completed (and replaced) on every recorded ack. Waiters capture it before testing their
        // condition, so an ack landing mid-test still wakes them. RunContinuationsAsynchronously
        // keeps a waiter's continuation off the thread that recorded the ack — that thread is on the
        // schema-apply path and must not be handed someone else's gate evaluation.
        private TaskCompletionSource ackSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task AckArrived => Volatile.Read(ref ackSignal).Task;

        /// <summary>
        /// Releases every current waiter and arms a fresh signal for the next round. The swap happens
        /// before the completion so a waiter that wakes and immediately re-captures gets the new
        /// signal rather than one that has already fired.
        /// </summary>
        public void SignalAck()
        {
            TaskCompletionSource previous = Interlocked.Exchange(
                ref ackSignal, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

            previous.TrySetResult();
        }
    }
}
