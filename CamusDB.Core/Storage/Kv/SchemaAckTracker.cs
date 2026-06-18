/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
/// is infinite, so production blocks on every member until E2 supplies a real heartbeat.
///
/// <para>
/// <b>H5 quorum backstop:</b> when <c>quorumBackstopDelay</c> is finite, the gate also accepts
/// once a majority (⌊N/2⌋+1) of live members have acked and the backstop delay has elapsed.
/// This bounds DDL latency to <c>quorumBackstopDelay</c> even when minority followers are slow
/// or unreachable — the committed Raft log already guarantees durability on the majority, so DDL
/// is safe to proceed. See <see cref="SchemaAckOutcome"/> and
///
/// </para>
/// </summary>
internal sealed class SchemaAckTracker
{
    private readonly object sync = new();
    private readonly Dictionary<string, DatabaseAcks> databases = new(StringComparer.Ordinal);

    public void RecordApplied(string database, string node, long schemaVersion)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(node);

        lock (sync)
        {
            DatabaseAcks acks = GetOrCreate(database);

            long version = acks.Nodes.TryGetValue(node, out NodeAck existing)
                ? Math.Max(existing.Version, schemaVersion)
                : schemaVersion;

            // Always refresh LastSeen — every RecordApplied (even an idempotent re-apply of the
            // same version) is a liveness signal for the interim lease (E1). E2 replaces this
            // apply-derived signal with a real heartbeat.
            acks.Nodes[node] = new NodeAck(version, DateTime.UtcNow);
        }
    }

    /// <summary>
    /// Polls until every endpoint returned by <paramref name="getLiveMembers"/> has acked
    /// <paramref name="schemaVersion"/> (full convergence), OR — when
    /// <paramref name="quorumBackstopDelay"/> is finite — until a majority (⌊N/2⌋+1) of those
    /// members has acked and the backstop delay has elapsed (quorum backstop).
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

            IReadOnlyCollection<string> liveMembers = getLiveMembers();
            DateTime now = DateTime.UtcNow;

            lock (sync)
            {
                databases.TryGetValue(database, out DatabaseAcks? acks);

                if (HasEveryLiveNodeAcked(acks, schemaVersion, liveMembers, liveNodeLease, now))
                    return SchemaAckOutcome.FullConvergence;

                if (backstopEnabled && now >= backstopDeadline &&
                    HasQuorumAcked(acks, schemaVersion, liveMembers, liveNodeLease, now))
                    return SchemaAckOutcome.QuorumBackstop;
            }

            if (now >= deadline)
                return SchemaAckOutcome.Timeout;

            TimeSpan remaining = deadline - now;
            TimeSpan delay = remaining < TimeSpan.FromMilliseconds(25)
                ? remaining
                : TimeSpan.FromMilliseconds(25);

            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
    }

    private DatabaseAcks GetOrCreate(string database)
    {
        if (!databases.TryGetValue(database, out DatabaseAcks? acks))
        {
            acks = new();
            databases[database] = acks;
        }

        return acks;
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
        // production keeps the strict "wait for every member" behaviour until E2 supplies a real
        // heartbeat; only an explicit finite lease (tests / operators accepting the limitation)
        // activates apply-derived expiry.
        bool leaseFinite = liveNodeLease != Timeout.InfiniteTimeSpan;

        foreach (string node in liveMembers)
        {
            if (acks is null || !acks.Nodes.TryGetValue(node, out NodeAck ack))
            {
                // A node with no ack record for this database hasn't opened it yet and isn't
                // serving it. It will catch up via RestoreAsync when it does open.
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

        lock (sync)
        {
            databases.TryGetValue(database, out DatabaseAcks? acks);

            foreach (string node in liveMembers)
            {
                if (acks is not null && acks.Nodes.TryGetValue(node, out NodeAck ack) && ack.Version >= schemaVersion)
                    continue; // acked the target version or newer

                lagging.Add(node);
            }
        }

        return lagging;
    }

    private readonly record struct NodeAck(long Version, DateTime LastSeen);

    private sealed class DatabaseAcks
    {
        public Dictionary<string, NodeAck> Nodes { get; } = new(StringComparer.Ordinal);
    }
}
