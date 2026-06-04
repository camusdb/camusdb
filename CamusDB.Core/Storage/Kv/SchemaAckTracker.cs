/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Tracks, per database and per node endpoint, the highest schema version each node has applied
/// ("acked"). This is the data behind the <b>two-version invariant</b>: a DDL proposer waits
/// via <see cref="WaitForAllLiveAsync"/> until every live node has acked the previous version
/// before proposing the next, so the cluster never spans more than two adjacent schema versions.
/// See <c>docs/distributed-schema-architecture.md</c> §6.2.
///
/// Live membership is sourced from Raft (<c>GetNodes()</c> + local endpoint) at query time,
/// not from a manual register set. This closes the gap where a deregistered-but-up node still
/// counted as live. A finite <c>liveNodeLease</c> additionally drops a live member from the gate
/// once its last ack is older than the lease (interim apply-derived liveness); the default lease
/// is infinite, so production blocks on every member until E2 supplies a real heartbeat.
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
    /// <paramref name="schemaVersion"/>, or until the timeout elapses.
    /// <paramref name="liveNodeLease"/> is reserved for E2 heartbeat-based expiry and unused here.
    /// </summary>
    public async Task<bool> WaitForAllLiveAsync(
        string database,
        long schemaVersion,
        TimeSpan timeout,
        Func<IReadOnlyCollection<string>> getLiveMembers,
        TimeSpan liveNodeLease,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(database);
        ArgumentNullException.ThrowIfNull(getLiveMembers);

        DateTime deadline = DateTime.UtcNow.Add(timeout);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            IReadOnlyCollection<string> liveMembers = getLiveMembers();

            lock (sync)
            {
                databases.TryGetValue(database, out DatabaseAcks? acks);
                if (HasEveryLiveNodeAcked(acks, schemaVersion, liveMembers, liveNodeLease, DateTime.UtcNow))
                    return true;
            }

            DateTime now = DateTime.UtcNow;
            if (now >= deadline)
                return false;

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
                return false; // never reported — can't time-expire without a LastSeen; wait for it.

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

    private readonly record struct NodeAck(long Version, DateTime LastSeen);

    private sealed class DatabaseAcks
    {
        public Dictionary<string, NodeAck> Nodes { get; } = new(StringComparer.Ordinal);
    }
}
