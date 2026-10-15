/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// The pull half of the schema-ack gate for one DDL: asks peers directly which version of the
/// database they have applied, and feeds the answers into the <see cref="SchemaAckTracker"/> the
/// gate waits on.
///
/// <para><b>Why a pull exists next to the pushed acks.</b> The gate decides who must ack from two
/// inputs, and both are incomplete. Membership comes from the proposer's Raft activity view, which
/// under replica placement holds only the followers of partitions this node leads: a node that
/// shares no partition with the proposer is never in it, so the gate never waited for it even when
/// it held the database open. And the records come from acks a node pushed to whoever it believed
/// led the schema partition: an ack that was lost, or that went to a previous leader, leaves no
/// record, and a member without a record is skipped as if it had never opened the database. Both
/// made a DDL return before every node that held the database open had applied it, so a read on
/// such a node saw the old schema until the checkpoint probe caught up.</para>
///
/// <para><b>What it does.</b> Before the gate first evaluates, <see cref="DiscoverAsync"/> asks every
/// roster peer that Raft activity does not show, in parallel and bounded by one probe timeout; a
/// peer that answers is live by evidence and joins the member set (<see cref="Reached"/>) with a
/// correct record. Then, while the gate waits, <see cref="RunUntilCancelledAsync"/> asks every
/// member still behind, once per interval, so a lost or redirected ack is recovered within one
/// interval instead of stranding the gate until the backstop. A pushed ack still wakes the gate at
/// once, so in a healthy, fully replicated cluster the first interval usually never elapses and no
/// probe is sent at all.</para>
///
/// <para><b>What an answer means.</b> A version is recorded through
/// <see cref="SchemaAckTracker.RecordApplied"/>, the same call a pushed ack makes, so the two paths
/// cannot disagree. "Not open" drops the member's record (<see cref="SchemaAckTracker.RecordClosed"/>):
/// a node that closed or evicted the database cannot be behind on it, and the stale record would
/// otherwise hold the gate to its backstop. No answer changes nothing; the member keeps whatever
/// record it had, and if it had none it is skipped exactly as before this class existed.</para>
///
/// <para>One instance serves one gate; it is not reused. At most one probe per peer is in flight.</para>
/// </summary>
internal sealed class SchemaAckProber
{
    private readonly ISchemaAckSender sender;

    private readonly SchemaAckTracker tracker;

    private readonly string database;

    private readonly long schemaVersion;

    private readonly string localEndpoint;

    private readonly Func<IReadOnlyList<string>> getRosterPeers;

    private readonly Func<IReadOnlyCollection<string>> getRaftActivePeers;

    private readonly TimeSpan probeTimeout;

    private readonly object sync = new();

    private readonly HashSet<string> reached = new(StringComparer.Ordinal);

    private readonly HashSet<string> inFlight = new(StringComparer.Ordinal);

    /// <param name="getRosterPeers">Every configured peer except the local node.</param>
    /// <param name="getRaftActivePeers">The peers Raft activity on this node shows as live.</param>
    /// <param name="probeTimeout">How long one probe may take before the peer counts as unreachable for that round.</param>
    public SchemaAckProber(
        ISchemaAckSender sender,
        SchemaAckTracker tracker,
        string database,
        long schemaVersion,
        string localEndpoint,
        Func<IReadOnlyList<string>> getRosterPeers,
        Func<IReadOnlyCollection<string>> getRaftActivePeers,
        TimeSpan probeTimeout)
    {
        this.sender = sender;
        this.tracker = tracker;
        this.database = database;
        this.schemaVersion = schemaVersion;
        this.localEndpoint = localEndpoint;
        this.getRosterPeers = getRosterPeers;
        this.getRaftActivePeers = getRaftActivePeers;
        this.probeTimeout = probeTimeout;
    }

    /// <summary>
    /// The peers that answered a probe during this gate. They are live by direct evidence, so the
    /// gate counts them as members whether or not Raft activity on this node shows them. A snapshot;
    /// the gate re-reads it on every evaluation.
    /// </summary>
    public IReadOnlyCollection<string> Reached
    {
        get
        {
            lock (sync)
                return reached.Count == 0 ? [] : [.. reached];
        }
    }

    /// <summary>
    /// Asks every roster peer that Raft activity does not show, and returns once each has answered or
    /// timed out. Must complete before the gate evaluates for the first time: a gate that evaluated
    /// against the Raft view alone could pass before a peer outside that view had been heard, which
    /// is the hole this class closes. Costs nothing when every peer is Raft-active, the fully
    /// replicated case.
    /// </summary>
    public async Task DiscoverAsync(CancellationToken cancellationToken)
    {
        IReadOnlyList<string> peers = getRosterPeers();
        if (peers.Count == 0)
            return;

        IReadOnlyCollection<string> active = getRaftActivePeers();
        List<Task>? probes = null;

        foreach (string peer in peers)
        {
            if (string.Equals(peer, localEndpoint, StringComparison.Ordinal) || active.Contains(peer))
                continue;

            (probes ??= new List<Task>(peers.Count)).Add(ProbeOneAsync(peer, cancellationToken));
        }

        if (probes is not null)
            await Task.WhenAll(probes).ConfigureAwait(false);
    }

    /// <summary>
    /// Once per <paramref name="interval"/>, asks every member of <paramref name="getLiveMembers"/>
    /// that has not yet acked the gate's version. The first round runs after one interval, not at
    /// once, because a pushed ack normally lands within milliseconds and the gate has then already
    /// returned. Returns when <paramref name="cancellationToken"/> is cancelled, which the gate does
    /// as soon as it has its verdict. Never throws.
    /// </summary>
    public async Task RunUntilCancelledAsync(
        Func<IReadOnlyCollection<string>> getLiveMembers,
        TimeSpan interval,
        CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);

                IReadOnlyList<string> lagging = tracker.GetLaggingNodes(database, schemaVersion, getLiveMembers());

                foreach (string node in lagging)
                {
                    if (string.Equals(node, localEndpoint, StringComparison.Ordinal))
                        continue;

                    // Fire and forget by design: the answer reaches the gate through the tracker,
                    // which wakes it, and ProbeOneAsync never faults. The in-flight set keeps one
                    // probe per peer outstanding, so a slow peer is not asked again until it answers.
                    _ = ProbeOneAsync(node, cancellationToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // The gate has its verdict.
        }
    }

    private async Task ProbeOneAsync(string node, CancellationToken cancellationToken)
    {
        lock (sync)
        {
            if (!inFlight.Add(node))
                return;
        }

        try
        {
            using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(probeTimeout);

            SchemaAppliedProbe answer = await sender.ProbeSchemaAppliedAsync(node, database, timeout.Token).ConfigureAwait(false);

            switch (answer.Outcome)
            {
                case SchemaAppliedProbeOutcome.Open:
                    lock (sync)
                        reached.Add(node);
                    tracker.RecordApplied(database, node, answer.Version);
                    Diagnostics.SchemaDiag.Log($"PROBE-OPEN node={localEndpoint} peer={node} db={database} ver={answer.Version} gateVer={schemaVersion}");
                    break;

                case SchemaAppliedProbeOutcome.NotOpen:
                    lock (sync)
                        reached.Add(node);
                    tracker.RecordClosed(database, node);
                    Diagnostics.SchemaDiag.Log($"PROBE-NOTOPEN node={localEndpoint} peer={node} db={database} gateVer={schemaVersion}");
                    break;

                default:
                    Diagnostics.SchemaDiag.Log($"PROBE-UNREACHABLE node={localEndpoint} peer={node} db={database} gateVer={schemaVersion}");
                    break;
            }
        }
        catch (Exception ex)
        {
            // A transport that throws instead of answering Unreachable, or the gate's own cancellation.
            Diagnostics.SchemaDiag.Log($"PROBE-ERR node={localEndpoint} peer={node} db={database} ex={ex.GetType().Name}:{ex.Message}");
        }
        finally
        {
            lock (sync)
                inFlight.Remove(node);
        }
    }
}
