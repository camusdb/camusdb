/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Cluster;

/// <summary>One assembly a node reported loading, and the version it reported.</summary>
public sealed record NodeComponent(string Name, string Version);

/// <summary>
/// What one node reported about itself at capture time: which build it is running, whether it was
/// ready, and the configuration it resolved.
///
/// <para>Every field is optional because a probe that could not answer must not lose the ones that
/// did. A node that refused the SQL surface still contributes its versions, and the reasons it could
/// not answer are kept in <see cref="Errors"/> — a missing fact and an unasked question look the same
/// in a JSON file otherwise, and only one of them invalidates a comparison.</para>
/// </summary>
/// <param name="Variables">
/// What <c>SHOW VARIABLES</c> reported: the configuration layer. An engine key the operator never set
/// reads as empty here, because CamusDB's baseline sits underneath that layer.
/// </param>
/// <param name="EngineSettings">
/// What the engine actually resolved, from <c>/v1/engine-settings</c>. This is the durability
/// evidence: it reports the value in force whether or not anyone configured it, which
/// <paramref name="Variables"/> cannot. Empty when the node predates the endpoint.
/// </param>
public sealed record NodeFacts(
    string Node,
    string BaseUrl,
    string? Server,
    string? Runtime,
    IReadOnlyList<NodeComponent> Components,
    bool? Ready,
    IReadOnlyDictionary<string, string> Variables,
    IReadOnlyDictionary<string, string> EngineSettings,
    IReadOnlyList<string> Errors)
{
    /// <summary>
    /// The partitions this node believed it led at capture time, ascending.
    ///
    /// <para>Leadership is a local belief, so it is recorded per node rather than as one cluster-wide
    /// map: each partition has exactly one leader, and collecting what every node claims is the only
    /// way to see the whole distribution. Two nodes claiming one partition is a real observation about
    /// a cluster mid-election, not a bug in the capture, and it survives to the file rather than being
    /// resolved away here.</para>
    ///
    /// <para>Empty when the node did not answer, which is why it is separate from
    /// <see cref="Errors"/> rather than inferred from an absence.</para>
    /// </summary>
    public IReadOnlyList<int> LeadsPartitions { get; init; } = [];
}

/// <summary>
/// One partition of the committed placement map, with the node that claimed to lead it.
///
/// <para><paramref name="Leader"/> is a node name rather than a Raft endpoint, because everything
/// else a run records — every metric sample, every per-node table — is keyed by the name the operator
/// gave the node. It is null when no node claimed the partition, and it names all the claimants when
/// more than one did.</para>
/// </summary>
public sealed record PartitionFacts(
    int PartitionId,
    string State,
    long Generation,
    int EffectiveReplicationFactor,
    string? Leader,
    IReadOnlyList<string> Replicas);

/// <summary>
/// The cluster's own description of itself, captured next to a run's results.
///
/// <para>A run manifest built only from command-line arguments records what the operator asked for.
/// This records what answered: the versions the image actually loaded, whether every node was ready,
/// the durability settings each node resolved, and where the data sat. Those are the fields that
/// silently differ between two runs an operator believes are comparable.</para>
///
/// <para><see cref="DurabilityFingerprint"/> reduces the parts that must not differ — every node's
/// engine configuration and every node's component versions — to one string, so a comparison can
/// refuse a mismatched pair with a single equality check instead of a field-by-field diff nobody
/// runs.</para>
/// </summary>
public sealed record ClusterFacts(
    string CapturedAtUtc,
    IReadOnlyList<NodeFacts> Nodes,
    IReadOnlyList<IReadOnlyDictionary<string, string>> Ranges,
    IReadOnlyList<string> Errors,
    string DurabilityFingerprint)
{
    /// <summary>
    /// The committed partition map, with each partition's leader resolved to a node name.
    ///
    /// <para>Separate from <see cref="Ranges"/> because the two answer different questions and only
    /// one of them is table-scoped. <c>SHOW RANGES</c> says which partition a table's key space landed
    /// on; this says who leads that partition and who else holds a replica of it. A hotspot needs
    /// both: the first shows tables colliding on one partition, the second shows that partition's work
    /// falling on one node.</para>
    ///
    /// <para>Empty on a capture where no node answered the placement endpoint.</para>
    /// </summary>
    public IReadOnlyList<PartitionFacts> Partitions { get; init; } = [];
}
