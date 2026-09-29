/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// The owner stamp this process writes into its lifecycle markers (drop-intent, dropping,
/// pending-create), and the test startup recovery uses to tell its own crash remnants apart from
/// live markers.
/// </summary>
internal sealed class RegistryMarkerOwner
{
    // Stable local Raft node id (from configured NodeId or a hash of the node name; survives restart).
    // Stamped into every marker this node writes so startup recovery can reclaim only its own crash
    // remnants and never delete a live marker owned by another cluster node.
    private readonly int localNodeId;

    // A token minted once per DatabaseRegistry instance (i.e. once per process start). It is stamped
    // into every lifecycle marker this run writes so startup recovery can tell a marker THIS run created
    // (current epoch — a live, in-flight operation) from one left by a PRIOR run that crashed (a
    // different epoch — a genuine remnant). Without it, a startup scrub that clears "own" markers could
    // delete a marker the concurrently-started reclaimer or an incoming relink just acquired.
    private readonly string startupEpoch = Guid.NewGuid().ToString("N");

    public RegistryMarkerOwner(int localNodeId)
    {
        this.localNodeId = localNodeId;
    }

    public int LocalNodeId => localNodeId;

    // Value stamped into a node's own lifecycle markers (drop-intent, dropping): "{nodeId}:{epoch}".
    // The node-id distinguishes this node's markers from a *different* live node's; the epoch
    // distinguishes this run's live markers from a prior run's crash remnants.
    public byte[] Value => System.Text.Encoding.UTF8.GetBytes($"{localNodeId}:{startupEpoch}");

    /// <summary>
    /// True if <paramref name="value"/> is a marker owned by <em>this</em> node but written by a
    /// <em>prior</em> run (a crash remnant): the node-id matches but the epoch differs (or is absent, as
    /// in a pre-epoch marker — always treated as prior-run). A marker from the current run (matching
    /// epoch) returns false, so startup recovery never touches a live, in-flight operation's marker.
    /// </summary>
    public bool IsOwnStaleMarker(byte[]? value)
    {
        if (value is null)
            return false;

        // The stored value is "{nodeId}:{epoch}" optionally followed by ":{acquisitionToken}" (the lease
        // fence appends one per acquisition). Parse the first two fields positionally rather than
        // splitting on the LAST colon: with a token present, a last-colon split puts "{nodeId}:{epoch}"
        // in the node field, no marker ever matches this node, and startup recovery silently stops
        // reclaiming its own crash remnants — so an interrupted drop's purge is never resumed and its
        // row/index/meta keys leak with no other reclaim.
        string s = System.Text.Encoding.UTF8.GetString(value);
        string[] parts = s.Split(':');
        string nodePart = parts[0];
        string epochPart = parts.Length > 1 ? parts[1] : "";
        return nodePart == localNodeId.ToString() && epochPart != startupEpoch;
    }
}
