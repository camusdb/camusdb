/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core.Storage.Kv;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Storage;

/// <summary>
/// The placement snapshot model and the standalone fast path of
/// <see cref="EmbeddedKahuna.GetPlacement"/>. Cluster-mode span composition (range map +
/// replica sets) is covered by the cluster test project; here the contract under test is the
/// arithmetic and the standalone short-circuit — a single local span produced without asking
/// Kahuna or Raft anything.
/// </summary>
[NonParallelizable]
public sealed class TestTablePlacement : BaseTest
{
    private static PlacementSpan Span(bool leaderIsLocal, string? leader = "n1:7070") =>
        new(
            StartKey: null,
            EndKey: null,
            PartitionId: 1,
            Generation: 0,
            LeaderEndpoint: leader,
            ReplicaEndpoints: [],
            LeaderIsLocal: leaderIsLocal,
            HostedLocally: true);

    [Test]
    public void RemoteLeaderFraction_AllLocal_IsZero()
    {
        TablePlacement placement = new("5:1:r", isKeyRange: false, [Span(true), Span(true)], 0);

        Assert.AreEqual(0.0, placement.RemoteLeaderFraction);
        Assert.IsTrue(placement.AllLeadersLocal);
    }

    [Test]
    public void RemoteLeaderFraction_HalfRemote_IsHalf()
    {
        TablePlacement placement = new("5:1:r", isKeyRange: true, [Span(true), Span(false)], 0);

        Assert.AreEqual(0.5, placement.RemoteLeaderFraction);
        Assert.IsFalse(placement.AllLeadersLocal);
    }

    [Test]
    public void RemoteLeaderFraction_UnknownLeader_CountsAsRemote()
    {
        // A span whose leader hint is unknown must be charged as remote — the cost model
        // may over-estimate shipping but never under-estimate it.
        TablePlacement placement = new("5:1:r", isKeyRange: true,
            [Span(leaderIsLocal: false, leader: null)], 0);

        Assert.AreEqual(1.0, placement.RemoteLeaderFraction);
    }

    [Test]
    public void StandaloneGetPlacement_SingleLocalSpan_AndCachedIdentity()
    {
        EmbeddedKahuna node = TestNode!;

        Assert.IsFalse(node.IsClusterMode, "BaseTest boots a standalone node");

        TablePlacement placement = node.GetPlacement("5:1:r");

        Assert.AreEqual(1, placement.Spans.Count, "Standalone placement is one unbounded span");
        Assert.IsTrue(placement.Spans[0].LeaderIsLocal);
        Assert.IsTrue(placement.Spans[0].HostedLocally);
        Assert.IsNull(placement.Spans[0].StartKey);
        Assert.IsNull(placement.Spans[0].EndKey);
        Assert.AreEqual(0.0, placement.RemoteLeaderFraction);
        Assert.IsFalse(placement.IsKeyRange);

        // Standalone placements never go stale, so the same instance is served again.
        Assert.AreSame(placement, node.GetPlacement("5:1:r"));

        // Invalidation drops the entry; the rebuilt one is equivalent but a fresh instance.
        node.InvalidatePlacement("5:1:r");
        TablePlacement rebuilt = node.GetPlacement("5:1:r");
        Assert.AreNotSame(placement, rebuilt);
        Assert.AreEqual(1, rebuilt.Spans.Count);
        Assert.IsTrue(rebuilt.Spans[0].LeaderIsLocal);
    }
}
