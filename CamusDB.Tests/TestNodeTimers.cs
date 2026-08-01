/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;

using Kahuna;

namespace CamusDB.Tests;

/// <summary>
/// Raft timings for the single-node, in-memory nodes that tests boot. The embedded defaults are sized
/// for a real node that must replay a WAL and join peers before it may participate in elections; a
/// test node has neither, so it spends that grace period idle and every node-booting test pays it.
///
/// <para>Measured on this repository: a default node takes ~3.5 s to reach start → leader → flush, of
/// which ~2.5 s is <see cref="EmbeddedKahunaOptions.TimerInitialDelay"/> elapsing before the
/// check-leader timer fires for the first time. With these values the same boot costs ~1.0 s, and the
/// remainder is a fixed poll interval inside Kommander's join path rather than anything configurable
/// from here.</para>
///
/// <para><b>Single-node only.</b> Do not apply this to a multi-node in-process cluster. With several
/// Raft groups from several nodes contending for the shared thread pool, a follower misses a
/// heartbeat inside a short election timeout and starts a spurious election, churning partition
/// leadership — the cluster fixtures deliberately run *longer* timeouts for exactly that reason (see
/// the rationale in <c>CamusDB.Cluster.Tests/InProcessSchemaCluster.cs</c>). A single node wins its
/// own election uncontested, so the same aggression is safe here regardless of partition count.</para>
///
/// <para>The values move together: Kommander's <c>RaftConfiguration.Validate()</c> rejects a
/// heartbeat interval that is not well below the election timeout (guideline: at most a fifth of it),
/// and likewise for the leader-check interval. Changing one means rechecking the others.</para>
/// </summary>
public static class TestNodeTimers
{
    /// <summary>
    /// Applies the fast single-node timings and returns the same instance, so it can be chained onto
    /// an object initializer at a node construction site.
    /// </summary>
    public static EmbeddedKahunaOptions WithFastTestTimers(this EmbeddedKahunaOptions options)
    {
        options.TimerInitialDelay = TimeSpan.FromMilliseconds(100);
        options.StartElectionTimeout = 150;
        options.EndElectionTimeout = 300;
        options.HeartbeatInterval = TimeSpan.FromMilliseconds(20);
        options.CheckLeaderInterval = TimeSpan.FromMilliseconds(25);
        options.VotingTimeout = TimeSpan.FromMilliseconds(300);

        return options;
    }
}
