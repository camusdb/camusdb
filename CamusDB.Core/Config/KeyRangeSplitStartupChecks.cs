
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config.Models;

namespace CamusDB.Core.Config;

/// <summary>
/// Startup diagnostics for key-range routing and its automatic split policy.
///
/// <para>Every state reported here starts the node successfully and then does nothing an operator
/// asked for. Range auto-split depends on preconditions that live in four separate config keys, and
/// a missing one produces no error at any layer: the split checker simply never fires, or fires and
/// relieves nothing. That reads as a broken feature, so each cause is named at startup instead.</para>
///
/// <para>These are warnings, never refusals. A node that cannot split is still a correct node, and a
/// deployment may be mid-rollout. <see cref="KahunaOptionsConfig.Validate"/> owns the cases that are
/// genuinely wrong rather than merely inert.</para>
/// </summary>
public static class KeyRangeSplitStartupChecks
{
    /// <summary>
    /// Returns one message per condition that makes key-range routing or load-based auto-split inert.
    /// The list is empty when nothing is wrong, and the caller logs each entry as a warning.
    ///
    /// <para>Several messages can hold at once, and each is emitted, because each names a different
    /// remedy. An operator who fixes only the first cause and restarts would otherwise discover the
    /// second one restart by restart.</para>
    /// </summary>
    public static IReadOnlyList<string> Inspect(ConfigDefinition config, CamusDBOptions options)
    {
        List<string> warnings = [];

        // Key-range routing with a single partition. Registration still succeeds and the space is
        // genuinely key-range routed, so this is not a no-op — but a range has nowhere to move to,
        // so the table stays on one leader and the mode distributes nothing.
        if (options.KeyRangeShardingEnabled && config.InitialPartitions < 2)
            warnings.Add(
                $"key_range_sharding is enabled but initial_partitions={config.InitialPartitions} < 2; " +
                "key spaces are key-range routed, but with one partition a range cannot move, so write " +
                "coordination stays on a single leader. Set initial_partitions >= 2 in config.yml to " +
                "distribute a table across partitions.");

        double loadThreshold = config.Kahuna.RangeSplitLoadThreshold ?? 0;
        if (loadThreshold <= 0)
            return warnings;

        // Hash routing registers no range descriptor, so the split checker has nothing to act on.
        if (!options.KeyRangeShardingEnabled)
            warnings.Add(
                $"kahuna.range_split_load_threshold={loadThreshold} is set but key_range_sharding is off; " +
                "a hash-routed key space has no range descriptor to split, so load-based auto-split can " +
                "never fire. Set key_range_sharding: true in config.yml.");

        if (config.InitialPartitions < 2)
            warnings.Add(
                $"kahuna.range_split_load_threshold={loadThreshold} is set but initial_partitions=" +
                $"{config.InitialPartitions} < 2; a split needs a second partition to place the child " +
                "range on. Set initial_partitions >= 2 in config.yml.");

        // A standalone node does split — the relief guard only refuses when no peer is visible at all,
        // and a single node still sees its own witness. The child simply lands on another partition of
        // the same process, so the load never leaves this machine.
        if (!config.IsClusterMode)
            warnings.Add(
                $"kahuna.range_split_load_threshold={loadThreshold} is set but this node runs standalone; " +
                "a hot range still divides, but both children stay in this process, so the split buys " +
                "a second local Raft group and moves no load off this machine. Run in cluster mode for " +
                "auto-split to relieve anything.");

        bool leaderBalancer = config.Kahuna.EnableLeaderBalancer ?? false;
        bool placementRebalancer = config.Kahuna.EnablePlacementRebalancer ?? false;
        bool replicated = (config.Kahuna.ReplicationFactor ?? 0) > 0;
        bool loadReports = config.Kahuna.EnableLoadReports ?? false;

        // The trap that looks most like a broken feature. Load reports are gossiped only when
        // something asks for them. Without gossip this node reads 0 operations per second for every
        // partition led elsewhere, so the predicate never holds and the branch is silently dead.
        if (!leaderBalancer && !placementRebalancer && !replicated && !loadReports)
            warnings.Add(
                $"kahuna.range_split_load_threshold={loadThreshold} is set but no load-report source is " +
                "enabled (kahuna.enable_leader_balancer, kahuna.enable_placement_rebalancer, " +
                "kahuna.replication_factor and kahuna.enable_load_reports are all off or zero); a " +
                "partition whose leader lives on another node then reports 0 ops/sec and is never seen " +
                "as hot. Set kahuna.enable_load_reports: true for the signal alone, or " +
                "kahuna.enable_leader_balancer: true for the signal and the relief.");

        // The second trap. The heartbeats of a live peer satisfy Kahuna's relief guard, so the split
        // is granted — but nothing then moves the child leader off this node, so the hot node keeps
        // both halves and pays for one more Raft group.
        else if (!leaderBalancer)
            warnings.Add(
                $"kahuna.range_split_load_threshold={loadThreshold} is set but " +
                "kahuna.enable_leader_balancer is off; a split is granted and its child leader stays on " +
                "the same overloaded node, so the split costs an extra Raft group and relieves nothing. " +
                "Set kahuna.enable_leader_balancer: true to move the child leader away.");

        return warnings;
    }
}
