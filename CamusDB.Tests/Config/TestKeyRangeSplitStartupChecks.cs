
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;

namespace CamusDB.Tests.Config;

/// <summary>
/// Every case here starts the node and then silently does nothing, so a missing warning is the
/// difference between a five-minute config fix and an afternoon spent reading engine source.
/// </summary>
[TestFixture]
public sealed class TestKeyRangeSplitStartupChecks
{
    private static readonly CamusDBOptions Sharded = CamusDBOptions.Default with { KeyRangeShardingEnabled = true };

    /// <summary>A cluster configuration that satisfies every precondition, for a test to break one field of.</summary>
    private static ConfigDefinition HealthyCluster() => new()
    {
        Mode = "cluster",
        InitialPartitions = 3,
        Kahuna = new KahunaOptionsConfig
        {
            RangeSplitLoadThreshold = 200,
            EnableLeaderBalancer = true,
        },
    };

    [Test]
    public void HealthyConfiguration_WarnsAboutNothing()
    {
        Assert.That(KeyRangeSplitStartupChecks.Inspect(HealthyCluster(), Sharded), Is.Empty);
    }

    [Test]
    public void LoadSplitOff_IsNeverWarnedAbout()
    {
        // The preconditions only matter to a deployment that asked for auto-split. A node with the
        // threshold at its default must stay quiet, whatever the rest of the configuration says.
        ConfigDefinition config = new() { Mode = "standalone", InitialPartitions = 1 };

        Assert.That(KeyRangeSplitStartupChecks.Inspect(config, CamusDBOptions.Default), Is.Empty);
    }

    [Test]
    public void KeyRangeShardingOff_IsWarnedAbout()
    {
        ConfigDefinition config = HealthyCluster();

        string[] warnings = [.. KeyRangeSplitStartupChecks.Inspect(config, CamusDBOptions.Default)];

        Assert.That(warnings.Any(w => w.Contains("key_range_sharding is off")), Is.True, string.Join("\n", warnings));
    }

    [Test]
    public void SinglePartition_IsWarnedAbout()
    {
        ConfigDefinition config = HealthyCluster();
        config.InitialPartitions = 1;

        string[] warnings = [.. KeyRangeSplitStartupChecks.Inspect(config, Sharded)];

        Assert.That(warnings.Any(w => w.Contains("range_split_load_threshold") && w.Contains("initial_partitions=1")),
            Is.True, string.Join("\n", warnings));
    }

    [Test]
    public void StandaloneMode_IsWarnedAbout()
    {
        ConfigDefinition config = HealthyCluster();
        config.Mode = "standalone";

        string[] warnings = [.. KeyRangeSplitStartupChecks.Inspect(config, Sharded)];

        Assert.That(warnings.Any(w => w.Contains("standalone")), Is.True, string.Join("\n", warnings));

        // The message must not claim the split cannot happen: a standalone node does split, it just
        // keeps both children in the same process.
        string message = warnings.First(w => w.Contains("standalone"));
        Assert.That(message, Does.Contain("stay in this process"));
    }

    [Test]
    public void NoLoadReportSource_IsWarnedAbout()
    {
        // The trap that looks exactly like a broken feature: without gossip this node reads 0 ops/sec
        // for every partition led elsewhere, so the predicate never holds.
        ConfigDefinition config = HealthyCluster();
        config.Kahuna.EnableLeaderBalancer = false;

        string[] warnings = [.. KeyRangeSplitStartupChecks.Inspect(config, Sharded)];

        Assert.That(warnings.Any(w => w.Contains("no load-report source")), Is.True, string.Join("\n", warnings));

        // The message must name the remedy, not only the fault.
        string message = warnings.First(w => w.Contains("no load-report source"));
        Assert.That(message, Does.Contain("kahuna.enable_load_reports"));
        Assert.That(message, Does.Contain("kahuna.enable_leader_balancer"));
    }

    [Test]
    public void LoadReportsWithoutTheBalancer_IsWarnedAbout()
    {
        // The second trap: the signal arrives and the split is granted, but the child leader stays on
        // the same overloaded node, so the split relieves nothing.
        ConfigDefinition config = HealthyCluster();
        config.Kahuna.EnableLeaderBalancer = false;
        config.Kahuna.EnableLoadReports = true;

        string[] warnings = [.. KeyRangeSplitStartupChecks.Inspect(config, Sharded)];

        Assert.That(warnings.Any(w => w.Contains("relieves nothing")), Is.True, string.Join("\n", warnings));
        Assert.That(warnings.Any(w => w.Contains("no load-report source")), Is.False,
            "load reports are on, so the gossip warning must not fire");
    }

    [Test]
    public void ReplicationFactorAlone_SatisfiesTheGossipCheck()
    {
        // A non-zero replication factor already makes Kahuna gossip load reports, so only the missing
        // balancer is worth a warning here.
        ConfigDefinition config = HealthyCluster();
        config.Kahuna.EnableLeaderBalancer = false;
        config.Kahuna.ReplicationFactor = 3;

        string[] warnings = [.. KeyRangeSplitStartupChecks.Inspect(config, Sharded)];

        Assert.That(warnings.Any(w => w.Contains("no load-report source")), Is.False, string.Join("\n", warnings));
        Assert.That(warnings.Any(w => w.Contains("relieves nothing")), Is.True, string.Join("\n", warnings));
    }

    [Test]
    public void ShardedSinglePartition_IsWarnedAboutWithoutLoadSplit()
    {
        // The pre-existing routing warning is independent of the split policy and must survive.
        ConfigDefinition config = new() { Mode = "cluster", InitialPartitions = 1 };

        string[] warnings = [.. KeyRangeSplitStartupChecks.Inspect(config, Sharded)];

        Assert.That(warnings.Length, Is.EqualTo(1));
        Assert.That(warnings[0], Does.Contain("key_range_sharding is enabled"));
    }
}
