/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;

namespace CamusDB.Tests.Config;

/// <summary>
/// Covers the per-key provenance recorded through the configuration merge, which is what lets
/// <c>SHOW VARIABLES</c> report <em>where</em> a value came from rather than only what it is.
///
/// <para>The property that matters is that the recorded layer is the layer whose value actually won.
/// Provenance is written by each layer as it applies itself, precisely so the two cannot drift apart —
/// a separate pass that tried to infer the source afterwards would be guessing, and a provenance
/// column that is quietly wrong is worse than none at all.</para>
///
/// <para>Serial: one case sets and restores <c>CAMUS_KEY_RANGE_SHARDING</c>, and environment variables
/// are process-wide.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestConfigValueSources
{
    private static ConfigValueSource SourceOf(IReadOnlyDictionary<string, ConfigValueSource> sources, string key)
        => sources.TryGetValue(key, out ConfigValueSource source) ? source : ConfigValueSource.Default;

    [Test]
    public void KeysPresentInTheDocumentAreRecordedAsFileSourced()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: cluster\nhttp_port: 6001");

        Assert.That(SourceOf(config.KeySources, "mode"), Is.EqualTo(ConfigValueSource.ConfigFile));
        Assert.That(SourceOf(config.KeySources, "http_port"), Is.EqualTo(ConfigValueSource.ConfigFile));

        // A key the document never mentioned is a default, not a file value.
        Assert.That(SourceOf(config.KeySources, "spill_enabled"), Is.EqualTo(ConfigValueSource.Default));
    }

    /// <summary>
    /// Nested sections are recorded per sub-key, not per section: an operator asking where
    /// <c>kahuna.wal_sync_writes</c> came from wants that answered, not the section it lives under.
    /// </summary>
    [Test]
    public void NestedSectionKeysAreRecordedIndividually()
    {
        ConfigDefinition config = new ConfigReader().Read("kahuna:\n  wal_sync_writes: false\n  locks_workers: 4");

        Assert.That(SourceOf(config.KeySources, "kahuna.wal_sync_writes"), Is.EqualTo(ConfigValueSource.ConfigFile));
        Assert.That(SourceOf(config.KeySources, "kahuna.locks_workers"), Is.EqualTo(ConfigValueSource.ConfigFile));
        Assert.That(SourceOf(config.KeySources, "kahuna.storage"), Is.EqualTo(ConfigValueSource.Default));

        // The section itself is a root key of the document, so it is recorded too — but it is not a
        // setting, and nothing reports it as one.
        Assert.That(SourceOf(config.KeySources, "kahuna"), Is.EqualTo(ConfigValueSource.ConfigFile));
    }

    /// <summary>
    /// A flag beats the file, and the recorded layer moves with the value. Every CLI override records
    /// itself — previously only three keys left any trace at all, which made a flag-supplied value
    /// indistinguishable from a file-supplied one.
    /// </summary>
    [Test]
    public void CommandLineOverridesRecordThemselvesAndReplaceTheFileLayer()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: standalone\nhttp_port: 6001\nraft_port: 7001");

        ConfigResolver.ApplyCliOverrides(config, new ConfigCliOverrides { HttpPort = 9999, NodeName = "node-a" });

        Assert.That(config.HttpPort, Is.EqualTo(9999));
        Assert.That(SourceOf(config.KeySources, "http_port"), Is.EqualTo(ConfigValueSource.CommandLine));

        // A flag with no file value behind it is still recorded.
        Assert.That(SourceOf(config.KeySources, "node_name"), Is.EqualTo(ConfigValueSource.CommandLine));

        // A key the flags did not touch keeps its file provenance.
        Assert.That(SourceOf(config.KeySources, "raft_port"), Is.EqualTo(ConfigValueSource.ConfigFile));
    }

    [Test]
    public void EnvironmentOverrideIsRecordedAsEnvironment()
    {
        string? previous = Environment.GetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING");
        try
        {
            Environment.SetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING", "true");

            ConfigDefinition config = new ConfigReader().Read("key_range_sharding: false");
            CamusDBOptions resolved = ConfigResolver.Resolve(config);

            Assert.That(resolved.KeyRangeShardingEnabled, Is.True, "the environment must win over the file");
            Assert.That(
                SourceOf(resolved.ValueSources, "key_range_sharding"),
                Is.EqualTo(ConfigValueSource.Environment),
                "the layer that won the value must be the layer reported");
        }
        finally
        {
            Environment.SetEnvironmentVariable("CAMUS_KEY_RANGE_SHARDING", previous);
        }
    }

    /// <summary>
    /// The resolved options carry a snapshot, not a live view: the definition stays writable after
    /// resolution while the options record does not, and an options instance that changed underneath a
    /// reader would defeat the immutability the record exists for.
    /// </summary>
    [Test]
    public void ResolvedOptionsCarryASnapshotOfTheProvenance()
    {
        ConfigDefinition config = new ConfigReader().Read("ttl_enabled: false");

        CamusDBOptions resolved = ConfigResolver.Resolve(config);
        Assert.That(SourceOf(resolved.ValueSources, "ttl_enabled"), Is.EqualTo(ConfigValueSource.ConfigFile));

        config.RecordSource("spill_enabled", ConfigValueSource.CommandLine);

        Assert.That(
            SourceOf(resolved.ValueSources, "spill_enabled"),
            Is.EqualTo(ConfigValueSource.Default),
            "a later write to the definition must not mutate already-resolved options");
    }

    /// <summary>
    /// An options record built directly — as every test and any in-process composition does — reports
    /// everything as a default rather than inventing a layer it cannot know about.
    /// </summary>
    [Test]
    public void OptionsBuiltWithoutAMergeReportNoProvenance()
    {
        Assert.That(CamusDBOptions.Default.ValueSources, Is.Empty);
        Assert.That(SourceOf(CamusDBOptions.Default.ValueSources, "ttl_enabled"), Is.EqualTo(ConfigValueSource.Default));
    }
}
