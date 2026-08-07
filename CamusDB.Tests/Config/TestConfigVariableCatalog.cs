/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;

namespace CamusDB.Tests.Config;

/// <summary>
/// Guards the reflection-driven variable catalog against the two ways it can silently go wrong.
///
/// <para>Because <see cref="ConfigVariableCatalog"/> enumerates <see cref="CamusDBOptions"/> by
/// reflection, a new tunable appears in <c>SHOW VARIABLES</c> for free — which is the point, since a
/// hand-written registry would have drifted the first time someone forgot a row. What reflection
/// cannot get right is the <em>name</em>: it derives one mechanically, and if the option was wired
/// into the YAML under a different key, the statement reports a name no operator can put in their
/// config file. <see cref="EveryVariableIsAConfigurableKeyOrIsDeclaredNotToBe"/> is the check that
/// makes that mismatch impossible to ship unnoticed — it fails until the author either wires the key
/// or states, in the list below, that the setting is deliberately not file-configurable.</para>
/// </summary>
[TestFixture]
internal sealed class TestConfigVariableCatalog
{
    /// <summary>
    /// Settings that are genuinely not settable from <c>config.yml</c> and so have no allow-listed
    /// YAML key: the security-sensitive ones the host reads only from the environment (a plaintext
    /// token key or bootstrap password in a config file would be a leak), and the internal tunables
    /// that <see cref="ConfigResolver.Resolve"/> deliberately never maps.
    ///
    /// <para>Adding a name here is a decision, not a formality — it asserts the setting is meant to be
    /// unreachable from the configuration file. An option that simply has not been wired yet belongs
    /// in <see cref="ConfigReader.AllowedRootKeys"/> and <see cref="ConfigResolver.Resolve"/>, not
    /// here.</para>
    /// </summary>
    private static readonly HashSet<string> NotFileConfigurable = new(StringComparer.Ordinal)
    {
        // Read from the environment only, never from a file.
        "authentication_enabled",
        "access_token_server_key",
        "access_token_ttl",
        "bootstrap_superuser",
        "bootstrap_superuser_password",
        "node_secret",

        // Internal tunables with no YAML surface.
        "authentication_cache_max_entries",
        "authentication_cache_ttl",
        "borrowed_decode",
        "default_decision_durability",
        "default_read_validation",
        "fence_lease_ms",
        "fence_lease_renew_interval_ms",
        "force_spill_threshold_rows",
        "hash_join_max_build_rows",
        "index_scan_fetch_batch_size",
        "keyspace_purge_batch_size",
        "lock_tracing_enabled",
        "login_kdf_max_concurrency",
        "login_max_attempts_per_minute",
        "login_rate_limit_max_entries",
        "net_weight",
        "password_hash_iterations",
        "query_tracing_enabled",
        "slot_backed_decode",
        "spill_max_frame_bytes",
        "pitr_window_seconds",
    };

    private static IReadOnlyList<ConfigVariable> Catalog() => ConfigVariableCatalog.Describe(CamusDBOptions.Default);

    /// <summary>
    /// The name-drift guard. Every reported variable must be a key an operator could actually write —
    /// a root key, a key of the flattened <c>kahuna:</c> section — or be declared above as
    /// deliberately not file-configurable.
    /// </summary>
    [Test]
    public void EveryVariableIsAConfigurableKeyOrIsDeclaredNotToBe()
    {
        List<string> unknown = [];

        foreach (ConfigVariable variable in Catalog())
        {
            bool known = variable.Name.StartsWith("kahuna.", StringComparison.Ordinal)
                ? KahunaAllowedYamlKeys().Contains(variable.Name["kahuna.".Length..])
                : ConfigReaderAllowedRootKeys().Contains(variable.Name);

            if (!known && !NotFileConfigurable.Contains(variable.Name))
                unknown.Add(variable.Name);
        }

        Assert.IsEmpty(
            unknown,
            "These settings are reported under a name no config file accepts. Either wire the key into " +
            "ConfigReader.AllowedRootKeys and ConfigResolver.Resolve, add a rename to " +
            "ConfigVariableCatalog.YamlNameOverrides, or declare it not file-configurable in this test: " +
            string.Join(", ", unknown));
    }

    /// <summary>
    /// Every option is reported, exactly once. A silently skipped property would leave an operator
    /// unable to see a setting that is nonetheless in force.
    /// </summary>
    [Test]
    public void EveryOptionIsReportedExactlyOnce()
    {
        int optionCount = typeof(CamusDBOptions)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Count(p => p.SetMethod is not null &&
                        p.Name is not (nameof(CamusDBOptions.Kahuna) or nameof(CamusDBOptions.ValueSources)));

        int kahunaCount = typeof(KahunaOptionsConfig)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Count(p => p.SetMethod is not null);

        List<string> names = Catalog().Select(v => v.Name).ToList();

        Assert.AreEqual(optionCount + kahunaCount, names.Count);
        Assert.AreEqual(names.Count, names.Distinct(StringComparer.Ordinal).Count(), "names must be unique");
    }

    /// <summary>
    /// A property computed from other settings is a view of the configuration, not part of it. Listing
    /// it would hand the operator a name no config file accepts and double-count what it derives from.
    /// </summary>
    [Test]
    public void ComputedPropertiesAreNotReportedAsSettings()
    {
        HashSet<string> names = Catalog().Select(v => v.Name).ToHashSet(StringComparer.Ordinal);

        CollectionAssert.DoesNotContain(names, "spill_effective_threshold");

        // The settings it derives from are listed, so nothing is actually hidden.
        CollectionAssert.Contains(names, "spill_threshold_rows");
        CollectionAssert.Contains(names, "force_spill_threshold_rows");
    }

    [Test]
    public void VariablesAreOrdinalSortedByName()
    {
        List<string> names = Catalog().Select(v => v.Name).ToList();

        CollectionAssert.AreEqual(names.OrderBy(n => n, StringComparer.Ordinal).ToList(), names);
    }

    /// <summary>
    /// A configured secret is masked; an unconfigured one stays empty, so "no secret set" and "a secret
    /// is set" remain tellable apart without ever printing one.
    /// </summary>
    [Test]
    public void SecretsAreMaskedOnlyWhenTheyHoldSomething()
    {
        const string secret = "s3cr3t-material";

        CamusDBOptions configured = CamusDBOptions.Default with
        {
            BootstrapSuperuserPassword = secret,
            AccessTokenServerKey = secret,
            NodeSecret = secret,
        };

        Dictionary<string, ConfigVariable> masked = ConfigVariableCatalog.Describe(configured)
            .ToDictionary(v => v.Name, StringComparer.Ordinal);
        Dictionary<string, ConfigVariable> unset = Catalog().ToDictionary(v => v.Name, StringComparer.Ordinal);

        foreach (string name in new[] { "bootstrap_superuser_password", "access_token_server_key", "node_secret" })
        {
            Assert.AreEqual("********", masked[name].Value, name);
            Assert.AreEqual("", unset[name].Value, name);
        }

        Assert.IsFalse(
            ConfigVariableCatalog.Describe(configured).Any(v => v.Value == secret || v.Default == secret),
            "no column may carry the secret");
    }

    /// <summary>
    /// Certificate and key-file settings hold paths, not key material, and an operator debugging a
    /// misconfigured deployment needs to see them. This pins that as a decision rather than an
    /// oversight, so a later change that starts masking them has to be deliberate.
    /// </summary>
    [Test]
    public void FilePathSettingsAreNotMasked()
    {
        CamusDBOptions options = CamusDBOptions.Default with
        {
            Kahuna = new KahunaOptionsConfig { BackupMacKeyFile = "/etc/camusdb/backup.key" },
        };

        ConfigVariable keyFile = ConfigVariableCatalog.Describe(options)
            .Single(v => v.Name == "kahuna.backup_mac_key_file");

        Assert.AreEqual("/etc/camusdb/backup.key", keyFile.Value);
    }

    /// <summary>
    /// The four renames exist because the option record and the YAML key genuinely disagree; if one is
    /// ever unified, this test says so rather than letting a now-wrong override sit unnoticed.
    /// </summary>
    [Test]
    public void RenamedOptionsAreReportedUnderTheirConfigFileKey()
    {
        HashSet<string> names = Catalog().Select(v => v.Name).ToHashSet(StringComparer.Ordinal);

        foreach (string key in new[]
        {
            "data_dir", "initial_partitions", "key_range_sharding", "query_result_cache_singleflight_wait_ms",
        })
            CollectionAssert.Contains(names, key);

        // The mechanically-underscored spellings must not also appear.
        foreach (string derived in new[]
        {
            "data_directory", "cluster_partition_count", "key_range_sharding_enabled",
            "query_result_cache_single_flight_wait_ms",
        })
            CollectionAssert.DoesNotContain(names, derived);
    }

    /// <summary>Absent provenance means nobody overrode the setting, which is a default, not a guess.</summary>
    [Test]
    public void UnrecordedProvenanceIsDefault()
    {
        Assert.IsTrue(Catalog().All(v => v.Source == ConfigValueSource.Default));

        CamusDBOptions options = CamusDBOptions.Default with
        {
            ValueSources = new Dictionary<string, ConfigValueSource>(StringComparer.OrdinalIgnoreCase)
            {
                ["ttl_enabled"] = ConfigValueSource.ConfigFile,
                ["kahuna.wal_sync_writes"] = ConfigValueSource.ConfigFile,
            },
        };

        Dictionary<string, ConfigVariable> described = ConfigVariableCatalog.Describe(options)
            .ToDictionary(v => v.Name, StringComparer.Ordinal);

        Assert.AreEqual(ConfigValueSource.ConfigFile, described["ttl_enabled"].Source);
        Assert.AreEqual(ConfigValueSource.ConfigFile, described["kahuna.wal_sync_writes"].Source);
        Assert.AreEqual(ConfigValueSource.Default, described["spill_enabled"].Source);
    }

    // ConfigReader.AllowedRootKeys and KahunaOptionsConfig.AllowedYamlKeys are internal to
    // CamusDB.Core; reach them reflectively rather than widening their visibility for a test.
    private static HashSet<string> ConfigReaderAllowedRootKeys()
        => (HashSet<string>)typeof(ConfigReader)
            .GetField("AllowedRootKeys", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

    private static HashSet<string> KahunaAllowedYamlKeys()
        => (HashSet<string>)typeof(KahunaOptionsConfig)
            .GetField("AllowedYamlKeys", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;
}
