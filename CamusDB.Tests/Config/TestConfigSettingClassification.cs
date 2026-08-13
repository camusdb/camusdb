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
/// Guards the setting classification the same way the variable catalog is guarded: by making the
/// omission fail the build's tests instead of defaulting silently.
///
/// <para>Every settable option must declare mutability (runtime / restart) and scope (cluster /
/// node) via <see cref="ConfigSettingAttribute"/>. Defaulting an unclassified setting to restart
/// would hide new knobs from operators forever; defaulting to runtime would promise hot-swap for a
/// value read once in a constructor. Neither failure is visible at the moment it is introduced,
/// which is why the attribute is required rather than optional.</para>
/// </summary>
[TestFixture]
internal sealed class TestConfigSettingClassification
{
    /// <summary>
    /// The exact property set the catalog enumerates: settable, and not the flattened Kahuna
    /// section or the provenance metadata. Kept identical to <see cref="ConfigVariableCatalog"/>'s
    /// rule so a property can never be a reported variable without a classification or vice versa.
    /// </summary>
    private static IEnumerable<PropertyInfo> SettingProperties() =>
        typeof(CamusDBOptions)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.SetMethod is not null &&
                        p.Name is not (nameof(CamusDBOptions.Kahuna) or nameof(CamusDBOptions.ValueSources)));

    [Test]
    public void EverySettingCarriesAClassification()
    {
        List<string> unclassified = SettingProperties()
            .Where(p => p.GetCustomAttribute<ConfigSettingAttribute>() is null)
            .Select(p => p.Name)
            .ToList();

        Assert.IsEmpty(
            unclassified,
            "These settings carry no [ConfigSetting(mutability, scope)] attribute. Classify each one: " +
            "runtime only if the component that reads it genuinely picks up a new value without a " +
            "restart, restart otherwise; cluster if the fleet must agree, node if it is deliberately " +
            "per-node: " + string.Join(", ", unclassified));
    }

    /// <summary>
    /// The catalog reports the classification for every variable, and the nested <c>kahuna.*</c>
    /// section is restart/node by rule — the embedded node is built once from those values before
    /// anything else starts and is never rebuilt.
    /// </summary>
    [Test]
    public void CatalogReportsClassificationForEveryVariable()
    {
        IReadOnlyList<ConfigVariable> catalog = ConfigVariableCatalog.Describe(CamusDBOptions.Default);

        Assert.IsNotEmpty(catalog);

        foreach (ConfigVariable variable in catalog.Where(v => v.Name.StartsWith("kahuna.", StringComparison.Ordinal)))
        {
            Assert.AreEqual(ConfigMutability.Restart, variable.Mutability, variable.Name);
            Assert.AreEqual(ConfigScope.Node, variable.Scope, variable.Name);
        }
    }

    /// <summary>
    /// Settings whose value is baked into something constructed once per process must not claim
    /// runtime mutability. This list pins the claims that are structural rather than judgment
    /// calls: the data directory and node identity define the process, the secrets are read once
    /// at boot, and the test-only spill override has no production reader at all.
    /// </summary>
    [Test]
    public void StructurallyRestartBoundSettingsAreNotClassifiedRuntime()
    {
        string[] restartBound =
        [
            nameof(CamusDBOptions.DataDirectory),
            nameof(CamusDBOptions.ClusterPartitionCount),
            nameof(CamusDBOptions.KeyRangeShardingEnabled),
            nameof(CamusDBOptions.AuthenticationEnabled),
            nameof(CamusDBOptions.BootstrapSuperuser),
            nameof(CamusDBOptions.BootstrapSuperuserPassword),
            nameof(CamusDBOptions.AccessTokenServerKey),
            nameof(CamusDBOptions.NodeSecret),
            nameof(CamusDBOptions.ForceSpillThresholdRows),
        ];

        foreach (string name in restartBound)
        {
            ConfigSettingAttribute attribute = typeof(CamusDBOptions).GetProperty(name)!
                .GetCustomAttribute<ConfigSettingAttribute>()!;

            Assert.AreEqual(ConfigMutability.Restart, attribute.Mutability, name);
        }
    }
}
