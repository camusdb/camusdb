/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;

namespace CamusDB.Tests.Config;

/// <summary>
/// Covers the <c>engine_metrics_enabled</c> knob that gates the in-process listener behind
/// <c>SHOW ENGINE STATS</c>: it must be an accepted root key, reach <see cref="CamusDBOptions"/>, and
/// default to on so the statement is useful on an unconfigured node.
/// </summary>
[TestFixture]
public sealed class TestConfigReaderEngineMetrics
{
    [Test]
    public void EngineMetricsKey_AcceptedByReader_ValueApplied()
    {
        ConfigDefinition config = new ConfigReader().Read("engine_metrics_enabled: false");

        Assert.That(config.EngineMetricsEnabled, Is.False);
    }

    /// <summary>
    /// Default on: the feature exists to be reachable from a console without configuring anything, and
    /// the flag is only an escape hatch for benchmarking the measurement overhead away.
    /// </summary>
    [Test]
    public void EngineMetricsKey_Omitted_DefaultsToEnabled()
    {
        Assert.That(new ConfigReader().Read("mode: standalone").EngineMetricsEnabled, Is.True);
        Assert.That(CamusDBOptions.Default.EngineMetricsEnabled, Is.True);
    }

    /// <summary>
    /// The reader rejects unknown root keys, so a typo must fail startup rather than silently leaving
    /// collection at its default and producing a confusing empty result.
    /// </summary>
    [Test]
    public void MisspelledEngineMetricsKey_Rejected()
    {
        Assert.Throws<CamusDBException>(() => new ConfigReader().Read("engine_metrics: false"));
    }

    [Test]
    public void EngineMetricsSetting_ReachesResolvedOptions()
    {
        ConfigDefinition config = new ConfigReader().Read("engine_metrics_enabled: false");
        CamusDBOptions options = ConfigResolver.Resolve(config);

        Assert.That(options.EngineMetricsEnabled, Is.False);
    }
}
