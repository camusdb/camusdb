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
/// Verifies that the spill-to-disk YAML knobs are parsed, validated, and applied to
/// <see cref="CamusDBOptions"/> correctly, and that an unknown or misspelled spill key
/// is rejected at startup rather than silently dropped.
///
/// <para>Non-parallelizable: applying a config rewrites <b>every</b> process-wide knob from that one
/// definition, not just the keys the YAML mentions, so a fixture that applies a config concurrently
/// with this one would reset the spill values back to their defaults mid-assertion.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestConfigReaderSpill
{
    // ── Reader: accepts valid spill config ───────────────────────────────────

    /// <summary>
    /// A config.yml containing all three spill keys is accepted (not rejected as unknown)
    /// and the parsed values match the YAML.
    /// </summary>
    [Test]
    public void SpillKeys_AcceptedByReader_ValuesApplied()
    {
        const string yml = """
            spill_enabled: true
            spill_threshold_rows: 10000
            spill_merge_fan_in: 8
            """;

        ConfigDefinition config = new ConfigReader().Read(yml);

        Assert.That(config.SpillEnabled,       Is.True,    "spill_enabled must be read as true");
        Assert.That(config.SpillThresholdRows, Is.EqualTo(10000), "spill_threshold_rows must be read");
        Assert.That(config.SpillMergeFanIn,    Is.EqualTo(8),     "spill_merge_fan_in must be read");
    }

    /// <summary>
    /// When all three spill keys are omitted, the built-in defaults are preserved.
    /// </summary>
    [Test]
    public void SpillKeys_Omitted_DefaultsUnchanged()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: standalone");

        Assert.That(config.SpillEnabled,       Is.False,              "default spill_enabled must be false");
        Assert.That(config.SpillThresholdRows, Is.EqualTo(500_000),   "default spill_threshold_rows must be 500000");
        Assert.That(config.SpillMergeFanIn,    Is.EqualTo(16),        "default spill_merge_fan_in must be 16");
    }

    /// <summary>
    /// An unknown spill-related key (e.g. a misspelling) is rejected, proving the
    /// allow-list covers these keys and that a typo is caught at startup.
    /// </summary>
    [Test]
    public void UnknownSpillKey_RejectedAsUnknown()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("spill_treshold_rows: 1000"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("spill_treshold_rows"),
            "Error message should name the unknown key");
    }

    // ── Validation: rejects invalid values ───────────────────────────────────

    /// <summary>
    /// <c>spill_threshold_rows: 0</c> must be rejected because 0 is not a valid row cap.
    /// </summary>
    [Test]
    public void SpillThresholdRows_Zero_InvalidConfig()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("spill_threshold_rows: 0"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("spill_threshold_rows"));
    }

    /// <summary>
    /// <c>spill_threshold_rows: -1</c> must also be rejected.
    /// </summary>
    [Test]
    public void SpillThresholdRows_Negative_InvalidConfig()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("spill_threshold_rows: -1"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
    }

    /// <summary>
    /// <c>spill_merge_fan_in: 0</c> must be rejected because 0 is not a valid k-way fan-in.
    /// </summary>
    [Test]
    public void SpillMergeFanIn_Zero_InvalidConfig()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("spill_merge_fan_in: 0"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("spill_merge_fan_in"));
    }

    /// <summary>
    /// <c>spill_merge_fan_in: -4</c> must also be rejected.
    /// </summary>
    [Test]
    public void SpillMergeFanIn_Negative_InvalidConfig()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("spill_merge_fan_in: -4"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
    }

    // ── Resolver: values reach the resolved options ──────────────────────────

    /// <summary>
    /// The resolver must carry all three spill fields onto the options it produces — those options are
    /// what an engine is built from, so a field the resolver drops is a YAML key that silently does
    /// nothing.
    /// </summary>
    [Test]
    public void Resolver_CarriesSpillKnobsOntoTheResolvedOptions()
    {
        const string yml = """
            spill_enabled: true
            spill_threshold_rows: 25000
            spill_merge_fan_in: 32
            """;

        CamusDBOptions resolved = ConfigResolver.Resolve(new ConfigReader().Read(yml));

        Assert.That(resolved.SpillEnabled,       Is.True,            "resolver must carry SpillEnabled");
        Assert.That(resolved.SpillThresholdRows, Is.EqualTo(25_000), "resolver must carry SpillThresholdRows");
        Assert.That(resolved.SpillMergeFanIn,    Is.EqualTo(32),     "resolver must carry SpillMergeFanIn");
    }

    /// <summary>
    /// When the spill keys are absent from config.yml, the resolved options must hold the built-in
    /// defaults rather than anything left over from another configuration.
    /// </summary>
    [Test]
    public void Resolver_OmittedSpillKeys_PreservesDefaults()
    {
        CamusDBOptions resolved = ConfigResolver.Resolve(new ConfigReader().Read("mode: standalone"));

        Assert.That(resolved.SpillEnabled,       Is.False,            "omitted spill_enabled must default to false");
        Assert.That(resolved.SpillThresholdRows, Is.EqualTo(500_000), "omitted spill_threshold_rows must default to 500000");
        Assert.That(resolved.SpillMergeFanIn,    Is.EqualTo(16),      "omitted spill_merge_fan_in must default to 16");
    }
}
