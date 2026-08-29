/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using CamusDB.App.Services;
using Kahuna;
using NUnit.Framework;

namespace CamusDB.Tests.Diagnostics;

/// <summary>
/// Covers the resolved-engine-settings surface. Its whole reason to exist is that
/// <c>SHOW VARIABLES</c> reports the configuration layer, so a durability knob nobody wrote into a
/// file reads as unset there — leaving a benchmark unable to state whether synchronous WAL was on for
/// the run it is about to compare.
/// </summary>
[TestFixture]
public sealed class EngineSettingsEndpointTests
{
    /// <summary>
    /// Reads the settings straight from an options instance. No engine is constructed: this is string
    /// formatting, and standing one up would register process-wide meters and leave a background
    /// engine running for no benefit to what is being asserted.
    /// </summary>
    private static IReadOnlyDictionary<string, string> Settings(EmbeddedKahunaOptions options)
        => EngineSettingsReader.Describe(options);

    private static EmbeddedKahunaOptions Options() => new() { Storage = "memory", WalStorage = "memory" };

    [Test]
    public void ReportsASettingNobodyConfigured()
    {
        // The point of the endpoint: the value in force, whether or not it was written down.
        IReadOnlyDictionary<string, string> settings = Settings(Options());

        Assert.That(settings.ContainsKey("RaftWalGroupCommitLingerMs"), Is.True);
        Assert.That(settings.ContainsKey("WalSyncWrites"), Is.True);
        Assert.That(settings.ContainsKey("RaftWalSingleFsyncCommit"), Is.True);
    }

    [Test]
    public void ReportsTheValueTheEngineActuallyReceived()
    {
        EmbeddedKahunaOptions options = Options();
        options.RaftWalGroupCommitLingerMs = 7;
        options.WalSyncWrites = true;

        IReadOnlyDictionary<string, string> settings = Settings(options);

        Assert.That(settings["RaftWalGroupCommitLingerMs"], Is.EqualTo("7"));
        Assert.That(settings["WalSyncWrites"], Is.EqualTo("true"));
    }

    [Test]
    public void RedactsACredentialButStillSaysWhetherOneIsSet()
    {
        // Whether auth is configured changes behaviour and belongs in a manifest; the token does not.
        EmbeddedKahunaOptions options = Options();
        options.HttpAuthBearerToken = "super-secret-value";

        IReadOnlyDictionary<string, string> settings = Settings(options);

        Assert.That(settings["HttpAuthBearerToken"], Is.EqualTo("***redacted***"));
        Assert.That(settings.Values, Has.None.Contains("super-secret-value"));
    }

    [Test]
    public void DoesNotRedactABatchingKnobThatMerelyContainsKey()
    {
        // A substring match on "key" would hide KeyValueWriteLingerMs, one of the settings this
        // endpoint exists to report.
        IReadOnlyDictionary<string, string> settings = Settings(Options());

        Assert.That(settings["KeyValueWriteLingerMs"], Does.Not.Contain("redacted"));
    }

    [Test]
    public void OrdersSettingsSoTwoNodesCompareAsText()
    {
        List<string> names = Settings(Options()).Keys.ToList();

        Assert.That(names, Is.EqualTo(names.OrderBy(n => n, StringComparer.Ordinal).ToList()));
    }

    [Test]
    public void ReportsEverySettingRatherThanAChosenFew()
    {
        // Read by reflection on purpose: a hand-written list stops reporting a field added later, and
        // a missing setting looks exactly like one that was never configured.
        Assert.That(Settings(Options()), Has.Count.GreaterThan(100));
    }
}
