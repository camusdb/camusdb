/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Runtime;
using NUnit.Framework;

using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;

namespace CamusDB.Tests.Config;

/// <summary>
/// A node whose server GC adapts the young generation to the load, while the write aggregator runs
/// short cycles, starts and serves normally and can lose a tenth to a quarter of a leader's time to
/// collections under load. These tests pin which combinations of GC state and aggregator settings
/// produce the startup warning, and that the warning names the settings that remove it.
/// </summary>
[TestFixture]
public sealed class TestGcStartupChecks
{
    [Test]
    public void AdaptationOff_WarnsAboutNothing()
    {
        // The state the server build ships in, with every aggregator setting at its default.
        Assert.That(GcStartupChecks.Inspect(new KahunaOptionsConfig(), isClusterMode: true, isServerGc: true, dynamicAdaptationMode: 0), Is.Empty);
    }

    [Test]
    public void AdaptationNotReported_WarnsAboutNothing()
    {
        Assert.That(GcStartupChecks.Inspect(new KahunaOptionsConfig(), isClusterMode: true, isServerGc: true, dynamicAdaptationMode: null), Is.Empty);
    }

    [Test]
    public void WorkstationGc_WarnsAboutNothing()
    {
        // The runtime reports mode 1 under workstation GC too, where the mode does nothing.
        Assert.That(GcStartupChecks.Inspect(new KahunaOptionsConfig(), isClusterMode: true, isServerGc: false, dynamicAdaptationMode: 1), Is.Empty);
    }

    [Test]
    public void AdaptationOn_WithDefaultAggregatorSettings_Warns()
    {
        // The defaults end the hold at its deadline, so a deployment that only sets
        // DOTNET_GCDynamicAdaptationMode=1 is the exposed configuration.
        IReadOnlyList<string> warnings = GcStartupChecks.Inspect(new KahunaOptionsConfig(), isClusterMode: true, isServerGc: true, dynamicAdaptationMode: 1);

        Assert.That(warnings, Has.Count.EqualTo(1));
        Assert.That(warnings[0], Does.Contain("DOTNET_GCDynamicAdaptationMode"));
        Assert.That(warnings[0], Does.Contain("kahuna.key_value_write_precise_wake is on"));
        Assert.That(warnings[0], Does.Contain("System.GC.DynamicAdaptationMode = 1"));
    }

    [Test]
    public void AdaptationOn_WithTheTimerQueueHold_WarnsAboutNothing()
    {
        // On the timer queue a hold of 1 ms or more lasts a whole kernel tick, and no run collapsed.
        KahunaOptionsConfig timerQueue = new() { KeyValueWritePreciseWake = false };
        Assert.That(GcStartupChecks.Inspect(timerQueue, isClusterMode: true, isServerGc: true, dynamicAdaptationMode: 1), Is.Empty);

        KahunaOptionsConfig longerHold = new() { KeyValueWritePreciseWake = false, KeyValueWritePostCompletionHoldMs = 2 };
        Assert.That(GcStartupChecks.Inspect(longerHold, isClusterMode: true, isServerGc: true, dynamicAdaptationMode: 1), Is.Empty);
    }

    [Test]
    public void AdaptationOn_WithNoHold_Warns_OnEitherWakePath()
    {
        // With no hold each round follows the one before it at once, whichever timer fires the wakes.
        KahunaOptionsConfig timerQueue = new() { KeyValueWritePreciseWake = false, KeyValueWritePostCompletionHoldMs = 0 };
        IReadOnlyList<string> onTimerQueue = GcStartupChecks.Inspect(timerQueue, isClusterMode: true, isServerGc: true, dynamicAdaptationMode: 1);

        Assert.That(onTimerQueue, Has.Count.EqualTo(1));
        Assert.That(onTimerQueue[0], Does.Contain("kahuna.key_value_write_post_completion_hold_ms is 0"));

        KahunaOptionsConfig timerThread = new() { KeyValueWritePostCompletionHoldMs = 0 };
        Assert.That(GcStartupChecks.Inspect(timerThread, isClusterMode: true, isServerGc: true, dynamicAdaptationMode: 1), Has.Count.EqualTo(1));
    }

    [Test]
    public void AdaptationOn_OnAStandaloneNode_Warns_WithAnUnsetHold_OnEitherWakePath()
    {
        // A standalone node runs with no hold unless one is configured, so the timer queue alone
        // does not lengthen its cycles.
        KahunaOptionsConfig timerQueue = new() { KeyValueWritePreciseWake = false };
        IReadOnlyList<string> onTimerQueue = GcStartupChecks.Inspect(timerQueue, isClusterMode: false, isServerGc: true, dynamicAdaptationMode: 1);

        Assert.That(onTimerQueue, Has.Count.EqualTo(1));
        Assert.That(onTimerQueue[0], Does.Contain("kahuna.key_value_write_post_completion_hold_ms is 0"));
        Assert.That(GcStartupChecks.Inspect(new KahunaOptionsConfig(), isClusterMode: false, isServerGc: true, dynamicAdaptationMode: 1), Has.Count.EqualTo(1));

        // A configured hold on the timer queue lasts a whole kernel tick there too.
        KahunaOptionsConfig held = new() { KeyValueWritePreciseWake = false, KeyValueWritePostCompletionHoldMs = 1 };
        Assert.That(GcStartupChecks.Inspect(held, isClusterMode: false, isServerGc: true, dynamicAdaptationMode: 1), Is.Empty);
    }

    [Test]
    public void ReadDynamicAdaptationMode_AgreesWithInspectOnThisProcess()
    {
        // The test host runs workstation GC, so whatever mode the runtime reports here, the check
        // stays silent. The read itself must not throw on a runtime that reports no such value.
        long? mode = GcStartupChecks.ReadDynamicAdaptationMode();

        Assert.That(mode is null or >= 0, Is.True);
        if (!GCSettings.IsServerGC)
            Assert.That(GcStartupChecks.Inspect(new KahunaOptionsConfig(), isClusterMode: true, GCSettings.IsServerGC, mode), Is.Empty);
    }
}
