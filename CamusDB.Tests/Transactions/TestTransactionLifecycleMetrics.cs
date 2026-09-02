/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// The transaction lifecycle instruments — <c>camus.transaction.count</c> and
/// <c>camus.transaction.active</c> — recorded from the transaction manager's tracking map.
///
/// <para>What these tests defend is the property that makes the instruments usable rather than
/// merely present: <b>exactly one begin and exactly one terminal event per transaction, and a gauge
/// that returns to zero.</b> A commit counted twice on a retried finalize would overstate throughput
/// in every run that used it, and an up/down counter that decrements one time fewer than it
/// increments climbs forever and reads as a transaction leak that is not happening. Both failure
/// modes are silent, which is why they are pinned here rather than left to inspection.</para>
///
/// <para>Non-parallelizable: <see cref="ServerDiagnostics.Enabled"/> is a process-wide gate and the
/// listener below observes a process-wide meter, so a concurrent fixture recording transactions would
/// be counted into these assertions.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestTransactionLifecycleMetrics
{
    private sealed record Captured(string Instrument, long Value, string? Operation, string? Outcome, string? Mode);

    private static (MeterListener Listener, List<Captured> Events) StartListener()
    {
        List<Captured> events = new();
        MeterListener listener = new();

        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == ServerDiagnostics.MeterName &&
                instrument.Name is "camus.transaction.count" or "camus.transaction.active")
                l.EnableMeasurementEvents(instrument);
        };

        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            // Copied out of the ref struct before anything else: a ReadOnlySpan cannot be captured by
            // the local function that would otherwise read it.
            string? operation = null, outcome = null, mode = null;
            foreach (KeyValuePair<string, object?> tag in tags)
            {
                switch (tag.Key)
                {
                    case "operation": operation = tag.Value?.ToString(); break;
                    case "outcome": outcome = tag.Value?.ToString(); break;
                    case "transaction_mode": mode = tag.Value?.ToString(); break;
                }
            }

            lock (events)
                events.Add(new Captured(instrument.Name, value, operation, outcome, mode));
        });

        listener.Start();
        return (listener, events);
    }

    private static async Task<(EmbeddedKahuna Node, KvTransactionsManager Manager)> CreateAsync(
        string warmupKey, CamusDBOptions? options = null)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{warmupKey}/warmup", CancellationToken.None);

        return (node, new KvTransactionsManager(node.Kahuna, options ?? CamusDBOptions.Default));
    }

    private static int Count(List<Captured> events, string operation, string outcome)
    {
        lock (events)
            return events.Count(e =>
                e.Instrument == "camus.transaction.count" && e.Operation == operation && e.Outcome == outcome);
    }

    /// <summary>Net value of the up/down counter — the number of transactions it claims are live.</summary>
    private static long ActiveGauge(List<Captured> events)
    {
        lock (events)
            return events.Where(e => e.Instrument == "camus.transaction.active").Sum(e => e.Value);
    }

    [TearDown]
    public void ResetGate() => ServerDiagnostics.Enabled = false;

    [Test]
    public async Task CommitAndRollbackAreEachCountedOnce()
    {
        (MeterListener listener, List<Captured> events) = StartListener();
        using MeterListener _ = listener;

        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("tlm1");
        await using EmbeddedKahuna __ = node;

        ServerDiagnostics.Enabled = true;

        await mgr.CommitAsync(await mgr.BeginAsync());
        await mgr.RollbackAsync(await mgr.BeginAsync());

        Assert.That(Count(events, ServerDiagnostics.Tags.Operation.Begin, ServerDiagnostics.Tags.Outcome.Ok),
            Is.EqualTo(2), "one begin per transaction");
        Assert.That(Count(events, ServerDiagnostics.Tags.Operation.Commit, ServerDiagnostics.Tags.Outcome.Ok),
            Is.EqualTo(1));
        Assert.That(Count(events, ServerDiagnostics.Tags.Operation.Rollback, ServerDiagnostics.Tags.Outcome.Ok),
            Is.EqualTo(1));
        Assert.That(ActiveGauge(events), Is.Zero, "the gauge must come back to zero once nothing is live");
    }

    [Test]
    public async Task ARefusedSecondFinalizeIsNotCountedAgain()
    {
        (MeterListener listener, List<Captured> events) = StartListener();
        using MeterListener _ = listener;

        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("tlm2");
        await using EmbeddedKahuna __ = node;

        ServerDiagnostics.Enabled = true;

        KvTransaction committed = await mgr.BeginAsync();
        await mgr.CommitAsync(committed);
        Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(committed));

        KvTransaction rolledBack = await mgr.BeginAsync();
        await mgr.RollbackAsync(rolledBack);
        Assert.ThrowsAsync<CamusDBException>(() => mgr.RollbackAsync(rolledBack));

        // The second call throws before touching the tracking map, and even if it did not, the
        // pair-remove has already consumed this transaction's single exit.
        Assert.That(Count(events, ServerDiagnostics.Tags.Operation.Commit, ServerDiagnostics.Tags.Outcome.Ok),
            Is.EqualTo(1), "a double commit must not count two commits");
        Assert.That(Count(events, ServerDiagnostics.Tags.Operation.Rollback, ServerDiagnostics.Tags.Outcome.Ok),
            Is.EqualTo(1), "a double rollback must not count two rollbacks");
        Assert.That(ActiveGauge(events), Is.Zero, "nor may it decrement the gauge twice");
    }

    [Test]
    public async Task AFailedCommitIsCountedAsItsTerminalOutcome()
    {
        (MeterListener listener, List<Captured> events) = StartListener();
        using MeterListener _ = listener;

        // A serializable read-write transaction that has outlived the maximum lifetime is aborted by
        // commit itself: it rolls the handle back and throws. A one-millisecond lifetime makes that
        // the outcome of the very first commit attempt, so the failure path is exercised without
        // racing two transactions for a conflict.
        CamusDBOptions expiresImmediately = CamusDBOptions.Default with { MaxSerializableTransactionLifetimeMs = 1 };

        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("tlm3", expiresImmediately);
        await using EmbeddedKahuna __ = node;

        ServerDiagnostics.Enabled = true;

        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        await Task.Delay(20);

        CamusDBException? failure = Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(tx));
        Assert.That(failure!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionLifetimeExceeded));
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.RolledBack));

        Assert.That(Count(events, ServerDiagnostics.Tags.Operation.Commit, ServerDiagnostics.Tags.Outcome.Ok),
            Is.Zero, "a commit that threw must never be counted as a committed transaction");
        Assert.That(Count(events, ServerDiagnostics.Tags.Operation.Rollback, ServerDiagnostics.Tags.Outcome.Ok),
            Is.EqualTo(1), "it ended rolled back, and that is what the terminal event must say");
        Assert.That(ActiveGauge(events), Is.Zero, "a failed commit still releases the transaction");
    }

    [Test]
    public async Task TheGaugeTracksWhatIsLiveAndIsTaggedByMode()
    {
        (MeterListener listener, List<Captured> events) = StartListener();
        using MeterListener _ = listener;

        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("tlm4");
        await using EmbeddedKahuna __ = node;

        ServerDiagnostics.Enabled = true;

        KvTransaction first = await mgr.BeginAsync();
        KvTransaction second = await mgr.BeginAsync();
        Assert.That(ActiveGauge(events), Is.EqualTo(2));

        await mgr.CommitAsync(first);
        Assert.That(ActiveGauge(events), Is.EqualTo(1));

        await mgr.RollbackAsync(second);
        Assert.That(ActiveGauge(events), Is.Zero);

        lock (events)
            Assert.That(
                events.Where(e => e.Instrument == "camus.transaction.active").Select(e => e.Mode).Distinct(),
                Is.EquivalentTo(new[] { ServerDiagnostics.Tags.TransactionMode.ReadWrite }),
                "the increment and the decrement must carry the same mode tag, or the series splits and drifts");
    }

    [Test]
    public async Task DisposingAManagerWithLiveTransactionsReleasesTheGauge()
    {
        (MeterListener listener, List<Captured> events) = StartListener();
        using MeterListener _ = listener;

        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("tlm5");
        await using EmbeddedKahuna __ = node;

        ServerDiagnostics.Enabled = true;

        await mgr.BeginAsync();
        await mgr.BeginAsync();
        Assert.That(ActiveGauge(events), Is.EqualTo(2));

        mgr.Dispose();

        Assert.That(ActiveGauge(events), Is.Zero,
            "a disposed manager has no active transactions; a gauge left standing reads as a leak forever");
        Assert.That(Count(events, ServerDiagnostics.Tags.Operation.Rollback, ServerDiagnostics.Tags.Outcome.Canceled),
            Is.EqualTo(2), "dropped rather than finalized — recorded as such, so begins and terminals still balance");
    }

    [Test]
    public async Task NothingIsRecordedWhileDiagnosticsAreOff()
    {
        (MeterListener listener, List<Captured> events) = StartListener();
        using MeterListener _ = listener;

        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("tlm6");
        await using EmbeddedKahuna __ = node;

        ServerDiagnostics.Enabled = false;

        await mgr.CommitAsync(await mgr.BeginAsync());

        lock (events)
            Assert.That(events, Is.Empty, "a node that has not opted in must do no diagnostics work at all");
    }
}
