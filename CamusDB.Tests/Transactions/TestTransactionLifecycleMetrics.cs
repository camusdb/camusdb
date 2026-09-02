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
/// <para>Non-parallelizable: <see cref="ServerDiagnostics.Enabled"/> is a process-wide gate, so a
/// concurrent fixture flipping it would silence the events asserted here. The meter is process-wide
/// too, but that is handled by attribution rather than by scheduling — see <see cref="OwningFlow"/>:
/// running alone is not enough, because background work from engines that earlier fixtures left
/// alive records on the same instruments at its own pace.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestTransactionLifecycleMetrics
{
    private sealed record Captured(string Instrument, long Value, string? Operation, string? Outcome, string? Mode);

    /// <summary>
    /// Marks the execution context whose measurements a test owns.
    ///
    /// <para>The two instruments are process-wide, and their tags carry no manager identity (a
    /// per-manager tag would be unbounded cardinality in production). Every transaction manager
    /// alive in the test process records on them: a leaked fixture's debounced statistics flush, an
    /// auto-analyze or TTL sweep, an orphan reclaim — all on by default, all begun on their own
    /// schedule. One of those landing a begin inside a test's window, and ending after its
    /// assertions, read as a gauge that never came back to zero.</para>
    ///
    /// <para>What does separate the events is the execution context. A measurement is recorded
    /// synchronously inside Track/Untrack, on the async flow of whoever called BeginAsync, CommitAsync,
    /// RollbackAsync or Dispose — for everything asserted here, the test method's own flow, which the
    /// marker set in <see cref="StartListener"/> follows across every await. Background work started
    /// elsewhere runs on a flow that never saw the marker, so its measurements are dropped at capture
    /// time, deterministically, rather than tolerated by a looser assertion.</para>
    /// </summary>
    private static readonly AsyncLocal<object?> OwningFlow = new();

    private static (MeterListener Listener, List<Captured> Events) StartListener()
    {
        List<Captured> events = new();
        MeterListener listener = new();

        // Set on the caller's flow: this method runs synchronously on the test method's execution
        // context, so the marker is what every later await in that test carries.
        object flow = new();
        OwningFlow.Value = flow;

        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == ServerDiagnostics.MeterName &&
                instrument.Name is "camus.transaction.count" or "camus.transaction.active")
                l.EnableMeasurementEvents(instrument);
        };

        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            // The callback runs on the recorder's flow. Anything not descending from this test's
            // context belongs to another manager in the process — see OwningFlow.
            if (!ReferenceEquals(OwningFlow.Value, flow))
                return;

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

    [Test]
    public async Task ATransactionFromAnotherManagerInTheProcessIsNotAttributedToThisFixture()
    {
        (MeterListener listener, List<Captured> events) = StartListener();
        using MeterListener _ = listener;

        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("tlm7");
        await using EmbeddedKahuna __ = node;

        // A second manager stands in for every other engine alive in this test process — a leaked
        // fixture's debounced statistics flush, an auto-analyze or TTL sweep — whose transactions
        // record on the same process-wide instruments. It begins on an execution context that does
        // not descend from this test, exactly as background work does, and holds its transaction
        // open across the assertions below, which is the interleaving that inflated the gauge in CI.
        using KvTransactionsManager foreign = new(node.Kahuna, CamusDBOptions.Default);
        TaskCompletionSource<KvTransaction> foreignBegun = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        ServerDiagnostics.Enabled = true;

        Task foreignWork;
        using (ExecutionContext.SuppressFlow())
        {
            foreignWork = Task.Run(async () =>
            {
                KvTransaction foreignTx = await foreign.BeginAsync();
                foreignBegun.SetResult(foreignTx);
                await release.Task;
                await foreign.RollbackAsync(foreignTx);
            });
        }

        await foreignBegun.Task;

        await mgr.CommitAsync(await mgr.BeginAsync());

        Assert.That(Count(events, ServerDiagnostics.Tags.Operation.Begin, ServerDiagnostics.Tags.Outcome.Ok),
            Is.EqualTo(1), "only this test's own begin is attributed to it");
        Assert.That(Count(events, ServerDiagnostics.Tags.Operation.Commit, ServerDiagnostics.Tags.Outcome.Ok),
            Is.EqualTo(1));
        Assert.That(ActiveGauge(events), Is.Zero,
            "the foreign transaction is still live, and it is not this fixture's to count");

        release.SetResult();
        await foreignWork;
    }
}
