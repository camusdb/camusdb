/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// Admission priority plumbing and the gate itself.
///
/// <para>Two distinct things are covered, and the distinction matters: most of these tests assert
/// that a chosen priority <b>reaches</b> the transaction (plumbing), which is all that can be
/// observed while the gate is off. The gating tests at the end configure a real concurrency ceiling
/// and assert the gate actually orders and refuses work — without those, a green run here would
/// prove only that a value was copied around.</para>
///
/// <para>Each gating test builds its <b>own</b> node with its own ceiling: the gate is fixed when the
/// Kahuna node is constructed, so setting a ceiling afterwards is a no-op that still passes.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestTransactionPriority
{
    private static CamusDBOptions BaseOptions => CamusDBOptions.Default;

    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr)> CreateAsync(
        string tag, CamusDBOptions? options = null, EmbeddedKahunaOptions? nodeOptions = null)
    {
        EmbeddedKahuna node = nodeOptions is null ? new() : new(nodeOptions);
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);

        HLCTimestampMinter mint = new(node);
        return (node, new KvTransactionsManager(node.Kahuna, options ?? BaseOptions, mint.Mint));
    }

    /// <summary>Supplies the local-HLC minter <c>BeginAsync</c> needs for the deferred-start path.</summary>
    private sealed class HLCTimestampMinter(EmbeddedKahuna node)
    {
        public Kommander.Time.HLCTimestamp Mint(Kommander.Time.HLCTimestamp? _) =>
            node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId());
    }

    // ------------------------------------------------------------------
    // 1. Precedence: explicit > config default > Normal
    // ------------------------------------------------------------------

    [Test]
    public async Task BeginAsync_NoPriority_UsesNormalByDefault()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("prio-default");
        await using EmbeddedKahuna _ = node;

        KvTransaction tx = await mgr.BeginAsync();

        Assert.That(tx.Priority, Is.EqualTo(TransactionPriority.Normal));
        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task BeginAsync_ExplicitPriority_OverridesConfigDefault()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync(
            "prio-explicit", BaseOptions with { DefaultTransactionPriority = TransactionPriority.Low });
        await using EmbeddedKahuna _ = node;

        KvTransaction tx = await mgr.BeginAsync(priority: TransactionPriority.High);

        Assert.That(tx.Priority, Is.EqualTo(TransactionPriority.High));
        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task BeginAsync_NoPriority_UsesConfiguredDefault()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync(
            "prio-configured", BaseOptions with { DefaultTransactionPriority = TransactionPriority.Background });
        await using EmbeddedKahuna _ = node;

        KvTransaction tx = await mgr.BeginAsync();

        Assert.That(tx.Priority, Is.EqualTo(TransactionPriority.Background));
        await mgr.RollbackAsync(tx);
    }

    [TestCase(TransactionPriority.Background)]
    [TestCase(TransactionPriority.Low)]
    [TestCase(TransactionPriority.Normal)]
    [TestCase(TransactionPriority.High)]
    [TestCase(TransactionPriority.Critical)]
    public async Task BeginAsync_EveryPriority_RoundTrips(TransactionPriority priority)
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("prio-roundtrip");
        await using EmbeddedKahuna _ = node;

        KvTransaction tx = await mgr.BeginAsync(priority: priority);

        Assert.That(tx.Priority, Is.EqualTo(priority));
        await mgr.RollbackAsync(tx);
    }

    // ------------------------------------------------------------------
    // 2. ApplyPriority (SET TRANSACTION PRIORITY) rejection cases
    // ------------------------------------------------------------------

    [Test]
    public async Task ApplyPriority_BeforeSessionStarts_ChangesPriority()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("prio-apply");
        await using EmbeddedKahuna _ = node;

        // Deferred start leaves TransactionId == Zero until the first operation, which is the only
        // window in which the priority can still change — the gate consumes it at session start.
        KvTransaction tx = await mgr.BeginAsync(deferStart: true);
        Assert.That(tx.Priority, Is.EqualTo(TransactionPriority.Normal));

        tx.ApplyPriority(TransactionPriority.Background);

        Assert.That(tx.Priority, Is.EqualTo(TransactionPriority.Background));
        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task ApplyPriority_AfterStatementExecuted_Throws()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("prio-after-stmt");
        await using EmbeddedKahuna _ = node;

        KvTransaction tx = await mgr.BeginAsync(deferStart: true);
        tx.MarkStatementExecuted();

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => tx.ApplyPriority(TransactionPriority.High))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));

        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task ApplyPriority_AfterSessionStarted_Throws()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("prio-after-session");
        await using EmbeddedKahuna _ = node;

        // Eager start: the session (and therefore admission) has already happened, so accepting a
        // change here would record a priority that governs nothing.
        KvTransaction tx = await mgr.BeginAsync();

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => tx.ApplyPriority(TransactionPriority.High))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));

        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task ApplyPriority_OnReadOnlyTransaction_Throws()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("prio-readonly");
        await using EmbeddedKahuna _ = node;

        KvTransaction tx = mgr.CreateReadOnlyTransaction();

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => tx.ApplyPriority(TransactionPriority.High))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
    }

    [Test]
    public async Task DeferredStart_UsesPrioritySetAfterBegin_NotAtBeginTime()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("prio-deferred-live");
        await using EmbeddedKahuna _ = node;

        // The SessionStarter must read the transaction's CURRENT priority when the session opens,
        // not the value captured at BeginAsync — otherwise SET TRANSACTION PRIORITY would parse,
        // mutate the transaction, and still be ignored by the gate.
        KvTransaction tx = await mgr.BeginAsync(deferStart: true);
        tx.ApplyPriority(TransactionPriority.Critical);

        await tx.EnsureSessionStartedAsync(CancellationToken.None);

        Assert.That(tx.TransactionId, Is.Not.EqualTo(Kommander.Time.HLCTimestamp.Zero),
            "the deferred session should now be open");
        Assert.That(tx.Priority, Is.EqualTo(TransactionPriority.Critical));

        await mgr.RollbackAsync(tx);
    }

    // ------------------------------------------------------------------
    // 3. The gate itself — a node built with a real concurrency ceiling
    //
    // Everything above passes whether or not the gate exists. These do not.
    // ------------------------------------------------------------------

    /// <summary>
    /// A ceiling of one, with one session held open, must defer a second transaction rather than
    /// admitting it. Without a gate the second Begin returns immediately.
    /// </summary>
    [Test]
    public async Task WithCeiling_SecondTransaction_DoesNotStartWhileTheSlotIsHeld()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync(
            "prio-gate-blocks",
            nodeOptions: new EmbeddedKahunaOptions { MaxConcurrentSessions = 1 });
        await using EmbeddedKahuna _ = node;

        KvTransaction held = await mgr.BeginAsync();

        Task<KvTransaction> queued = mgr.BeginAsync();
        Task completed = await Task.WhenAny(queued, Task.Delay(1_500));

        Assert.That(completed, Is.Not.SameAs(queued),
            "the second transaction must queue at the gate while the only slot is occupied");

        // Releasing the slot lets the queued transaction through.
        await mgr.RollbackAsync(held);

        KvTransaction admitted = await queued.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.That(admitted.TransactionId, Is.Not.EqualTo(Kommander.Time.HLCTimestamp.Zero));
        await mgr.RollbackAsync(admitted);
    }

    /// <summary>
    /// With one slot free and two waiters, the higher-priority one is admitted first. This is the
    /// only test that observes the gate's ordering — the whole point of the feature.
    /// </summary>
    [Test]
    public async Task WithCeiling_HigherPriorityWaiter_IsAdmittedFirst()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync(
            "prio-gate-order",
            nodeOptions: new EmbeddedKahunaOptions
            {
                MaxConcurrentSessions = 1,
                // Disable aging: at the 1 s default the Background waiter would be promoted to High
                // within ~3 s and could win on arrival order, making this assert the clock rather
                // than the ordering.
                TransactionPriorityAgingThreshold = 0,
            });
        await using EmbeddedKahuna _ = node;

        KvTransaction held = await mgr.BeginAsync();

        // Queue the LOW-priority waiter first, so arrival order and priority order disagree. If the
        // gate ignored priority, first-come-first-served would admit background first and this test
        // would fail — which is exactly what makes it meaningful.
        Task<KvTransaction> background = mgr.BeginAsync(priority: TransactionPriority.Background);
        await Task.Delay(300);
        Task<KvTransaction> high = mgr.BeginAsync(priority: TransactionPriority.High);
        await Task.Delay(300);

        Assert.That(background.IsCompleted, Is.False, "background must still be queued");
        Assert.That(high.IsCompleted, Is.False, "high must still be queued");

        await mgr.RollbackAsync(held);

        Task first = await Task.WhenAny(background, high);
        Assert.That(first, Is.SameAs(high),
            "the High-priority waiter must be admitted ahead of the Background one that arrived first");

        KvTransaction highTx = await high;
        await mgr.RollbackAsync(highTx);

        KvTransaction backgroundTx = await background.WaitAsync(TimeSpan.FromSeconds(30));
        await mgr.RollbackAsync(backgroundTx);
    }

    /// <summary>
    /// A full wait queue is refused rather than queued without bound. The refusal is retryable
    /// (CADB0504) because nothing was started — a caller may simply try again.
    /// </summary>
    [Test]
    public async Task WithFullQueue_FurtherTransactions_AreRefusedRetryably()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync(
            "prio-gate-shed",
            nodeOptions: new EmbeddedKahunaOptions
            {
                MaxConcurrentSessions = 1,
                TransactionPriorityMaxQueued = 1,
            });
        await using EmbeddedKahuna _ = node;

        KvTransaction held = await mgr.BeginAsync();

        Task<KvTransaction> queued = mgr.BeginAsync();   // fills the single queue slot
        await Task.Delay(300);

        // The queue is full, so this one is refused outright rather than parked.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await mgr.BeginAsync())!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry),
            "a refusal must be reported as retryable — nothing was started");

        await mgr.RollbackAsync(held);
        KvTransaction admitted = await queued.WaitAsync(TimeSpan.FromSeconds(30));
        await mgr.RollbackAsync(admitted);
    }

    /// <summary>
    /// A transaction that cannot be admitted waits on the <b>admission</b> clock, not on its own
    /// lifetime. The engine asks for an hour-long session (the default serializable lifetime) and a
    /// one-second door-wait; the refusal must arrive on the latter. Were the two still one quantity,
    /// this would park for an hour — the reason the gate could not be turned on at all.
    ///
    /// <para>The node's own default budget is raised well above the engine's here on purpose: a
    /// refusal near one second can then only mean the caller's budget crossed the wire, not that the
    /// server fell back to a default that happened to be short.</para>
    /// </summary>
    [Test]
    public async Task WithCeiling_RefusalArrivesOnTheAdmissionClock_NotTheSessionLifetime()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync(
            "prio-gate-wait",
            options: BaseOptions with { TransactionAdmissionWaitMs = 1_000 },
            nodeOptions: new EmbeddedKahunaOptions
            {
                MaxConcurrentSessions = 1,
                DefaultAdmissionWaitMs = 25_000,
                MaxAdmissionWaitMs = 30_000,
            });
        await using EmbeddedKahuna _ = node;

        KvTransaction held = await mgr.BeginAsync();

        Stopwatch sw = Stopwatch.StartNew();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await mgr.BeginAsync())!;
        sw.Stop();

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry),
            "an admission refusal is retryable — nothing was started");

        // Generously bounded: the point is that it returned on the one-second budget rather than on
        // the node's 25 s default or the hour-long session lifetime.
        Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(15)),
            $"expected the refusal on the caller's admission budget, waited {sw.Elapsed.TotalSeconds:F1}s");

        await mgr.RollbackAsync(held);
    }

    /// <summary>
    /// With no engine-side budget configured the node's own default applies, and it still bounds the
    /// wait to seconds. This is what makes a ceiling safe to enable without also tuning the budget:
    /// the hour-long session lifetime is never what a queued transaction waits on.
    /// </summary>
    [Test]
    public async Task WithCeiling_NoConfiguredBudget_StillRefusesOnTheNodeDefault()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync(
            "prio-gate-wait-default",
            nodeOptions: new EmbeddedKahunaOptions
            {
                MaxConcurrentSessions = 1,
                DefaultAdmissionWaitMs = 1_000,
            });
        await using EmbeddedKahuna _ = node;

        Assert.That(BaseOptions.TransactionAdmissionWaitMs, Is.Zero,
            "this test is only meaningful while the engine asks for no budget of its own");

        KvTransaction held = await mgr.BeginAsync();

        Stopwatch sw = Stopwatch.StartNew();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await mgr.BeginAsync())!;
        sw.Stop();

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry));
        Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(15)),
            $"expected the refusal on the node's default budget, waited {sw.Elapsed.TotalSeconds:F1}s");

        await mgr.RollbackAsync(held);
    }

    /// <summary>
    /// The default configuration must be completely transparent: with no ceiling, a Background
    /// transaction starts as promptly as any other. This is the guarantee that lets the whole
    /// feature ship dark.
    /// </summary>
    [Test]
    public async Task WithoutCeiling_LowPriorityWork_IsNotDelayed()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("prio-gate-off");
        await using EmbeddedKahuna _ = node;

        KvTransaction held = await mgr.BeginAsync();

        Stopwatch sw = Stopwatch.StartNew();
        KvTransaction background = await mgr.BeginAsync(priority: TransactionPriority.Background);
        sw.Stop();

        Assert.That(background.TransactionId, Is.Not.EqualTo(Kommander.Time.HLCTimestamp.Zero));
        Assert.That(sw.ElapsedMilliseconds, Is.LessThan(5_000),
            "with no ceiling configured, admission must be transparent");

        await mgr.RollbackAsync(background);
        await mgr.RollbackAsync(held);
    }
}
