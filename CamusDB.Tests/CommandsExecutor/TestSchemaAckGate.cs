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

using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The schema-ack gate: how a DDL proposer waits for the rest of the cluster to apply the previous
/// version before proposing the next one.
///
/// <para>The gate is held open for as long as convergence takes, which makes two properties matter
/// beyond its verdicts. It must wake when the ack it is waiting for actually arrives, rather than on
/// the next sweep of a poll — and it must still wake for the things no ack announces, because the
/// blocker that finally clears is sometimes a silent node's lease expiring rather than a message.
/// A gate that only reacted to acks would hang exactly there. And a gate on one database must not
/// hold up DDL on another, since these waits are measured in cluster round-trips.</para>
///
/// <para>Unit-level on purpose: the tracker is driven directly so a case can hold a member silent, or
/// let a lease lapse, without needing a cluster that behaves that way on demand.</para>
/// </summary>
[TestFixture]
public sealed class TestSchemaAckGate
{
    private static readonly TimeSpan NoLease = Timeout.InfiniteTimeSpan;
    private static readonly TimeSpan NoBackstop = Timeout.InfiniteTimeSpan;

    private static Func<System.Collections.Generic.IReadOnlyCollection<string>> Members(params string[] members)
        => () => members;

    // ── Verdicts ─────────────────────────────────────────────────────────────────────────────

    /// <summary>Every live member acked: full convergence, returned without waiting.</summary>
    [Test]
    public async Task ConvergesImmediatelyWhenEveryMemberHasAcked()
    {
        SchemaAckTracker tracker = new();
        tracker.RecordApplied("db", "A", 3);
        tracker.RecordApplied("db", "B", 3);

        SchemaAckOutcome outcome = await tracker.WaitForAllLiveAsync(
            "db", 3, TimeSpan.FromSeconds(5), Members("A", "B"), NoLease, NoBackstop, CancellationToken.None);

        Assert.AreEqual(SchemaAckOutcome.FullConvergence, outcome);
    }

    /// <summary>
    /// A member that is behind and never catches up holds the gate to its timeout, because with an
    /// infinite lease it never stops counting as live.
    /// </summary>
    [Test]
    public async Task TimesOutWhenAMemberStaysBehind()
    {
        SchemaAckTracker tracker = new();
        tracker.RecordApplied("db", "A", 3);
        tracker.RecordApplied("db", "B", 2); // behind, and never catches up

        SchemaAckOutcome outcome = await tracker.WaitForAllLiveAsync(
            "db", 3, TimeSpan.FromMilliseconds(400), Members("A", "B"), NoLease, NoBackstop, CancellationToken.None);

        Assert.AreEqual(SchemaAckOutcome.Timeout, outcome);
    }

    /// <summary>
    /// A live member with <em>no ack record at all</em> does not block, once some other node has
    /// recorded one. Absence of a record means that node has never opened this database, so it is not
    /// serving it and will catch up through restore when it does — waiting for it would block DDL on
    /// every database behind every node that happens not to use it.
    ///
    /// <para>Pinned because it is the difference between "silent" and "behind", and it is easy to
    /// mistake one for the other when writing a test: a case that gives the blocking member no record
    /// converges immediately and passes for the wrong reason.</para>
    /// </summary>
    [Test]
    public async Task AMemberThatHasNeverOpenedTheDatabaseDoesNotBlock()
    {
        SchemaAckTracker tracker = new();
        tracker.RecordApplied("db", "A", 3);
        // B has no record: it has never opened this database.

        SchemaAckOutcome outcome = await tracker.WaitForAllLiveAsync(
            "db", 3, TimeSpan.FromMilliseconds(400), Members("A", "B"), NoLease, NoBackstop, CancellationToken.None);

        Assert.AreEqual(SchemaAckOutcome.FullConvergence, outcome);
    }

    /// <summary>
    /// With a finite backstop, a majority ack plus the backstop delay is enough to proceed even though
    /// a minority is still behind.
    /// </summary>
    [Test]
    public async Task AcceptsQuorumOnceTheBackstopDelayHasElapsed()
    {
        SchemaAckTracker tracker = new();
        tracker.RecordApplied("db", "A", 4);
        tracker.RecordApplied("db", "B", 4);
        // C is behind; 2 of 3 is a majority.
        tracker.RecordApplied("db", "C", 3);

        SchemaAckOutcome outcome = await tracker.WaitForAllLiveAsync(
            "db", 4, TimeSpan.FromSeconds(5), Members("A", "B", "C"),
            NoLease, quorumBackstopDelay: TimeSpan.FromMilliseconds(150), CancellationToken.None);

        Assert.AreEqual(SchemaAckOutcome.QuorumBackstop, outcome);
    }

    // ── Woken by the ack, not by a poll ──────────────────────────────────────────────────────

    /// <summary>
    /// The gate returns promptly after the ack it was waiting for lands — it is signalled, not polled.
    ///
    /// <para>The assertion is on the delay <em>between the ack and the return</em>, not on total
    /// elapsed time: a gate that polled on a coarse interval would pass a total-time assertion by
    /// luck, and fail this one about as often as the interval is long.</para>
    /// </summary>
    [Test]
    public async Task WakesPromptlyWhenTheAwaitedAckArrives()
    {
        SchemaAckTracker tracker = new();
        tracker.RecordApplied("db", "A", 6);
        tracker.RecordApplied("db", "B", 5); // behind: this is what actually holds the gate open

        Stopwatch watch = new();

        Task<SchemaAckOutcome> gate = tracker.WaitForAllLiveAsync(
            "db", 6, TimeSpan.FromSeconds(10), Members("A", "B"), NoLease, NoBackstop, CancellationToken.None);

        // Let the gate reach its wait, then release it.
        await Task.Delay(300);
        watch.Start();
        tracker.RecordApplied("db", "B", 6);

        SchemaAckOutcome outcome = await gate;
        watch.Stop();

        Assert.AreEqual(SchemaAckOutcome.FullConvergence, outcome);
        Assert.Less(
            watch.ElapsedMilliseconds, 150,
            "The gate must be released by the ack itself, not by the next liveness re-check");
    }

    // ── Still wakes for what no ack announces ────────────────────────────────────────────────

    /// <summary>
    /// A member that goes silent stops blocking the gate once its lease lapses — with no further acks
    /// from anyone.
    ///
    /// <para>This is the case that makes a pure ack signal wrong. The event the gate is waiting for
    /// here is the <em>absence</em> of messages passing a deadline, so nothing can signal it; without
    /// a timer alongside the signal, this gate would wait out its full timeout instead of proceeding.
    /// </para>
    /// </summary>
    [Test]
    public async Task AMemberWhoseLeaseExpiresStopsBlockingWithoutAnyFurtherAck()
    {
        SchemaAckTracker tracker = new();
        tracker.RecordApplied("db", "A", 9);
        tracker.RecordApplied("db", "B", 8); // behind, and about to go silent

        Stopwatch watch = Stopwatch.StartNew();

        SchemaAckOutcome outcome = await tracker.WaitForAllLiveAsync(
            "db", 9,
            timeout: TimeSpan.FromSeconds(10),
            Members("A", "B"),
            liveNodeLease: TimeSpan.FromMilliseconds(200),
            NoBackstop,
            CancellationToken.None);

        watch.Stop();

        Assert.AreEqual(SchemaAckOutcome.FullConvergence, outcome,
            "Once B's lease lapses it is presumed down and must stop blocking the gate");
        Assert.Less(
            watch.ElapsedMilliseconds, 5_000,
            "The gate must notice the lapsed lease on its own, not sit until the overall timeout");
    }

    // ── Databases do not contend ─────────────────────────────────────────────────────────────

    /// <summary>
    /// A gate held open on one database must not delay a gate on another. The tracker keys its state
    /// per database with a lock per entry; a single tracker-wide lock would serialize these two waits,
    /// and the wait is as long as a cluster takes to converge.
    /// </summary>
    [Test]
    public async Task AGateOnOneDatabaseDoesNotDelayAnother()
    {
        SchemaAckTracker tracker = new();

        // "slow" will never converge within its timeout: B is behind and stays behind.
        tracker.RecordApplied("slow", "A", 2);
        tracker.RecordApplied("slow", "B", 1);

        // "fast" is already converged.
        tracker.RecordApplied("fast", "A", 2);
        tracker.RecordApplied("fast", "B", 2);

        Task<SchemaAckOutcome> slowGate = tracker.WaitForAllLiveAsync(
            "slow", 2, TimeSpan.FromSeconds(3), Members("A", "B"), NoLease, NoBackstop, CancellationToken.None);

        Stopwatch watch = Stopwatch.StartNew();
        SchemaAckOutcome fast = await tracker.WaitForAllLiveAsync(
            "fast", 2, TimeSpan.FromSeconds(3), Members("A", "B"), NoLease, NoBackstop, CancellationToken.None);
        watch.Stop();

        Assert.AreEqual(SchemaAckOutcome.FullConvergence, fast);
        Assert.Less(
            watch.ElapsedMilliseconds, 500,
            "A converged database's gate must return while another database's gate is still open");

        Assert.AreEqual(SchemaAckOutcome.Timeout, await slowGate);
    }

    /// <summary>
    /// Recording an ack for one database must not disturb another's waiters into a wrong verdict —
    /// the signal is per database, and a spurious wake simply re-evaluates.
    /// </summary>
    [Test]
    public async Task AnAckForOneDatabaseDoesNotSatisfyAnother()
    {
        SchemaAckTracker tracker = new();
        tracker.RecordApplied("db1", "A", 1);
        tracker.RecordApplied("db1", "B", 0); // behind on db1

        Task<SchemaAckOutcome> gate = tracker.WaitForAllLiveAsync(
            "db1", 1, TimeSpan.FromMilliseconds(600), Members("A", "B"), NoLease, NoBackstop, CancellationToken.None);

        // Traffic on an unrelated database while db1's gate is open.
        for (int i = 0; i < 5; i++)
        {
            tracker.RecordApplied("db2", "B", i);
            await Task.Delay(20);
        }

        Assert.AreEqual(SchemaAckOutcome.Timeout, await gate,
            "db2's acks must not be mistaken for the ack db1 is waiting on");
    }

    // ── Interaction with releasing a database's state ────────────────────────────────────────

    /// <summary>
    /// Forgetting a database's ack state while a gate is open must not strand that gate on a signal
    /// nothing will ever fire again. Releasing a descriptor forgets its acks, and although eviction
    /// refuses to run while DDL is in flight, the gate must not depend on that for its liveness.
    /// </summary>
    [Test]
    public async Task ForgettingADatabaseDoesNotStrandAnOpenGate()
    {
        SchemaAckTracker tracker = new();
        tracker.RecordApplied("db", "A", 5);
        tracker.RecordApplied("db", "B", 4); // behind, so the gate is genuinely open

        Task<SchemaAckOutcome> gate = tracker.WaitForAllLiveAsync(
            "db", 5, TimeSpan.FromSeconds(10), Members("A", "B"), NoLease, NoBackstop, CancellationToken.None);

        await Task.Delay(100);

        Stopwatch watch = Stopwatch.StartNew();
        tracker.Forget("db");

        // The state is rebuilt exactly as a reopened database would rebuild it.
        tracker.RecordApplied("db", "A", 5);
        tracker.RecordApplied("db", "B", 5);

        SchemaAckOutcome outcome = await gate;
        watch.Stop();

        Assert.AreEqual(
            SchemaAckOutcome.FullConvergence, outcome,
            "A gate must pick up the rebuilt ack state rather than waiting on the detached one");

        // Timeliness is the real assertion. Without it this passes even when the gate sits on a
        // detached signal until its final deadline and only then notices the rebuilt state — which is
        // indistinguishable from "worked" in the verdict alone.
        Assert.Less(
            watch.ElapsedMilliseconds, 1_000,
            "The gate must re-resolve the rebuilt entry promptly, not at its overall timeout");
    }

    /// <summary>Cancelling the gate propagates, rather than running to its timeout.</summary>
    [Test]
    public void CancellationPropagates()
    {
        SchemaAckTracker tracker = new();
        tracker.RecordApplied("db", "A", 1);
        tracker.RecordApplied("db", "B", 0); // behind, so the gate actually waits

        using CancellationTokenSource cts = new();

        Task<SchemaAckOutcome> gate = tracker.WaitForAllLiveAsync(
            "db", 1, TimeSpan.FromSeconds(30), Members("A", "B"), NoLease, NoBackstop, cts.Token);

        cts.CancelAfter(100);

        Assert.CatchAsync<OperationCanceledException>(async () => await gate);
    }
}
