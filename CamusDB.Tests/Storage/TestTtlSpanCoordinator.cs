
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Ttl;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// The span-claim protocol: the mechanism that lets workers on several nodes divide one table's
/// keyspace without assignment, membership knowledge, or a coordinator that must stay alive.
///
/// <para>These are interleaving tests, not sequence tests. Claiming a span twice in a row proves
/// nothing about two workers claiming it at the same instant, which is the only case the CAS exists
/// for — so the contention tests here start their racers together and assert on how many won.</para>
/// </summary>
[TestFixture]
// Serial: boots embedded Kahuna nodes, like every other node-booting fixture.
[NonParallelizable]
public sealed class TestTtlSpanCoordinator
{
    private const int LeaseMs = 2_000;
    private const int RenewMs = 400;

    private static async Task<EmbeddedKahuna> CreateNodeAsync(string warmupKey)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{warmupKey}/warmup", CancellationToken.None);
        return node;
    }

    private static TtlSpanCoordinator CoordinatorFor(EmbeddedKahuna node, string owner, int leaseMs = LeaseMs) =>
        new(node.Kahuna, owner, leaseMs, RenewMs);

    // ── Span division ─────────────────────────────────────────────────────────

    [Test]
    public void SpansPartitionTheRowIdSpaceWithNoGapAndNoOverlap()
    {
        const int spanCount = 16;

        (ObjectIdValue? firstStart, _) = TtlSpanCoordinator.SpanBounds(0, spanCount);
        Assert.IsNull(firstStart, "The first span must have no lower bound, or rows below it are never swept");

        (_, ObjectIdValue? lastEnd) = TtlSpanCoordinator.SpanBounds(spanCount - 1, spanCount);
        Assert.IsNull(lastEnd, "The last span must have no upper bound, or the tail of the table is never swept");

        // Span N's exclusive end must be exactly span N+1's inclusive start. Any gap silently retains
        // rows forever; any overlap has two workers deleting the same rows and double-counting them.
        for (int i = 0; i < spanCount - 1; i++)
        {
            (_, ObjectIdValue? end) = TtlSpanCoordinator.SpanBounds(i, spanCount);
            (ObjectIdValue? nextStart, _) = TtlSpanCoordinator.SpanBounds(i + 1, spanCount);

            Assert.IsNotNull(end);
            Assert.IsNotNull(nextStart);
            Assert.AreEqual(end!.Value.ToString(), nextStart!.Value.ToString(),
                $"Span {i}'s end must be span {i + 1}'s start");
        }
    }

    /// <summary>
    /// Comparing boundary <em>values</em> proves the arithmetic lines up; it says nothing about whether a
    /// row holding one of those values is ever read. This drives real rows through real scans, which is
    /// the only thing that catches a both-ends-exclusive range — the shape that silently drops exactly
    /// the rows sitting on span edges.
    /// </summary>
    [Test]
    public async Task EveryRowIsVisitedByExactlyOneSpanIncludingRowsOnTheBoundaries()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-boundary-rows");
        await AssertSpansCoverBoundaryRowsAsync(node, "bt", ancestor: false);
    }

    /// <summary>
    /// The same coverage guarantee on a branch database, whose scan is a k-way merge across lineage
    /// levels rather than a single ordered stream — a separate implementation, and so a separate chance
    /// to get the boundary wrong.
    /// </summary>
    [Test]
    public async Task BoundaryRowsAreCoveredOnABranchDatabaseToo()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-boundary-branch");
        await AssertSpansCoverBoundaryRowsAsync(node, "bb", ancestor: true);
    }

    private static async Task AssertSpansCoverBoundaryRowsAsync(EmbeddedKahuna node, string tableId, bool ancestor)
    {
        const int spanCount = 4;

        KvTableStore? ancestorStore = null;
        if (ancestor)
            ancestorStore = new KvTableStore(node.Kahuna, CamusDBOptions.Default, "anc-" + tableId, tableId);

        KvTableStore store = ancestorStore is null
            ? new KvTableStore(node.Kahuna, CamusDBOptions.Default, "db-" + tableId, tableId)
            : new KvTableStore(node.Kahuna, CamusDBOptions.Default, "db-" + tableId, tableId,
                ancestorStores: [(ancestorStore, HLCTimestamp.Zero)]);

        _ = ancestorStore; // the branch keeps its own level-0 rows; ancestry only changes the scan path

        List<TableColumnSchema> cols = [new("n", "n", ColumnType.Integer64, false, null)];
        TableSchema schema = new()
        {
            Id = tableId, Name = "t", Version = 0, Columns = cols,
            SchemaHistory = [new TableSchemaHistory { Version = 0, Columns = cols }]
        };

        // Every boundary, plus its immediate neighbours on each side. The boundary itself is the case
        // that a both-ends-exclusive range loses.
        List<ObjectIdValue> expected = [];
        for (int s = 1; s < spanCount; s++)
        {
            (ObjectIdValue? start, _) = TtlSpanCoordinator.SpanBounds(s, spanCount);
            ObjectIdValue boundary = start!.Value;

            expected.Add(new ObjectIdValue(boundary.a - 1, int.MaxValue, int.MaxValue)); // just below
            expected.Add(boundary);                                                      // exactly on
            expected.Add(new ObjectIdValue(boundary.a, 0, 1));                           // just above
        }

        KvTransaction tx = await BeginTransactionAsync(node.Kahuna, "boundary-insert-" + tableId);
        foreach (ObjectIdValue id in expected)
        {
            byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue> { ["n"] = new(ColumnType.Integer64, 1L) }, id);
            await store.InsertRow(tx, id, data);
        }
        await node.Kahuna.LocateAndCommitTransaction(tx.Handle, CancellationToken.None);

        // Drive every span exactly as the sweeper does.
        List<string> visited = [];
        for (int s = 0; s < spanCount; s++)
        {
            (ObjectIdValue? start, ObjectIdValue? end) = TtlSpanCoordinator.SpanBounds(s, spanCount);
            await foreach ((ObjectIdValue rowId, _) in store.ScanRows(
                KvTransaction.CreateReadOnly(), untilRowId: end, fromRowId: start))
                visited.Add(rowId.ToString());
        }

        foreach (ObjectIdValue id in expected)
        {
            Assert.AreEqual(1, visited.Count(v => v == id.ToString()),
                $"Row {id} must be visited by exactly one span (it is {(expected.IndexOf(id) % 3 == 1 ? "ON a span boundary" : "adjacent to a boundary")})");
        }

        Assert.AreEqual(expected.Count, visited.Count, "No row may be visited twice");
    }

    private static async Task<KvTransaction> BeginTransactionAsync(IKahuna kahuna, string uniqueId)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = uniqueId, Locking = KeyValueTransactionLocking.Pessimistic },
            CancellationToken.None);

        Assert.AreEqual(KeyValueResponseType.Set, type);
        return new KvTransaction(handle.TransactionId, uniqueId);
    }

    [Test]
    public async Task ResumingFromACheckpointDoesNotRevisitTheCheckpointedRow()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-resume-exclusive");

        KvTableStore store = new(node.Kahuna, CamusDBOptions.Default, "resumedb", "rt");
        List<TableColumnSchema> cols = [new("n", "n", ColumnType.Integer64, false, null)];
        TableSchema schema = new()
        {
            Id = "rt", Name = "t", Version = 0, Columns = cols,
            SchemaHistory = [new TableSchemaHistory { Version = 0, Columns = cols }]
        };

        // Span 1's inclusive start, plus two rows above it.
        (ObjectIdValue? spanStart, ObjectIdValue? spanEnd) = TtlSpanCoordinator.SpanBounds(1, 4);
        ObjectIdValue first = spanStart!.Value;
        ObjectIdValue second = new(first.a, 0, 5);
        ObjectIdValue third = new(first.a, 0, 9);

        KvTransaction tx = await BeginTransactionAsync(node.Kahuna, "resume-insert");
        foreach (ObjectIdValue id in new[] { first, second, third })
        {
            byte[] data = RowEncoder.Encode(schema, new Dictionary<string, ColumnValue> { ["n"] = new(ColumnType.Integer64, 1L) }, id);
            await store.InsertRow(tx, id, data);
        }
        await node.Kahuna.LocateAndCommitTransaction(tx.Handle, CancellationToken.None);

        // Resume as if `first` had already been processed. The inclusive span start must NOT drag it
        // back in — progress and extent are different bounds and must not cancel each other out.
        List<string> resumed = [];
        await foreach ((ObjectIdValue rowId, _) in store.ScanRows(
            KvTransaction.CreateReadOnly(), afterRowId: first, untilRowId: spanEnd, fromRowId: spanStart))
            resumed.Add(rowId.ToString());

        Assert.AreEqual(new[] { second.ToString(), third.ToString() }, resumed,
            "A resumed scan must skip the checkpointed row while still honouring the span's extent");
    }

    [Test]
    public void SpanBoundariesAscendInOrdinalHexOrder()
    {
        const int spanCount = 64;
        string? previous = null;

        for (int i = 1; i < spanCount; i++)
        {
            (ObjectIdValue? after, _) = TtlSpanCoordinator.SpanBounds(i, spanCount);
            string hex = after!.Value.ToString();

            if (previous is not null)
                Assert.That(string.CompareOrdinal(previous, hex), Is.LessThan(0),
                    "Boundaries must ascend in the same ordinal hex order the scan uses");

            previous = hex;
        }
    }

    [Test]
    public void SingleSpanCoversTheWholeKeyspace()
    {
        (ObjectIdValue? after, ObjectIdValue? until) = TtlSpanCoordinator.SpanBounds(0, 1);

        Assert.IsNull(after);
        Assert.IsNull(until);
    }

    // ── Claiming ──────────────────────────────────────────────────────────────

    [Test]
    public async Task ConcurrentClaimsOnOneSpanProduceExactlyOneOwner()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-claim-race");

        const int racers = 8;
        List<TtlSpanCoordinator> coordinators = [];
        for (int i = 0; i < racers; i++)
            coordinators.Add(CoordinatorFor(node, $"worker-{i}"));

        try
        {
            // Start every racer at once. Sequential calls would pass even with no CAS at all — the
            // second caller would simply see the key present — so the race has to be real.
            using Barrier gate = new(racers);
            Task<long?>[] attempts = coordinators.Select(c => Task.Run(async () =>
            {
                gate.SignalAndWait();
                return await c.TryClaimSpanAsync("db1", "t1", 3, CancellationToken.None);
            })).ToArray();

            long?[] results = await Task.WhenAll(attempts);

            Assert.AreEqual(1, results.Count(token => token is not null),
                "Exactly one worker may own a span; more than one means two workers delete the same rows");
        }
        finally
        {
            foreach (TtlSpanCoordinator c in coordinators)
                await c.DisposeAsync();
        }
    }

    [Test]
    public async Task DifferentSpansAreClaimedIndependently()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-claim-independent");

        await using TtlSpanCoordinator a = CoordinatorFor(node, "worker-a");
        await using TtlSpanCoordinator b = CoordinatorFor(node, "worker-b");

        Assert.IsNotNull(await a.TryClaimSpanAsync("db1", "t1", 0, CancellationToken.None));
        Assert.IsNotNull(await b.TryClaimSpanAsync("db1", "t1", 1, CancellationToken.None),
            "A claim on one span must not block another — that is the whole point of splitting the keyspace");
    }

    [Test]
    public async Task SpansOfDifferentTablesDoNotCollide()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-claim-tables");

        await using TtlSpanCoordinator a = CoordinatorFor(node, "worker-a");
        await using TtlSpanCoordinator b = CoordinatorFor(node, "worker-b");

        Assert.IsNotNull(await a.TryClaimSpanAsync("db1", "t1", 0, CancellationToken.None));
        Assert.IsNotNull(await b.TryClaimSpanAsync("db1", "t2", 0, CancellationToken.None));
    }

    [Test]
    public async Task ReleasingASpanLetsAnotherWorkerClaimItImmediately()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-claim-release");

        await using TtlSpanCoordinator a = CoordinatorFor(node, "worker-a");
        await using TtlSpanCoordinator b = CoordinatorFor(node, "worker-b");

        long? tokenA = await a.TryClaimSpanAsync("db1", "t1", 5, CancellationToken.None);
        Assert.IsNotNull(tokenA);
        Assert.IsNull(await b.TryClaimSpanAsync("db1", "t1", 5, CancellationToken.None));

        await a.ReleaseSpanAsync("db1", "t1", 5, tokenA!.Value);

        // Immediately, not after the lease lapses: an orderly handoff must not cost a lease period.
        Assert.IsNotNull(await b.TryClaimSpanAsync("db1", "t1", 5, CancellationToken.None));
    }

    [Test]
    public async Task ADeadWorkersSpanIsReclaimedOnceItsLeaseLapses()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-claim-lapse");

        // A short lease with NO renewer stands in for a worker that died holding the span: nothing
        // re-stamps the expiry, so the claim lapses on its own. Without this the span would be
        // unreclaimable and the run would stall forever on one dead node.
        TtlSpanCoordinator dead = new(node.Kahuna, "dead-worker", 1_000, 100_000);
        await using TtlSpanCoordinator survivor = CoordinatorFor(node, "survivor");

        Assert.IsNotNull(await dead.TryClaimSpanAsync("db1", "t1", 2, CancellationToken.None));
        Assert.IsNull(await survivor.TryClaimSpanAsync("db1", "t1", 2, CancellationToken.None),
            "While the lease is live the span is genuinely held");

        await Task.Delay(2_500);

        Assert.IsNotNull(await survivor.TryClaimSpanAsync("db1", "t1", 2, CancellationToken.None),
            "A lapsed lease must free the span");
    }

    [Test]
    public async Task ALiveOwnerKeepsItsSpanAcrossSeveralLeasePeriods()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-claim-renew");

        await using TtlSpanCoordinator owner = CoordinatorFor(node, "long-running");
        await using TtlSpanCoordinator thief = CoordinatorFor(node, "thief");

        long? ownerToken = await owner.TryClaimSpanAsync("db1", "t1", 7, CancellationToken.None);
        Assert.IsNotNull(ownerToken);

        // Outlive the lease. A span whose work legitimately runs longer than one lease period must not
        // be stolen mid-delete — that is what the background renewer is for.
        await Task.Delay(LeaseMs + 1_500);

        Assert.IsTrue(owner.StillOwnsSpan("db1", "t1", 7, ownerToken!.Value), "The renewer must keep the claim alive");
        Assert.IsNull(await thief.TryClaimSpanAsync("db1", "t1", 7, CancellationToken.None),
            "A renewed lease must still exclude other workers");
    }

    // ── Fencing: a lapsed owner must not disturb its successor ────────────────

    [Test]
    public async Task ALapsedOwnersReleaseDoesNotFreeItsSuccessorsSpan()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-fence-release");

        // A stalls past its lease (short lease, renewer too slow to save it), B takes the span, then A
        // finally reaches its release. An unconditional delete here would free a span B is working, and
        // a third worker would move in alongside B.
        TtlSpanCoordinator stalled = new(node.Kahuna, "stalled", 1_000, 100_000);
        await using TtlSpanCoordinator successor = CoordinatorFor(node, "successor");
        await using TtlSpanCoordinator thirdParty = CoordinatorFor(node, "third-party");

        long? stalledToken = await stalled.TryClaimSpanAsync("db1", "t1", 1, CancellationToken.None);
        Assert.IsNotNull(stalledToken);

        await Task.Delay(2_000); // the lease lapses; nothing renews it

        long? successorToken = await successor.TryClaimSpanAsync("db1", "t1", 1, CancellationToken.None);
        Assert.IsNotNull(successorToken, "The successor must be able to take a lapsed span");

        // The late release. It must be a no-op.
        await stalled.ReleaseSpanAsync("db1", "t1", 1, stalledToken!.Value);

        Assert.IsNull(await thirdParty.TryClaimSpanAsync("db1", "t1", 1, CancellationToken.None),
            "A lapsed owner's release must not hand its successor's span to someone else");
        Assert.IsTrue(successor.StillOwnsSpan("db1", "t1", 1, successorToken!.Value));

        await stalled.DisposeAsync();
    }

    [Test]
    public async Task ALapsedOwnerCannotOverwriteItsSuccessorsCheckpoint()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-fence-checkpoint");

        TtlSpanCoordinator stalled = new(node.Kahuna, "stalled", 1_000, 100_000);
        await using TtlSpanCoordinator successor = CoordinatorFor(node, "successor");

        long? stalledToken = await stalled.TryClaimSpanAsync("db1", "t1", 2, CancellationToken.None);
        Assert.IsNotNull(stalledToken);

        await Task.Delay(2_000);

        long? successorToken = await successor.TryClaimSpanAsync("db1", "t1", 2, CancellationToken.None);
        Assert.IsNotNull(successorToken);

        TtlSpanCheckpoint successorProgress = new()
        {
            RunId = "run-1",
            LastRowIdHex = new ObjectIdValue(500, 0, 0).ToString(),
            RowsDeleted = 400,
        };
        Assert.IsTrue(await successor.TryWriteCheckpointAsync(
            "db1", "t1", 2, successorToken!.Value, null, successorProgress, CancellationToken.None));

        // The lapsed owner tries to record the progress it made before stalling — which is BEHIND the
        // successor's. Accepting it would rewind the resume point and make the successor re-scan, or
        // worse, let a later write jump past rows neither worker examined.
        bool accepted = await stalled.TryWriteCheckpointAsync(
            "db1", "t1", 2, stalledToken!.Value, null,
            new TtlSpanCheckpoint { RunId = "run-1", LastRowIdHex = new ObjectIdValue(100, 0, 0).ToString() },
            CancellationToken.None);

        Assert.IsFalse(accepted, "A lapsed owner's checkpoint write must be refused");

        TtlSpanCheckpoint? stored = await successor.ReadCheckpointAsync("db1", "t1", 2, "run-1", CancellationToken.None);
        Assert.AreEqual(successorProgress.LastRowIdHex, stored!.LastRowIdHex, "The successor's progress must survive");
        Assert.AreEqual(400, stored.RowsDeleted);

        await stalled.DisposeAsync();
    }

    [Test]
    public async Task ACheckpointMayNotMoveBackwardsOrUnmarkDone()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-checkpoint-monotonic");
        await using TtlSpanCoordinator c = CoordinatorFor(node, "worker");

        long? token = await c.TryClaimSpanAsync("db1", "t1", 6, CancellationToken.None);
        Assert.IsNotNull(token);

        TtlSpanCheckpoint first = new()
        {
            RunId = "run-1",
            LastRowIdHex = new ObjectIdValue(900, 0, 0).ToString(),
        };
        Assert.IsTrue(await c.TryWriteCheckpointAsync("db1", "t1", 6, token!.Value, null, first, CancellationToken.None));

        // Backwards is refused even for the legitimate current owner: monotonicity is a property of the
        // span's progress, not a consolation prize for losing the lease.
        Assert.IsFalse(await c.TryWriteCheckpointAsync("db1", "t1", 6, token!.Value, first,
            new TtlSpanCheckpoint { RunId = "run-1", LastRowIdHex = new ObjectIdValue(100, 0, 0).ToString() },
            CancellationToken.None), "A checkpoint must never move backwards");

        TtlSpanCheckpoint done = new()
        {
            RunId = "run-1",
            LastRowIdHex = new ObjectIdValue(900, 0, 0).ToString(),
            Done = true,
        };
        Assert.IsTrue(await c.TryWriteCheckpointAsync("db1", "t1", 6, token!.Value, first, done, CancellationToken.None));

        Assert.IsFalse(await c.TryWriteCheckpointAsync("db1", "t1", 6, token!.Value, done,
            new TtlSpanCheckpoint { RunId = "run-1", LastRowIdHex = done.LastRowIdHex, Done = false },
            CancellationToken.None), "A finished span must not be re-opened");
    }

    [Test]
    public async Task ReacquiringTheSameSpanDoesNotLetTheOldRenewerEvictTheNewClaim()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-fence-aba");
        await using TtlSpanCoordinator c = CoordinatorFor(node, "worker");

        // acquire → release → re-acquire on ONE coordinator. The first renewer's teardown runs after the
        // second claim is installed; if it removed the entry by key alone it would evict a live claim,
        // and this worker would then believe it had lost a span it actually holds.
        long? first = await c.TryClaimSpanAsync("db1", "t1", 8, CancellationToken.None);
        Assert.IsNotNull(first);
        await c.ReleaseSpanAsync("db1", "t1", 8, first!.Value);

        long? second = await c.TryClaimSpanAsync("db1", "t1", 8, CancellationToken.None);
        Assert.IsNotNull(second);
        Assert.AreNotEqual(first, second, "Each acquisition must mint a distinct token");

        // Give the first renewer's teardown time to run.
        await Task.Delay(500);

        Assert.IsTrue(c.StillOwnsSpan("db1", "t1", 8, second!.Value),
            "The new claim must survive the old renewer's teardown");
        Assert.IsFalse(c.StillOwnsSpan("db1", "t1", 8, first!.Value),
            "The superseded token must no longer be recognized");
    }

    [Test]
    public async Task ReleasingWithASupersededTokenIsANoOp()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-fence-stale-release");
        await using TtlSpanCoordinator c = CoordinatorFor(node, "worker");
        await using TtlSpanCoordinator other = CoordinatorFor(node, "other");

        long? first = await c.TryClaimSpanAsync("db1", "t1", 9, CancellationToken.None);
        await c.ReleaseSpanAsync("db1", "t1", 9, first!.Value);
        long? second = await c.TryClaimSpanAsync("db1", "t1", 9, CancellationToken.None);

        // A duplicated or delayed release carrying the old token must not free the current claim.
        await c.ReleaseSpanAsync("db1", "t1", 9, first!.Value);

        Assert.IsNull(await other.TryClaimSpanAsync("db1", "t1", 9, CancellationToken.None),
            "A stale-token release must not free the live claim");
        Assert.IsTrue(c.StillOwnsSpan("db1", "t1", 9, second!.Value));
    }

    [Test]
    public async Task EachGrantOfASpanReturnsAStrictlyGreaterFencingToken()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-fencing-order");
        await using TtlSpanCoordinator a = CoordinatorFor(node, "worker-a");
        await using TtlSpanCoordinator b = CoordinatorFor(node, "worker-b");

        // Kahuna increments the fencing token on every grant. That ordering is the whole reason to use
        // its locks rather than a self-minted identifier: equality can only tell a writer "you are not
        // the owner", whereas order tells a stored record "the writer that produced you has since been
        // superseded" — which it can decide alone, without consulting the lock.
        long? first = await a.TryClaimSpanAsync("db1", "t1", 12, CancellationToken.None);
        Assert.IsNotNull(first);
        await a.ReleaseSpanAsync("db1", "t1", 12, first!.Value);

        long? second = await b.TryClaimSpanAsync("db1", "t1", 12, CancellationToken.None);
        Assert.IsNotNull(second);

        Assert.That(second!.Value, Is.GreaterThan(first.Value),
            "A later owner must hold a strictly greater fencing token");
    }

    [Test]
    public async Task ACheckpointIsRefusedWhenTheStoredRecordCarriesAHigherFencingToken()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-fencing-refuse");

        // The stale worker must still BELIEVE it owns the span, or the local claim check refuses its
        // write before the token comparison is ever reached and this test proves nothing about ordering.
        // So: a short lease with a renewer too slow to fire — the lease lapses server-side while the
        // worker's own bookkeeping stays convinced.
        TtlSpanCoordinator a = new(node.Kahuna, "worker-a", 1_000, 100_000);
        await using TtlSpanCoordinator b = CoordinatorFor(node, "worker-b");

        long? oldToken = await a.TryClaimSpanAsync("db1", "t1", 13, CancellationToken.None);
        Assert.IsNotNull(oldToken);

        await Task.Delay(2_000); // the lease lapses; nothing renews it

        long? newToken = await b.TryClaimSpanAsync("db1", "t1", 13, CancellationToken.None);
        Assert.IsNotNull(newToken);

        Assert.IsTrue(a.StillOwnsSpan("db1", "t1", 13, oldToken!.Value),
            "The stale worker must still believe it holds the span, or this test bypasses the token guard");

        TtlSpanCheckpoint newerProgress = new()
        {
            RunId = "run-1",
            LastRowIdHex = new ObjectIdValue(700, 0, 0).ToString(),
        };
        Assert.IsTrue(await b.TryWriteCheckpointAsync(
            "db1", "t1", 13, newToken!.Value, null, newerProgress, CancellationToken.None));

        Assert.AreEqual(newToken.Value, newerProgress.OwnerFencingToken,
            "The write must stamp the token that authorized it, or a later writer has nothing to compare against");

        // Now the superseded owner presents a checkpoint based on the same stored record. The bytes it
        // compares against are current, so a CAS alone would let it through — the token ordering is what
        // stops it.
        bool accepted = await a.TryWriteCheckpointAsync(
            "db1", "t1", 13, oldToken.Value, newerProgress,
            new TtlSpanCheckpoint { RunId = "run-1", LastRowIdHex = new ObjectIdValue(800, 0, 0).ToString() },
            CancellationToken.None);

        Assert.IsFalse(accepted, "A write carrying a superseded fencing token must be refused");

        await a.DisposeAsync();
    }

    // ── Manifest publication is conditional ───────────────────────────────────

    [Test]
    public async Task TwoPlannersRacingToMintARunProduceExactlyOne()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-manifest-race");
        await using TtlSpanCoordinator a = CoordinatorFor(node, "planner-a");
        await using TtlSpanCoordinator b = CoordinatorFor(node, "planner-b");

        // Leadership can flip between reading the manifest and publishing one, so both a former and a
        // current leader can reach this point believing they may mint. Only one may win, or the table
        // ends up with two horizons and spans checkpointed under whichever landed last.
        TtlRunManifest fromA = new() { RunId = "run-a", TableId = "t1", TableName = "s", SpanCount = 4 };
        TtlRunManifest fromB = new() { RunId = "run-b", TableId = "t1", TableName = "s", SpanCount = 4 };

        using Barrier gate = new(2);
        Task<bool> ta = Task.Run(async () => { gate.SignalAndWait(); return await a.TryWriteManifestAsync("db1", null, fromA, CancellationToken.None); });
        Task<bool> tb = Task.Run(async () => { gate.SignalAndWait(); return await b.TryWriteManifestAsync("db1", null, fromB, CancellationToken.None); });

        bool[] results = await Task.WhenAll(ta, tb);

        Assert.AreEqual(1, results.Count(won => won), "Exactly one planner may publish a run");

        TtlRunManifest? stored = await a.ReadManifestAsync("db1", "t1", CancellationToken.None);
        Assert.IsNotNull(stored);
        Assert.That(stored!.RunId, Is.EqualTo("run-a").Or.EqualTo("run-b"));
    }

    [Test]
    public async Task AStaleLeaderCannotCompleteARunThatHasSinceBeenReplaced()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-manifest-stale");
        await using TtlSpanCoordinator c = CoordinatorFor(node, "planner");

        TtlRunManifest original = new() { RunId = "run-1", TableId = "t1", TableName = "s", SpanCount = 4 };
        Assert.IsTrue(await c.TryWriteManifestAsync("db1", null, original, CancellationToken.None));

        // A successor retires that run and publishes a fresh one.
        TtlRunManifest replacement = new() { RunId = "run-2", TableId = "t1", TableName = "s", SpanCount = 4 };
        Assert.IsTrue(await c.TryWriteManifestAsync("db1", original, replacement, CancellationToken.None));

        // The former leader, still holding the record it read, tries to mark ITS run complete. Accepting
        // that would retire a sweep that had barely started and push the table's next sweep a whole
        // cadence away.
        TtlRunManifest staleComplete = new()
        {
            RunId = "run-1", TableId = "t1", TableName = "s", SpanCount = 4, CompletedPhysical = 12345,
        };

        Assert.IsFalse(await c.TryWriteManifestAsync("db1", original, staleComplete, CancellationToken.None),
            "A write based on a superseded manifest must be refused");

        TtlRunManifest? stored = await c.ReadManifestAsync("db1", "t1", CancellationToken.None);
        Assert.AreEqual("run-2", stored!.RunId, "The newer run must survive");
        Assert.IsFalse(stored.IsComplete, "The newer run must not have been marked complete by a stale writer");
    }

    // ── Rate limiting is shared, not per-caller ───────────────────────────────

    [Test]
    public async Task ConcurrentCallersShareOneRateBudgetRatherThanOneEach()
    {
        // Eight callers, 40 rows each, capped at 100 rows/second. If each enforced the cap on its own
        // they would all finish in ~0.4s and the table would actually see 800 rows/second — a rate limit
        // that scales with concurrency is not a rate limit. Shared, 320 rows must take ~3.2s.
        TtlRateLimiter limiter = new(100);

        System.Diagnostics.Stopwatch sw = System.Diagnostics.Stopwatch.StartNew();

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ =>
            limiter.ThrottleAsync(40, CancellationToken.None)));

        sw.Stop();

        Assert.That(sw.Elapsed.TotalSeconds, Is.GreaterThan(2.5),
            "320 rows at 100 rows/second must take about 3.2s across all callers, not 0.4s each");
    }

    [Test]
    public async Task AnUnlimitedRateLimiterDoesNotDelay()
    {
        TtlRateLimiter limiter = new(0);

        System.Diagnostics.Stopwatch sw = System.Diagnostics.Stopwatch.StartNew();
        await limiter.ThrottleAsync(100_000, CancellationToken.None);
        sw.Stop();

        Assert.That(sw.Elapsed.TotalSeconds, Is.LessThan(0.5), "0 means unlimited, not stopped");
    }

    [Test]
    public async Task IdleTimeDoesNotAccumulateIntoABurstAllowance()
    {
        // The limiter charges pay-forward: a caller reserves its slice and waits only if the cursor is
        // already ahead of now, so exactly one call is ever free. What must NOT happen is idle time
        // banking *more* free calls — a limiter that reserved from its stale cursor instead of clamping
        // to now would let a quiet sweep burst for as long as it had been quiet.
        TtlRateLimiter limiter = new(100);

        await limiter.ThrottleAsync(50, CancellationToken.None); // reserves; returns immediately
        await Task.Delay(1_500);                                 // idle far longer than that reservation

        System.Diagnostics.Stopwatch sw = System.Diagnostics.Stopwatch.StartNew();
        await limiter.ThrottleAsync(50, CancellationToken.None); // free again — the cursor was in the past
        await limiter.ThrottleAsync(50, CancellationToken.None); // must wait: idling bought nothing extra
        sw.Stop();

        Assert.That(sw.Elapsed.TotalSeconds, Is.GreaterThan(0.4),
            "After one free call the rate must apply again; idle time must not buy additional free work");
    }

    // ── Checkpoints ───────────────────────────────────────────────────────────

    [Test]
    public async Task CheckpointOutlivesTheClaimSoAReclaimerResumesRatherThanRestarts()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-checkpoint-outlives");

        TtlSpanCoordinator dead = new(node.Kahuna, "dead", 1_000, 100_000);
        await using TtlSpanCoordinator reclaimer = CoordinatorFor(node, "reclaimer");

        long? deadToken = await dead.TryClaimSpanAsync("db1", "t1", 4, CancellationToken.None);
        Assert.IsNotNull(deadToken);
        Assert.IsTrue(await dead.TryWriteCheckpointAsync("db1", "t1", 4, deadToken!.Value, null, new TtlSpanCheckpoint
        {
            RunId = "run-1",
            LastRowIdHex = new ObjectIdValue(42, 0, 0).ToString(),
            RowsDeleted = 900,
        }, CancellationToken.None));

        await Task.Delay(2_500);

        Assert.IsNotNull(await reclaimer.TryClaimSpanAsync("db1", "t1", 4, CancellationToken.None));

        TtlSpanCheckpoint? resumed = await reclaimer.ReadCheckpointAsync("db1", "t1", 4, "run-1", CancellationToken.None);

        // The claim expired; the progress must not have. Otherwise every crash re-scans a whole span.
        Assert.IsNotNull(resumed);
        Assert.AreEqual(new ObjectIdValue(42, 0, 0).ToString(), resumed!.LastRowIdHex);
        Assert.AreEqual(900, resumed.RowsDeleted);
    }

    [Test]
    public async Task ACheckpointFromAnotherRunIsIgnored()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-checkpoint-run");

        await using TtlSpanCoordinator c = CoordinatorFor(node, "worker");

        long? token = await c.TryClaimSpanAsync("db1", "t1", 0, CancellationToken.None);
        Assert.IsNotNull(token);
        Assert.IsTrue(await c.TryWriteCheckpointAsync("db1", "t1", 0, token!.Value, null, new TtlSpanCheckpoint
        {
            RunId = "old-run",
            LastRowIdHex = new ObjectIdValue(99, 0, 0).ToString(),
        }, CancellationToken.None));

        // A previous run's progress was measured against a different horizon. Honouring it would skip
        // rows that the OLD run had already passed but the NEW run still considers expired.
        Assert.IsNull(await c.ReadCheckpointAsync("db1", "t1", 0, "new-run", CancellationToken.None));
        Assert.IsNotNull(await c.ReadCheckpointAsync("db1", "t1", 0, "old-run", CancellationToken.None));
    }

    [Test]
    public async Task AbsentCheckpointReadsAsNull()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-checkpoint-absent");
        await using TtlSpanCoordinator c = CoordinatorFor(node, "worker");

        Assert.IsNull(await c.ReadCheckpointAsync("db1", "t1", 11, "run-1", CancellationToken.None));
    }

    // ── Run manifest ──────────────────────────────────────────────────────────

    [Test]
    public async Task ManifestRoundTripsIncludingItsHorizon()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-manifest");
        await using TtlSpanCoordinator c = CoordinatorFor(node, "planner");

        TtlRunManifest written = new()
        {
            RunId = "run-abc",
            TableId = "t1",
            TableName = "sessions",
            HorizonNode = 3,
            HorizonPhysical = 1_700_000_000_000,
            HorizonCounter = 17,
            SpanCount = 32,
            StartedPhysical = 1_700_000_000_000,
        };

        Assert.IsTrue(await c.TryWriteManifestAsync("db1", null, written, CancellationToken.None));
        TtlRunManifest? read = await c.ReadManifestAsync("db1", "t1", CancellationToken.None);

        Assert.IsNotNull(read);
        Assert.AreEqual("run-abc", read!.RunId);
        Assert.AreEqual(32, read.SpanCount);

        // Every component of the horizon must survive: a run whose horizon shifted on reload would
        // apply a different expiry cutoff after a leader change than before it.
        Assert.AreEqual(written.Horizon.N, read.Horizon.N);
        Assert.AreEqual(written.Horizon.L, read.Horizon.L);
        Assert.AreEqual(written.Horizon.C, read.Horizon.C);
    }

    [Test]
    public async Task AbsentManifestReadsAsNull()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-manifest-absent");
        await using TtlSpanCoordinator c = CoordinatorFor(node, "planner");

        Assert.IsNull(await c.ReadManifestAsync("db1", "never-swept", CancellationToken.None));
    }

    [Test]
    public async Task DeletingARunRemovesItsManifestAndEverySpanRecord()
    {
        await using EmbeddedKahuna node = await CreateNodeAsync("ttl-manifest-delete");
        await using TtlSpanCoordinator c = CoordinatorFor(node, "planner");

        await c.TryWriteManifestAsync("db1", null, new TtlRunManifest
        {
            RunId = "run-x", TableId = "t9", TableName = "sessions", SpanCount = 4,
        }, CancellationToken.None);

        for (int i = 0; i < 4; i++)
        {
            long? token = await c.TryClaimSpanAsync("db1", "t9", i, CancellationToken.None);
            await c.TryWriteCheckpointAsync("db1", "t9", i, token!.Value, null,
                new TtlSpanCheckpoint { RunId = "run-x", Done = true }, CancellationToken.None);
        }

        await c.DeleteRunAsync("db1", "t9", 4, CancellationToken.None);

        Assert.IsNull(await c.ReadManifestAsync("db1", "t9", CancellationToken.None));
        for (int i = 0; i < 4; i++)
        {
            Assert.IsNull(await c.ReadCheckpointAsync("db1", "t9", i, "run-x", CancellationToken.None),
                $"Span {i}'s checkpoint must not survive its run, or the next run would resume mid-span");
        }
    }
}
