
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Tests.Storage;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Failure-path coverage for the cross-node drop/create fence: an indeterminate branch-create abort
/// must retain the branch's recovery state instead of destroying a still-registered branch, and an
/// indeterminate drop-intent read must never be reported as "no drop".
/// </summary>
public sealed class TestBranchCreateFaultInjection : BaseTest
{
    /// <summary>
    /// Fault fake: forces the post-publication drop-intent check to observe a concurrent drop (so the
    /// create aborts) AND makes the delete of one branch's registry name key throw (so UnregisterAsync
    /// fails). Every other operation passes through to the real node.
    /// </summary>
    private sealed class AbortThenUnregisterFailsKahuna : DelegatingKahuna
    {
        private readonly string failingDeleteKeySuffix;

        public AbortThenUnregisterFailsKahuna(IKahuna inner, string branchName)
            : base(inner) => failingDeleteKeySuffix = $"dbregistry/db:{branchName.ToLowerInvariant()}";

        public override Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
            HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp,
            KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            // Any drop-intent read reports a present marker → branch-create aborts after publishing.
            if (key.Contains("dbregistry/drop-intent:", StringComparison.Ordinal))
                return Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>(
                    (KeyValueResponseType.Get, null));

            return base.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability,
                cancellationToken, coordinatorKey, operationId);
        }

        public override Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(
            HLCTimestamp transactionId, string key, KeyValueDurability durability,
            CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (key.EndsWith(failingDeleteKeySuffix, StringComparison.Ordinal))
                throw new InvalidOperationException("injected registry delete failure (UnregisterAsync)");

            return base.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken, coordinatorKey, operationId);
        }
    }

    /// <summary>
    /// Fault fake: returns a scripted sequence of statuses for the drop-intent GET, so the retry /
    /// present / indeterminate semantics of <see cref="DatabaseRegistry.HasDropIntentAsync"/> can be
    /// asserted deterministically.
    /// </summary>
    private sealed class ScriptedDropIntentKahuna : DelegatingKahuna
    {
        private readonly Queue<KeyValueResponseType> statuses;
        private readonly bool throwInstead;
        public int GetCalls { get; private set; }

        public ScriptedDropIntentKahuna(IKahuna inner, IEnumerable<KeyValueResponseType>? statuses, bool throwInstead = false)
            : base(inner)
        {
            this.statuses = new Queue<KeyValueResponseType>(statuses ?? []);
            this.throwInstead = throwInstead;
        }

        public override Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
            HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp,
            KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (key.Contains("dbregistry/drop-intent:", StringComparison.Ordinal))
            {
                GetCalls++;
                if (throwInstead)
                    throw new InvalidOperationException("injected KV read failure");

                KeyValueResponseType next = statuses.Count > 0 ? statuses.Dequeue() : KeyValueResponseType.MustRetry;
                return Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((next, null));
            }

            return base.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability,
                cancellationToken, coordinatorKey, operationId);
        }
    }

    /// <summary>Fault fake: makes acquiring the drop-intent fence throw; every other op passes through.</summary>
    private sealed class FenceAcquireThrowsKahuna : DelegatingKahuna
    {
        public FenceAcquireThrowsKahuna(IKahuna inner) : base(inner) { }

        public override Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
            HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision,
            KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken,
            long routedGeneration = 0, string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (key.Contains("dbregistry/drop-intent:", StringComparison.Ordinal))
                throw new InvalidOperationException("injected drop-fence acquire failure");

            return base.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags,
                expiresMs, durability, cancellationToken, routedGeneration, coordinatorKey, operationId);
        }
    }

    private CommandExecutor BuildExecutorWith(DatabaseRegistry registry)
        => new(new CommandValidator(), new CatalogsManager(logger), logger,
               sharedNode: TestNode!, registry: registry, isClusterMode: false);

    private static async Task<int> CountMetaKeysAsync(DatabaseDescriptor readVia, string dbId)
    {
        IKahuna kahuna = readVia.Kahuna.Kahuna;
        KvTransaction tx = await readVia.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite).ConfigureAwait(false);
        try
        {
            int count = 0;
            await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
                tx.TransactionId, $"{dbId}/meta", null, true, null, true, 512,
                HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
            {
                if (key.StartsWith($"{dbId}/meta/", StringComparison.Ordinal))
                    count++;
            }
            return count;
        }
        finally
        {
            await readVia.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// When a branch-create aborts after publishing the child and the subsequent UnregisterAsync fails,
    /// the branch is left registered — so the destructive cleanup (hold release + metadata purge +
    /// pending-marker clear) MUST be skipped and an indeterminate error surfaced, leaving a full
    /// recovery handle. Otherwise the registry would point at a purged namespace with no snapshot floor.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task IndeterminateAbort_RetainsHoldMetadataAndMarker_WhenUnregisterFails()
    {
        string branchName = "b_" + Guid.NewGuid().ToString("n");

        AbortThenUnregisterFailsKahuna fault = new(TestNode!.Kahuna, branchName);
        await using DatabaseRegistry faultRegistry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault);
        CommandExecutor executor = BuildExecutorWith(faultRegistry);

        // Root created through the fault registry (only drop-intent reads / branch-name deletes are faulted).
        string rootName = "r_" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor rootDb = await executor.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        // Give the root a table so there is real schema metadata to copy into the branch (and to prove
        // was not purged). Table DDL does not touch the faulted registry operations.
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE t (id OID PRIMARY KEY, name STRING)", parameters: null));

        // Attempt the branch create — it must fail with an indeterminate (retryable) error.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.CreateDatabase(new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName)));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry),
            "an indeterminate branch-create abort must surface a retryable error");

        // The branch is still registered (unregister failed).
        DatabaseRegistryEntry? branchEntry = faultRegistry.Get(branchName);
        Assert.That(branchEntry, Is.Not.Null, "the still-registered branch entry must be retained");

        // Its snapshot hold on the parent is still live (not released).
        (_, int live) = await rootDb.Kahuna.Kahuna.GetSnapshotFloor(CancellationToken.None);
        Assert.That(live, Is.GreaterThanOrEqualTo(1),
            "the branch's snapshot hold must NOT be released while it remains registered");

        // The pending-create recovery marker is still present.
        Assert.That(await faultRegistry.PendingMarkerExistsForTestingAsync(branchEntry!.Id), Is.True,
            "the pending-create marker must be retained as the recovery handle");

        // The branch metadata namespace was NOT purged.
        int rootMeta = await CountMetaKeysAsync(rootDb, rootDb.Id);
        int branchMeta = await CountMetaKeysAsync(rootDb, branchEntry.Id);
        Assert.That(rootMeta, Is.GreaterThan(0), $"sanity: root must have meta keys (root={rootMeta})");
        Assert.That(branchMeta, Is.GreaterThan(0),
            $"the branch metadata must NOT be purged while the branch remains registered (root={rootMeta}, branch={branchMeta})");
    }

    /// <summary>
    /// A transient drop-intent read status is retried and, once the marker is observed present, reports
    /// a drop in progress — it must never collapse a transient status into "no drop".
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task HasDropIntent_RetriesTransient_ThenReportsPresent()
    {
        ScriptedDropIntentKahuna fault = new(TestNode!.Kahuna,
            [KeyValueResponseType.MustRetry, KeyValueResponseType.WaitingForReplication, KeyValueResponseType.Get]);
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault);

        bool present = await registry.HasDropIntentAsync("some-source-id");

        Assert.That(present, Is.True, "a present marker after transient retries must report a drop in progress");
        Assert.That(fault.GetCalls, Is.EqualTo(3), "transient statuses must be retried, not treated as absent");
    }

    /// <summary>Authoritative key-absence is the only result that clears the fence.</summary>
    [Test]
    [NonParallelizable]
    public async Task HasDropIntent_DoesNotExist_ReportsAbsent()
    {
        ScriptedDropIntentKahuna fault = new(TestNode!.Kahuna, [KeyValueResponseType.DoesNotExist]);
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault);

        Assert.That(await registry.HasDropIntentAsync("some-source-id"), Is.False,
            "only an authoritative DoesNotExist may report no drop");
    }

    /// <summary>
    /// A persistently failing (throwing) drop-intent read is indeterminate — it must throw a retryable
    /// error, never silently return false, which would let a branch publish while its parent is purged.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task HasDropIntent_ReadFailure_Throws_NotFalse()
    {
        ScriptedDropIntentKahuna fault = new(TestNode!.Kahuna, statuses: null, throwInstead: true);
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await registry.HasDropIntentAsync("some-source-id"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry),
            "an indeterminate fence read must throw a retryable error, not return false");
    }

    /// <summary>
    /// Standalone (single-node) mode has no other node to race, so a drop-intent acquire failure must
    /// NOT fail the drop closed — the local semaphore guard is sufficient and the drop proceeds. This
    /// guards against the cluster fail-closed change regressing single-node drops.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task StandaloneDrop_Proceeds_WhenFenceAcquireThrows()
    {
        FenceAcquireThrowsKahuna fault = new(TestNode!.Kahuna);
        await using DatabaseRegistry faultRegistry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault, isClusterMode: false);
        CommandExecutor executor = BuildExecutorWith(faultRegistry);

        string name = "s_" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name, ifNotExists: false));

        // Must not throw — standalone falls through the fence failure and completes the drop.
        await executor.DropDatabase(new DropDatabaseTicket(name));

        Assert.That(faultRegistry.Get(name), Is.Null, "standalone drop must still unregister the database");
    }

    /// <summary>An exhausted transient retry budget is indeterminate and must throw, not return false.</summary>
    [Test]
    [NonParallelizable]
    public async Task HasDropIntent_ExhaustedTransientRetries_Throws()
    {
        // Always MustRetry: the bounded retry loop exhausts and the result stays indeterminate.
        ScriptedDropIntentKahuna fault = new(TestNode!.Kahuna,
            Enumerable.Repeat(KeyValueResponseType.MustRetry, 50));
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await registry.HasDropIntentAsync("some-source-id"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry));
    }
}
