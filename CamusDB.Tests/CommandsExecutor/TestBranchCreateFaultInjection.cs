
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
using Kahuna.Shared.Sequences;
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
        => new(new CommandValidator(Options), new CatalogsManager(logger), logger, Options,
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
        await using DatabaseRegistry faultRegistry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault, Options);
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
        (_, _, int live) = await rootDb.Kahuna.Kahuna.GetSnapshotFloor(CancellationToken.None);
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
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault, Options);

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
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault, Options);

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
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault, Options);

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
        await using DatabaseRegistry faultRegistry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault, Options, isClusterMode: false);
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
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault, Options);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await registry.HasDropIntentAsync("some-source-id"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry));
    }

    /// <summary>
    /// Fault fake: while armed, every write to the registry's cache-coherence generation stamp throws
    /// a transport exception. The stamp write is the post-commit notification issued after every
    /// registry mutation has already durably committed.
    /// </summary>
    private sealed class GenerationWriteThrowsKahuna : DelegatingKahuna
    {
        internal bool Armed;

        public GenerationWriteThrowsKahuna(IKahuna inner) : base(inner) { }

        public override Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
            HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision,
            KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken,
            long routedGeneration = 0, string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (Armed && key.EndsWith("dbregistry/generation", StringComparison.Ordinal))
                throw new InvalidOperationException("injected notification transport failure");

            return base.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision,
                flags, expiresMs, durability, cancellationToken, routedGeneration, coordinatorKey, operationId);
        }
    }

    /// <summary>
    /// Fault fake: while armed, finalizing any transaction commit through this registry throws a
    /// transport exception, so the commit outcome stays unknown (the transaction is left Finalizing
    /// and its exclusive locks stay held by the live coordinator session). Also records the database
    /// id allocated from the registry's id sequence while armed, because an aborted create whose
    /// registration never became visible leaves no other client-side record of the id — and the
    /// unresolved write intent makes a registry bucket scan unable to settle, by design.
    /// </summary>
    private sealed class CommitThrowsKahuna : DelegatingKahuna
    {
        internal bool Armed;
        internal string? LastArmedDatabaseId;

        public CommitThrowsKahuna(IKahuna inner) : base(inner) { }

        public override Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(
            TransactionHandle handle, CancellationToken cancellationToken)
        {
            if (Armed)
                throw new InvalidOperationException("injected commit transport failure");

            return base.LocateAndCommitTransaction(handle, cancellationToken);
        }

        public override async Task<(SequenceResponseType, SequenceAllocation)> LocateAndNextSequenceValue(
            string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
        {
            (SequenceResponseType type, SequenceAllocation allocation) =
                await base.LocateAndNextSequenceValue(name, idempotencyKey, durability, cancellationToken);

            if (Armed && type == SequenceResponseType.Success && name.EndsWith("dbregistry/seq", StringComparison.Ordinal))
                LastArmedDatabaseId = CamusDB.Core.Util.Base62.Encode(allocation.Start);

            return (type, allocation);
        }
    }

    /// <summary>
    /// A transport failure on the post-commit coherence notification must not fail the create: the
    /// branch's registration is already durably committed when the notification is written, so the
    /// old behavior — surfacing the failure and letting the abort path purge the branch's metadata,
    /// release its snapshot hold, and clear its recovery marker while the registry entry survived —
    /// destroyed a published branch. The create must succeed and the branch must stay fully intact.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task RegistrationNotificationFailure_DoesNotDestroyPublishedBranch()
    {
        GenerationWriteThrowsKahuna fault = new(TestNode!.Kahuna);
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault, Options);
        CommandExecutor executor = BuildExecutorWith(registry);

        string rootName = "r_" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor rootDb = await executor.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE t (id OID PRIMARY KEY, name STRING)", parameters: null));

        string branchName = "b_" + Guid.NewGuid().ToString("n");
        fault.Armed = true;
        try
        {
            DatabaseDescriptor branch = await executor.CreateDatabase(
                new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
            Assert.That(branch, Is.Not.Null,
                "the registration is committed before the notification, so the create must succeed");
        }
        finally
        {
            fault.Armed = false;
        }
        TrackDatabase(branchName, executor);

        DatabaseRegistryEntry? entry = registry.Get(branchName);
        Assert.That(entry, Is.Not.Null, "the committed registration must survive");

        (KeyValueResponseType metaType, _) = await TestNode.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, entry!.Id + "/meta/version", -1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None);
        Assert.That(metaType, Is.EqualTo(KeyValueResponseType.Get),
            "the published branch's schema metadata must not be purged");

        (_, _, int liveHolds) = await rootDb.Kahuna.Kahuna.GetSnapshotFloor(CancellationToken.None);
        Assert.That(liveHolds, Is.GreaterThanOrEqualTo(1),
            "the published branch's snapshot hold must not be released");

        Assert.That(await registry.PendingMarkerExistsForTestingAsync(entry.Id), Is.False,
            "a successful create must clear its pending-create marker");
    }

    /// <summary>
    /// The same notification hardening applies to every registry mutation: a drop whose unregister
    /// has committed must not fail (or leave the name resolvable) because the coherence stamp write
    /// threw afterwards.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task UnregisterNotificationFailure_DoesNotFailTheDrop()
    {
        GenerationWriteThrowsKahuna fault = new(TestNode!.Kahuna);
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault, Options);
        CommandExecutor executor = BuildExecutorWith(registry);

        string name = "s_" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name, ifNotExists: false));

        fault.Armed = true;
        try
        {
            await executor.DropDatabase(new DropDatabaseTicket(name));
        }
        finally
        {
            fault.Armed = false;
        }

        Assert.That(registry.Get(name), Is.Null, "the drop must unregister the database");
    }

    /// <summary>
    /// Retraction is identity-checked: it removes the name only while the persistent entry still
    /// carries the compensating create's id, so a replacement registered under the same name by
    /// another create can never be deleted by a stale compensation.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task RetractRegistration_IsIdentityChecked()
    {
        string name = "ret_" + Guid.NewGuid().ToString("n");
        string id = await sharedRegistry!.AllocateIdAsync();
        await sharedRegistry.RegisterAsync(name, id);

        Assert.That(await sharedRegistry.RetractRegistrationAsync(name, "not-the-owner"),
            Is.EqualTo(RegistryRetraction.OwnedByOther),
            "a mismatched id must not remove the live entry");
        Assert.That(sharedRegistry.Get(name), Is.Not.Null,
            "the live entry must survive a mismatched retraction");

        Assert.That(await sharedRegistry.RetractRegistrationAsync(name, id),
            Is.EqualTo(RegistryRetraction.Retracted),
            "the owning id must retract its own entry");
        Assert.That(sharedRegistry.Get(name), Is.Null,
            "a retracted entry must no longer resolve");

        Assert.That(await sharedRegistry.RetractRegistrationAsync(name, id),
            Is.EqualTo(RegistryRetraction.Absent),
            "retracting an absent name reports absence");
    }

    /// <summary>
    /// When the branch registration's commit outcome is unknown (a transport fault during commit),
    /// the entry may still become durable later — so the abort path must NOT run its destructive
    /// cleanup. The snapshot hold, the copied metadata, and the pending-create marker must all be
    /// retained, and the create must surface a retryable indeterminate error.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task UnresolvedRegistrationCommit_RetainsHoldMetadataAndMarker()
    {
        CommitThrowsKahuna fault = new(TestNode!.Kahuna);
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, fault, Options);
        CommandExecutor executor = BuildExecutorWith(registry);

        string rootName = "r_" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor rootDb = await executor.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE t (id OID PRIMARY KEY, name STRING)", parameters: null));

        string branchName = "b_" + Guid.NewGuid().ToString("n");
        fault.Armed = true;
        CamusDBException? ex;
        try
        {
            ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.CreateDatabase(new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName)));
        }
        finally
        {
            fault.Armed = false;
        }
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry),
            "an unknown registration commit outcome must surface a retryable indeterminate error");

        // The branch id was allocated inside the aborted create; the fault fake captured it from
        // the id-sequence call, because the unresolved registration leaves no other visible record.
        string? branchId = fault.LastArmedDatabaseId;
        Assert.That(branchId, Is.Not.Null, "sanity: the create must have allocated a branch id");

        Assert.That(await registry.PendingMarkerExistsForTestingAsync(branchId!), Is.True,
            "the pending-create marker must be retained as the recovery handle");

        (_, _, int liveHolds) = await rootDb.Kahuna.Kahuna.GetSnapshotFloor(CancellationToken.None);
        Assert.That(liveHolds, Is.GreaterThanOrEqualTo(1),
            "the snapshot hold must NOT be released while the registration may still land");

        int branchMeta = await CountMetaKeysAsync(rootDb, branchId!);
        Assert.That(branchMeta, Is.GreaterThan(0),
            "the copied branch metadata must NOT be purged while the registration may still land");
    }

    /// <summary>
    /// Fault fake: pauses a branch-create at the moment it is about to acquire the exclusive lock on
    /// the branch's own registry name key — i.e. after the fork timestamp, snapshot hold, pending
    /// marker, and metadata copy, but before any part of the registration touches the store. Pausing
    /// here (and not later) matters: once the name lock is taken, an uncommitted registration blocks a
    /// concurrent drop's descendant scan, which is a different (already-fenced) interleaving. The gate
    /// is one-shot so the abort path's own retraction of the same name key passes through freely.
    /// It also records the branch id from the pending-create marker write, since an aborted create
    /// leaves no other visible record of the id it allocated.
    /// </summary>
    private sealed class PauseBeforeBranchNameLockKahuna : DelegatingKahuna
    {
        private readonly string gatedKeySuffix;
        private readonly TaskCompletionSource paused = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource resume = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int gateArmed = 1;

        public PauseBeforeBranchNameLockKahuna(IKahuna inner, string branchName)
            : base(inner) => gatedKeySuffix = $"dbregistry/db:{branchName.ToLowerInvariant()}";

        /// <summary>Completes when the branch-create has reached (and is parked at) the gate.</summary>
        public Task Paused => paused.Task;

        /// <summary>The branch id captured from the pending-create marker write, if it happened.</summary>
        public string? ObservedBranchId { get; private set; }

        public void Resume() => resume.TrySetResult();

        public override Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
            HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision,
            KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken,
            long routedGeneration = 0, string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            const string pendingMarker = "dbregistry/pending:";
            int idx = key.IndexOf(pendingMarker, StringComparison.Ordinal);
            if (idx >= 0)
                ObservedBranchId = key[(idx + pendingMarker.Length)..];

            return base.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision,
                flags, expiresMs, durability, cancellationToken, routedGeneration, coordinatorKey, operationId);
        }

        public override async Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveLock(
            HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability,
            CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (key.EndsWith(gatedKeySuffix, StringComparison.Ordinal) && Interlocked.Exchange(ref gateArmed, 0) == 1)
            {
                paused.TrySetResult();
                await resume.Task.ConfigureAwait(false);
            }

            return await base.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability,
                cancellationToken, coordinatorKey, operationId).ConfigureAwait(false);
        }
    }

    /// <summary>Counts KV keys in the branch meta namespace without needing a live descriptor.</summary>
    private static async Task<int> CountMetaKeysHeadlessAsync(IKahuna kahuna, string dbId)
    {
        int count = 0;
        await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, $"{dbId}/meta", null, true, null, true, 512,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith($"{dbId}/meta/", StringComparison.Ordinal))
                count++;
        }
        return count;
    }

    private static async Task WaitForPauseAsync(PauseBeforeBranchNameLockKahuna gate)
    {
        Task first = await Task.WhenAny(gate.Paused, Task.Delay(TimeSpan.FromSeconds(60)));
        Assert.That(first, Is.SameAs(gate.Paused),
            "the branch-create must reach the branch-name registration point");
    }

    /// <summary>
    /// Asserts the full clean-abort outcome of a paused-then-resumed branch-create whose parent
    /// disappeared: no registry entry, no pending-create marker, no copied metadata, no live hold.
    /// </summary>
    private static async Task AssertBranchFullyCleanedUpAsync(
        DatabaseRegistry authoritativeRegistry, PauseBeforeBranchNameLockKahuna gate,
        IKahuna kahuna, string branchName, int expectedLiveHolds)
    {
        Assert.That(await authoritativeRegistry.TryResolveEntryAsync(branchName), Is.Null,
            "the aborted branch must not remain registered");

        Assert.That(gate.ObservedBranchId, Is.Not.Null,
            "sanity: the create must have written its pending-create marker before pausing");

        Assert.That(await authoritativeRegistry.PendingMarkerExistsForTestingAsync(gate.ObservedBranchId!), Is.False,
            "the pending-create marker must be cleared after a clean abort");

        int branchMeta = await CountMetaKeysHeadlessAsync(kahuna, gate.ObservedBranchId!);
        Assert.That(branchMeta, Is.EqualTo(0),
            "the copied branch metadata must be purged after a clean abort");

        (_, _, int liveHolds) = await kahuna.GetSnapshotFloor(CancellationToken.None);
        Assert.That(liveHolds, Is.EqualTo(expectedLiveHolds),
            "the aborted branch's snapshot hold must be released");
    }

    /// <summary>
    /// The drop-intent marker only fences a drop still in flight — it is released once the drop
    /// completes. A branch-create paused between its metadata copy and its registration must NOT be
    /// able to resume and publish after a concurrent drop of the parent has fully finished (intent
    /// released, keyspace purged or orphaned): the authoritative post-publication liveness re-read of
    /// the parent's immutable id must abort the create and run the full clean-abort compensation.
    /// Covered for both the FORCE (immediate purge) and the deferred (orphan-record) drop paths.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchCreate_ParentDropCompletedDuringPublicationWindow_Aborts(
        [Values(true, false)] bool force)
    {
        string branchName = "b_" + Guid.NewGuid().ToString("n");
        PauseBeforeBranchNameLockKahuna gate = new(TestNode!.Kahuna, branchName);
        await using DatabaseRegistry creatorRegistry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, gate, Options);
        await using CommandExecutor creator = BuildExecutorWith(creatorRegistry);

        string rootName = "r_" + Guid.NewGuid().ToString("n");
        await creator.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));
        await creator.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE t (id OID PRIMARY KEY, name STRING)", parameters: null));

        // "Remote node": independent registry (own cache, own descriptor/semaphore) and executor over
        // the same store, opened after the root exists so its cache resolves the root.
        await using DatabaseRegistry remoteRegistry = await DatabaseRegistry.OpenAsync(TestNode!, Options);
        await using CommandExecutor remote = BuildExecutorWith(remoteRegistry);

        Task<DatabaseDescriptor> createTask = Task.Run(() => creator.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName)));
        await WaitForPauseAsync(gate);

        // The remote node's drop runs to FULL completion — descendant scan (sees no child),
        // unregister, purge/orphan, intent release — while the create is parked.
        await remote.DropDatabase(new DropDatabaseTicket(rootName, ifExists: false, force: force));

        gate.Resume();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => createTask);
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.DatabaseDoesntExist),
            "resuming past a fully-completed parent drop must abort the branch publication");

        await AssertBranchFullyCleanedUpAsync(remoteRegistry, gate, TestNode!.Kahuna, branchName, expectedLiveHolds: 0);
    }

    /// <summary>
    /// The post-publication liveness gate must key on the parent's immutable id, never its name: when
    /// the parent is dropped AND a fresh database is recreated under the same name (new id) while the
    /// create is parked, the resumed create must still abort — its fork point, metadata copy, and
    /// snapshot hold all belong to the dead id — and the compensation must not touch the recreated
    /// database (retraction is identity-checked on the branch name only).
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchCreate_ParentRecreatedUnderNewIdDuringPublicationWindow_Aborts()
    {
        string branchName = "b_" + Guid.NewGuid().ToString("n");
        PauseBeforeBranchNameLockKahuna gate = new(TestNode!.Kahuna, branchName);
        await using DatabaseRegistry creatorRegistry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, gate, Options);
        await using CommandExecutor creator = BuildExecutorWith(creatorRegistry);

        string rootName = "r_" + Guid.NewGuid().ToString("n");
        await creator.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));
        await creator.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE t (id OID PRIMARY KEY, name STRING)", parameters: null));

        await using DatabaseRegistry remoteRegistry = await DatabaseRegistry.OpenAsync(TestNode!, Options);
        await using CommandExecutor remote = BuildExecutorWith(remoteRegistry);

        string oldRootId = (await remoteRegistry.TryResolveEntryAsync(rootName))!.Id;

        Task<DatabaseDescriptor> createTask = Task.Run(() => creator.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName)));
        await WaitForPauseAsync(gate);

        await remote.DropDatabase(new DropDatabaseTicket(rootName, ifExists: false, force: true));
        DatabaseDescriptor recreated = await remote.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, remote);
        Assert.That(recreated.Id, Is.Not.EqualTo(oldRootId), "sanity: the recreate must mint a new id");

        gate.Resume();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => createTask);
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.DatabaseDoesntExist),
            "a same-name recreate under a new id must not satisfy the parent liveness gate");

        await AssertBranchFullyCleanedUpAsync(remoteRegistry, gate, TestNode!.Kahuna, branchName, expectedLiveHolds: 0);

        DatabaseRegistryEntry? survivor = await remoteRegistry.TryResolveEntryAsync(rootName);
        Assert.That(survivor, Is.Not.Null, "the recreated database must survive the aborted create's compensation");
        Assert.That(survivor!.Id, Is.EqualTo(recreated.Id),
            "the recreated database's registration must be untouched");
    }

    /// <summary>
    /// The same completed-drop window exists when the branch source is itself a branch: dropping the
    /// intermediate parent (immediate purge — branch drops are never deferred) also releases the hold
    /// that parent owned on ITS parent, so a grandchild published against the dead intermediate would
    /// reference both a purged namespace and an unpinned snapshot chain. The resumed create must abort,
    /// and the grandparent must remain live and untouched.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchCreate_IntermediateParentDropCompletedDuringPublicationWindow_Aborts()
    {
        string branchName = "b_" + Guid.NewGuid().ToString("n");
        PauseBeforeBranchNameLockKahuna gate = new(TestNode!.Kahuna, branchName);
        await using DatabaseRegistry creatorRegistry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, gate, Options);
        await using CommandExecutor creator = BuildExecutorWith(creatorRegistry);

        string rootName = "r_" + Guid.NewGuid().ToString("n");
        await creator.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, creator);
        await creator.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE t (id OID PRIMARY KEY, name STRING)", parameters: null));

        // The intermediate parent: a branch of the root, created before the gate can trigger
        // (the gate only matches the leaf branch's name key).
        string midName = "m_" + Guid.NewGuid().ToString("n");
        await creator.CreateDatabase(new CreateDatabaseTicket(midName, ifNotExists: false, branchFrom: rootName));

        await using DatabaseRegistry remoteRegistry = await DatabaseRegistry.OpenAsync(TestNode!, Options);
        await using CommandExecutor remote = BuildExecutorWith(remoteRegistry);

        Task<DatabaseDescriptor> createTask = Task.Run(() => creator.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: midName)));
        await WaitForPauseAsync(gate);

        // The leaf is not registered yet, so the intermediate has no live descendants and its drop
        // completes — releasing the hold the intermediate owned on the root.
        await remote.DropDatabase(new DropDatabaseTicket(midName));

        gate.Resume();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => createTask);
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.DatabaseDoesntExist),
            "resuming past a fully-completed intermediate-parent drop must abort the branch publication");

        await AssertBranchFullyCleanedUpAsync(remoteRegistry, gate, TestNode!.Kahuna, branchName, expectedLiveHolds: 0);

        Assert.That(await remoteRegistry.TryResolveEntryAsync(rootName), Is.Not.Null,
            "the grandparent must remain live after the aborted create");
    }

    /// <summary>
    /// The counterpart that keeps the liveness gate honest in the other direction: a concurrent RENAME
    /// of the parent preserves its immutable id, so a branch-create parked across the rename must
    /// resume and publish successfully — a name-based re-check would wrongly abort it. The published
    /// branch's ancestry must reference the parent's (unchanged) id and its hold must stay live.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchCreate_ParentRenamedDuringPublicationWindow_Succeeds()
    {
        string branchName = "b_" + Guid.NewGuid().ToString("n");
        PauseBeforeBranchNameLockKahuna gate = new(TestNode!.Kahuna, branchName);
        await using DatabaseRegistry creatorRegistry = await DatabaseRegistry.OpenForTestingAsync(TestNode!, gate, Options);
        await using CommandExecutor creator = BuildExecutorWith(creatorRegistry);

        string rootName = "r_" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor rootDb = await creator.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));
        await creator.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE t (id OID PRIMARY KEY, name STRING)", parameters: null));

        await using DatabaseRegistry remoteRegistry = await DatabaseRegistry.OpenAsync(TestNode!, Options);
        await using CommandExecutor remote = BuildExecutorWith(remoteRegistry);

        Task<DatabaseDescriptor> createTask = Task.Run(() => creator.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName)));
        await WaitForPauseAsync(gate);

        string renamed = "rn_" + Guid.NewGuid().ToString("n");
        await remote.RenameDatabase(new RenameDatabaseTicket(rootName, renamed));
        TrackDatabase(renamed, remote);

        gate.Resume();

        DatabaseDescriptor branch = await createTask;
        TrackDatabase(branchName, creator);

        Assert.That(branch, Is.Not.Null, "a rename of the parent must not abort the branch publication");
        DatabaseRegistryEntry? branchEntry = await remoteRegistry.TryResolveEntryAsync(branchName);
        Assert.That(branchEntry, Is.Not.Null, "the branch must be durably registered");
        Assert.That(branchEntry!.Ancestors[0].DatabaseId, Is.EqualTo(rootDb.Id),
            "the branch ancestry must reference the parent's unchanged immutable id");

        (_, _, int liveHolds) = await TestNode!.Kahuna.GetSnapshotFloor(CancellationToken.None);
        Assert.That(liveHolds, Is.GreaterThanOrEqualTo(1),
            "the published branch's snapshot hold on the renamed parent must stay live");
    }
}
