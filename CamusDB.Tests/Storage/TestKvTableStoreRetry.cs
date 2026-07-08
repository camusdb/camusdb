
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using static CamusDB.Core.Util.ObjectIds.ObjectIdGenerator;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Generation-fence retry audit.
///
/// Every key-range-touching call site in <see cref="KvTableStore"/> must absorb a transient
/// <see cref="KeyValueResponseType.MustRetry"/> (the generation-fence response during a
/// key-range split) and re-resolve rather than propagate the error to the caller.
///
/// <list type="bullet">
///   <item>Single-key point ops go through <c>RetryOnMustRetry</c> (private static helper).</item>
///   <item>Batch lock/set/delete ops have their own per-key inline retry loops
///         (<c>AcquireManyWithRetry</c>, <c>SetManyWithRetry</c>, <c>DeleteManyWithRetry</c>).</item>
///   <item>Scan ops (<c>ScanRows</c>, <c>ScanIndex</c>, <c>DropIndexEntries</c>) call
///         <c>LocateAndScanRange</c> which handles <c>MustRetry</c> internally inside Kahuna's
///         <c>KeyValuesManager</c> page-level backoff + cursor-resume loop — no CamusDB-side
///         retry is needed or tested here.</item>
/// </list>
/// </summary>
[TestFixture]
public sealed class TestKvTableStoreRetry
{
    // -----------------------------------------------------------------------
    // Fault-injecting IKahuna wrapper
    // -----------------------------------------------------------------------

    /// <summary>
    /// Wraps a real <see cref="IKahuna"/> and returns
    /// <see cref="KeyValueResponseType.MustRetry"/> N times for specific operations before
    /// delegating to the underlying node. All other interface methods are delegated unchanged.
    /// </summary>
    private sealed class FaultInjectingKahuna(IKahuna inner) : IKahuna
    {
        // Decrement post-check counters. Set to N before the call under test;
        // the wrapper returns MustRetry N times then delegates to the real node.
        public int InjectAcquireLockFaults;
        public int InjectGetValueFaults;
        public int InjectSetKeyValueFaults;
        public int InjectDeleteKeyValueFaults;
        public int InjectAcquireManyFaults;
        public int InjectSetManyFaults;
        public int InjectDeleteManyFaults;

        // ---- intercepted: single-key exclusive lock ----
        public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> LocateAndTryAcquireExclusiveLock(
            HLCTimestamp txId, string key, int expiresMs, KeyValueDurability durability, CancellationToken ct)
        {
            if (InjectAcquireLockFaults-- > 0)
                return Task.FromResult((KeyValueResponseType.MustRetry, string.Empty, durability, HLCTimestamp.Zero));
            return inner.LocateAndTryAcquireExclusiveLock(txId, key, expiresMs, durability, ct);
        }

        // ---- intercepted: single-key get ----
        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
            HLCTimestamp txId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken ct)
        {
            if (InjectGetValueFaults-- > 0)
                return Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((KeyValueResponseType.MustRetry, null));
            return inner.LocateAndTryGetValue(txId, key, revision, readTimestamp, durability, ct);
        }

        // ---- intercepted: single-key set ----
        public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
            HLCTimestamp txId, string key, byte[]? value, byte[]? compareValue, long compareRevision,
            KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken ct, long routedGeneration = 0)
        {
            if (InjectSetKeyValueFaults-- > 0)
                return Task.FromResult((KeyValueResponseType.MustRetry, -1L, HLCTimestamp.Zero));
            return inner.LocateAndTrySetKeyValue(txId, key, value, compareValue, compareRevision, flags, expiresMs, durability, ct, routedGeneration);
        }

        // ---- intercepted: single-key delete ----
        public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(
            HLCTimestamp txId, string key, KeyValueDurability durability, CancellationToken ct)
        {
            if (InjectDeleteKeyValueFaults-- > 0)
                return Task.FromResult((KeyValueResponseType.MustRetry, -1L, HLCTimestamp.Zero));
            return inner.LocateAndTryDeleteKeyValue(txId, key, durability, ct);
        }

        // ---- pass-through: MVCC snapshot floor ----
        public Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)> LocateAndAcquireSnapshotHold(string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken ct)
            => inner.LocateAndAcquireSnapshotHold(holderId, timestamp, leaseMs, ct);

        public Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)> LocateAndRenewSnapshotHold(string holdId, int leaseMs, CancellationToken ct)
            => inner.LocateAndRenewSnapshotHold(holdId, leaseMs, ct);

        public Task<KeyValueResponseType> LocateAndReleaseSnapshotHold(string holdId, CancellationToken ct)
            => inner.LocateAndReleaseSnapshotHold(holdId, ct);

        public Task<(HLCTimestamp EffectiveFloor, int LiveHolds)> GetSnapshotFloor(CancellationToken ct)
            => inner.GetSnapshotFloor(ct);

        // ---- intercepted: batch acquire locks ----
        public Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> LocateAndTryAcquireManyExclusiveLocks(
            HLCTimestamp txId, List<(string key, int expiresMs, KeyValueDurability durability)> keys, CancellationToken ct)
        {
            if (InjectAcquireManyFaults-- > 0)
                return Task.FromResult(keys.Select(k => (KeyValueResponseType.MustRetry, k.key, k.durability, HLCTimestamp.Zero)).ToList());
            return inner.LocateAndTryAcquireManyExclusiveLocks(txId, keys, ct);
        }

        // ---- intercepted: batch set ----
        // For the whole-batch case (InjectSetManyFaults), ALL keys return MustRetry and nothing
        // is written to the real node on the faulted call(s).
        //
        // For the partial-mix case (SetManyPartialFaultPredicate), keys matching the predicate
        // return MustRetry WITHOUT being sent to the real node; all other keys are delegated
        // immediately. The predicate is cleared after the first activation so the retry (which
        // sends only the faulted keys) goes straight to the real node. This exercises the guard
        // in SetManyWithRetry that rebuilds pending from MustRetry-only responses rather than
        // resending the full original batch — if that guard regressed, an already-Set unique key
        // would come back NotSet → false DuplicateUniqueKeyValue.
        public Func<string, bool>? SetManyPartialFaultPredicate;

        public async Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(
            List<KahunaSetKeyValueRequestItem> items, CancellationToken ct)
        {
            if (InjectSetManyFaults-- > 0)
                return items.Select(i => new KahunaSetKeyValueResponseItem { Key = i.Key, Type = KeyValueResponseType.MustRetry }).ToList();

            if (SetManyPartialFaultPredicate is { } pred)
            {
                SetManyPartialFaultPredicate = null; // one-shot: next call goes straight to inner
                List<KahunaSetKeyValueRequestItem> toSet   = items.Where(i => !pred(i.Key ?? "")).ToList();
                List<KahunaSetKeyValueRequestItem> toFault = items.Where(i =>  pred(i.Key ?? "")).ToList();

                List<KahunaSetKeyValueResponseItem> innerResults = toSet.Count > 0
                    ? await inner.LocateAndTrySetManyKeyValue(toSet, ct).ConfigureAwait(false)
                    : [];

                return [..innerResults, ..toFault.Select(i => new KahunaSetKeyValueResponseItem { Key = i.Key, Type = KeyValueResponseType.MustRetry })];
            }

            return await inner.LocateAndTrySetManyKeyValue(items, ct).ConfigureAwait(false);
        }

        // ---- intercepted: batch delete ----
        public Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(
            List<KahunaDeleteKeyValueRequestItem> items, CancellationToken ct)
        {
            if (InjectDeleteManyFaults-- > 0)
                return Task.FromResult(items.Select(i => new KahunaDeleteKeyValueResponseItem { Key = i.Key, Type = KeyValueResponseType.MustRetry }).ToList());
            return inner.LocateAndTryDeleteManyKeyValue(items, ct);
        }

        // ---- delegated: transaction lifecycle ----
        public Task<(KeyValueResponseType, HLCTimestamp)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken ct)
            => inner.LocateAndStartTransaction(options, ct);

        public Task<KeyValueResponseType> LocateAndCommitTransaction(
            string uniqueId, HLCTimestamp ts,
            List<KeyValueTransactionModifiedKey> locks, List<KeyValueTransactionModifiedKey> modified,
            List<KeyValueTransactionReadKey> reads, CancellationToken ct)
            => inner.LocateAndCommitTransaction(uniqueId, ts, locks, modified, reads, ct);

        public Task<KeyValueResponseType> LocateAndRollbackTransaction(
            string uniqueId, HLCTimestamp ts,
            List<KeyValueTransactionModifiedKey> locks, List<KeyValueTransactionModifiedKey> modified,
            CancellationToken ct)
            => inner.LocateAndRollbackTransaction(uniqueId, ts, locks, modified, ct);

        // ---- delegated: scan (Kahuna handles MustRetry internally per page) ----
        public IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> LocateAndScanRange(
            HLCTimestamp txId, string prefix,
            string? startKey, bool startInclusive,
            string? endKey, bool endInclusive,
            int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken ct)
            => inner.LocateAndScanRange(txId, prefix, startKey, startInclusive, endKey, endInclusive, pageSize, readTimestamp, durability, ct);

        // ---- delegated: key-range registration ----
        public void RegisterKeyRange(string keySpace) => inner.RegisterKeyRange(keySpace);
        public Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken ct = default) => inner.RegisterKeyRangeAsync(keySpace, ct);

        // ---- delegated: range and prefix lock release (needed for rollback) ----
        public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp txId, string key, KeyValueDurability durability, CancellationToken ct)
            => inner.LocateAndTryReleaseExclusiveLock(txId, key, durability, ct);

        public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(HLCTimestamp txId, List<(string key, KeyValueDurability durability)> keys, CancellationToken ct)
            => inner.LocateAndTryReleaseManyExclusiveLocks(txId, keys, ct);

        // ---- all remaining IKahuna members: not called by KvTableStore in these tests ----
        public Task<(LockResponseType, long)> LocateAndTryLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<(LockResponseType, long)> LocateAndTryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<LockResponseType> LocateAndTryUnlock(string resource, byte[] owner, LockDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<(LockResponseType, ReadOnlyLockEntry?)> LocateAndGetLock(string resource, LockDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<(LockResponseType, long)> TryLock(string resource, byte[] owner, int expiresMs, LockDurability durability) => throw new NotSupportedException();
        public Task<(LockResponseType, long)> TryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability) => throw new NotSupportedException();
        public Task<LockResponseType> TryUnlock(string resource, byte[] owner, LockDurability durability) => throw new NotSupportedException();
        public Task<(LockResponseType, ReadOnlyLockEntry?)> GetLock(string resource, LockDurability durability) => throw new NotSupportedException();
        public Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue2(List<KahunaSetKeyValueRequestItem> items, CancellationToken ct) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryExistsValue(HLCTimestamp txId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<KeyValueResponseType> LocateAndTryCheckWriteIntent(HLCTimestamp txId, string key, KeyValueDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(HLCTimestamp txId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys, CancellationToken ct) => throw new NotSupportedException();
        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(HLCTimestamp txId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys, CancellationToken ct) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(HLCTimestamp txId, string key, int expiresMs, KeyValueDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<KeyValueGetByBucketResult> LocateAndGetByBucket(HLCTimestamp txId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<KeyValueGetByRangeResult> LocateAndGetByRange(HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(HLCTimestamp txId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, long routedGeneration = 0) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(HLCTimestamp txId, string key, int expiresMs, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(HLCTimestamp txId, string key, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<List<KahunaDeleteKeyValueResponseItem>> DeleteManyNodeKeyValue(List<KahunaDeleteKeyValueRequestItem> items) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(HLCTimestamp txId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryGetManyValues(HLCTimestamp txId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(HLCTimestamp txId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryExistsManyValues(HLCTimestamp txId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys) => throw new NotSupportedException();
        public Task<KeyValueResponseType> TryCheckWriteIntentValue(HLCTimestamp txId, string key, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<KeyValueResponseType> LocateAndTryAcquireExclusivePrefixLock(HLCTimestamp txId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, HLCTimestamp)> LocateAndTryAcquireExclusiveRangeLock(HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, HLCTimestamp)> LocateAndTryAcquireRangeLock(HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode, CancellationToken ct) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, HLCTimestamp)> TryAcquireRangeLock(HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode) => throw new NotSupportedException();
        public Task<List<KeyValueRangeLock>> GetRangeLocks(string keySpace) => throw new NotSupportedException();
        public Task ImportRangeLocks(string keySpace, List<KeyValueRangeLock> locks) => throw new NotSupportedException();
        public Task<KeyValueResponseType> LocateAndTryReleaseExclusivePrefixLock(HLCTimestamp txId, string prefixKey, KeyValueDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<KeyValueResponseType> LocateAndTryReleaseExclusiveRangeLock(HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(HLCTimestamp txId, HLCTimestamp commitId, string key, KeyValueDurability durability, CancellationToken ct, long routedGeneration = 0) => throw new NotSupportedException();
        public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(HLCTimestamp txId, HLCTimestamp commitId, List<(string key, KeyValueDurability durability)> keys, CancellationToken ct) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp txId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(HLCTimestamp txId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken ct) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp txId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(HLCTimestamp txId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken ct) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> TryAcquireExclusiveLock(HLCTimestamp txId, string key, int expiresMs, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(HLCTimestamp txId, string prefixKey, int expiresMs, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, HLCTimestamp)> TryAcquireExclusiveRangeLock(HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(HLCTimestamp txId, string key, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(HLCTimestamp txId, string prefixKey, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(HLCTimestamp txId, HLCTimestamp commitId, string key, KeyValueDurability durability, long routedGeneration = 0) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, long)> TryCommitMutations(HLCTimestamp txId, string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, long)> TryRollbackMutations(HLCTimestamp txId, string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<KeyValueTransactionResult> TryExecuteTransactionScript(byte[] script, string? hash, List<KeyValueParameter>? parameters) => throw new NotSupportedException();
        public Task<KeyValueGetByBucketResult> GetByBucket(HLCTimestamp txId, string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<KeyValueGetByBucketResult> ScanByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability) => throw new NotSupportedException();
        public Task<KeyValueGetByBucketResult> ScanAllByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<(KeyValueResponseType, HLCTimestamp)> StartTransaction(KeyValueTransactionOptions options) => throw new NotSupportedException();
        public Task<KeyValueResponseType> CommitTransaction(HLCTimestamp ts, List<KeyValueTransactionModifiedKey> locks, List<KeyValueTransactionModifiedKey> modified, List<KeyValueTransactionReadKey> reads) => throw new NotSupportedException();
        public Task<KeyValueResponseType> RollbackTransaction(HLCTimestamp ts, List<KeyValueTransactionModifiedKey> locks, List<KeyValueTransactionModifiedKey> modified) => throw new NotSupportedException();
        public Task<(SequenceResponseType, ReadOnlySequenceEntry?)> LocateAndGetSequence(string name, SequenceDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<(SequenceResponseType, long)> LocateAndCreateSequence(string name, long initialValue, long increment, long? maxValue, SequenceDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<(SequenceResponseType, SequenceAllocation)> LocateAndNextSequenceValue(string name, string? idempotencyKey, SequenceDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<(SequenceResponseType, SequenceAllocation)> LocateAndReserveSequenceRange(string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<SequenceResponseType> LocateAndDeleteSequence(string name, SequenceDurability durability, CancellationToken ct) => throw new NotSupportedException();
        public Task<bool> OnLogRestored(int partitionId, RaftLog log) => throw new NotSupportedException();
        public Task<bool> OnReplicationReceived(int partitionId, RaftLog log) => throw new NotSupportedException();
        public void OnReplicationError(int partitionId, RaftLog log) => throw new NotSupportedException();
        public Task FlushPersistenceAsync() => throw new NotSupportedException();
        public Task<int> TriggerAutoSplitAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task<int> TriggerAutoMergeAsync(CancellationToken ct = default) => throw new NotSupportedException();

        public bool IsBackupConfigured => inner.IsBackupConfigured;

        public Task BootstrapFromPitrBackupAsync(string backupDir, Guid leafBackupId, HLCTimestamp targetTime, IWAL walAdapter, TimeSpan pitrWindow, TimeSpan baseSnapshotInterval)
            => inner.BootstrapFromPitrBackupAsync(backupDir, leafBackupId, targetTime, walAdapter, pitrWindow, baseSnapshotInterval);

        public Task<KahunaBackupInfo> TakeFullBackupAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task<KahunaBackupInfo> TakeIncrementalBackupAsync(Guid parentBackupId, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<KahunaBackupInfo> TakeCoordinatedBackupAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<KahunaBackupInfo>> ListBackupsAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<KahunaBackupInfo>> GetBackupChainAsync(Guid leafBackupId, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<KahunaRestoreResponse> RestoreToAsync(Guid leafBackupId, string targetDir, long targetTimeMs, CancellationToken ct = default) => throw new NotSupportedException();
    }

    // -----------------------------------------------------------------------
    // Transaction helpers
    // -----------------------------------------------------------------------

    private static async Task<KvTransaction> BeginTransaction(IKahuna kahuna, string uniqueId)
    {
        (KeyValueResponseType type, HLCTimestamp txId) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { UniqueId = uniqueId, Locking = KeyValueTransactionLocking.Pessimistic },
            CancellationToken.None
        );
        Assert.AreEqual(KeyValueResponseType.Set, type);
        return new KvTransaction(txId, uniqueId);
    }

    private static async Task CommitTransaction(IKahuna kahuna, KvTransaction tx)
    {
        KeyValueResponseType result = await kahuna.LocateAndCommitTransaction(
            tx.UniqueId,
            tx.TransactionId,
            tx.GetAcquiredLocks(),
            tx.GetModifiedKeys(),
            [],
            CancellationToken.None
        );
        Assert.AreEqual(KeyValueResponseType.Committed, result);
    }

    // -----------------------------------------------------------------------
    // Node / store factory
    // -----------------------------------------------------------------------

    private static async Task<(EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store)> CreateStoreAsync(string tableId)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tableId}/warmup", CancellationToken.None);
        FaultInjectingKahuna stub = new(node.Kahuna);
        return (node, stub, new KvTableStore(stub, "testdb", tableId));
    }

    // -----------------------------------------------------------------------
    // Single-key retry tests (RetryOnMustRetry)
    // -----------------------------------------------------------------------

    [Test]
    public async Task GetRow_SurvivesMustRetryOnGet()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_get");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = Generate();
        byte[] data = [1, 2, 3, 4];

        KvTransaction writeTx = await BeginTransaction(stub, "rt_get_w");
        await store.InsertRow(writeTx, rowId, data);
        await CommitTransaction(stub, writeTx);

        stub.InjectGetValueFaults = 2;

        KvTransaction readTx = await BeginTransaction(stub, "rt_get_r");
        byte[]? result = await store.GetRow(readTx, rowId);
        await CommitTransaction(stub, readTx);

        Assert.IsNotNull(result);
        Assert.AreEqual(data, result);
        Assert.AreEqual(-1, stub.InjectGetValueFaults, "All 2 injected faults were consumed before success");
    }

    [Test]
    public async Task InsertRow_AcquireLock_SurvivesMustRetry()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_lock");
        await using EmbeddedKahuna __ = node;

        stub.InjectAcquireLockFaults = 2;

        ObjectIdValue rowId = Generate();
        byte[] data = [10, 20, 30];

        KvTransaction tx = await BeginTransaction(stub, "rt_lock_w");
        await store.InsertRow(tx, rowId, data);
        await CommitTransaction(stub, tx);

        Assert.AreEqual(-1, stub.InjectAcquireLockFaults, "All 2 injected lock faults were consumed");

        KvTransaction readTx = await BeginTransaction(stub, "rt_lock_r");
        byte[]? result = await store.GetRow(readTx, rowId);
        await CommitTransaction(stub, readTx);

        Assert.IsNotNull(result);
        Assert.AreEqual(data, result);
    }

    [Test]
    public async Task InsertRow_SetKeyValue_SurvivesMustRetry()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_set");
        await using EmbeddedKahuna __ = node;

        stub.InjectSetKeyValueFaults = 2;

        ObjectIdValue rowId = Generate();
        byte[] data = [5, 6, 7, 8];

        KvTransaction tx = await BeginTransaction(stub, "rt_set_w");
        await store.InsertRow(tx, rowId, data);
        await CommitTransaction(stub, tx);

        Assert.AreEqual(-1, stub.InjectSetKeyValueFaults, "All 2 injected set faults were consumed");

        KvTransaction readTx = await BeginTransaction(stub, "rt_set_r");
        byte[]? result = await store.GetRow(readTx, rowId);
        await CommitTransaction(stub, readTx);

        Assert.IsNotNull(result);
        Assert.AreEqual(data, result);
    }

    [Test]
    public async Task DeleteRow_SurvivesMustRetry()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_del");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = Generate();

        KvTransaction writeTx = await BeginTransaction(stub, "rt_del_w");
        await store.InsertRow(writeTx, rowId, [1, 2, 3]);
        await CommitTransaction(stub, writeTx);

        stub.InjectDeleteKeyValueFaults = 2;

        KvTransaction deleteTx = await BeginTransaction(stub, "rt_del_d");
        await store.DeleteRow(deleteTx, rowId);
        await CommitTransaction(stub, deleteTx);

        Assert.AreEqual(-1, stub.InjectDeleteKeyValueFaults, "All 2 injected delete faults were consumed");

        KvTransaction readTx = await BeginTransaction(stub, "rt_del_r");
        byte[]? result = await store.GetRow(readTx, rowId);
        await CommitTransaction(stub, readTx);

        Assert.IsNull(result, "Row must be absent after DeleteRow");
    }

    // -----------------------------------------------------------------------
    // Batch retry tests — write path
    // -----------------------------------------------------------------------

    [Test]
    public async Task WriteRowsBatch_AcquireManyWithRetry_SurvivesMustRetry()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_batchlock");
        await using EmbeddedKahuna __ = node;

        const int Count = 5;
        List<(ObjectIdValue rowId, byte[] data)> expected = Enumerable.Range(0, Count)
            .Select(i => (Generate(), new byte[] { (byte)i }))
            .ToList();

        List<KvTableStore.RowWrite> batch = expected.Select(e => new KvTableStore.RowWrite
        {
            RowId = e.rowId,
            RowData = e.data,
        }).ToList();

        stub.InjectAcquireManyFaults = 2;

        KvTransaction tx = await BeginTransaction(stub, "rt_batchlock_w");
        await store.WriteRowsBatch(tx, batch);
        await CommitTransaction(stub, tx);

        Assert.AreEqual(-1, stub.InjectAcquireManyFaults, "All 2 batch-acquire faults were consumed");

        KvTransaction readTx = await BeginTransaction(stub, "rt_batchlock_r");
        foreach ((ObjectIdValue rowId, byte[] data) in expected)
        {
            byte[]? result = await store.GetRow(readTx, rowId);
            Assert.IsNotNull(result, $"Row {rowId} must be readable after WriteRowsBatch");
            Assert.AreEqual(data, result);
        }
        await CommitTransaction(stub, readTx);
    }

    [Test]
    public async Task WriteRowsBatch_SetManyWithRetry_SurvivesMustRetry()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_batchset");
        await using EmbeddedKahuna __ = node;

        const int Count = 5;
        List<(ObjectIdValue rowId, byte[] data)> expected = Enumerable.Range(0, Count)
            .Select(i => (Generate(), new byte[] { (byte)(i + 10) }))
            .ToList();

        List<KvTableStore.RowWrite> batch = expected.Select(e => new KvTableStore.RowWrite
        {
            RowId = e.rowId,
            RowData = e.data,
        }).ToList();

        stub.InjectSetManyFaults = 2;

        KvTransaction tx = await BeginTransaction(stub, "rt_batchset_w");
        await store.WriteRowsBatch(tx, batch);
        await CommitTransaction(stub, tx);

        Assert.AreEqual(-1, stub.InjectSetManyFaults, "All 2 batch-set faults were consumed");

        KvTransaction readTx = await BeginTransaction(stub, "rt_batchset_r");
        foreach ((ObjectIdValue rowId, byte[] data) in expected)
        {
            byte[]? result = await store.GetRow(readTx, rowId);
            Assert.IsNotNull(result, $"Row {rowId} must be readable after WriteRowsBatch");
            Assert.AreEqual(data, result);
        }
        await CommitTransaction(stub, readTx);
    }

    // -----------------------------------------------------------------------
    // Batch retry tests — delete path
    // -----------------------------------------------------------------------

    [Test]
    public async Task DeleteRowsBatch_AcquireManyWithRetry_SurvivesMustRetry()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_delock");
        await using EmbeddedKahuna __ = node;

        const int Count = 4;
        List<ObjectIdValue> rowIds = Enumerable.Range(0, Count).Select(_ => Generate()).ToList();

        // Write rows without fault injection
        List<KvTableStore.RowWrite> batch = rowIds.Select(id => new KvTableStore.RowWrite
        {
            RowId = id,
            RowData = [99],
        }).ToList();

        KvTransaction writeTx = await BeginTransaction(stub, "rt_delock_w");
        await store.WriteRowsBatch(tx: writeTx, rows: batch);
        await CommitTransaction(stub, writeTx);

        // Now delete with injected AcquireMany faults
        List<KvTableStore.RowDelete> deletes = rowIds.Select(id => new KvTableStore.RowDelete { RowId = id }).ToList();

        stub.InjectAcquireManyFaults = 2;

        KvTransaction deleteTx = await BeginTransaction(stub, "rt_delock_d");
        await store.DeleteRowsBatch(deleteTx, deletes);
        await CommitTransaction(stub, deleteTx);

        Assert.AreEqual(-1, stub.InjectAcquireManyFaults, "All 2 batch-acquire-for-delete faults were consumed");

        KvTransaction readTx = await BeginTransaction(stub, "rt_delock_r");
        foreach (ObjectIdValue rowId in rowIds)
        {
            byte[]? result = await store.GetRow(readTx, rowId);
            Assert.IsNull(result, $"Row {rowId} must be absent after DeleteRowsBatch");
        }
        await CommitTransaction(stub, readTx);
    }

    [Test]
    public async Task DeleteRowsBatch_DeleteManyWithRetry_SurvivesMustRetry()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_delmany");
        await using EmbeddedKahuna __ = node;

        const int Count = 4;
        List<ObjectIdValue> rowIds = Enumerable.Range(0, Count).Select(_ => Generate()).ToList();

        List<KvTableStore.RowWrite> batch = rowIds.Select(id => new KvTableStore.RowWrite
        {
            RowId = id,
            RowData = [42],
        }).ToList();

        KvTransaction writeTx = await BeginTransaction(stub, "rt_delmany_w");
        await store.WriteRowsBatch(tx: writeTx, rows: batch);
        await CommitTransaction(stub, writeTx);

        List<KvTableStore.RowDelete> deletes = rowIds.Select(id => new KvTableStore.RowDelete { RowId = id }).ToList();

        stub.InjectDeleteManyFaults = 2;

        KvTransaction deleteTx = await BeginTransaction(stub, "rt_delmany_d");
        await store.DeleteRowsBatch(deleteTx, deletes);
        await CommitTransaction(stub, deleteTx);

        Assert.AreEqual(-1, stub.InjectDeleteManyFaults, "All 2 batch-delete faults were consumed");

        KvTransaction readTx = await BeginTransaction(stub, "rt_delmany_r");
        foreach (ObjectIdValue rowId in rowIds)
        {
            byte[]? result = await store.GetRow(readTx, rowId);
            Assert.IsNull(result, $"Row {rowId} must be absent after DeleteRowsBatch");
        }
        await CommitTransaction(stub, readTx);
    }

    // -----------------------------------------------------------------------
    // Partial-mix SetMany test — pins the duplicate-avoidance guard
    // -----------------------------------------------------------------------

    /// <summary>
    /// Covers the realistic split scenario: the first <c>LocateAndTrySetManyKeyValue</c> call
    /// returns a partial response — the unique index entry is <c>Set</c>, while the (non-unique) row
    /// key returns <c>MustRetry</c> without being written. The retry must resend <em>only</em> the
    /// <c>MustRetry</c> (row) key; if it mistakenly resent the already-<c>Set</c> unique index key
    /// too, that key would come back <c>NotSet</c> on re-attempt, which <c>SetManyWithRetry</c>
    /// promotes to <c>DuplicateUniqueKeyValue</c> — a false positive. The unique key must be the
    /// already-Set one, since re-Setting the non-unique row (<c>Flags.Set</c>) is a harmless overwrite.
    ///
    /// The <see cref="FaultInjectingKahuna.SetManyPartialFaultPredicate"/> is used to split the
    /// batch on the first call: it delegates all non-faulted keys immediately (they are committed
    /// to the real node) and returns <c>MustRetry</c> for the matching key without writing it.
    /// The predicate is cleared so the retry goes straight to the real node.
    /// </summary>
    [Test]
    public async Task WriteRowsBatch_SetManyPartialMustRetry_DoesNotResendAlreadySetUniqueKey()
    {
        (EmbeddedKahuna node, FaultInjectingKahuna stub, KvTableStore store) = await CreateStoreAsync("tbl_retry_partial");
        await using EmbeddedKahuna __ = node;

        ObjectIdValue rowId = Generate();
        byte[] rowData = [7, 8, 9];

        // Unique index entry — this is the key that gets SET on the first (partial) call and must
        // therefore NOT be resent. Fixed composite value so its KV key reliably contains ":i:".
        CompositeColumnValue indexKey = new([new ColumnValue(ColumnType.Integer64, 42L)]);
        const string IndexId = "idx_unique_test";

        KvTableStore.RowWrite row = new()
        {
            RowId = rowId,
            RowData = rowData,
        };
        row.IndexEntries.Add(new KvTableStore.IndexWrite(IndexId, indexKey, Unique: true));

        // Fault predicate: the first call faults the NON-index keys (the row key "{tableId}:r/…")
        // and SETs the unique index key. The retry must resend only the row key; if it also resent
        // the already-Set unique index key, that key would come back NotSet → DuplicateUniqueKeyValue.
        // (The unique key MUST be the already-Set one — re-Setting a non-unique row is a harmless
        // overwrite, so faulting the index key instead would not exercise this guard.)
        stub.SetManyPartialFaultPredicate = key => !key.Contains(":i:", StringComparison.Ordinal);

        KvTransaction tx = await BeginTransaction(stub, "rt_partial_w");
        // Should complete without throwing DuplicateUniqueKeyValue.
        await store.WriteRowsBatch(tx, [row]);
        await CommitTransaction(stub, tx);

        Assert.IsNull(stub.SetManyPartialFaultPredicate, "Predicate must have been consumed on the first call");

        // Verify the row is readable.
        KvTransaction readTx = await BeginTransaction(stub, "rt_partial_r");
        byte[]? result = await store.GetRow(readTx, rowId);
        await CommitTransaction(stub, readTx);

        Assert.IsNotNull(result, "Row must be readable after WriteRowsBatch with partial MustRetry");
        Assert.AreEqual(rowData, result);

        // Verify the unique index entry is present by doing a point lookup.
        KvTransaction lookupTx = await BeginTransaction(stub, "rt_partial_lu");
        ObjectIdValue? found = await store.LookupUnique(lookupTx, IndexId, indexKey);
        await CommitTransaction(stub, lookupTx);

        Assert.AreEqual(rowId, found, "Unique index entry must map to the inserted row");
    }
}
