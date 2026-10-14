
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Transactions;
using Kahuna;
using Kahuna.Shared.KeyValue;

namespace CamusDB.Core.Catalogs.Meta;

/// <summary>
/// Reads and writes a single catalog metadata key inside the caller's DDL transaction. Every
/// catalog write goes through here, so the two rules below are enforced once instead of at each
/// of the two dozen call sites.
///
/// <para><b>Both operations lock the key first, then act.</b> The exclusive lock and the write are
/// separate Kahuna round-trips, and each is retried independently with linear backoff up to
/// <see cref="MetaKeyMaxRetries"/> attempts. The lock goes through
/// <see cref="CatalogKeyLock.AcquireExclusiveAsync"/>: a key another transaction holds
/// (<c>AlreadyLocked</c>) is retried until the holder leaves, and a lock still held when the budget
/// runs out raises the retryable <see cref="CamusDBErrorCodes.TransactionMustRetry"/>. The write
/// retries a transient status (<c>MustRetry</c>, <c>WaitingForReplication</c>). Anything else is a hard
/// failure and raises <see cref="CamusDBErrorCodes.SystemSpaceCorrupt"/>. Do not treat contention as
/// corruption.</para>
///
/// <para><b>The write's operation id is allocated once, outside the retry loop, and reused by every
/// attempt.</b> That is what makes a replayed call after a lost response fold into the coordinator's
/// working set rather than apply twice. Allocating a fresh id per attempt would make the DDL
/// transaction's commit-from-working-set miss the key, so the metadata write would be silently
/// dropped at commit. This is the single most important detail in this class. The lock follows the
/// same rule for <c>MustRetry</c>, but takes a fresh id after <c>AlreadyLocked</c>, because that
/// attempt is finished and a repeated id only replays its denial (see
/// <see cref="CatalogKeyLock"/>).</para>
///
/// <para>Every successful operation calls <c>KvTransaction.TrackModified</c>, which is how the
/// commit and the rollback paths learn the key was touched. A write that skipped it would not be
/// undone by a rollback.</para>
/// </summary>
internal static class MetaKeyWriter
{
    /// <summary>
    /// Attempt ceiling for the lock and for the write, counted separately. Reached only when the
    /// partition stays unavailable, or another transaction holds the key, across roughly five seconds
    /// of linear backoff.
    /// </summary>
    private const int MetaKeyMaxRetries = 32;

    internal static async Task WriteMetaKey(IKahuna kahuna, KvTransaction tx, string key, byte[] value)
    {
        await CatalogKeyLock.AcquireExclusiveAsync(kahuna, tx, key, MetaKeyMaxRetries, "meta").ConfigureAwait(false);

        // A stable operation id reused across the retry loop so a replayed call after a lost response
        // folds once into the coordinator working set instead of applying twice. The write must fold, or
        // the DDL transaction's commit-from-working-set would not persist this meta key.
        TransactionOperationId setOperationId = TransactionOperationId.NewRandom();

        KeyValueResponseType setType;
        int setRetries = 0;

        do
        {
            if (setRetries > 0)
                await Task.Delay(setRetries * 10).ConfigureAwait(false);

            (setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
                tx.TransactionId, key, value, null, -1,
                KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, CancellationToken.None,
                coordinatorKey: tx.CoordinatorKey, operationId: setOperationId
            ).ConfigureAwait(false);
        }
        while (setType is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && ++setRetries < MetaKeyMaxRetries);

        if (setType != KeyValueResponseType.Set)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to write meta key '{key}': {setType}"
            );

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    internal static async Task DeleteMetaKey(IKahuna kahuna, KvTransaction tx, string key)
    {
        await CatalogKeyLock.AcquireExclusiveAsync(kahuna, tx, key, MetaKeyMaxRetries, "meta").ConfigureAwait(false);

        // A stable operation id reused across the retry loop (see WriteMetaKey) so the delete folds once
        // into the coordinator working set and the DDL commit persists the removal.
        TransactionOperationId deleteOperationId = TransactionOperationId.NewRandom();

        KeyValueResponseType deleteType;
        int deleteRetries = 0;

        do
        {
            if (deleteRetries > 0)
                await Task.Delay(deleteRetries * 10).ConfigureAwait(false);

            (deleteType, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
                tx.TransactionId, key, KeyValueDurability.Persistent, CancellationToken.None,
                coordinatorKey: tx.CoordinatorKey, operationId: deleteOperationId
            ).ConfigureAwait(false);
        }
        while (deleteType is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && ++deleteRetries < MetaKeyMaxRetries);

        if (deleteType is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to delete meta key '{key}': {deleteType}"
            );

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }
}
