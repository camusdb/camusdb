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
/// Takes the exclusive lock on one system key (catalog metadata, the database registry, the auth
/// keyspace) inside the caller's transaction, with a short bounded retry. Every lock loop over those
/// keys goes through here, because the operation-id rule below is easy to get wrong and silent when
/// it is wrong.
///
/// <para><b>A retry after <c>AlreadyLocked</c> needs a fresh operation id.</b> The lock is registered
/// with the transaction coordinator under an operation id, and Kahuna answers a repeated id with the
/// outcome it cached for that id, a failure included. That replay is what a retry wants after
/// <c>MustRetry</c>: the first attempt can still be pending, and the replay folds it into the
/// coordinator's lock set once. It is not what a retry wants after <c>AlreadyLocked</c>: that attempt
/// is finished and recorded no lock, so the same id only replays the denial, and the loop never sees
/// the holder commit and leave. A fresh id is minted after every <c>AlreadyLocked</c>, and kept after
/// <c>MustRetry</c>.</para>
///
/// <para><b>Lock lifetime.</b> With no lease the lock lives as long as the caller's transaction: until its
/// commit or rollback, or until the session ceiling when its node is gone. A lease makes Kahuna drop the lock
/// when it runs out, which a caller asks for when others wait for the key and must not wait for a transaction
/// that is gone. A write staged under a lease that ran out can no longer commit: Kahuna refuses the commit
/// for certain. The commit itself claims the lock, which then ignores its lease.</para>
///
/// <para>A lock still contended when the budget runs out is reported as
/// <see cref="CamusDBErrorCodes.TransactionMustRetry"/>: another writer holds the key, or its partition
/// is briefly unavailable, and nothing is wrong with the store. Any other refusal is unexpected for a
/// system key and is reported as <see cref="CamusDBErrorCodes.SystemSpaceCorrupt"/>. Both are thrown
/// before the caller's commit request.</para>
/// </summary>
internal static class CatalogKeyLock
{
    /// <summary>
    /// Locks <paramref name="key"/> for <paramref name="tx"/>, making at most
    /// <paramref name="maxAttempts"/> attempts with linear backoff (10 ms times the attempt number).
    /// <paramref name="keyKind"/> names the keyspace in the error message ("meta", "registry", "auth").
    /// <paramref name="leaseMs"/> is the lease of the lock; zero ties it to the transaction. A call for a key
    /// that the transaction holds starts its lease again.
    /// </summary>
    internal static async Task AcquireExclusiveAsync(
        IKahuna kahuna, KvTransaction tx, string key, int maxAttempts, string keyKind, int leaseMs = 0)
    {
        KeyValueResponseType lockType;
        int attempts = 0;
        TransactionOperationId operationId = TransactionOperationId.NewRandom();

        do
        {
            if (attempts > 0)
                await Task.Delay(attempts * 10).ConfigureAwait(false);

            (lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                tx.TransactionId, key, leaseMs,
                KeyValueDurability.Persistent, CancellationToken.None,
                coordinatorKey: tx.CoordinatorKey, operationId: operationId
            ).ConfigureAwait(false);

            if (lockType == KeyValueResponseType.AlreadyLocked)
                operationId = TransactionOperationId.NewRandom();
        }
        while (lockType is KeyValueResponseType.AlreadyLocked or KeyValueResponseType.MustRetry
               && ++attempts < maxAttempts);

        if (lockType == KeyValueResponseType.Locked)
            return;

        if (lockType is KeyValueResponseType.AlreadyLocked or KeyValueResponseType.MustRetry)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionMustRetry,
                $"The {keyKind} key '{key}' is still locked by another transaction after {maxAttempts} attempts ({lockType}); retry the statement");

        throw new CamusDBException(
            CamusDBErrorCodes.SystemSpaceCorrupt,
            $"Failed to lock {keyKind} key '{key}': {lockType}");
    }
}
