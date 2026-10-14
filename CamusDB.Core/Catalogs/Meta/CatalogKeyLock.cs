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
    /// </summary>
    internal static async Task AcquireExclusiveAsync(IKahuna kahuna, KvTransaction tx, string key, int maxAttempts, string keyKind)
    {
        KeyValueResponseType lockType;
        int attempts = 0;
        TransactionOperationId operationId = TransactionOperationId.NewRandom();

        do
        {
            if (attempts > 0)
                await Task.Delay(attempts * 10).ConfigureAwait(false);

            (lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                tx.TransactionId, key, 0,
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
