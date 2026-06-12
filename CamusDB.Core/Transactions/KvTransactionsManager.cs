
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace CamusDB.Core.Transactions;

/// <summary>
/// Transaction lifecycle manager backed by Kahuna.
///
/// Replaces the legacy <c>TransactionsManager</c> (B+Tree + BufferPool based) with
/// a thin coordinator that delegates begin/commit/rollback to the embedded
/// <see cref="IKahuna"/> instance.
///
/// Concurrency model: <b>pessimistic locking</b>.
/// Each write first acquires an exclusive Kahuna lock on the key, then writes.
/// This gives standard read-committed isolation without client-side retry loops.
/// Optimistic (MVCC snapshot + CAS at commit) would reduce lock contention on
/// read-heavy workloads but requires conflict detection and retry; deferred to a
/// future phase once the basic executor is wired.
///
/// Usage pattern:
/// <code>
///   KvTransaction tx = await manager.BeginAsync(ct);
///   try {
///       await store.InsertRow(tx, id, data, ct);
///       await manager.CommitAsync(tx, ct);
///   } catch {
///       await manager.RollbackIfNotCompletedAsync(tx, ct);
///       throw;
///   }
/// </code>
/// </summary>
public sealed class KvTransactionsManager
{
    private readonly IKahuna kahuna;
    private readonly Lock activeSync = new();
    private readonly List<KvTransaction> activeTransactions = [];

    public KvTransactionsManager(IKahuna kahuna)
    {
        ArgumentNullException.ThrowIfNull(kahuna);
        this.kahuna = kahuna;
    }

    /// <summary>
    /// Rolls back every transaction still marked <see cref="KvTransactionStatus.Active"/>.
    /// Used by test fixtures that reuse a long-lived Kahuna node across methods.
    /// </summary>
    public async Task RollbackAllActiveAsync(CancellationToken cancellationToken = default)
    {
        List<KvTransaction> snapshot;
        lock (activeSync)
            snapshot = [.. activeTransactions];

        foreach (KvTransaction tx in snapshot)
        {
            try
            {
                await RollbackIfNotCompletedAsync(tx, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // best-effort cleanup
            }
        }

        lock (activeSync)
            activeTransactions.RemoveAll(tx => tx.Status != KvTransactionStatus.Active);
    }

    private void Track(KvTransaction tx)
    {
        lock (activeSync)
            activeTransactions.Add(tx);
    }

    private void Untrack(KvTransaction tx)
    {
        lock (activeSync)
            activeTransactions.Remove(tx);
    }

    /// <summary>
    /// Starts a new Kahuna transaction and returns a <see cref="KvTransaction"/> that
    /// carries the timestamp and accumulates keys for the 2PC commit.
    /// </summary>
    public async Task<KvTransaction> BeginAsync(CancellationToken cancellationToken = default)
    {
        string uniqueId = Guid.NewGuid().ToString("N");

        (KeyValueResponseType type, Kommander.Time.HLCTimestamp txId) =
            await kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    UniqueId = uniqueId,
                    Locking  = KeyValueTransactionLocking.Pessimistic
                },
                cancellationToken
            ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Set)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                $"Failed to start Kahuna transaction: {type}"
            );

        KvTransaction tx = new KvTransaction(txId, uniqueId);
        Track(tx);
        return tx;
    }

    /// <summary>
    /// Returns a synthetic read-only transaction whose <see cref="KvTransaction.TransactionId"/>
    /// is <see cref="HLCTimestamp.Zero"/>. Kahuna treats the zero timestamp as a signal to perform
    /// non-transactional reads (latest committed value, read-committed per key). No Kahuna
    /// <c>StartTransaction</c> or <c>CommitTransaction</c> round-trips are needed.
    /// </summary>
    public KvTransaction CreateReadOnlyTransaction() => KvTransaction.CreateReadOnly();

    /// <summary>
    /// Commits the transaction via Kahuna 2PC.
    /// Throws <see cref="CamusDBException"/> if the transaction was already completed
    /// or if Kahuna aborts the commit.
    /// </summary>
    public async Task CommitAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tx);

        if (tx.IsReadOnly)
            return; // read-only transactions have no Kahuna state to commit

        if (tx.Status != KvTransactionStatus.Active)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                $"Transaction {tx.UniqueId} is already {tx.Status}"
            );

        tx.ValidateSchemaPins();

        // MustRetry from LocateAndCommitTransaction is a transient routing failure (leader flip
        // during the commit round-trip); the transaction is still server-side Pending, so we can
        // safely retry the commit call. Aborted is permanent and must not be retried.
        const int MaxCommitRetries = 5;
        KeyValueResponseType result = KeyValueResponseType.MustRetry;
        for (int attempt = 0; attempt <= MaxCommitRetries; attempt++)
        {
            result = await kahuna.LocateAndCommitTransaction(
                tx.UniqueId,
                tx.TransactionId,
                tx.GetAcquiredLocks(),
                tx.GetModifiedKeys(),
                // CamusDB does not yet track per-transaction read keys (no read-key validation);
                // pass an empty set to preserve current commit semantics under the new Kahuna API.
                [],
                cancellationToken
            ).ConfigureAwait(false);

            if (result != KeyValueResponseType.MustRetry)
                break;

            if (attempt < MaxCommitRetries)
                await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }

        if (result == KeyValueResponseType.Committed)
        {
            tx.Status = KvTransactionStatus.Committed;
            await ReleaseHeldRangeLocksAsync(tx, cancellationToken).ConfigureAwait(false);
            Untrack(tx);
            return;
        }

        // Roll back the client-side transaction state so a subsequent
        // RollbackIfNotCompletedAsync is a no-op.
        tx.Status = KvTransactionStatus.RolledBack;
        await ReleaseHeldRangeLocksAsync(tx, cancellationToken).ConfigureAwait(false);
        Untrack(tx);

        // MustRetry after all retries exhausted = transient routing failure; caller must restart
        // the whole operation. Use CADB0504 so callers can distinguish this from a permanent abort.
        if (result == KeyValueResponseType.MustRetry)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionMustRetry,
                $"Transaction {tx.UniqueId} commit returned MustRetry after {MaxCommitRetries} retries; retry the operation from BeginAsync"
            );

        // Kahuna aborted — the transaction is permanently dead (conflict, timeout, etc.).
        throw new CamusDBException(
            CamusDBErrorCodes.TransactionAlreadyCompleted,
            $"Transaction {tx.UniqueId} commit returned {result}"
        );
    }

    /// <summary>
    /// Rolls back the transaction via Kahuna, releasing all acquired locks.
    /// </summary>
    public async Task RollbackAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tx);

        if (tx.IsReadOnly)
            return; // read-only transactions have no Kahuna state to roll back

        if (tx.Status != KvTransactionStatus.Active)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                $"Transaction {tx.UniqueId} is already {tx.Status}"
            );

        tx.Status = KvTransactionStatus.RolledBack;
        Untrack(tx);

        await kahuna.LocateAndRollbackTransaction(
            tx.UniqueId,
            tx.TransactionId,
            tx.GetAcquiredLocks(),
            tx.GetModifiedKeys(),
            cancellationToken
        ).ConfigureAwait(false);

        await ReleaseHeldRangeLocksAsync(tx, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Releases the exclusive prefix (range) locks the transaction acquired for serializable
    /// scans. The 2PC commit/rollback only finalizes intents on <em>modified</em> keys, so a
    /// read-only prefix lock must be released here or it would linger until its safety-net expiry.
    /// Release is idempotent (a no-op if Kahuna already cleared it); failures are swallowed so a
    /// best-effort cleanup never masks the commit/rollback outcome (the expiry is the backstop).
    /// </summary>
    /// <summary>
    /// Releases every read-only lock the transaction holds — both hash-mode prefix locks and
    /// key-range locks. Both kinds bypass the 2PC finalize path (which only clears write intents),
    /// so they are released explicitly here on commit and rollback. Best-effort throughout.
    /// </summary>
    private async Task ReleaseHeldRangeLocksAsync(KvTransaction tx, CancellationToken cancellationToken)
    {
        await ReleasePrefixLocksAsync(tx, cancellationToken).ConfigureAwait(false);
        await ReleaseRangeLocksAsync(tx, cancellationToken).ConfigureAwait(false);
    }

    private async Task ReleasePrefixLocksAsync(KvTransaction tx, CancellationToken cancellationToken)
    {
        IReadOnlyList<(string prefix, KeyValueDurability durability)> prefixLocks = tx.GetAcquiredPrefixLocks();
        if (prefixLocks.Count == 0)
            return;

        foreach ((string prefix, KeyValueDurability durability) in prefixLocks)
        {
            try
            {
                await kahuna.LocateAndTryReleaseExclusivePrefixLock(
                    tx.TransactionId, prefix, durability, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort: the lock's safety-net expiry releases it if this fails.
            }
        }
    }

    /// <summary>
    /// Releases any Kahuna key-range locks the transaction acquired (key-range routed spaces), over
    /// the same bounds they were taken on. Like prefix locks these are read-only and not finalized by
    /// the 2PC, so they are released explicitly here. Best-effort — the lock expiry is the backstop.
    /// </summary>
    private async Task ReleaseRangeLocksAsync(KvTransaction tx, CancellationToken cancellationToken)
    {
        IReadOnlyList<RangeLockBounds> rangeLocks = tx.GetAcquiredRangeLocks();
        if (rangeLocks.Count == 0)
            return;

        foreach (RangeLockBounds bounds in rangeLocks)
        {
            try
            {
                await kahuna.LocateAndTryReleaseExclusiveRangeLock(
                    tx.TransactionId, bounds.Prefix,
                    bounds.StartKey, bounds.StartInclusive, bounds.EndKey, bounds.EndInclusive,
                    bounds.Durability, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort: the lock's safety-net expiry releases it if this fails.
            }
        }
    }

    /// <summary>
    /// Rolls back the transaction only if it has not already been committed or rolled back.
    /// Safe to call in a <c>finally</c> or <c>catch</c> block without knowing the outcome.
    /// </summary>
    public async Task RollbackIfNotCompletedAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tx);

        if (tx.Status != KvTransactionStatus.Active)
            return;

        await RollbackAsync(tx, cancellationToken).ConfigureAwait(false);
    }
}
