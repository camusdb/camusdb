
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
    private readonly object activeSync = new();
    private readonly List<KvTransaction> activeTransactions = new();

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
            snapshot = activeTransactions.ToList();

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
    /// Commits the transaction via Kahuna 2PC.
    /// Throws <see cref="CamusDBException"/> if the transaction was already completed
    /// or if Kahuna aborts the commit.
    /// </summary>
    public async Task CommitAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tx);

        if (tx.Status != KvTransactionStatus.Active)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                $"Transaction {tx.UniqueId} is already {tx.Status}"
            );

        KeyValueResponseType result = await kahuna.LocateAndCommitTransaction(
            tx.UniqueId,
            tx.TransactionId,
            tx.GetAcquiredLocks(),
            tx.GetModifiedKeys(),
            cancellationToken
        ).ConfigureAwait(false);

        if (result == KeyValueResponseType.Committed)
        {
            tx.Status = KvTransactionStatus.Committed;
            Untrack(tx);
            return;
        }

        // Kahuna aborted — the transaction is dead; mark it rolled back so
        // a subsequent RollbackIfNotCompletedAsync is a no-op.
        tx.Status = KvTransactionStatus.RolledBack;
        Untrack(tx);

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
