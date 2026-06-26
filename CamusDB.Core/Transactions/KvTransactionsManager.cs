
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

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
public sealed class KvTransactionsManager : IDisposable
{
    private readonly IKahuna kahuna;
    private readonly ILogger logger;

    /// <summary>
    /// Mints a local HLC timestamp without opening a Kahuna transaction. Used by the cheap
    /// serializable read-only snapshot path (<see cref="BeginReadOnlyAsync"/> with Serializable
    /// default) — a purely in-memory clock bump, no Raft or network round-trip.
    /// <c>null</c> when not wired (legacy test paths); falls back to the zero-snapshot fast path.
    /// </summary>
    private readonly Func<Kommander.Time.HLCTimestamp?, Kommander.Time.HLCTimestamp>? mintLocalT;

    private readonly Lock activeSync = new();
    private readonly List<KvTransaction> activeTransactions = [];

    // Cancellation sources for per-transaction range-lock heartbeat tasks.
    // Keyed by TransactionId (the Kahuna HLC timestamp that uniquely identifies the tx).
    private readonly Lock heartbeatSync = new();
    private readonly Dictionary<Kommander.Time.HLCTimestamp, CancellationTokenSource> heartbeats = [];

    public KvTransactionsManager(IKahuna kahuna, Func<Kommander.Time.HLCTimestamp?, Kommander.Time.HLCTimestamp>? mintLocalT = null, ILogger<ICamusDB>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(kahuna);
        this.kahuna = kahuna;
        this.mintLocalT = mintLocalT;
        this.logger = logger ?? NullLogger<ICamusDB>.Instance;
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
    ///
    /// <para><b>Special case — Serializable + ReadOnly:</b> picks one server HLC read timestamp
    /// <c>T</c> for the whole transaction and stores it in
    /// <see cref="KvTransaction.ReadTimestamp"/>. The Kahuna transaction minted to obtain <c>T</c>
    /// is immediately rolled back so no server-side transaction state persists — the RO serializable
    /// tx is stateless on the Kahuna side (no commit/rollback needed at end) and every read is issued
    /// as-of <c>T</c>. <see cref="KvTransaction.TransactionId"/> is
    /// <see cref="HLCTimestamp.Zero"/>, identical to the Read Committed fast path, so
    /// commit/rollback remain no-ops.</para>
    ///
    /// <para><b>All other combinations:</b> <paramref name="isolationLevel"/> and
    /// <paramref name="transactionMode"/> are carried as metadata only, with no change to the
    /// underlying Kahuna transaction. Selection precedence: explicit argument →
    /// <see cref="CamusDBConfig.DefaultIsolationLevel"/> → <see cref="CamusIsolationLevel.ReadCommitted"/>.</para>
    /// </summary>
    public async Task<KvTransaction> BeginAsync(
        CamusIsolationLevel? isolationLevel = null,
        CamusTransactionMode? transactionMode = null,
        int? mutationLimitOverride = null,
        CancellationToken cancellationToken = default)
    {
        CamusIsolationLevel level = isolationLevel ?? CamusDBConfig.DefaultIsolationLevel;
        CamusTransactionMode mode = transactionMode ?? CamusTransactionMode.ReadWrite;

        // Serializable + ReadOnly: mint a server HLC timestamp T and return a stateless
        // zero-identity transaction that carries T as its read snapshot.
        if (level == CamusIsolationLevel.Serializable && mode == CamusTransactionMode.ReadOnly)
            return await BeginSerializableReadOnlyAsync(cancellationToken).ConfigureAwait(false);

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

        int mutationLimit = mutationLimitOverride ?? CamusDBConfig.MaxMutationsPerTransaction;
        KvTransaction tx = new(txId, uniqueId, isReadOnly: false, level, mode, mutationLimit: mutationLimit);
        Track(tx);

        // Start the range-lock heartbeat for Serializable+RW transactions. This background loop
        // periodically re-acquires every range lock the transaction holds, refreshing their TTL
        // so they stay valid for arbitrarily long transactions.
        if (level == CamusIsolationLevel.Serializable && mode == CamusTransactionMode.ReadWrite)
            StartHeartbeat(tx);

        return tx;
    }

    /// <summary>
    /// Starts a Kahuna transaction to mint one server HLC timestamp <c>T</c>, then returns a
    /// Serializable read-only transaction whose <see cref="KvTransaction.ReadTimestamp"/> is pinned
    /// to <c>T</c> for the lifetime of the transaction.
    ///
    /// <para>Unlike the old approach (start + immediately rollback → zero-identity), the Kahuna
    /// transaction is kept alive so the caller receives a real, registerable
    /// <see cref="KvTransaction.TransactionId"/>. This allows the HTTP layer to track and resume the
    /// transaction across multiple requests. The transaction never acquires any locks and never writes
    /// any data — it is finalized via rollback on commit or rollback (see
    /// <see cref="CommitAsync"/>).</para>
    /// </summary>
    private async Task<KvTransaction> BeginSerializableReadOnlyAsync(CancellationToken cancellationToken)
    {
        string uniqueId = Guid.NewGuid().ToString("N");

        (KeyValueResponseType type, Kommander.Time.HLCTimestamp t) =
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
                $"Failed to mint read-timestamp for serializable RO transaction: {type}"
            );

        // Keep the Kahuna transaction alive as the tracking handle — its HLC timestamp T doubles as
        // the snapshot read timestamp. Reads go out with readTimestamp=T (no Kahuna write intents);
        // the empty transaction is finalized (rolled back) by CommitAsync / RollbackAsync.
        KvTransaction tx = new(
            transactionId:   t,
            uniqueId:        uniqueId,
            isReadOnly:      true,
            isolationLevel:  CamusIsolationLevel.Serializable,
            transactionMode: CamusTransactionMode.ReadOnly,
            readTimestamp:   t
        );
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
    /// Begins a read-only transaction for a SELECT, choosing between two strategies:
    ///
    /// <para><b>Zero-snapshot fast path</b> (default, and always in single-partition / hash mode):
    /// returns a synthetic transaction at <see cref="HLCTimestamp.Zero"/> — no Kahuna round-trips,
    /// read-committed per key, commit/rollback are no-ops. Used for point reads and whenever
    /// key-range sharding is disabled (range locks are no-ops there, so a snapshot lock would buy
    /// nothing).</para>
    ///
    /// <para><b>Promoted real transaction</b> (when <paramref name="promote"/> is set <i>and</i>
    /// key-range sharding is enabled): mints a real server-assigned transaction identity via
    /// <c>LocateAndStartTransaction</c>. A scan can then hold a <em>shared</em> range lock for a
    /// serializable, phantom-free read — which is impossible under <see cref="HLCTimestamp.Zero"/>
    /// because that sentinel has no identity to own or release a lock. The caller <b>must</b>
    /// finalize a promoted transaction (commit or rollback) so its shared range locks are released
    /// and its MVCC snapshot is cleaned; this matches an implicit single-statement transaction.</para>
    /// </summary>
    public async Task<KvTransaction> BeginReadOnlyAsync(
        bool promote,
        Kommander.Time.HLCTimestamp? causalToken = null,
        CancellationToken cancellationToken = default)
    {
        if (!promote || !CamusDBConfig.KeyRangeShardingEnabled)
        {
            // Serializable default: mint a local HLC timestamp T ≥ causalToken (when provided)
            // and return a zero-identity snapshot transaction at T — no Kahuna round-trip.
            // RC default: keep the HLCTimestamp.Zero fast path (latest-committed per key).
            if (CamusDBConfig.DefaultIsolationLevel == CamusIsolationLevel.Serializable &&
                mintLocalT is not null)
                return KvTransaction.CreateSnapshotReadOnly(mintLocalT(causalToken));

            return KvTransaction.CreateReadOnly();
        }

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
                $"Failed to start read-only transaction: {type}"
            );

        KvTransaction tx = new(txId, uniqueId, isReadOnly: true,
            transactionMode: CamusTransactionMode.ReadOnly);
        Track(tx);
        return tx;
    }

    /// <summary>
    /// Commits the transaction via Kahuna 2PC.
    /// Throws <see cref="CamusDBException"/> if the transaction was already completed
    /// or if Kahuna aborts the commit.
    /// </summary>
    public async Task<Kommander.Time.HLCTimestamp> CommitAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tx);

        if (tx.TransactionId == Kommander.Time.HLCTimestamp.Zero)
        {
            // Zero-snapshot fast path: no Kahuna transaction to finalize.
            // Return the minted snapshot T if available (Serializable snapshot reads),
            // otherwise return the current local HLC so RC reads also emit a usable token.
            if (!tx.ReadTimestamp.IsNull())
                return tx.ReadTimestamp;
            return mintLocalT?.Invoke(null) ?? Kommander.Time.HLCTimestamp.Zero;
        }

        if (tx.Status != KvTransactionStatus.Active)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                $"Transaction {tx.UniqueId} is already {tx.Status}"
            );

        // Serializable+ReadOnly transactions hold no write intents and no range locks — a lightweight
        // rollback of the empty Kahuna transaction is equivalent to commit and avoids the full 2PC
        // roundtrip. Promoted ReadCommitted+ReadOnly scan transactions are excluded: they may hold
        // shared range locks that must be released via ReleaseHeldRangeLocksAsync, so they go through
        // the normal commit path below.
        if (tx.IsReadOnly && tx.TransactionMode == CamusTransactionMode.ReadOnly &&
            tx.IsolationLevel == CamusIsolationLevel.Serializable)
        {
            tx.Status = KvTransactionStatus.Committed;
            Untrack(tx);
            try
            {
                await kahuna.LocateAndRollbackTransaction(
                    tx.UniqueId, tx.TransactionId, [], [], cancellationToken
                ).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort: the empty tx expires server-side on its own.
            }
            return mintLocalT?.Invoke(null) ?? tx.TransactionId;
        }

        // Stop the range-lock heartbeat before committing so no concurrent renewal fires during 2PC.
        StopHeartbeat(tx.TransactionId);

        tx.ValidateSchemaPins();

        // Enforce the serializable transaction lifetime deadline before touching Kahuna. Only
        // Serializable+RW transactions acquire range locks whose TTL could expire mid-transaction;
        // ReadCommitted and Serializable+RO transactions are exempt. A Serializable+RW transaction
        // that has outlived MaxSerializableTransactionLifetimeMs is aborted here so its range locks
        // never expire while still considered "live" — that would silently break serializable isolation.
        // The caller must roll back and retry from BeginAsync.
        if (tx.IsolationLevel == CamusIsolationLevel.Serializable &&
            tx.TransactionMode == CamusTransactionMode.ReadWrite &&
            tx.IsExpired(CamusDBConfig.MaxSerializableTransactionLifetimeMs))
        {
            tx.Status = KvTransactionStatus.RolledBack;
            await ReleaseHeldRangeLocksAsync(tx, cancellationToken).ConfigureAwait(false);
            Untrack(tx);
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionLifetimeExceeded,
                $"Serializable transaction {tx.UniqueId} exceeded the maximum lifetime " +
                $"({CamusDBConfig.MaxSerializableTransactionLifetimeMs} ms); roll back and retry from BeginAsync");
        }

        // MustRetry from LocateAndCommitTransaction is a strictly pre-execution routing signal.
        // Kahuna returns it only when AmILeader(partitionId) was false (so no commit was attempted)
        // but WaitForLeader then resolved to the local endpoint — a transient leadership flip.
        // CommitTransaction is never called before MustRetry is returned, so the transaction is
        // still server-side Pending and commit idempotency is not required. Aborted is permanent.
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
            Log.LogTransactionFinalized(logger, "committed", tx.UniqueId, tx.GetAcquiredLocks().Count);
            await ReleaseHeldRangeLocksAsync(tx, cancellationToken).ConfigureAwait(false);
            Untrack(tx);
            return mintLocalT?.Invoke(null) ?? tx.TransactionId;
        }

        // Roll back the client-side transaction state so a subsequent
        // RollbackIfNotCompletedAsync is a no-op.
        tx.Status = KvTransactionStatus.RolledBack;
        await ReleaseHeldRangeLocksAsync(tx, cancellationToken).ConfigureAwait(false);
        Untrack(tx);

        // MustRetry after all retries exhausted = transient routing failure; caller must restart
        // the whole operation from BeginAsync. CADB0504 (not CADB0501) signals this is transient.
        // This is intentionally NOT auto-retried at the executor level: the operation may have been
        // partially applied and the tx is spent (Status = RolledBack), so re-running the DML on the
        // same tx is unsafe. Contrast with CADB0503 (fence), which fires before any write and IS
        // auto-retried inside ExecuteNonSQLQuery on the same, still-usable transaction.
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

        if (tx.TransactionId == Kommander.Time.HLCTimestamp.Zero)
            return; // Zero-snapshot read-only fast path: no Kahuna transaction to roll back

        if (tx.Status != KvTransactionStatus.Active)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                $"Transaction {tx.UniqueId} is already {tx.Status}"
            );

        StopHeartbeat(tx.TransactionId);

        tx.Status = KvTransactionStatus.RolledBack;
        Untrack(tx);

        Log.LogTransactionFinalized(logger, "rolled back", tx.UniqueId, tx.GetAcquiredLocks().Count);

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
                Log.LogRangeLockReleased(logger, "Prefix", prefix, "-∞", "+∞", tx.UniqueId);
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
                Log.LogRangeLockReleased(
                    logger, bounds.Mode.ToString(), bounds.Prefix,
                    bounds.StartKey ?? "-∞", bounds.EndKey ?? "+∞", tx.UniqueId);
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

    // -----------------------------------------------------------------------
    // Range-lock heartbeat — keeps Serializable+RW range locks alive for the
    // full duration of the transaction by periodically re-acquiring them.
    // -----------------------------------------------------------------------

    private void StartHeartbeat(KvTransaction tx)
    {
        CancellationTokenSource cts = new();
        lock (heartbeatSync)
            heartbeats[tx.TransactionId] = cts;

        _ = RangeLockHeartbeatLoopAsync(tx, cts.Token);
    }

    private void StopHeartbeat(Kommander.Time.HLCTimestamp txId)
    {
        CancellationTokenSource? cts;
        lock (heartbeatSync)
        {
            if (!heartbeats.Remove(txId, out cts))
                return;
        }
        cts.Cancel();
        cts.Dispose();
    }

    /// <summary>
    /// Cancels every outstanding range-lock heartbeat loop and releases its cancellation source.
    /// Called when the owning database is closed/disposed: an unstopped loop keeps awaiting
    /// <see cref="Task.Delay(int, CancellationToken)"/> forever, which roots this manager (and
    /// therefore the entire Kahuna node it references) and prevents it from being collected.
    /// Safe to call multiple times.
    /// </summary>
    public void StopAllHeartbeats()
    {
        List<CancellationTokenSource> sources;
        lock (heartbeatSync)
        {
            sources = [.. heartbeats.Values];
            heartbeats.Clear();
        }

        foreach (CancellationTokenSource cts in sources)
        {
            try
            {
                cts.Cancel();
                cts.Dispose();
            }
            catch
            {
                // best-effort: a source already disposed by a racing StopHeartbeat is fine
            }
        }
    }

    public void Dispose()
    {
        StopAllHeartbeats();

        lock (activeSync)
            activeTransactions.Clear();
    }

    private async Task RangeLockHeartbeatLoopAsync(KvTransaction tx, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(CamusDBConfig.RangeLockHeartbeatIntervalMs, ct).ConfigureAwait(false);

                if (ct.IsCancellationRequested || tx.Status != KvTransactionStatus.Active)
                    break;

                IReadOnlyList<RangeLockBounds> locks = tx.GetAcquiredRangeLocks();
                foreach (RangeLockBounds b in locks)
                {
                    if (ct.IsCancellationRequested)
                        break;
                    try
                    {
                        // Use the loop's cancellation token (not None) so a StopHeartbeat fired by
                        // CommitAsync/RollbackAsync cancels an in-flight renewal. Otherwise a renewal
                        // racing the finalize could re-acquire a range lock the commit/rollback just
                        // released (Kahuna's acquire handler does no tx-state check and would create a
                        // fresh lock), leaving a committed transaction's lock lingering for one TTL.
                        await kahuna.LocateAndTryAcquireRangeLock(
                            tx.TransactionId, b.Prefix,
                            b.StartKey, b.StartInclusive, b.EndKey, b.EndInclusive,
                            CamusDBConfig.RangeLockExpiresMs, b.Durability, b.Mode,
                            ct
                        ).ConfigureAwait(false);
                    }
                    catch
                    {
                        // Best-effort renewal: a transient failure is tolerated. If the lock
                        // expires before the next heartbeat fires, the transaction will fail on
                        // the next operation (which is the correct safety outcome).
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown — commit or rollback cancelled the loop.
        }
        catch
        {
            // Any other exception in the background loop is swallowed; the foreground path
            // will detect a stale lock on the next operation.
        }
    }
}
