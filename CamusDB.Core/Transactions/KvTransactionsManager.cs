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
using CamusDB.Core.Cache;
using CamusDB.Core.Util.Diagnostics;

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

    // Bounded back-off for opening a Kahuna transaction while a partition leader is still being
    // elected / catching up (transient MustRetry / WaitingForReplication). 32 attempts at 25 ms
    // covers ~0.8s of node warmup, enough to absorb the startup race that background bootstrap work
    // (e.g. the orphan-branch scrub) hits before failing with a retryable error.
    private const int MaxStartRetries = 32;
    private const int StartRetryDelayMs = 25;

    // Bounded same-handle retry for commit/rollback when the coordinator returns the non-terminal
    // MustRetry ("outcome not known yet, or transient work remains — retry with the same handle").
    // On the best-effort path this is a leadership flip mid-finalize or an in-progress drain, both
    // short-lived, so a capped exponential back-off (~2s across the attempts) resolves them without
    // ever fabricating a terminal result. If it still has not resolved, the caller is told to retry
    // the SAME finalize (CADB0509) — never to replay the business operation, which could double-apply.
    private const int MaxFinalizeRetries = 12;
    private const int FinalizeRetryBaseDelayMs = 25;
    private const int FinalizeRetryMaxDelayMs = 250;

    /// <summary>Capped exponential back-off for the finalize retry loop: 25, 50, 100, 200, 250, 250…</summary>
    private static int FinalizeRetryDelayMs(int attempt) =>
        (int)Math.Min((long)FinalizeRetryBaseDelayMs << Math.Min(attempt, 16), FinalizeRetryMaxDelayMs);

    /// <summary>
    /// Mints a local HLC timestamp without opening a Kahuna transaction. Used by the cheap
    /// serializable read-only snapshot path (<see cref="BeginReadOnlyAsync"/> with Serializable
    /// default) — a purely in-memory clock bump, no Raft or network round-trip.
    /// <c>null</c> when not wired (legacy test paths); falls back to the zero-snapshot fast path.
    /// </summary>
    private readonly Func<Kommander.Time.HLCTimestamp?, Kommander.Time.HLCTimestamp>? mintLocalT;

    /// <summary>
    /// Optional cache reference. When non-null, <see cref="CommitAsync"/> drives the
    /// <see cref="CachePublishGate"/> write protocol (mark in-flight → commit/abort) and
    /// calls <see cref="IQueryResultCache.InvalidateByModifiedKeys"/> after a successful commit
    /// so stale entries are evicted before any subsequent cache probe on this node.
    /// Null when the cache is disabled; all gate calls are skipped without overhead.
    /// </summary>
    private readonly IQueryResultCache? _cache;

    private readonly Lock activeSync = new();
    private readonly List<KvTransaction> activeTransactions = [];

    public KvTransactionsManager(
        IKahuna kahuna,
        Func<Kommander.Time.HLCTimestamp?, Kommander.Time.HLCTimestamp>? mintLocalT = null,
        ILogger<ICamusDB>? logger = null,
        IQueryResultCache? cache = null)
    {
        ArgumentNullException.ThrowIfNull(kahuna);
        this.kahuna = kahuna;
        this.mintLocalT = mintLocalT;
        this.logger = logger ?? NullLogger<ICamusDB>.Instance;
        _cache = cache;
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
        KeyValueTransactionLocking? locking = null,
        ReadValidation? readValidation = null,
        DecisionDurability? decisionDurability = null,
        bool deferStart = false,
        CancellationToken cancellationToken = default)
    {
        CamusIsolationLevel level = isolationLevel ?? CamusDBConfig.DefaultIsolationLevel;
        CamusTransactionMode mode = transactionMode ?? CamusTransactionMode.ReadWrite;

        // Serializable + ReadOnly: mint a server HLC timestamp T and return a stateless
        // zero-identity transaction that carries T as its read snapshot.
        if (level == CamusIsolationLevel.Serializable && mode == CamusTransactionMode.ReadOnly)
            return await BeginSerializableReadOnlyAsync(cancellationToken).ConfigureAwait(false);

        // Concurrency strategy: caller-chosen, defaulting to pessimistic (acquire-then-write, block on
        // conflict). Optimistic defers conflict detection to the coordinator's read-set validation at
        // commit; it is opt-in per transaction. See CamusDBConfig.DefaultTransactionLocking.
        KeyValueTransactionLocking lockingMode = locking ?? CamusDBConfig.DefaultTransactionLocking;
        ReadValidation readValidationMode = readValidation ?? CamusDBConfig.DefaultReadValidation;
        DecisionDurability decisionDurabilityMode = decisionDurability ?? CamusDBConfig.DefaultDecisionDurability;

        string uniqueId = Guid.NewGuid().ToString("N");
        int mutationLimit = mutationLimitOverride ?? CamusDBConfig.MaxMutationsPerTransaction;

        // Deferred start is opt-in and used ONLY for explicit client transactions begun through
        // /start-transaction, where a subsequent SET TRANSACTION LOCKING may reconfigure the locking
        // mode before the Kahuna session opens. Internally-begun transactions (catalog/registry/stats
        // and autocommit statements) start eagerly: they never run SET TRANSACTION, and eager start
        // keeps every write/lock path — including those outside KvTableStore — valid without a
        // deferred-session guard. A deferred transaction can only reach KvTableStore and CatalogsManager
        // write/lock paths, which call EnsureSessionStartedAsync.
        if (deferStart && mintLocalT is not null)
        {
            // Build a KvTransaction shell with a locally-minted client id but no Kahuna session yet.
            // The session opens lazily on the first write/lock/folded-read operation via SessionStarter,
            // using the locking/isolation/mode values current at that point (after any SET TRANSACTION
            // statements). Kahuna pins the locking mode at session start, so this is the only way
            // SET TRANSACTION LOCKING can take effect.
            Kommander.Time.HLCTimestamp clientId = mintLocalT(null);
            KvTransaction txDeferred = new(
                Kommander.Time.HLCTimestamp.Zero, uniqueId, isReadOnly: false, level, mode,
                mutationLimit: mutationLimit, locking: lockingMode, readValidation: readValidationMode,
                clientId: clientId);

            DecisionDurability capturedDecision = decisionDurabilityMode;
            txDeferred.SessionStarter = async ct =>
            {
                TransactionHandle h = await StartKahunaTransactionAsync(
                    uniqueId, "start Kahuna transaction",
                    txDeferred.Locking, txDeferred.ReadValidation, capturedDecision, ct
                ).ConfigureAwait(false);
                txDeferred.SetSessionId(h.TransactionId);
            };

            Track(txDeferred);
            return txDeferred;
        }

        // Eager start (the default): starts the Kahuna session immediately and sets
        // TransactionId = ClientId = the Kahuna handle. Used for every internally-begun and autocommit
        // transaction, and whenever deferral was not requested.
        TransactionHandle handle = await StartKahunaTransactionAsync(
            uniqueId, "start Kahuna transaction", lockingMode, readValidationMode, decisionDurabilityMode,
            cancellationToken).ConfigureAwait(false);

        KvTransaction tx = new(handle.TransactionId, uniqueId, isReadOnly: false, level, mode,
            mutationLimit: mutationLimit, locking: lockingMode, readValidation: readValidationMode);
        Track(tx);

        // No client-side range-lock heartbeat: range-lock acquisitions are registered with the
        // coordinator, which renews their lease for the life of the session and releases them on
        // finalize. An abandoned session's locks are bounded by the session Timeout + server reaper.

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

        // A serializable read-only snapshot acquires no locks and writes nothing, so the locking mode
        // is immaterial; pessimistic keeps the start options identical to the read-write default.
        TransactionHandle handle = await StartKahunaTransactionAsync(
            uniqueId, "mint read-timestamp for serializable RO transaction",
            KeyValueTransactionLocking.Pessimistic, ReadValidation.None, DecisionDurability.BestEffort,
            cancellationToken).ConfigureAwait(false);
        Kommander.Time.HLCTimestamp t = handle.TransactionId;

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
    /// Opens a Kahuna transaction, tolerating the transient startup/leadership signals
    /// <see cref="KeyValueResponseType.MustRetry"/> and <see cref="KeyValueResponseType.WaitingForReplication"/>
    /// with a bounded back-off before giving up.
    ///
    /// <para>Kahuna returns these while a partition's leader is still being elected or its log is
    /// catching up — most visibly right after process start, when background bootstrap work (e.g. the
    /// orphan-branch scrub) begins a transaction before the embedded node has warmed up. They are
    /// <b>not</b> failures, so a single-shot start is wrong: it turns a normal warmup race into an
    /// error. Every other Kahuna call path in the engine already retries these signals; the
    /// transaction-start path must do the same.</para>
    ///
    /// <para>If the signal persists past <see cref="MaxStartRetries"/> the transaction genuinely
    /// cannot be routed yet, so this throws <see cref="CamusDBErrorCodes.TransactionMustRetry"/>
    /// (CADB0504, transient — the caller should retry the whole operation) rather than the permanent
    /// CADB0501. Any other non-<see cref="KeyValueResponseType.Set"/> response is a real failure and
    /// is surfaced immediately.</para>
    /// </summary>
    private async Task<TransactionHandle> StartKahunaTransactionAsync(
        string uniqueId, string operation, KeyValueTransactionLocking locking,
        ReadValidation readValidation, DecisionDurability decisionDurability, CancellationToken cancellationToken)
    {
        KeyValueResponseType type = KeyValueResponseType.MustRetry;
        TransactionHandle handle = default;

        for (int attempt = 0; attempt <= MaxStartRetries; attempt++)
        {
            (type, handle) = await kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    // The coordinator pins the server-side session to one partition leader by this key,
                    // and every registered operation is routed by it. Reused as the transaction's
                    // stable routing identity for commit/rollback.
                    CoordinatorKey    = uniqueId,
                    Locking           = locking,
                    ReadValidation    = readValidation,
                    DecisionDurability = decisionDurability,
                    // Bound an abandoned session server-side: the Kahuna reaper reclaims a session that
                    // is never finalized after this window plus its grace period, releasing its range
                    // locks. Non-positive leaves the server default. This replaces the client-side
                    // heartbeat as the backstop for a transaction whose client disconnects.
                    Timeout           = CamusDBConfig.MaxSerializableTransactionLifetimeMs
                },
                cancellationToken
            ).ConfigureAwait(false);

            if (type is not (KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication))
                break;

            if (attempt < MaxStartRetries)
                await Task.Delay(StartRetryDelayMs, cancellationToken).ConfigureAwait(false);
        }

        if (type == KeyValueResponseType.Set)
            return handle;

        // A still-transient signal after exhausting retries is retryable (CADB0504); anything else is
        // a real, non-retryable failure.
        if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionMustRetry,
                $"Failed to {operation}: {type} after {MaxStartRetries} retries; the node may still be starting up — retry the operation"
            );

        throw new CamusDBException(
            CamusDBErrorCodes.TransactionAlreadyCompleted,
            $"Failed to {operation}: {type}"
        );
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

        // Route through the shared start helper so a promoted read-only scan gets the same session
        // timeout (MaxSerializableTransactionLifetimeMs, not Kahuna's 5 s default) and the same
        // MustRetry/WaitingForReplication warmup retry as a read-write Begin. A bare start here would
        // give the scan's shared range locks a 5 s session lease and no retry against a still-electing
        // partition, so a slow serializable scan could have its snapshot reaped mid-read.
        TransactionHandle handle = await StartKahunaTransactionAsync(
            uniqueId, "start read-only transaction",
            KeyValueTransactionLocking.Pessimistic, ReadValidation.None, DecisionDurability.BestEffort,
            cancellationToken).ConfigureAwait(false);

        KvTransaction tx = new(handle.TransactionId, uniqueId, isReadOnly: true,
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
            // Zero-snapshot fast path: no Kahuna transaction to finalize. Covers serializable read-only
            // snapshots and a deferred-start transaction committed before its session ever opened (e.g.
            // BEGIN then COMMIT with no statement). Finalize once and untrack so a tracked transaction
            // does not linger as Active in activeTransactions; Untrack is a no-op for the untracked pure
            // snapshot, and the Active guard keeps a repeat call idempotent without flipping status.
            if (tx.Status == KvTransactionStatus.Active)
            {
                tx.Status = KvTransactionStatus.Committed;
                Untrack(tx);
            }

            // Return the minted snapshot T if available (Serializable snapshot reads),
            // otherwise return the current local HLC so RC reads also emit a usable token.
            if (!tx.ReadTimestamp.IsNull())
                return tx.ReadTimestamp;
            return mintLocalT?.Invoke(null) ?? Kommander.Time.HLCTimestamp.Zero;
        }

        // A transaction that already reached a terminal outcome cannot be finalized again. A
        // Finalizing transaction is NOT terminal: it is the resume path for a commit that returned
        // the non-terminal MustRetry, so it must fall through and retry the same handle.
        if (tx.Status is KvTransactionStatus.Committed or KvTransactionStatus.RolledBack)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                $"Transaction {tx.UniqueId} is already {tx.Status}"
            );

        // Serializable+ReadOnly transactions hold no write intents and no range locks — a lightweight
        // rollback of the empty Kahuna transaction is equivalent to commit and avoids the full 2PC
        // roundtrip. Promoted ReadCommitted+ReadOnly scan transactions are excluded: they may hold
        // shared range locks that the coordinator's finalize must release, so they go through the
        // normal commit path below.
        if (tx.IsReadOnly && tx.TransactionMode == CamusTransactionMode.ReadOnly &&
            tx.IsolationLevel == CamusIsolationLevel.Serializable)
        {
            tx.Status = KvTransactionStatus.Committed;
            Untrack(tx);
            try
            {
                await kahuna.LocateAndRollbackTransaction(tx.Handle, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort: the empty tx expires server-side on its own.
            }
            return mintLocalT?.Invoke(null) ?? tx.TransactionId;
        }

        // Pre-finalize checks run only on the first attempt (Active). A resumed commit (Finalizing,
        // retrying its handle after a non-terminal MustRetry) has already passed them and must not be
        // diverted — least of all into the lifetime-expiry rollback below, which would abort a commit
        // that may already be in flight.
        bool firstFinalizeAttempt = tx.Status == KvTransactionStatus.Active;
        if (firstFinalizeAttempt)
        {
            tx.ValidateSchemaPins();

            // Enforce the serializable transaction lifetime deadline before touching Kahuna. Only
            // Serializable+RW transactions acquire range locks whose TTL could expire mid-transaction;
            // ReadCommitted and Serializable+RO transactions are exempt. A Serializable+RW transaction
            // that has outlived MaxSerializableTransactionLifetimeMs is aborted here so its range locks
            // never expire while still considered "live" — that would silently break serializable
            // isolation. The staged writes/locks/session are handed to a coordinator rollback (not just
            // abandoned to the reaper); the caller then rolls back and retries from BeginAsync.
            if (tx.IsolationLevel == CamusIsolationLevel.Serializable &&
                tx.TransactionMode == CamusTransactionMode.ReadWrite &&
                tx.IsExpired(CamusDBConfig.MaxSerializableTransactionLifetimeMs))
            {
                tx.Status = KvTransactionStatus.Finalizing;
                try
                {
                    // Best-effort handle rollback so the coordinator drops the staged working set and
                    // session; if it stays unresolved, the session's own timeout is the backstop.
                    await RollbackHandleWithRetryAsync(tx, cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    // Never let cleanup mask the lifetime error the caller must see.
                }
                tx.Status = KvTransactionStatus.RolledBack;
                Untrack(tx);
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionLifetimeExceeded,
                    $"Serializable transaction {tx.UniqueId} exceeded the maximum lifetime " +
                    $"({CamusDBConfig.MaxSerializableTransactionLifetimeMs} ms); roll back and retry from BeginAsync");
            }
        }

        // Install the finalize fence locally: from here the transaction is no longer Active, so a
        // stray data operation is rejected (see KvTransaction.EnsureSessionStartedAsync), and a
        // concurrent/duplicate finalize sees a non-Active status. The status only advances to a
        // terminal value once Kahuna returns one.
        tx.Status = KvTransactionStatus.Finalizing;

        // Cache invalidation protocol — gate write path.
        // The keys to invalidate are sourced from the coordinator's FROZEN server-owned working set,
        // captured by closing the transaction to new registrations first (see CloseServerWorkingSetAsync).
        // This is the authoritative set that the commit below finalizes against. Sourcing from it — rather
        // than the client-side modified-key mirror — closes the window where a concurrent op folds a key
        // into the working set after a client mirror was copied but before the fence installs. The
        // affected keyspace buckets are marked in-flight so concurrent cache probes bypass rather than
        // racing to publish against an uncommitted write.
        List<(string key, KeyValueDurability durability)>? modKeys = null;
        List<string>? keyspaces = null;
        bool markedInFlight = false;
        if (_cache is not null && !tx.IsReadOnly)
        {
            modKeys = await CloseServerWorkingSetAsync(tx, cancellationToken).ConfigureAwait(false);
            if (modKeys.Count > 0)
            {
                keyspaces = ExtractUniqueKeyspaces(modKeys);
                if (keyspaces.Count > 0)
                {
                    _cache.PublishGate.MarkWriteInFlight(keyspaces);
                    markedInFlight = true;
                }
            }
        }

        // Finalize outcome semantics (Kahuna coordinator contract):
        //   Committed  → terminal success.
        //   MustRetry  → NON-terminal: the outcome is not known yet (leadership flip mid-finalize, an
        //                in-progress drain, or a durable decision not yet Completed). Retry the SAME
        //                handle; never fabricate a rollback and never replay the business operation —
        //                the write may already have committed, so a replay could double-apply.
        //   Aborted    → terminal, definite non-commit (conflict/deadline). The write did NOT commit,
        //                so replaying from BeginAsync is safe → surfaced as CADB0502 (retryable).
        //   Errored    → the handle is unknown/expired and the outcome is unavailable. Do NOT
        //                reinterpret it as a conflict (no auto-replay) → surfaced as CADB0501.
        KeyValueResponseType result = KeyValueResponseType.MustRetry;
        // Time the Kahuna 2PC commit itself — this is the span dominated by WAL fsync latency,
        // so surfacing it in the finalize log makes slow-commit diagnosis (e.g. native fsync cost)
        // directly observable per transaction.
        ValueStopwatch commitTimer = ValueStopwatch.StartNew();
        try
        {
            for (int attempt = 0; ; attempt++)
            {
                // The coordinator owns the working set: every confirmed write and exclusive point lock
                // was folded server-side as the operation completed, so commit supplies only the routing
                // handle.
                (result, string? anchor) = await kahuna.LocateAndCommitTransaction(tx.Handle, cancellationToken)
                    .ConfigureAwait(false);

                // Fold the coordinator's canonical record anchor onto the handle the moment it is known —
                // including alongside a non-terminal MustRetry — so a finalize retried after the live
                // coordinator session is lost still routes to the durable decision rather than returning
                // an unknown Errored. No-op (stays null) on the best-effort path.
                tx.CaptureRecordAnchor(anchor);

                if (result != KeyValueResponseType.MustRetry || attempt >= MaxFinalizeRetries)
                    break;

                await Task.Delay(FinalizeRetryDelayMs(attempt), cancellationToken).ConfigureAwait(false);
            }

            if (result == KeyValueResponseType.Committed)
            {
                tx.Status = KvTransactionStatus.Committed;
                if (logger.IsEnabled(LogLevel.Debug))
                {
                    long elapsedMs = commitTimer.GetElapsedMilliseconds();
                    Log.LogTransactionFinalized(logger, "committed", tx.UniqueId, elapsedMs);
                }

                // Gate: bump generation, evict stale entries, clear in-flight mark — all inside
                // the gate's write lock so no concurrent publish can insert a stale entry.
                if (markedInFlight)
                {
                    _cache!.PublishGate.CommitWrite(keyspaces!, _ => _cache.InvalidateByModifiedKeys(modKeys!));
                    markedInFlight = false;
                }

                Untrack(tx);
                return mintLocalT?.Invoke(null) ?? tx.TransactionId;
            }

            if (result == KeyValueResponseType.MustRetry)
            {
                // Non-terminal: outcome unknown, coordinator session still live. Leave the transaction
                // Finalizing and TRACKED, and leave the coordinator-owned locks in place — the caller
                // must retry the SAME commit on the SAME handle. Only the cache in-flight mark is
                // cleared so concurrent probes are not blocked while the resolution is pending.
                if (markedInFlight)
                {
                    _cache!.PublishGate.AbortWrite(keyspaces!);
                    markedInFlight = false;
                }
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionFinalizeUnresolved,
                    $"Transaction {tx.UniqueId} commit outcome is not yet resolved after {MaxFinalizeRetries} retries; " +
                    "retry COMMIT on the same transaction (do not re-run the operation)"
                );
            }

            // Terminal, non-committing outcome (Aborted / Errored / unexpected): the transaction is
            // now dead. Mark it rolled back, drop the cache mark, and untrack. The coordinator's
            // finalize already released the working set (point + range locks, read snapshots).
            tx.Status = KvTransactionStatus.RolledBack;

            if (markedInFlight)
            {
                _cache!.PublishGate.AbortWrite(keyspaces!);
                markedInFlight = false;
            }

            Untrack(tx);

            // Aborted = definite non-commit (conflict/deadline). Nothing was committed, so replaying
            // the whole operation from a fresh BeginAsync is safe → CADB0502 (retried by
            // SerializableRetryHelper for autocommit statements).
            if (result == KeyValueResponseType.Aborted)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionConflict,
                    $"Transaction {tx.UniqueId} commit was aborted by the coordinator (conflict or deadline); retry the operation from BeginAsync"
                );

            // Errored / anything else: the coordinator session is gone and the outcome is unavailable.
            // Do NOT reinterpret it as a conflict (no auto-replay) — surface it as a non-retryable error.
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                $"Transaction {tx.UniqueId} commit returned {result}; the coordinator session is gone and the outcome is unavailable"
            );
        }
        finally
        {
            // Safety net: if an unexpected exception escaped the commit body before CommitWrite or
            // AbortWrite ran, clear the in-flight mark so concurrent probes are not blocked forever.
            if (markedInFlight)
                _cache!.PublishGate.AbortWrite(keyspaces!);
        }
    }

    /// <summary>
    /// Closes the transaction's coordinator session to new registrations and returns the confirmed
    /// modified keys from the <b>frozen server-owned working set</b> — the exact set the following commit
    /// finalizes against. This is the authoritative source for cache invalidation: it waits for any
    /// in-flight operation to drain and snapshots the result, so a key folded into the working set by a
    /// concurrent op is included, unlike the client-side modified-key mirror which could miss it.
    ///
    /// <para>Close shares the finalize slot with commit but does <b>not</b> decide the transaction: it
    /// leaves the session <see cref="KvTransactionStatus.Finalizing"/> and a later commit finalizes against
    /// the same stored snapshot (repeated closes return it). A drain-deadline <c>MustRetry</c> is retried
    /// with the same bounded back-off as commit; if the set still cannot be frozen, this falls back to the
    /// client mirror so invalidation still happens (best-effort) and the commit below resolves the outcome.
    /// The frozen record anchor is folded onto the handle here too, so it is known before the first commit.</para>
    /// </summary>
    private async Task<List<(string key, KeyValueDurability durability)>> CloseServerWorkingSetAsync(
        KvTransaction tx, CancellationToken cancellationToken)
    {
        KeyValueResponseType result = KeyValueResponseType.MustRetry;
        TransactionWorkingSet? snapshot = null;
        for (int attempt = 0; ; attempt++)
        {
            (result, snapshot) = await kahuna.LocateAndCloseTransaction(
                tx.CoordinatorKey, tx.TransactionId, cancellationToken).ConfigureAwait(false);

            if (result != KeyValueResponseType.MustRetry || attempt >= MaxFinalizeRetries)
                break;

            await Task.Delay(FinalizeRetryDelayMs(attempt), cancellationToken).ConfigureAwait(false);
        }

        if (result == KeyValueResponseType.Set && snapshot is not null)
        {
            // Fold the frozen record anchor onto the handle now, before the first commit attempt.
            tx.CaptureRecordAnchor(snapshot.RecordAnchorKey);

            List<(string key, KeyValueDurability durability)> serverKeys = new(snapshot.ModifiedKeys.Count);
            foreach (KeyValueTransactionModifiedKey k in snapshot.ModifiedKeys)
                serverKeys.Add((k.Key, k.Durability));
            return serverKeys;
        }

        // Could not freeze the server set (drain-deadline MustRetry, or an unexpected close result):
        // fall back to the client mirror so cache invalidation still happens on a best-effort basis.
        return tx.GetModifiedKeyPairs();
    }

    /// <summary>
    /// Extracts the unique keyspace bucket strings from a set of modified KV keys.
    /// Row keys (<c>{dbId}:{tableId}:r/{rowId}</c>) map to <c>{dbId}:{tableId}:r</c>;
    /// index keys (<c>{dbId}:{tableId}:i:{indexId}/...</c>) map to <c>{dbId}:{tableId}:i:{indexId}</c>.
    /// Keys that do not match either pattern (e.g. schema meta keys) are skipped — they are
    /// invalidated by explicit <c>InvalidateByTableId</c> calls from DDL paths instead.
    /// Delegates to <see cref="QueryResultCache.ExtractKeyspaceBucket"/> so the bucket derivation
    /// is defined once and all three sites (dep collector, gate keyspaces, dep-index matching) agree.
    /// </summary>
    private static List<string> ExtractUniqueKeyspaces(
        List<(string key, KeyValueDurability _)> modKeys)
    {
        HashSet<string> seen = new(StringComparer.Ordinal);
        List<string> result = [];
        foreach ((string key, _) in modKeys)
        {
            string bucket = QueryResultCache.ExtractKeyspaceBucket(key);
            if (bucket.Length > 0 && seen.Add(bucket))
                result.Add(bucket);
        }
        return result;
    }

    /// <summary>
    /// Rolls back the transaction via Kahuna, releasing all acquired locks.
    /// </summary>
    public async Task RollbackAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tx);

        if (tx.TransactionId == Kommander.Time.HLCTimestamp.Zero)
        {
            // Zero-snapshot read-only fast path: no Kahuna transaction to roll back. Finalize once and
            // untrack so a tracked snapshot / never-started deferred transaction does not linger as
            // Active; Untrack is a no-op for the untracked pure snapshot, and the Active guard keeps a
            // repeat call idempotent without flipping status.
            if (tx.Status == KvTransactionStatus.Active)
            {
                tx.Status = KvTransactionStatus.RolledBack;
                Untrack(tx);
            }
            return;
        }

        // Terminal transactions cannot be rolled back again; a Finalizing transaction is the resume
        // path for a rollback that returned the non-terminal MustRetry and must fall through to retry.
        if (tx.Status is KvTransactionStatus.Committed or KvTransactionStatus.RolledBack)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                $"Transaction {tx.UniqueId} is already {tx.Status}"
            );

        // Install the finalize fence but do NOT mark terminal or untrack yet: the client-visible
        // outcome must reflect what the coordinator actually returns. Rollback only reports RolledBack
        // once every intent-bearing release is acknowledged; until then it returns MustRetry, and the
        // handle must stay valid so the SAME rollback can be retried (marking RolledBack early and
        // dropping the entry — the previous behaviour — reported success while intents could remain and
        // left the caller unable to retry the handle Kahuna still expects).
        tx.Status = KvTransactionStatus.Finalizing;

        ValueStopwatch rollbackTimer = ValueStopwatch.StartNew();

        // The coordinator rolls back from its own working set (staged writes and folded point locks);
        // the client supplies only the routing handle.
        KeyValueResponseType result = await RollbackHandleWithRetryAsync(tx, cancellationToken).ConfigureAwait(false);

        if (result == KeyValueResponseType.MustRetry)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionFinalizeUnresolved,
                $"Transaction {tx.UniqueId} rollback outcome is not yet resolved after {MaxFinalizeRetries} retries; " +
                "retry ROLLBACK on the same transaction"
            );

        // RolledBack, Aborted (a definite non-committing outcome — for a rollback request that is the
        // intended result), or Errored (the handle is unknown/expired, so the session is already gone
        // and there is nothing left to undo): all terminal and non-committing.
        tx.Status = KvTransactionStatus.RolledBack;

        if (logger.IsEnabled(LogLevel.Debug))
        {
            long elapsedMs = rollbackTimer.GetElapsedMilliseconds();
            Log.LogTransactionFinalized(logger, "rolled back", tx.UniqueId, elapsedMs);
        }

        Untrack(tx);
    }

    /// <summary>
    /// Issues <c>LocateAndRollbackTransaction</c> on the transaction's handle, retrying the
    /// coordinator's non-terminal <see cref="KeyValueResponseType.MustRetry"/> a bounded number of
    /// times with capped back-off. Returns the final coordinator outcome — the caller decides whether
    /// a persistent <c>MustRetry</c> is surfaced (explicit rollback) or swallowed (best-effort cleanup
    /// such as the lifetime-expiry abort). Retrying the same handle is exactly what the coordinator
    /// contract requires: rollback returns <c>MustRetry</c> until every intent-bearing release is
    /// acknowledged, and only then <c>RolledBack</c>.
    /// </summary>
    private async Task<KeyValueResponseType> RollbackHandleWithRetryAsync(KvTransaction tx, CancellationToken cancellationToken)
    {
        KeyValueResponseType result = KeyValueResponseType.MustRetry;
        for (int attempt = 0; ; attempt++)
        {
            result = await kahuna.LocateAndRollbackTransaction(tx.Handle, cancellationToken).ConfigureAwait(false);

            if (result != KeyValueResponseType.MustRetry || attempt >= MaxFinalizeRetries)
                break;

            await Task.Delay(FinalizeRetryDelayMs(attempt), cancellationToken).ConfigureAwait(false);
        }
        return result;
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

    public void Dispose()
    {
        lock (activeSync)
            activeTransactions.Clear();
    }
}
