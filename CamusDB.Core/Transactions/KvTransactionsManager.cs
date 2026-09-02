/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using CamusDB.Core.Cache;
using CamusDB.Core.Config;
using CamusDB.Core.Storage;
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

    /// <summary>Configuration for the engine these transactions belong to; injected, never ambient.</summary>
    private CamusDBOptions options;

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Reference assignment is atomic and the
    /// record itself stays immutable; readers pin the field once at the top of an operation, so an
    /// in-flight operation keeps the snapshot it started with and a change takes effect at the
    /// next operation boundary.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;
    private readonly ILogger logger;

    // Bounded back-off for opening a Kahuna transaction while a partition leader is still being
    // elected / catching up (transient MustRetry / WaitingForReplication). 32 attempts at 25 ms
    // covers ~0.8s of node warmup, enough to absorb the startup race that background bootstrap work
    // (e.g. the orphan-branch scrub) hits before failing with a retryable error.
    private const int MaxStartRetries = 32;
    private const int StartRetryDelayMs = 25;

    // Same-handle retry for commit/rollback when the coordinator returns the non-terminal MustRetry
    // ("outcome not known yet, or transient work remains — retry with the same handle"). The retry is
    // bounded by a wall-clock budget rather than an attempt count: every condition behind MustRetry —
    // a leadership flip mid-finalize, an in-progress drain, a participant write shed after ageing in
    // the storage layer's pre-dispatch queue, a durable decision not yet marked complete — is measured
    // in time, and a saturated node makes each one take longer without taking more tries. An attempt
    // cap therefore shrinks the effective budget exactly when the node most needs it, turning a
    // recoverable in-doubt commit into a spurious error. Once the budget is spent the caller is told to
    // retry the SAME finalize (CADB0509) — never to replay the business operation, which could
    // double-apply.
    private const int FinalizeRetryBaseDelayMs = 25;
    private const int FinalizeRetryMaxDelayMs = 500;

    /// <summary>Capped exponential back-off for the finalize retry loop: 25, 50, 100, 200, 400, 500, 500…</summary>
    private static int FinalizeRetryDelayMs(int attempt) =>
        (int)Math.Min((long)FinalizeRetryBaseDelayMs << Math.Min(attempt, 16), FinalizeRetryMaxDelayMs);

    /// <summary>
    /// Waits out the back-off before the next same-handle finalize attempt, or reports that the retry
    /// budget is spent. Returns <see langword="false"/> — without sleeping — when the next delay would
    /// not leave room for another attempt inside
    /// <see cref="CamusDBOptions.TransactionFinalizeRetryBudgetMs"/>, so the loop never overshoots the
    /// budget by a whole back-off interval and a non-positive budget yields exactly one attempt.
    /// </summary>
    private async ValueTask<bool> DelayBeforeFinalizeRetryAsync(int attempt, ValueStopwatch elapsed, CancellationToken cancellationToken)
    {
        int delayMs = FinalizeRetryDelayMs(attempt);

        if (elapsed.GetElapsedMilliseconds() + delayMs > options.TransactionFinalizeRetryBudgetMs)
            return false;

        await Task.Delay(delayMs, cancellationToken).ConfigureAwait(false);
        return true;
    }

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

    /// <summary>
    /// Live transactions, keyed by <see cref="KvTransaction.ClientId"/> — the identity that is
    /// immutable from construction (a deferred-start transaction's <c>TransactionId</c> changes when
    /// its session starts, so it must not be the key). A concurrent dictionary keeps begin/finalize
    /// O(1) and lock-free; the previous <c>List</c> + global lock made every finalize an O(n) scan
    /// under a lock every other begin/finalize contended on. Consumed only by the test-fixture bulk
    /// rollback and disposal, so neither ordering nor duplicates matter.
    /// </summary>
    private readonly ConcurrentDictionary<Kommander.Time.HLCTimestamp, KvTransaction> activeTransactions = new();

    public KvTransactionsManager(
        IKahuna kahuna,
        CamusDBOptions options,
        Func<Kommander.Time.HLCTimestamp?, Kommander.Time.HLCTimestamp>? mintLocalT = null,
        ILogger<ICamusDB>? logger = null,
        IQueryResultCache? cache = null)
    {
        ArgumentNullException.ThrowIfNull(kahuna);
        this.kahuna = kahuna;
        this.options = options;
        this.mintLocalT = mintLocalT;
        this.logger = logger ?? NullLogger<ICamusDB>.Instance;
        _cache = cache;
        diskSpace = new DiskSpaceMonitor(options.DataDirectory, this.logger);
    }

    /// <summary>
    /// Watches free space on the volume that holds <see cref="CamusDBOptions.DataDirectory"/> for
    /// the write-admission gate. The data directory is <see cref="ConfigMutability.Restart"/>-class,
    /// so latching it here at construction is correct; the watermark itself is read from the live
    /// <see cref="options"/> snapshot on every check.
    /// </summary>
    private readonly DiskSpaceMonitor diskSpace;

    /// <summary>
    /// Write-admission gate installed on every read-write transaction as
    /// <see cref="KvTransaction.WriteAdmissionGate"/>: refuses a new mutation with
    /// <see cref="CamusDBErrorCodes.InsufficientDiskSpace"/> while the data volume's free space is
    /// below <see cref="CamusDBOptions.MinFreeDiskBytes"/>. Runs before the mutation is counted,
    /// so nothing reaches the storage engine — the refusal is what keeps a nearly-full disk out of
    /// a hard ENOSPC during a memtable flush. Reads the watermark from the current options
    /// snapshot on every call, so a runtime change takes effect at the next statement.
    /// </summary>
    private void EnsureDiskSpaceForWrites()
    {
        long minFreeBytes = options.MinFreeDiskBytes;
        if (minFreeBytes <= 0)
            return;

        if (!diskSpace.IsBelow(minFreeBytes, out long freeBytes))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InsufficientDiskSpace,
            $"Write rejected: the data volume has {freeBytes} bytes free, below the 'min_free_disk_bytes' " +
            $"watermark of {minFreeBytes}; free disk space on this node or lower the watermark, then retry");
    }

    /// <summary>
    /// True while any transaction started here has not reached a terminal state — either still
    /// <see cref="KvTransactionStatus.Active"/> or <see cref="KvTransactionStatus.Finalizing"/>.
    ///
    /// <para>Read by idle eviction, which must refuse to dispose a database whose transactions are
    /// still running. <c>Finalizing</c> counts as unfinished on purpose: a commit or rollback that
    /// returned the coordinator's non-terminal <c>MustRetry</c> stays in that state, deliberately
    /// tracked, because the client is told to retry the <em>same</em> finalize. Treating it as idle
    /// would let eviction dispose the very transaction the caller is coming back to resolve.</para>
    ///
    /// <para>Deliberately checks the status rather than the dictionary's count: a finalized transaction
    /// can linger in the map until its untrack lands, and treating that as "busy" would make a database
    /// with churn permanently un-evictable.</para>
    /// </summary>
    public bool HasUnfinishedTransactions
    {
        get
        {
            foreach (KvTransaction tx in activeTransactions.Values)
            {
                if (tx.Status is KvTransactionStatus.Active or KvTransactionStatus.Finalizing)
                    return true;
            }

            return false;
        }
    }

    /// <summary>
    /// Rolls back every transaction still marked <see cref="KvTransactionStatus.Active"/>.
    /// Used by test fixtures that reuse a long-lived Kahuna node across methods.
    /// </summary>
    public async Task RollbackAllActiveAsync(CancellationToken cancellationToken = default)
    {
        foreach (KvTransaction tx in activeTransactions.Values)
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

        // Through Untrack, not a bare TryRemove: every exit from the tracking map must go through the
        // one place that keeps the active-transaction gauge equal to the map.
        foreach (KeyValuePair<Kommander.Time.HLCTimestamp, KvTransaction> entry in activeTransactions)
        {
            if (entry.Value.Status != KvTransactionStatus.Active)
                Untrack(entry.Value);
        }
    }

    /// <summary>
    /// Mints the tracking identity (<see cref="KvTransaction.ClientId"/>) for a transaction this
    /// manager registers. Every tracked identity must come from THIS node's clock: the HTTP/gRPC
    /// transaction coordinator keys its tracking map by <c>(ClientId.L, ClientId.C)</c> — the wire
    /// handle carries no node component — so identities minted by two different clocks can collide
    /// on that pair. A Kahuna session id is minted by whichever node leads the session's
    /// coordinator partition, i.e. by another node's clock for most sessions in a cluster; using it
    /// as the tracking identity let a session id equal a locally-minted id from the same
    /// millisecond, silently binding two live transactions to one tracking key. A statement or
    /// finalize for one transaction then resolved the other: a write could stage into a colliding
    /// autocommit read (which committed it as a one-legged transaction), and a commit could land on
    /// a colliding empty transaction and report success while its own staged writes were never
    /// committed. One local monotonic clock never produces the same (physical, counter) pair
    /// twice, which makes the key collision-free. Falls back to the session id only when no local
    /// mint was provided (single-clock test fixtures, where the ambiguity cannot arise).
    /// </summary>
    private Kommander.Time.HLCTimestamp MintTrackingId(Kommander.Time.HLCTimestamp sessionId)
        => mintLocalT?.Invoke(null) ?? sessionId;

    private void Track(KvTransaction tx)
    {
        // A collision here means two live transactions share a tracking identity — the exact
        // corruption the local-mint rule above exists to prevent. Refuse loudly instead of
        // overwriting: an overwritten entry silently routes one transaction's statements and
        // finalize to the other.
        if (!activeTransactions.TryAdd(tx.ClientId, tx))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Transaction tracking identity {tx.ClientId} is already registered to a live transaction");

        // Lifecycle accounting rides the tracking map, and only the tracking map. TryAdd above
        // succeeded exactly once for this transaction and the pair-remove in Untrack succeeds exactly
        // once, so the counter cannot double-count a retried commit and the up/down gauge cannot
        // drift: its value is the size of activeTransactions by construction. Recording at the ten
        // separate status assignments instead would have to be right at every one of them, and a
        // gauge that is wrong on one path reads as a leak forever.
        Diagnostics.ServerDiagnostics.RecordTransaction(
            Diagnostics.ServerDiagnostics.Tags.Operation.Begin,
            Diagnostics.ServerDiagnostics.Tags.Outcome.Ok);

        Diagnostics.ServerDiagnostics.AddActiveTransaction(ModeTag(tx), 1);
    }

    private void Untrack(KvTransaction tx)
    {
        // Pair-remove: only removes the entry when it still maps to this exact transaction, so a
        // later transaction reusing the same client id is never untracked by a stale finalize.
        if (!activeTransactions.TryRemove(new KeyValuePair<Kommander.Time.HLCTimestamp, KvTransaction>(tx.ClientId, tx)))
            return;

        Diagnostics.ServerDiagnostics.AddActiveTransaction(ModeTag(tx), -1);

        // The terminal status is assigned immediately before every untrack, so it is the outcome.
        // A transaction that leaves the map without one was dropped rather than finalized (only
        // Dispose does that, draining a manager that still holds live transactions), which is a
        // cancellation from the caller's point of view — recorded rather than silently omitted, so
        // begin and terminal counts stay balanced.
        (string operation, string outcome) = tx.Status switch
        {
            KvTransactionStatus.Committed =>
                (Diagnostics.ServerDiagnostics.Tags.Operation.Commit, Diagnostics.ServerDiagnostics.Tags.Outcome.Ok),
            KvTransactionStatus.RolledBack =>
                (Diagnostics.ServerDiagnostics.Tags.Operation.Rollback, Diagnostics.ServerDiagnostics.Tags.Outcome.Ok),
            _ =>
                (Diagnostics.ServerDiagnostics.Tags.Operation.Rollback, Diagnostics.ServerDiagnostics.Tags.Outcome.Canceled),
        };

        Diagnostics.ServerDiagnostics.RecordTransaction(operation, outcome);
    }

    /// <summary>
    /// The mode tag for the active-transaction gauge. Read from <see cref="KvTransaction.IsReadOnly"/>
    /// because it is fixed at construction: the increment and the decrement must carry the same tag or
    /// the gauge splits into two series that each drift, and a transaction's declared mode can be
    /// changed by <c>SET TRANSACTION</c> between the two.
    /// </summary>
    private static string ModeTag(KvTransaction tx)
        => tx.IsReadOnly
            ? Diagnostics.ServerDiagnostics.Tags.TransactionMode.ReadOnly
            : Diagnostics.ServerDiagnostics.Tags.TransactionMode.ReadWrite;

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
    /// <see cref="CamusDBOptions.DefaultIsolationLevel"/> → <see cref="CamusIsolationLevel.ReadCommitted"/>.</para>
    /// </summary>
    public async Task<KvTransaction> BeginAsync(
        CamusIsolationLevel? isolationLevel = null,
        CamusTransactionMode? transactionMode = null,
        int? mutationLimitOverride = null,
        KeyValueTransactionLocking? locking = null,
        ReadValidation? readValidation = null,
        DecisionDurability? decisionDurability = null,
        bool deferStart = false,
        TransactionPriority? priority = null,
        CancellationToken cancellationToken = default)
    {
        CamusIsolationLevel level = isolationLevel ?? options.DefaultIsolationLevel;
        CamusTransactionMode mode = transactionMode ?? CamusTransactionMode.ReadWrite;

        // Serializable + ReadOnly: mint a server HLC timestamp T and return a stateless
        // zero-identity transaction that carries T as its read snapshot.
        if (level == CamusIsolationLevel.Serializable && mode == CamusTransactionMode.ReadOnly)
            return await BeginSerializableReadOnlyAsync(
                priority ?? options.DefaultTransactionPriority, cancellationToken).ConfigureAwait(false);

        // Concurrency strategy: caller-chosen, defaulting to pessimistic (acquire-then-write, block on
        // conflict). Optimistic defers conflict detection to the coordinator's read-set validation at
        // commit; it is opt-in per transaction. See options.DefaultTransactionLocking.
        KeyValueTransactionLocking lockingMode = locking ?? options.DefaultTransactionLocking;
        ReadValidation readValidationMode = readValidation ?? options.DefaultReadValidation;
        DecisionDurability decisionDurabilityMode = decisionDurability ?? options.DefaultDecisionDurability;

        // Admission priority: caller-chosen, defaulting to the engine-wide setting. Consulted only by
        // the Kahuna node's admission gate when a concurrency ceiling is configured; with the default
        // ceiling of zero it is recorded and never gates. See options.DefaultTransactionPriority.
        TransactionPriority priorityMode = priority ?? options.DefaultTransactionPriority;

        string uniqueId = Guid.NewGuid().ToString("N");
        int mutationLimit = mutationLimitOverride ?? options.MaxMutationsPerTransaction;

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
                clientId: clientId, priority: priorityMode)
            {
                WriteAdmissionGate = EnsureDiskSpaceForWrites
            };

            DecisionDurability capturedDecision = decisionDurabilityMode;
            txDeferred.SessionStarter = async ct =>
            {
                // Priority is read from the transaction (not captured above) for the same reason
                // Locking is: a SET TRANSACTION PRIORITY between BeginAsync and the first operation
                // must be the value the admission gate sees, and the gate runs inside this call.
                TransactionHandle h = await StartKahunaTransactionAsync(
                    uniqueId, "start Kahuna transaction",
                    txDeferred.Locking, txDeferred.ReadValidation, capturedDecision,
                    txDeferred.Priority, ct
                ).ConfigureAwait(false);
                txDeferred.SetSessionId(h.TransactionId);
            };

            Track(txDeferred);
            return txDeferred;
        }

        // Eager start (the default): starts the Kahuna session immediately. Used for every
        // internally-begun and autocommit transaction, and whenever deferral was not requested. The
        // tracking identity is minted locally, never taken from the session handle — see
        // MintTrackingId for why a remote-minted session id must not key the tracking maps.
        TransactionHandle handle = await StartKahunaTransactionAsync(
            uniqueId, 
            "start Kahuna transaction", 
            lockingMode, 
            readValidationMode, 
            decisionDurabilityMode,
            priorityMode, 
            cancellationToken
        ).ConfigureAwait(false);

        KvTransaction tx = new(
            handle.TransactionId,
            uniqueId,
            isReadOnly: false,
            level,
            mode,
            mutationLimit: mutationLimit,
            locking: lockingMode,
            readValidation: readValidationMode,
            clientId: MintTrackingId(handle.TransactionId),
            priority: priorityMode
        )
        {
            WriteAdmissionGate = EnsureDiskSpaceForWrites
        };
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
    private async Task<KvTransaction> BeginSerializableReadOnlyAsync(
        TransactionPriority priority, 
        CancellationToken cancellationToken
    )
    {
        string uniqueId = Guid.NewGuid().ToString("N");

        // A serializable read-only snapshot acquires no locks and writes nothing, so the locking mode
        // is immaterial; pessimistic keeps the start options identical to the read-write default.
        TransactionHandle handle = await StartKahunaTransactionAsync(
            uniqueId, 
            "mint read-timestamp for serializable RO transaction",
            KeyValueTransactionLocking.Pessimistic, 
            ReadValidation.None, 
            DecisionDurability.BestEffort,
            priority, 
            cancellationToken
        ).ConfigureAwait(false);
        
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
            readTimestamp:   t,
            clientId:        MintTrackingId(t),
            priority:        priority
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
        string uniqueId, 
        string operation, 
        KeyValueTransactionLocking locking,
        ReadValidation readValidation, 
        DecisionDurability decisionDurability,
        TransactionPriority priority, 
        CancellationToken cancellationToken
    )
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
                    // Consulted by the node's admission gate to decide which queued transaction starts
                    // next. Inert unless a concurrency ceiling is configured on the node.
                    Priority          = priority,
                    // How long this start will queue at the gate before being refused, deliberately
                    // not the session Timeout below. Those measure unrelated things: Timeout is how
                    // long an admitted transaction may live, this is how long an unadmitted one waits
                    // to begin — and a transaction meant to run for an hour is not thereby willing to
                    // wait an hour at the door. Non-positive leaves the node's own default budget.
                    AdmissionWaitMs   = options.TransactionAdmissionWaitMs,
                    // Bound an abandoned session server-side: the Kahuna reaper reclaims a session that
                    // is never finalized after this window plus its grace period, releasing its range
                    // locks. Non-positive leaves the server default. This replaces the client-side
                    // heartbeat as the backstop for a transaction whose client disconnects.
                    Timeout           = options.MaxSerializableTransactionLifetimeMs
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

        // The node's admission gate refused the transaction outright (its concurrency ceiling and
        // queue are both full). Nothing was started, so the refusal is retryable by the caller —
        // but unlike a warmup MustRetry it is a load signal, not a routing race, so it surfaces
        // without burning the local retry loop (the server already applied its admission wait).
        if (type == KeyValueResponseType.AdmissionRefused)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionMustRetry,
                $"Failed to {operation}: the node refused admission (concurrency ceiling and queue full); nothing was started — retry the operation"
            );

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
        TransactionPriority? priority = null,
        CancellationToken cancellationToken = default)
    {
        if (!promote || !options.KeyRangeShardingEnabled)
        {
            // Serializable default: mint a local HLC timestamp T ≥ causalToken (when provided)
            // and return a zero-identity snapshot transaction at T — no Kahuna round-trip.
            // RC default: keep the HLCTimestamp.Zero fast path (latest-committed per key).
            if (options.DefaultIsolationLevel == CamusIsolationLevel.Serializable &&
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
        TransactionPriority priorityMode = priority ?? options.DefaultTransactionPriority;

        TransactionHandle handle = await StartKahunaTransactionAsync(
            uniqueId, 
            "start read-only transaction",
            KeyValueTransactionLocking.Pessimistic, 
            ReadValidation.None, 
            DecisionDurability.BestEffort,
            priorityMode, 
            cancellationToken
        ).ConfigureAwait(false);

        KvTransaction tx = new(
            handle.TransactionId,
            uniqueId,
            isReadOnly: true,
            transactionMode: CamusTransactionMode.ReadOnly,
            clientId: MintTrackingId(handle.TransactionId),
            priority: priorityMode
        );

        Track(tx);
        
        return tx;
    }

    /// <summary>
    /// Commits the transaction via Kahuna 2PC.
    /// Throws <see cref="CamusDBException"/> if the transaction was already completed
    /// or if Kahuna aborts the commit.
    ///
    /// <para>When a result cache is attached, this also drives the cache write protocol: the frozen
    /// server-owned modified keyspaces are marked in flight before the commit request, and every exit
    /// path resolves that mark. A commit and any <b>unknown</b> outcome (unresolved retries, an errored
    /// or lost response, a cancellation or exception after the first commit request) fence the
    /// generations and evict the frozen key set, because the write may already be readable on this
    /// node; only a definite abort leaves cached entries intact.</para>
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
        if (tx is { IsReadOnly: true, TransactionMode: CamusTransactionMode.ReadOnly, IsolationLevel: CamusIsolationLevel.Serializable })
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
            if (tx is { IsolationLevel: CamusIsolationLevel.Serializable, TransactionMode: CamusTransactionMode.ReadWrite } &&
                tx.IsExpired(options.MaxSerializableTransactionLifetimeMs))
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
                    $"({options.MaxSerializableTransactionLifetimeMs} ms); roll back and retry from BeginAsync");
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
        //
        // The result cache splits these on a different axis — "could this write be visible?" rather
        // than "did it commit?". Committed, MustRetry-exhausted and Errored all fence the affected
        // keyspaces and evict the frozen modified-key set, because the coordinator can apply a commit
        // and still fail to report it. Only Aborted (and a failure raised before any commit request)
        // leaves cached entries in place. Over-evicting costs a re-execution; under-evicting serves
        // rows that predate a committed write.
        KeyValueResponseType result = KeyValueResponseType.MustRetry;

        // Cache-fencing discriminator: set the instant the first commit request leaves this node.
        // Before it, any failure is a definite non-commit (nothing was ever asked to commit), so the
        // in-flight marks can simply be cleared. After it, the outcome may be unknown — the write can
        // have landed while its response was lost — and every non-committing exit must conservatively
        // fence and evict instead. See CachePublishGate's write protocol.
        bool commitRequestIssued = false;

        // Time the Kahuna 2PC commit itself — this is the span dominated by WAL fsync latency,
        // so surfacing it in the finalize log makes slow-commit diagnosis (e.g. native fsync cost)
        // directly observable per transaction.
        ValueStopwatch commitTimer = ValueStopwatch.StartNew();
        using System.Diagnostics.Activity? commitSpan =
            Diagnostics.ServerDiagnostics.StartSpan(Diagnostics.ServerDiagnostics.Spans.Commit);
        int commitAttempts = 0;
        try
        {
            try
            {
                for (int attempt = 0; ; attempt++)
                {
                    // The coordinator owns the working set: every confirmed write and exclusive point lock
                    // was folded server-side as the operation completed, so commit supplies only the routing
                    // handle.
                    commitRequestIssued = true;
                    commitAttempts++;

                    (result, string? anchor) = await kahuna.LocateAndCommitTransaction(
                            tx.Handle,
                            cancellationToken
                        ).ConfigureAwait(false);

                    // Fold the coordinator's canonical record anchor onto the handle the moment it is known —
                    // including alongside a non-terminal MustRetry — so a finalize retried after the live
                    // coordinator session is lost still routes to the durable decision rather than returning
                    // an unknown Errored. No-op (stays null) on the best-effort path.
                    tx.CaptureRecordAnchor(anchor);

                    if (result != KeyValueResponseType.MustRetry)
                        break;

                    if (!await DelayBeforeFinalizeRetryAsync(attempt, commitTimer, cancellationToken).ConfigureAwait(false))
                        break;
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (CamusDBException)
            {
                throw;
            }
            catch (Exception ex)
            {
                // A transport or unexpected fault thrown AFTER the commit request left this node (a
                // Kahuna internode RpcException when the coordinator's node dies mid-commit, a routing
                // failure, anything unmapped) says nothing about the transaction's outcome: the commit
                // may already be durable. Letting the raw exception escape surfaced it to clients as a
                // definite-looking generic error (CADB0000), and a commit that HAD landed was then
                // journalled as failed — observed as a leaked write in the fault-injection soaks. The
                // truthful contract is the same as the MustRetry-exhausted branch below: the outcome is
                // unresolved, the transaction stays Finalizing and tracked, and the caller must retry
                // the SAME commit on the SAME handle (never replay the operation). The finally block
                // fences and evicts the cache exactly as for any unknown outcome.
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionFinalizeUnresolved,
                    $"Transaction {tx.UniqueId} commit outcome is unknown after {commitAttempts} attempt(s) " +
                    $"over {commitTimer.GetElapsedMilliseconds()} ms ({ex.GetType().Name}: {ex.Message}); " +
                    "retry COMMIT on the same transaction (do not re-run the operation)");
            }

            if (result == KeyValueResponseType.Committed)
            {
                tx.Status = KvTransactionStatus.Committed;
                
                long committedElapsedMs = commitTimer.GetElapsedMilliseconds();
                
                Diagnostics.ServerDiagnostics.RecordCommitDuration(
                    Diagnostics.ServerDiagnostics.Tags.Outcome.Ok, 
                    committedElapsedMs
                );
                
                Diagnostics.ServerDiagnostics.RecordStagedMutations(tx.MutationCount);
                if (logger.IsEnabled(LogLevel.Debug))
                    Log.LogTransactionFinalized(logger, "committed", tx.UniqueId, committedElapsedMs);

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
                // must retry the SAME commit on the SAME handle.
                //
                // For the cache the unknown outcome must be treated as if the write had landed: the
                // coordinator can apply a commit and still lose the response, so the data may already
                // be readable on this node. Fence the generations and evict the frozen modified-key
                // set before clearing the in-flight marks, so no query started after this point can
                // still be served pre-write rows. If the retry later resolves, it fences the same
                // frozen set again, which is a no-op.
                if (markedInFlight)
                {
                    _cache!.PublishGate.FenceAndInvalidate(keyspaces!, _ => _cache.InvalidateByModifiedKeys(modKeys!));
                    markedInFlight = false;
                }
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionFinalizeUnresolved,
                    $"Transaction {tx.UniqueId} commit outcome is not yet resolved after {commitAttempts} attempts " +
                    $"over {commitTimer.GetElapsedMilliseconds()} ms; " +
                    "retry COMMIT on the same transaction (do not re-run the operation)"
                );
            }

            // Terminal, non-committing outcome (Aborted / Errored / unexpected): the transaction is
            // now dead. Mark it rolled back, drop the cache mark, and untrack. The coordinator's
            // finalize already released the working set (point + range locks, read snapshots).
            tx.Status = KvTransactionStatus.RolledBack;

            // Aborted is a definite non-commit, so cached entries stay valid and only the in-flight
            // marks are cleared. Errored (and any unexpected response) means the coordinator session
            // is gone and the outcome is unavailable — the write may have landed, so it is fenced and
            // evicted exactly like a commit.
            if (markedInFlight)
            {
                if (result == KeyValueResponseType.Aborted)
                    _cache!.PublishGate.AbortWrite(keyspaces!);
                else
                    _cache!.PublishGate.FenceAndInvalidate(keyspaces!, _ => _cache.InvalidateByModifiedKeys(modKeys!));
                
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
            // Safety net: if an unexpected exception or a cancellation escaped the commit body before
            // the gate was resolved, release the in-flight mark so concurrent probes are not blocked
            // forever. Which release is correct depends on whether a commit request was ever issued:
            // before that point nothing could have landed, after it the outcome is unknown and the
            // entries must be fenced and evicted.
            if (markedInFlight)
            {
                if (commitRequestIssued)
                    _cache!.PublishGate.FenceAndInvalidate(keyspaces!, _ => _cache.InvalidateByModifiedKeys(modKeys!));
                else
                    _cache!.PublishGate.AbortWrite(keyspaces!);
            }
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
        ValueStopwatch closeTimer = ValueStopwatch.StartNew();
        for (int attempt = 0; ; attempt++)
        {
            (result, snapshot) = await kahuna.LocateAndCloseTransaction(
                tx.CoordinatorKey, tx.TransactionId, cancellationToken).ConfigureAwait(false);

            if (result != KeyValueResponseType.MustRetry)
                break;

            if (!await DelayBeforeFinalizeRetryAsync(attempt, closeTimer, cancellationToken).ConfigureAwait(false))
                break;
        }

        if (result == KeyValueResponseType.Set && snapshot is not null)
        {
            // Fold the frozen record anchor onto the handle now, before the first commit attempt.
            tx.CaptureRecordAnchor(snapshot.RecordAnchorKey);

            List<(string key, KeyValueDurability durability)> serverKeys = new(snapshot.ModifiedKeys.Count);
            
            foreach (KeyValueTransactionModifiedKey k in snapshot.ModifiedKeys)
                serverKeys.Add((k.Key ?? string.Empty, k.Durability));
            
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
        List<(string key, KeyValueDurability _)> modKeys
    )
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

        // Whether this rollback is the transaction's first finalize, read before the fence below
        // overwrites the status. It gates the release-by-mirror fallback: a transaction that never
        // began a commit cannot have a prepared intent anywhere, because only a commit prepares one.
        // Releasing a prepared intent would discard the only route to a value that may already be
        // committed, so the fallback must never run for a transaction that could own one.
        //
        // Deliberately conservative in one case: a rollback resumed after an unresolved earlier
        // rollback arrives here already Finalizing, and gets no fallback even though nothing ever
        // prepared for it. The status alone cannot tell that resume apart from a resumed commit, and
        // withholding a release costs a wedge that the storage node still expires, while releasing a
        // prepared intent would cost a committed write.
        bool firstFinalize = tx.Status == KvTransactionStatus.Active;

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
        (KeyValueResponseType result, int attempts, long retryElapsedMs) =
            await RollbackHandleWithRetryAsync(tx, cancellationToken).ConfigureAwait(false);

        if (result == KeyValueResponseType.MustRetry)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionFinalizeUnresolved,
                $"Transaction {tx.UniqueId} rollback outcome is not yet resolved after {attempts} attempts " +
                $"over {retryElapsedMs} ms; retry ROLLBACK on the same transaction"
            );

        // Errored is the coordinator-unknown outcome: no session, no retained outcome, no durable
        // record to consult. It released nothing, because the working set it finalizes from died with
        // the session — so whatever this transaction planted is still at the participants unless the
        // client replays it from its own key mirror.
        if (result == KeyValueResponseType.Errored && firstFinalize)
            await ReleaseMirroredHoldingsAsync(tx, cancellationToken).ConfigureAwait(false);

        // RolledBack, Aborted (a definite non-committing outcome — for a rollback request that is the
        // intended result), or Errored (the handle is unknown/expired, so the session is already gone
        // and there is nothing left to undo): all terminal and non-committing. Terminal in every case,
        // including after the release pass above — a coordinator that does not know the transaction
        // will never know it, so re-issuing the same rollback can only repeat the same answer.
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
    private async Task<(KeyValueResponseType Result, int Attempts, long ElapsedMs)> RollbackHandleWithRetryAsync(
        KvTransaction tx, 
        CancellationToken cancellationToken
    )
    {
        KeyValueResponseType result = KeyValueResponseType.MustRetry;
        ValueStopwatch rollbackRetryTimer = ValueStopwatch.StartNew();
        int attempts = 0;

        for (int attempt = 0; ; attempt++)
        {
            attempts++;
            result = await kahuna.LocateAndRollbackTransaction(tx.Handle, cancellationToken).ConfigureAwait(false);

            if (result != KeyValueResponseType.MustRetry)
                break;

            if (!await DelayBeforeFinalizeRetryAsync(attempt, rollbackRetryTimer, cancellationToken).ConfigureAwait(false))
                break;
        }

        return (result, attempts, rollbackRetryTimer.GetElapsedMilliseconds());
    }

    /// <summary>
    /// Keys released per request in the release-by-mirror pass. A transaction that wrote a large batch
    /// mirrors hundreds of thousands of keys; one request per key would make a reaper sweep crawl,
    /// while one request for all of them would build a message no transport wants to carry. The
    /// storage layer groups a chunk by partition leader and releases the groups in parallel, so a
    /// chunk of this size is one round trip per leader rather than one per key.
    /// </summary>
    private const int MirroredReleaseChunkSize = 256;

    /// <summary>
    /// Attempts per chunk before its still-unreleased keys are abandoned to the storage node's own
    /// expiry. The retry covers the transient answer — a partition whose leader is mid-election —
    /// which clears in the time a couple of back-offs take. Anything that survives that is not
    /// transient, and this is a best-effort pass on an already-terminal transaction: spinning here
    /// would stall the reaper sweep that all the other abandoned transactions are queued behind.
    /// </summary>
    private const int MirroredReleaseMaxAttempts = 3;

    /// <summary>
    /// Releases the holdings of a transaction whose coordinator session is unknown, using the
    /// client-side modified-key mirror as the key list.
    ///
    /// <para><b>Why this exists.</b> A rollback that resolves to the coordinator-unknown outcome
    /// releases nothing: the coordinator finalizes from a working set that died with the session. The
    /// staged writes and point locks the transaction planted stay at the participants, where an
    /// undecided foreign intent blocks every snapshot scan of its key space for as long as it lives.
    /// The client still holds the one remaining record of which keys those are — the modified-key set
    /// it keeps for cache invalidation — so it replays that set as per-key releases.</para>
    ///
    /// <para><b>Why it is safe.</b> Each release is keyed by <c>(transaction id, key)</c> and only
    /// removes state that transaction still owns: a key another transaction now holds is refused, and
    /// a key whose transaction committed or aborted has nothing left to remove, so the pass is
    /// idempotent and cannot take anything from a live transaction. The age guard covers the one case
    /// the key check cannot: an unknown answer during a leadership change, where the session may still
    /// be alive on another node. Past
    /// <see cref="CamusDBOptions.AbandonedTransactionReleaseAfterMs"/> no session can be alive
    /// anywhere. The caller adds the third condition — the transaction never began a commit — so no
    /// prepared intent, whose fate belongs to the decision machinery, can be in the mirror.</para>
    ///
    /// <para>Best-effort and non-throwing by contract: it runs after the rollback's outcome is already
    /// decided, so nothing it does may change what the caller is told. Range locks are outside it —
    /// the coordinator owns them and the client cannot enumerate them.</para>
    /// </summary>
    private async Task ReleaseMirroredHoldingsAsync(KvTransaction tx, CancellationToken cancellationToken)
    {
        long? releaseAgeMs = KahunaSessionLifetime.AbandonedReleaseAgeMs(options);
        long? ageMs = tx.AgeMs;

        if (releaseAgeMs is null || ageMs is null)
        {
            // The release is disabled, or the transaction has no session clock to age (it never
            // started one, so it planted nothing). Either way there is nothing to do.
            Diagnostics.ServerDiagnostics.RecordCoordinatorUnknownFinalize(Diagnostics.ServerDiagnostics.Tags.CoordinatorUnknown.Disabled, 0);
            Log.LogCoordinatorUnknownTransaction(logger, tx.UniqueId, ageMs ?? -1);
            return;
        }

        List<(string key, KeyValueDurability durability)> mirrored = tx.GetModifiedKeyPairs();

        if (mirrored.Count == 0)
        {
            Diagnostics.ServerDiagnostics.RecordCoordinatorUnknownFinalize(Diagnostics.ServerDiagnostics.Tags.CoordinatorUnknown.NoKeys, 0);
            Log.LogCoordinatorUnknownTransaction(logger, tx.UniqueId, ageMs.Value);
            return;
        }

        // Too young to release: a session for this transaction may still be alive on a node the
        // finalize did not reach. The transaction is finished either way, but its key mirror is the
        // only remaining record of what it planted — dropping it here would strand those keys until
        // the storage node expires them. Hold the mirror instead and release it once the age is
        // reached; the reaper sweep drains what is due.
        if (ageMs <= releaseAgeMs)
        {
            EnqueueMirroredRelease(tx, mirrored, releaseAgeMs.Value - ageMs.Value);
            return;
        }

        await ReleaseMirroredKeysAsync(tx.TransactionId, tx.UniqueId, ageMs.Value, mirrored, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Runs the release pass over <paramref name="mirrored"/> and reports the outcome. Shared by the
    /// rollback that is already old enough and by the deferred pass the sweep drains later, so both
    /// count and log the same way.
    /// </summary>
    private async Task<int> ReleaseMirroredKeysAsync(
        Kommander.Time.HLCTimestamp transactionId,
        string uniqueId,
        long ageMs,
        List<(string key, KeyValueDurability durability)> mirrored,
        CancellationToken cancellationToken
    )
    {
        int released = 0;
        int unreleased = 0;

        for (int offset = 0; offset < mirrored.Count; offset += MirroredReleaseChunkSize)
        {
            int count = Math.Min(MirroredReleaseChunkSize, mirrored.Count - offset);
            List<(string key, KeyValueDurability durability)> chunk = mirrored.GetRange(offset, count);

            (int chunkReleased, int chunkUnreleased) =
                await ReleaseMirroredChunkAsync(transactionId, uniqueId, chunk, cancellationToken).ConfigureAwait(false);

            released += chunkReleased;
            unreleased += chunkUnreleased;

            if (cancellationToken.IsCancellationRequested)
            {
                // Shutdown mid-pass. The remaining keys keep the same fate they had before this pass
                // existed, so stop rather than fight the token — and report what was actually done.
                unreleased += mirrored.Count - (offset + count);
                break;
            }
        }

        Diagnostics.ServerDiagnostics.RecordCoordinatorUnknownFinalize(Diagnostics.ServerDiagnostics.Tags.CoordinatorUnknown.Released, released);
        Log.LogCoordinatorUnknownTransactionReleased(
            logger, uniqueId, ageMs, released, mirrored.Count, unreleased);

        return released;
    }

    /// <summary>
    /// Releases one chunk of mirrored keys, retrying only the keys the storage layer answered with its
    /// transient signal. Returns how many keys this chunk actually released and how many were left
    /// behind — a key another transaction now holds counts as neither, since nothing of this
    /// transaction's remained on it. Never throws: a transport failure ends the chunk and its keys are
    /// reported as left behind.
    /// </summary>
    private async Task<(int Released, int Unreleased)> ReleaseMirroredChunkAsync(
        Kommander.Time.HLCTimestamp transactionId,
        string uniqueId,
        List<(string key, KeyValueDurability durability)> chunk,
        CancellationToken cancellationToken
    )
    {
        int released = 0;
        int failed = 0;
        List<(string key, KeyValueDurability durability)> pending = chunk;

        for (int attempt = 0; attempt < MirroredReleaseMaxAttempts && pending.Count > 0; attempt++)
        {
            if (attempt > 0 && !await DelayBeforeMirroredReleaseRetryAsync(attempt, cancellationToken).ConfigureAwait(false))
                break;

            List<(KeyValueResponseType Type, string Key, KeyValueDurability Durability)> results;

            try
            {
                results = await kahuna
                    .LocateAndTryReleaseManyExclusiveLocks(transactionId, pending, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.LogMirroredReleaseFailed(logger, ex, uniqueId, pending.Count);
                return (released, failed + pending.Count);
            }

            // An answer per key is the contract; an empty list means the request reached no partition
            // at all, and re-issuing the same routing would answer the same way.
            if (results.Count == 0)
                return (released, failed + pending.Count);

            List<(string key, KeyValueDurability durability)> retryable = [];

            foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) in results)
            {
                switch (type)
                {
                    case KeyValueResponseType.Unlocked:
                        released++;
                        break;

                    // Nothing of this transaction's is on the key: it never landed, it was already
                    // settled, or another transaction owns the key now. All three are the pass doing
                    // its job — it removes only what this transaction still holds.
                    case KeyValueResponseType.DoesNotExist:
                    case KeyValueResponseType.AlreadyLocked:
                        break;

                    case KeyValueResponseType.MustRetry:
                        retryable.Add((key, durability));
                        break;

                    default:
                        // Not transient and not a release: left to the storage node's own expiry, and
                        // counted so the log reports what the pass actually left behind.
                        failed++;
                        break;
                }
            }

            pending = retryable;
        }

        return (released, failed + pending.Count);
    }

    /// <summary>
    /// The key mirror of a finished transaction whose coordinator session was unknown while the
    /// transaction was still too young to release, kept until the age is reached.
    /// </summary>
    /// <param name="TransactionId">The Kahuna identity every release is keyed by.</param>
    /// <param name="UniqueId">The transaction's own id, for the log line that reports the release.</param>
    /// <param name="Keys">The keys the transaction wrote — the only remaining record of what it planted.</param>
    /// <param name="DueAtTicks">Monotonic <see cref="Stopwatch.GetTimestamp"/> mark at which the release
    /// becomes safe. Monotonic rather than wall-clock so a clock step cannot bring it forward.</param>
    /// <param name="AgeAtDeferralMs">How old the transaction was when the rollback met the unknown
    /// outcome, so the eventual log line reports the transaction's real age rather than the wait.</param>
    private readonly record struct PendingMirroredRelease(
        Kommander.Time.HLCTimestamp TransactionId,
        string UniqueId,
        List<(string key, KeyValueDurability durability)> Keys,
        long DueAtTicks,
        long AgeAtDeferralMs);

    /// <summary>
    /// Deferred key mirrors, in the order they were parked. Guarded by <see cref="pendingReleaseSync"/>
    /// — the sweep that drains it and the rollbacks that add to it run on different threads.
    /// </summary>
    private readonly List<PendingMirroredRelease> pendingMirroredReleases = [];

    private readonly Lock pendingReleaseSync = new();

    /// <summary>Keys currently held across all deferred mirrors, kept under the cap below.</summary>
    private int pendingMirroredKeyCount;

    /// <summary>
    /// Ceiling on the keys held in deferred mirrors. A mirror is retained for as long as the session
    /// ceiling — up to an hour at the shipped defaults — so an unbounded queue would let a burst of
    /// abandoned bulk writes pin their key strings in memory for that whole window. Past the cap the
    /// newest mirror is dropped and logged: the storage node's own expiry is still the backstop, and
    /// refusing the newcomer keeps the mirrors already waiting, which are closest to being due.
    /// </summary>
    private const int MaxPendingMirroredKeys = 100_000;

    /// <summary>
    /// Holds a finished transaction's key mirror until <paramref name="waitMs"/> has passed, at which
    /// point no session can still own its holdings and the sweep may release them. Nothing about the
    /// transaction itself is retained — it is terminal and untracked; only the identity and the keys
    /// are, because they are what the release needs and what nothing else in the system still knows.
    /// </summary>
    private void EnqueueMirroredRelease(KvTransaction tx, List<(string key, KeyValueDurability durability)> mirrored, long waitMs)
    {
        long ageMs = tx.AgeMs ?? 0;
        bool accepted;

        lock (pendingReleaseSync)
        {
            accepted = pendingMirroredKeyCount + mirrored.Count <= MaxPendingMirroredKeys;

            if (accepted)
            {
                pendingMirroredReleases.Add(new PendingMirroredRelease(
                    tx.TransactionId,
                    tx.UniqueId,
                    mirrored,
                    Stopwatch.GetTimestamp() + (long)(waitMs * (Stopwatch.Frequency / 1000.0)),
                    ageMs));

                pendingMirroredKeyCount += mirrored.Count;
            }
        }

        Diagnostics.ServerDiagnostics.RecordCoordinatorUnknownFinalize(
            accepted
                ? Diagnostics.ServerDiagnostics.Tags.CoordinatorUnknown.Deferred
                : Diagnostics.ServerDiagnostics.Tags.CoordinatorUnknown.Dropped, 0);

        if (accepted)
            Log.LogCoordinatorUnknownTransactionDeferred(logger, tx.UniqueId, ageMs, mirrored.Count, waitMs);
        else
            Log.LogCoordinatorUnknownTransactionDropped(logger, tx.UniqueId, mirrored.Count);
    }

    /// <summary>
    /// Releases the deferred key mirrors that have reached their age, and reports how many keys were
    /// released. Called once per reaper sweep: a mirror is deferred precisely because it was not yet
    /// safe to release, and this is what makes that "not yet" mean "later" instead of "never".
    ///
    /// <para>Never throws — it is background cleanup, and the sweep that calls it must survive
    /// whatever one database's storage node is doing.</para>
    /// </summary>
    public async Task<int> ReleaseDueMirroredHoldingsAsync(CancellationToken cancellationToken = default)
    {
        long now = Stopwatch.GetTimestamp();
        List<PendingMirroredRelease> due = [];

        lock (pendingReleaseSync)
        {
            // Walk backwards so an entry can be taken out by index as it is claimed; the list is small
            // (it is capped by key count) and this keeps claim and removal in one pass under the lock.
            for (int i = pendingMirroredReleases.Count - 1; i >= 0; i--)
            {
                PendingMirroredRelease entry = pendingMirroredReleases[i];

                if (entry.DueAtTicks > now)
                    continue;

                due.Add(entry);
                pendingMirroredReleases.RemoveAt(i);
                pendingMirroredKeyCount -= entry.Keys.Count;
            }
        }

        if (due.Count == 0)
            return 0;

        int released = 0;

        foreach (PendingMirroredRelease entry in due)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            released += await ReleaseMirroredKeysAsync(
                entry.TransactionId, entry.UniqueId, entry.AgeAtDeferralMs, entry.Keys, cancellationToken)
                .ConfigureAwait(false);
        }

        return released;
    }

    /// <summary>
    /// Waits out the back-off before the next attempt at a chunk of mirrored releases. Returns
    /// <see langword="false"/> without waiting when the wait was cancelled, so the pass stops instead
    /// of surfacing a cancellation from an already-decided rollback.
    /// </summary>
    private static async Task<bool> DelayBeforeMirroredReleaseRetryAsync(int attempt, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(FinalizeRetryDelayMs(attempt - 1), cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
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

    public void Dispose()
    {
        // Drain through Untrack rather than clearing: a disposed manager has no active transactions,
        // and a gauge left standing at whatever was live when it died reads as a leak for the rest of
        // the process's life.
        foreach (KvTransaction tx in activeTransactions.Values)
            Untrack(tx);

        activeTransactions.Clear();

        // Parked key mirrors die with the manager. Their keys keep the storage node's own expiry as
        // their backstop, exactly as they would have if the process had exited instead.
        lock (pendingReleaseSync)
        {
            pendingMirroredReleases.Clear();
            pendingMirroredKeyCount = 0;
        }
    }
}
