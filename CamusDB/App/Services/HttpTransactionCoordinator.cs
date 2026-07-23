
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using CamusDB.App.Models;
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace CamusDB.App.Services;

/// <summary>
/// Tracks in-flight <see cref="KvTransaction"/> instances for the HTTP API and delegates
/// lifecycle operations to each database's <see cref="KvTransactionsManager"/>.
/// </summary>
public sealed class HttpTransactionCoordinator
{
    private readonly CommandExecutor executor;

    private readonly ConcurrentDictionary<(long L, uint C), ActiveTransaction> active = new();

    /// <summary>
    /// A tracked in-flight transaction plus a monotonic marker of when the client last touched it.
    /// The marker is a <see cref="Stopwatch.GetTimestamp"/> tick (not wall-clock) so idle detection
    /// is immune to NTP/clock jumps, consistent with the rest of the transaction machinery. It is
    /// refreshed on begin and on every <see cref="GetState"/> (i.e. every statement issued against
    /// the transaction), and read by the abandoned-transaction reaper.
    /// </summary>
    private sealed class ActiveTransaction(KvTransactionsManager manager, KvTransaction transaction)
    {
        public KvTransactionsManager Manager { get; } = manager;
        public KvTransaction Transaction { get; } = transaction;

        private long lastActivityTicks = Stopwatch.GetTimestamp();

        /// <summary>0 = idle, 1 = a commit/rollback is currently finalizing this transaction.</summary>
        private int finalizing;

        /// <summary>Marks the transaction as just-used, resetting its idle timer.</summary>
        public void Touch() => Volatile.Write(ref lastActivityTicks, Stopwatch.GetTimestamp());

        /// <summary>Elapsed monotonic time since the transaction was last touched.</summary>
        public TimeSpan IdleTime => Stopwatch.GetElapsedTime(Volatile.Read(ref lastActivityTicks));

        /// <summary>
        /// Claims exclusive finalize (commit/rollback) ownership of this transaction; exactly one
        /// caller wins with <see langword="true"/> and a concurrent finalize for the same id gets
        /// <see langword="false"/>. This closes the unsynchronized check-then-act window in
        /// <see cref="KvTransactionsManager.CommitAsync"/> (its <c>Status != Active</c> guard is a
        /// plain read), so two same-node requests — e.g. a client that timed out on a slow commit
        /// and re-issued it — can never both drive Kahuna 2PC on the same transaction concurrently.
        /// </summary>
        public bool TryBeginFinalize() => Interlocked.CompareExchange(ref finalizing, 1, 0) == 0;

        /// <summary>
        /// True while a commit or rollback owns the finalize claim for this transaction. Read by
        /// <see cref="GetState"/> so a data statement issued against a transaction that is already being
        /// finalized is rejected at the HTTP boundary rather than racing the commit on the same session.
        /// </summary>
        public bool IsFinalizing => Volatile.Read(ref finalizing) == 1;

        /// <summary>
        /// Releases a finalize claim after a <em>failed</em> attempt so a later sequential retry may
        /// proceed. Not called on success — a completed transaction is removed from the tracking
        /// map instead, so its claim can never be reused.
        /// </summary>
        public void EndFinalize() => Volatile.Write(ref finalizing, 0);
    }

    public HttpTransactionCoordinator(CommandExecutor executor)
    {
        this.executor = executor;
    }

    public async Task<KvTransaction> StartAsync(string databaseName, CancellationToken cancellationToken = default) =>
        await StartAsync(databaseName, isolationLevel: null, transactionMode: null, locking: null, cancellationToken: cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Starts a new transaction with the requested isolation level, mode, and locking strategy.
    /// When any argument is <see langword="null"/> the server default applies:
    /// <see cref="CamusDBConfig.DefaultIsolationLevel"/>, <see cref="CamusTransactionMode.ReadWrite"/>,
    /// and <see cref="CamusDBConfig.DefaultTransactionLocking"/> respectively.
    ///
    /// <para><paramref name="deferStart"/> requests deferred Kahuna-session start: the transaction is
    /// begun without opening a coordinator session so a following <c>SET TRANSACTION LOCKING</c> can
    /// still choose the locking mode (which Kahuna pins at session start). Pass it only for explicit
    /// interactive transactions (<c>/start-transaction</c>); autocommit statements start eagerly.</para>
    /// </summary>
    public async Task<KvTransaction> StartAsync(
        string databaseName,
        CamusIsolationLevel? isolationLevel,
        CamusTransactionMode? transactionMode,
        KeyValueTransactionLocking? locking = null,
        bool deferStart = false,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(databaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DatabaseName is required to start a transaction");

        DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);

        KvTransaction tx = await database.Transactions.BeginAsync(
            isolationLevel, transactionMode, locking: locking, deferStart: deferStart,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        Register(database.Transactions, tx);
        return tx;
    }

    /// <summary>
    /// Begins a read-only transaction for a standalone SELECT. By default (and always in
    /// single-partition / hash mode) this is a <c>HLCTimestamp.Zero</c> snapshot: Kahuna reads the
    /// latest committed value per key with no <c>StartTransaction</c> / <c>CommitTransaction</c>
    /// round-trips, so commit and rollback are no-ops and the transaction needs no tracking.
    ///
    /// When <paramref name="promote"/> is set and key-range sharding is enabled, the transaction is
    /// promoted to a real server-minted transaction so a scan can hold a shared range lock for a
    /// serializable, phantom-free read. A promoted transaction has a real identity, so it is
    /// registered here for cleanup and <b>must</b> be committed or rolled back by the caller.
    /// </summary>
    public async Task<KvTransaction> BeginReadOnlyAsync(
        string databaseName,
        bool promote,
        HLCTimestamp? causalToken = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(databaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DatabaseName is required");

        DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);
        KvTransaction tx = await database.Transactions.BeginReadOnlyAsync(promote, causalToken, cancellationToken).ConfigureAwait(false);

        // Zero-snapshot fast-path transactions carry no identity and need no tracking or cleanup;
        // a promoted (real-id) transaction is registered so commit/rollback can find and release it.
        if (tx.TransactionId != Kommander.Time.HLCTimestamp.Zero)
            Register(database.Transactions, tx);

        return tx;
    }

    public KvTransaction GetState(long txnIdPT, uint txnIdCounter)
    {
        if (!active.TryGetValue((txnIdPT, txnIdCounter), out ActiveTransaction? entry))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Unknown transaction");

        // Reject a statement issued against a transaction that a commit/rollback is already finalizing:
        // the coordinator has (or is about to) install its fence, so running a data statement now would
        // race the finalize on the same session. This closes the DML-vs-commit window at the HTTP
        // boundary, ahead of the server-side fence and the local Finalizing guard in the transaction.
        if (entry.IsFinalizing)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                "A commit or rollback is in progress for this transaction; no further statements may run");

        // Every statement issued against an explicit transaction resolves it through here; refresh
        // the idle timer so the reaper only reclaims transactions the client has genuinely stopped
        // using, not ones that are actively doing work between requests.
        entry.Touch();
        return entry.Transaction;
    }

    /// <summary>
    /// Rolls back every tracked transaction that has been idle (no statement issued against it)
    /// for at least <paramref name="idleTimeout"/>, releasing its locks and dropping it from the
    /// in-flight map. Called periodically by the background reaper to reclaim abandoned
    /// transactions — a client that opened a transaction and never committed/rolled back. Returns
    /// the number of transactions reaped.
    ///
    /// <para>Best-effort per entry: a rollback that throws (e.g. the transaction just committed on
    /// another thread, or a transient Kahuna error) is swallowed so one stuck entry cannot stall
    /// the sweep. A concurrent commit/rollback simply wins the <c>TryRemove</c> race; the loser
    /// no-ops via <see cref="KvTransactionsManager.RollbackIfNotCompletedAsync"/>.</para>
    /// </summary>
    public async Task<int> ReapIdleAsync(TimeSpan idleTimeout, CancellationToken cancellationToken = default)
    {
        int reaped = 0;

        foreach (KeyValuePair<(long L, uint C), ActiveTransaction> pair in active)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ActiveTransaction entry = pair.Value;
            if (entry.IdleTime < idleTimeout)
                continue;

            // Remove first so a client racing in with a commit/rollback for the same id either
            // already removed it (we skip) or finds it gone (its lookup fails cleanly).
            if (!active.TryRemove(pair.Key, out _))
                continue;

            // Then claim the finalize gate: a commit/rollback that resolved this entry before the
            // removal may already own it and be mid-flight — in that case skip, so the reaper and
            // the client never both finalize the same transaction.
            if (!entry.TryBeginFinalize())
                continue;

            try
            {
                await entry.Manager.RollbackIfNotCompletedAsync(entry.Transaction, cancellationToken).ConfigureAwait(false);
                reaped++;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch
            {
                // Best-effort: the transaction's lock TTL / lifetime cap is the ultimate backstop.
            }
        }

        return reaped;
    }

    /// <summary>
    /// Commits the transaction. When the transaction is tracked, a per-transaction finalize claim
    /// ensures at most one commit/rollback drives it at a time: a concurrent duplicate (e.g. a
    /// client that timed out on a slow commit and re-issued it) is rejected with
    /// <see cref="CamusDBErrorCodes.TransactionAlreadyCompleted"/> rather than racing a second
    /// Kahuna 2PC. A commit that fails releases the claim so a later <em>sequential</em> retry can
    /// still proceed. Zero-snapshot / untracked read-only transactions have no shared finalize state
    /// and fall through to the manager, whose own <c>Status</c> guard covers repeat calls.
    /// </summary>
    public async Task<HLCTimestamp> CommitAsync(DatabaseDescriptor database, KvTransaction tx, CancellationToken cancellationToken = default)
    {
        if (!active.TryGetValue(Key(tx), out ActiveTransaction? entry))
        {
            // Not tracked (zero-snapshot read-only fast path, or already finalized+removed by a
            // concurrent winner). Delegate to the manager, which no-ops the zero case and rejects a
            // repeat commit via its own Status check.
            HLCTimestamp untrackedToken = await database.Transactions.CommitAsync(tx, cancellationToken).ConfigureAwait(false);
            Unregister(tx);
            return untrackedToken;
        }

        if (!entry.TryBeginFinalize())
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                "A commit or rollback is already in progress for this transaction");

        try
        {
            HLCTimestamp token = await database.Transactions.CommitAsync(tx, cancellationToken).ConfigureAwait(false);
            Unregister(tx);
            return token;
        }
        catch
        {
            entry.EndFinalize();
            throw;
        }
    }

    /// <summary>
    /// Commits a tracked transaction using the manager recorded at <see cref="StartAsync"/> time,
    /// without requiring the caller to supply a <see cref="DatabaseDescriptor"/>. Intended for use
    /// by the gRPC service layer, where the transaction handle carries no database reference.
    ///
    /// <para>Shares the per-transaction finalize claim with <see cref="CommitAsync(DatabaseDescriptor,KvTransaction,CancellationToken)"/>
    /// so a concurrent duplicate commit is rejected rather than double-driven. Throws
    /// <see cref="CamusDBErrorCodes.InvalidInput"/> when the transaction is not tracked (i.e.
    /// the handle is unknown or already finalized).</para>
    /// </summary>
    public async Task<HLCTimestamp> CommitTrackedAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        if (!active.TryGetValue(Key(tx), out ActiveTransaction? entry))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Unknown transaction");

        if (!entry.TryBeginFinalize())
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                "A commit or rollback is already in progress for this transaction");

        try
        {
            HLCTimestamp token = await entry.Manager.CommitAsync(tx, cancellationToken).ConfigureAwait(false);
            Unregister(tx);
            return token;
        }
        catch
        {
            entry.EndFinalize();
            throw;
        }
    }

    /// <summary>
    /// Rolls back the transaction. Shares the per-transaction finalize claim with
    /// <see cref="CommitAsync"/> so an explicit rollback and a commit for the same id cannot run
    /// concurrently; the loser is rejected with <see cref="CamusDBErrorCodes.TransactionAlreadyCompleted"/>.
    /// </summary>
    public async Task RollbackAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        if (!active.TryGetValue(Key(tx), out ActiveTransaction? entry))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Unknown transaction");

        if (!entry.TryBeginFinalize())
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                "A commit or rollback is already in progress for this transaction");

        try
        {
            await entry.Manager.RollbackAsync(tx, cancellationToken).ConfigureAwait(false);
            Unregister(tx);
        }
        catch
        {
            entry.EndFinalize();
            throw;
        }
    }

    /// <summary>
    /// Rolls back the transaction identified by its client id, treating an id that is no longer
    /// tracked as a successful no-op and returning <see langword="false"/> for it.
    ///
    /// <para>This is the idempotent entry point for a client-issued ROLLBACK. A statement that
    /// fails inside an explicit transaction is already rolled back and untracked by the request
    /// handler that caught it, and the abandoned-transaction reaper does the same for an idle
    /// transaction — so a client that then sends its own rollback (the correct thing for it to do
    /// after an error) would otherwise get "Unknown transaction" for a transaction that is, in
    /// fact, rolled back. Rollback is defined by its end state, and that end state has been
    /// reached, so this reports success instead of an error.</para>
    ///
    /// <para>A finalize that is still <em>in flight</em> for the same id is rejected with
    /// <see cref="CamusDBErrorCodes.TransactionAlreadyCompleted"/> rather than being reported as a
    /// no-op: it may be a commit, whose outcome is the opposite of what the caller asked for, and
    /// its result is not yet known. Once that finalize completes the entry is gone, so a rollback
    /// arriving afterwards takes the no-op path — a client that raced its own commit against its
    /// own rollback cannot be told which one won, and never could.</para>
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when this call performed the rollback; <see langword="false"/> when
    /// the transaction was already finalized and no longer tracked.
    /// </returns>
    public async Task<bool> RollbackByIdAsync(long txnIdPT, uint txnIdCounter, CancellationToken cancellationToken = default)
    {
        if (!active.TryGetValue((txnIdPT, txnIdCounter), out ActiveTransaction? entry))
            return false;

        if (!entry.TryBeginFinalize())
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                "A commit or rollback is already in progress for this transaction");

        try
        {
            await entry.Manager.RollbackAsync(entry.Transaction, cancellationToken).ConfigureAwait(false);
            Unregister(entry.Transaction);
            return true;
        }
        catch
        {
            entry.EndFinalize();
            throw;
        }
    }

    /// <summary>
    /// Best-effort rollback used by DML cleanup paths. If another commit/rollback already owns the
    /// finalize claim this no-ops rather than throwing — the owning finalize is authoritative and
    /// this cleanup must never race it or mask the real outcome.
    /// </summary>
    public async Task RollbackIfNotCompletedAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        if (!active.TryGetValue(Key(tx), out ActiveTransaction? entry))
            return;

        if (!entry.TryBeginFinalize())
            return;

        try
        {
            await entry.Manager.RollbackIfNotCompletedAsync(tx, cancellationToken).ConfigureAwait(false);
            Unregister(tx);
        }
        catch
        {
            entry.EndFinalize();
            throw;
        }
    }

    private void Register(KvTransactionsManager manager, KvTransaction tx) =>
        active[Key(tx)] = new ActiveTransaction(manager, tx);

    private void Unregister(KvTransaction tx) =>
        active.TryRemove(Key(tx), out _);

    private static (long L, uint C) Key(KvTransaction tx) => (tx.ClientId.L, tx.ClientId.C);
}
