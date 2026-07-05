
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

        /// <summary>Marks the transaction as just-used, resetting its idle timer.</summary>
        public void Touch() => Volatile.Write(ref lastActivityTicks, Stopwatch.GetTimestamp());

        /// <summary>Elapsed monotonic time since the transaction was last touched.</summary>
        public TimeSpan IdleTime => Stopwatch.GetElapsedTime(Volatile.Read(ref lastActivityTicks));
    }

    public HttpTransactionCoordinator(CommandExecutor executor)
    {
        this.executor = executor;
    }

    public async Task<KvTransaction> StartAsync(string databaseName, CancellationToken cancellationToken = default) =>
        await StartAsync(databaseName, isolationLevel: null, transactionMode: null, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Starts a new transaction with the requested isolation level and mode. When either argument
    /// is <see langword="null"/> the server default (<see cref="CamusDBConfig.DefaultIsolationLevel"/>
    /// and <see cref="CamusTransactionMode.ReadWrite"/>) applies.
    /// </summary>
    public async Task<KvTransaction> StartAsync(
        string databaseName,
        CamusIsolationLevel? isolationLevel,
        CamusTransactionMode? transactionMode,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(databaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DatabaseName is required to start a transaction");

        DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);

        KvTransaction tx = await database.Transactions.BeginAsync(isolationLevel, transactionMode, cancellationToken: cancellationToken).ConfigureAwait(false);
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
            // already removed it (we skip) or finds it gone (its lookup fails cleanly). Only the
            // thread that wins the removal drives the rollback, so we never double-finalize.
            if (!active.TryRemove(pair.Key, out _))
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

    public async Task<HLCTimestamp> CommitAsync(DatabaseDescriptor database, KvTransaction tx, CancellationToken cancellationToken = default)
    {
        HLCTimestamp token = await database.Transactions.CommitAsync(tx, cancellationToken).ConfigureAwait(false);
        Unregister(tx);
        return token;
    }

    public async Task RollbackAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        if (!active.TryGetValue(Key(tx), out ActiveTransaction? entry))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Unknown transaction");

        await entry.Manager.RollbackAsync(tx, cancellationToken).ConfigureAwait(false);
        Unregister(tx);
    }

    public async Task RollbackIfNotCompletedAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        if (!active.TryGetValue(Key(tx), out ActiveTransaction? entry))
            return;

        await entry.Manager.RollbackIfNotCompletedAsync(tx, cancellationToken).ConfigureAwait(false);
        Unregister(tx);
    }

    private void Register(KvTransactionsManager manager, KvTransaction tx) =>
        active[Key(tx)] = new ActiveTransaction(manager, tx);

    private void Unregister(KvTransaction tx) =>
        active.TryRemove(Key(tx), out _);

    private static (long L, uint C) Key(KvTransaction tx) => (tx.TransactionId.L, tx.TransactionId.C);
}
