
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using CamusDB.App.Models;

namespace CamusDB.App.Services;

/// <summary>
/// Tracks in-flight <see cref="KvTransaction"/> instances for the HTTP API and delegates
/// lifecycle operations to each database's <see cref="KvTransactionsManager"/>.
/// </summary>
public sealed class HttpTransactionCoordinator
{
    private readonly CommandExecutor executor;

    private readonly ConcurrentDictionary<(long L, uint C), ActiveTransaction> active = new();

    private sealed record ActiveTransaction(KvTransactionsManager Manager, KvTransaction Transaction);

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

        // A serializable read-only transaction is stateless on the Kahuna side (zero identity, carries
        // only a snapshot read timestamp), so it cannot be registered and resumed by a transaction id
        // the way an explicit multi-statement transaction must be. Reject it here rather than hand back
        // an unusable (0,0) handle; serializable read-only reads are available as autocommit SELECTs.
        CamusIsolationLevel effectiveLevel = isolationLevel ?? CamusDBConfig.DefaultIsolationLevel;
        if (effectiveLevel == CamusIsolationLevel.Serializable && transactionMode == CamusTransactionMode.ReadOnly)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Explicit serializable read-only transactions are not supported; run serializable reads as autocommit SELECTs.");

        DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);

        KvTransaction tx = await database.Transactions.BeginAsync(isolationLevel, transactionMode, cancellationToken).ConfigureAwait(false);
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
    public async Task<KvTransaction> BeginReadOnlyAsync(string databaseName, bool promote, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(databaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DatabaseName is required");

        DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);
        KvTransaction tx = await database.Transactions.BeginReadOnlyAsync(promote, cancellationToken).ConfigureAwait(false);

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

        return entry.Transaction;
    }

    public async Task CommitAsync(DatabaseDescriptor database, KvTransaction tx, CancellationToken cancellationToken = default)
    {
        await database.Transactions.CommitAsync(tx, cancellationToken).ConfigureAwait(false);
        Unregister(tx);
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
