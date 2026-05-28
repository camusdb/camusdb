
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

    public async Task<KvTransaction> StartAsync(string databaseName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(databaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DatabaseName is required to start a transaction");

        DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);

        KvTransaction tx = await database.Transactions.BeginAsync(cancellationToken).ConfigureAwait(false);
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
