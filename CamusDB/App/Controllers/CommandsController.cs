
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Transactions;
using CamusDB.App.Services;

namespace CamusDB.App.Controllers;

public abstract class CommandsController : ControllerBase
{
    protected readonly CommandExecutor executor;

    protected readonly HttpTransactionCoordinator transactions;

    protected readonly ILogger<ICamusDB> logger;

    protected readonly JsonSerializerOptions jsonOptions;

    public CommandsController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger)
    {
        this.executor = executor;
        this.transactions = transactions;
        this.logger = logger;

        this.jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
    }

    protected async Task<(bool NewTransaction, KvTransaction Tx)> BeginOrResumeAsync(
        string? databaseName,
        long txnIdPT,
        uint txnIdCounter,
        bool readOnly = false,
        CancellationToken cancellationToken = default)
    {
        if (txnIdPT > 0)
            return (false, transactions.GetState(txnIdPT, txnIdCounter));

        if (string.IsNullOrEmpty(databaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DatabaseName is required to start a transaction");

        // Read-only queries use a zero-timestamp synthetic transaction: no LocateAndStartTransaction /
        // LocateAndCommitTransaction round-trips to Kahuna. Kahuna treats HLCTimestamp.Zero as
        // "read latest committed value" per key (non-transactional snapshot, read-committed per key).
        if (readOnly)
        {
            KvTransaction roTx = await transactions.BeginReadOnlyAsync(databaseName, cancellationToken).ConfigureAwait(false);
            return (false, roTx); // false → no commit call needed
        }

        KvTransaction tx = await transactions.StartAsync(databaseName, cancellationToken).ConfigureAwait(false);
        return (true, tx);
    }
}
