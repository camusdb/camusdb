
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
using CamusDB.App.Models;
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
        bool promoteReadOnly = false,
        CamusIsolationLevel? isolationLevel = null,
        CamusTransactionMode? transactionMode = null,
        CancellationToken cancellationToken = default)
    {
        if (txnIdPT > 0)
            return (false, transactions.GetState(txnIdPT, txnIdCounter));

        if (string.IsNullOrEmpty(databaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DatabaseName is required to start a transaction");

        // Read-only queries default to a zero-timestamp synthetic transaction: no
        // LocateAndStartTransaction / LocateAndCommitTransaction round-trips to Kahuna. Kahuna treats
        // HLCTimestamp.Zero as "read latest committed value" per key (read-committed per key).
        //
        // Scan SELECTs pass promoteReadOnly: in key-range sharding mode the read-only transaction is
        // then promoted to a real transaction so the scan can hold a shared range lock (serializable,
        // phantom-free read). A promoted transaction has a real identity and must be finalized, so we
        // report NewTransaction = true to make the caller commit it on success / roll back on error.
        if (readOnly)
        {
            KvTransaction roTx = await transactions.BeginReadOnlyAsync(databaseName, promoteReadOnly, cancellationToken).ConfigureAwait(false);
            bool promoted = roTx.TransactionId != Kommander.Time.HLCTimestamp.Zero;
            return (promoted, roTx);
        }

        KvTransaction tx = await transactions.StartAsync(databaseName, isolationLevel, transactionMode, cancellationToken).ConfigureAwait(false);
        return (true, tx);
    }

    /// <summary>
    /// Parses the optional <c>IsolationLevel</c> and <c>TransactionMode</c> string fields from
    /// a request into their typed enum equivalents. Unrecognised or null values resolve to
    /// <c>null</c> — the server default (<see cref="CamusDBConfig.DefaultIsolationLevel"/> /
    /// <see cref="CamusTransactionMode.ReadWrite"/>) applies.
    /// </summary>
    protected static (CamusIsolationLevel? level, CamusTransactionMode? mode) ParseRequestLevelMode(
        ExecuteSQLRequest request)
    {
        CamusIsolationLevel? level = null;
        CamusTransactionMode? mode  = null;

        if (request.IsolationLevel is not null &&
            Enum.TryParse(request.IsolationLevel, ignoreCase: true, out CamusIsolationLevel parsed))
            level = parsed;

        if (request.TransactionMode is not null &&
            Enum.TryParse(request.TransactionMode, ignoreCase: true, out CamusTransactionMode parsedMode))
            mode = parsedMode;

        return (level, mode);
    }
}
