
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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using CamusDB.App.Models;
using CamusDB.App.Services;
using Kahuna.Shared.KeyValue;

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

    /// <summary>
    /// Resolves the authenticated principal for the current request from the <c>Authorization: Bearer</c>
    /// header. Returns <c>null</c> when <see cref="CamusDBConfig.AuthenticationEnabled"/> is off (no auth
    /// in force). When on, a missing/invalid/expired token throws
    /// <see cref="CamusDBErrorCodes.AuthenticationFailed"/> (HTTP 401) — the engine gate then never sees
    /// a null principal while auth is enabled.
    /// </summary>
    protected async Task<Principal?> ResolveRequestPrincipalAsync()
    {
        if (!CamusDBConfig.AuthenticationEnabled)
            return null;

        string authorization = Request.Headers.Authorization.ToString();
        string? bearer = authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase)
            ? authorization["Bearer ".Length..].Trim()
            : null;

        return await executor.ResolvePrincipalAsync(bearer).ConfigureAwait(false);
    }

    protected async Task<(bool NewTransaction, KvTransaction Tx)> BeginOrResumeAsync(
        string? databaseName,
        long txnIdPT,
        uint txnIdCounter,
        bool readOnly = false,
        bool promoteReadOnly = false,
        CamusIsolationLevel? isolationLevel = null,
        CamusTransactionMode? transactionMode = null,
        KeyValueTransactionLocking? locking = null,
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
            KvTransaction roTx = await transactions.BeginReadOnlyAsync(databaseName, promoteReadOnly, causalToken: null, cancellationToken).ConfigureAwait(false);
            bool promoted = roTx.TransactionId != Kommander.Time.HLCTimestamp.Zero;
            return (promoted, roTx);
        }

        KvTransaction tx = await transactions.StartAsync(databaseName, isolationLevel, transactionMode, locking, cancellationToken: cancellationToken).ConfigureAwait(false);
        return (true, tx);
    }

    /// <summary>
    /// Parses the optional <c>IsolationLevel</c>, <c>TransactionMode</c>, and <c>Locking</c>
    /// string fields from a request into their typed enum equivalents. A null or absent field
    /// resolves to <c>null</c> so the server default applies (<see cref="CamusDBConfig.DefaultIsolationLevel"/>,
    /// <see cref="CamusTransactionMode.ReadWrite"/>, <see cref="CamusDBConfig.DefaultTransactionLocking"/>).
    /// A present-but-unrecognised value for any field throws <see cref="CamusDBException"/> with
    /// <see cref="CamusDBErrorCodes.InvalidInput"/> — the three fields validate identically, and
    /// consistently with the dedicated <c>/start-transaction</c> endpoint, rather than silently
    /// falling back to the default on a typo.
    /// </summary>
    protected static (CamusIsolationLevel? level, CamusTransactionMode? mode, KeyValueTransactionLocking? locking) ParseRequestLevelMode(
        ExecuteSQLRequest request)
    {
        CamusIsolationLevel? level = ParseEnumField<CamusIsolationLevel>(request.IsolationLevel, "isolation level");
        CamusTransactionMode? mode = ParseEnumField<CamusTransactionMode>(request.TransactionMode, "transaction mode");
        KeyValueTransactionLocking? locking = ParseEnumField<KeyValueTransactionLocking>(request.Locking, "locking mode");

        return (level, mode, locking);
    }

    /// <summary>
    /// Parses an optional request enum string field: <c>null</c>/absent yields <c>null</c> (use the
    /// server default); any present value must be a defined member of <typeparamref name="TEnum"/>
    /// (case-insensitive) or a <see cref="CamusDBErrorCodes.InvalidInput"/> is thrown. <see cref="Enum.IsDefined"/>
    /// is required because <see cref="Enum.TryParse{TEnum}(string, bool, out TEnum)"/> also accepts
    /// out-of-range numeric strings (e.g. <c>"5"</c>), which are not valid values.
    /// </summary>
    private static TEnum? ParseEnumField<TEnum>(string? value, string fieldName) where TEnum : struct, Enum
    {
        if (value is null)
            return null;

        if (!Enum.TryParse(value, ignoreCase: true, out TEnum parsed) || !Enum.IsDefined(parsed))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown {fieldName}: {value}");

        return parsed;
    }
}
