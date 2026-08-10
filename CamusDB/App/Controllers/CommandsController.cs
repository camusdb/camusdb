
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

    /// <summary>Configuration for the engine this controller serves; injected, never ambient.</summary>
    protected readonly CamusDBOptions options;

    public CommandsController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger, CamusDBOptions options)
    {
        this.executor = executor;
        this.transactions = transactions;
        this.logger = logger;
        this.options = options;

        this.jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
    }

    /// <summary>
    /// Resolves the authenticated principal for the current request from the <c>Authorization: Bearer</c>
    /// header. Returns <c>null</c> when <see cref="CamusDBOptions.AuthenticationEnabled"/> is off (no auth
    /// in force). When on, a missing/invalid/expired token throws
    /// <see cref="CamusDBErrorCodes.AuthenticationFailed"/> (HTTP 401) — the engine gate then never sees
    /// a null principal while auth is enabled.
    /// </summary>
    /// <summary>
    /// Refuses a credential-bearing request over a plaintext connection when authentication is enabled
    /// and <see cref="CamusDBOptions.RequireTlsWhenAuthEnabled"/> is set, so a bearer token or password
    /// is never accepted in the clear. A loopback peer is exempted for single-host development. A no-op
    /// when auth or the TLS requirement is off.
    /// </summary>
    protected void EnsureSecureTransport()
    {
        if (!options.AuthenticationEnabled || !options.RequireTlsWhenAuthEnabled)
            return;

        if (Request.IsHttps)
            return;

        System.Net.IPAddress? remote = HttpContext.Connection.RemoteIpAddress;
        if (remote is not null && System.Net.IPAddress.IsLoopback(remote))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InsecureTransport,
            "Authentication is enabled and requires a TLS (HTTPS) connection");
    }

    /// <summary>
    /// Logs a failed command at a level that matches who caused it — see
    /// <see cref="CommandFailureLog"/> for the classification and why client mistakes must not land
    /// in the operator's error stream.
    /// </summary>
    protected void LogCommandFailure(CamusDBException e) => CommandFailureLog.LogFailure(logger, e);

    protected async Task<Principal?> ResolveRequestPrincipalAsync()
    {
        if (!options.AuthenticationEnabled)
            return null;

        // The transport-wide AuthenticationMiddleware already authenticated the request and stashed the
        // principal — reuse it rather than resolving the token a second time.
        if (HttpContext.Items.TryGetValue("camus.principal", out object? stashed) && stashed is Principal cached)
            return cached;

        EnsureSecureTransport();

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
        TransactionPriority? priority = null,
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
            KvTransaction roTx = await transactions.BeginReadOnlyAsync(
                databaseName, promoteReadOnly, causalToken: null, priority: priority,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            bool promoted = roTx.TransactionId != Kommander.Time.HLCTimestamp.Zero;
            return (promoted, roTx);
        }

        KvTransaction tx = await transactions.StartAsync(
            databaseName, isolationLevel, transactionMode, locking, priority: priority,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        return (true, tx);
    }

    /// <summary>
    /// Parses the optional <c>IsolationLevel</c>, <c>TransactionMode</c>, <c>Locking</c> and
    /// <c>Priority</c> string fields from a request into their typed enum equivalents. A null or
    /// absent field resolves to <c>null</c> so the server default applies
    /// (<see cref="CamusDBOptions.DefaultIsolationLevel"/>, <see cref="CamusTransactionMode.ReadWrite"/>,
    /// <see cref="CamusDBOptions.DefaultTransactionLocking"/>,
    /// <see cref="CamusDBOptions.DefaultTransactionPriority"/>).
    /// A present-but-unrecognised value for any field throws <see cref="CamusDBException"/> with
    /// <see cref="CamusDBErrorCodes.InvalidInput"/> — the fields validate identically, and
    /// consistently with the dedicated <c>/start-transaction</c> endpoint, rather than silently
    /// falling back to the default on a typo.
    ///
    /// <para>Note this rejects an unknown <c>priority</c> where the Kahuna server would normalize an
    /// unknown one to <c>Normal</c>. The server normalizes because it receives raw ordinals from
    /// peers of unknown version; here the value is a name supplied by our own client, so an
    /// unrecognised one is a typo the caller wants reported.</para>
    /// </summary>
    protected static (CamusIsolationLevel? level, CamusTransactionMode? mode, KeyValueTransactionLocking? locking, TransactionPriority? priority) ParseRequestLevelMode(
        ExecuteSQLRequest request)
    {
        CamusIsolationLevel? level = ParseEnumField<CamusIsolationLevel>(request.IsolationLevel, "isolation level");
        CamusTransactionMode? mode = ParseEnumField<CamusTransactionMode>(request.TransactionMode, "transaction mode");
        KeyValueTransactionLocking? locking = ParseEnumField<KeyValueTransactionLocking>(request.Locking, "locking mode");
        TransactionPriority? priority = ParseEnumField<TransactionPriority>(request.Priority, "priority");

        return (level, mode, locking, priority);
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
