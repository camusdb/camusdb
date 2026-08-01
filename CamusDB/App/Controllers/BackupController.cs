/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.App.Models;
using CamusDB.App.Services;

namespace CamusDB.App.Controllers;

/// <summary>
/// Server-level backup and point-in-time-recovery admin API. Backups cover the whole node (every
/// database shares one Kahuna node), so these are versioned server-admin endpoints, not database-scoped
/// ones. Every operation is superuser-gated when authentication is enabled and returns
/// <c>503 BackupNotConfigured</c> unless <c>kahuna.backup_dir</c> is set. Restore is offline: it writes
/// a fresh target directory the operator then boots a new node against.
/// </summary>
[ApiController]
public sealed class BackupController : CommandsController
{
    public BackupController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger,
        CamusDBOptions options)
        : base(executor, transactions, logger, options)
    {
    }

    [HttpPost]
    [Route("/v1/backups/full")]
    public Task<JsonResult> TakeFullBackup() => TakeBackup(BackupKind.Full, parentBackupId: null);

    [HttpPost]
    [Route("/v1/backups/coordinated")]
    public Task<JsonResult> TakeCoordinatedBackup() => TakeBackup(BackupKind.Coordinated, parentBackupId: null);

    [HttpPost]
    [Route("/v1/backups/incremental")]
    public async Task<JsonResult> TakeIncrementalBackup()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            TakeBackupRequest? request = string.IsNullOrWhiteSpace(body)
                ? null
                : JsonSerializer.Deserialize<TakeBackupRequest>(body, jsonOptions);

            Guid parent = ParseGuid(request?.ParentBackupId, "parentBackupId");
            return await TakeBackup(BackupKind.Incremental, parent).ConfigureAwait(false);
        }
        catch (CamusDBException e)
        {
            return Failure(e);
        }
    }

    private async Task<JsonResult> TakeBackup(BackupKind kind, Guid? parentBackupId)
    {
        try
        {
            EnsureBackupTransportAllowed();
            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            TakeBackupTicket ticket = new(kind, parentBackupId, principal);
            BackupInfo info = await executor.TakeBackup(ticket).ConfigureAwait(false);

            return new JsonResult(new BackupResponse("ok") { Backup = BackupInfoModel.From(info) });
        }
        catch (CamusDBException e)
        {
            return Failure(e);
        }
    }

    [HttpGet]
    [Route("/v1/backups")]
    public async Task<JsonResult> ListBackups()
    {
        try
        {
            EnsureBackupTransportAllowed();
            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            IReadOnlyList<BackupInfo> backups = await executor.ListBackups(new ListBackupsTicket(principal)).ConfigureAwait(false);

            return new JsonResult(new BackupListResponse("ok")
            {
                Backups = backups.Select(BackupInfoModel.From).ToList(),
            });
        }
        catch (CamusDBException e)
        {
            return FailureList(e);
        }
    }

    [HttpGet]
    [Route("/v1/backups/{id}/chain")]
    public async Task<JsonResult> GetBackupChain(string id)
    {
        try
        {
            EnsureBackupTransportAllowed();
            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            Guid leaf = ParseGuid(id, "id");
            IReadOnlyList<BackupInfo> chain = await executor.GetBackupChain(new GetBackupChainTicket(leaf, principal)).ConfigureAwait(false);

            return new JsonResult(new BackupListResponse("ok")
            {
                Backups = chain.Select(BackupInfoModel.From).ToList(),
            });
        }
        catch (CamusDBException e)
        {
            return FailureList(e);
        }
    }

    [HttpPost]
    [Route("/v1/backups/gc")]
    public async Task<JsonResult> RunGarbageCollection([FromQuery] bool dryRun = false)
    {
        try
        {
            EnsureBackupTransportAllowed();
            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            BackupGcResult result = await executor.RunBackupGarbageCollection(dryRun, principal).ConfigureAwait(false);
            return new JsonResult(BackupGcResponse.Ok(result));
        }
        catch (CamusDBException e)
        {
            return new JsonResult(new BackupGcResponse("failed", e.Code, e.Message))
            {
                StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code),
            };
        }
    }

    [HttpPost]
    [Route("/v1/restore")]
    public async Task<JsonResult> Restore()
    {
        try
        {
            EnsureBackupTransportAllowed();
            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            RestoreRequest? request = string.IsNullOrWhiteSpace(body)
                ? null
                : JsonSerializer.Deserialize<RestoreRequest>(body, jsonOptions);
            if (request is null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Restore request is not valid");

            Guid leaf = ParseGuid(request.LeafBackupId, "leafBackupId");

            RestoreBackupTicket ticket = new(leaf, request.TargetDir ?? "", request.TargetTimeMs, principal);
            RestoreResult result = await executor.RestoreBackup(ticket).ConfigureAwait(false);

            return new JsonResult(RestoreResponse.Ok(result));
        }
        catch (CamusDBException e)
        {
            return new JsonResult(new RestoreResponse("failed", e.Code, e.Message))
            {
                StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code),
            };
        }
    }

    /// <summary>
    /// Fail-closed transport gate for the whole backup/restore admin surface. Refuses a credential over
    /// plaintext when auth+TLS are required, and — because these endpoints are otherwise anonymous when
    /// authentication is globally disabled — restricts them to loopback in that case, so a network peer
    /// can never trigger a node-wide backup or restore without authentication. When auth is enabled the
    /// superuser requirement is enforced downstream in the executor.
    /// </summary>
    private void EnsureBackupTransportAllowed()
    {
        EnsureSecureTransport();

        if (options.AuthenticationEnabled)
            return;

        IPAddress? remote = HttpContext.Connection.RemoteIpAddress;
        if (remote is null || !IPAddress.IsLoopback(remote))
            throw new CamusDBException(
                CamusDBErrorCodes.InsufficientPrivilege,
                "Backup administration over the network requires authentication to be enabled");
    }

    private static Guid ParseGuid(string? value, string field)
    {
        if (string.IsNullOrWhiteSpace(value) || !Guid.TryParse(value, out Guid parsed) || parsed == Guid.Empty)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"'{field}' must be a valid non-empty GUID");
        return parsed;
    }

    private static JsonResult Failure(CamusDBException e) => new(new BackupResponse("failed", e.Code, e.Message))
    {
        StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code),
    };

    private static JsonResult FailureList(CamusDBException e) => new(new BackupListResponse("failed", e.Code, e.Message))
    {
        StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code),
    };
}
