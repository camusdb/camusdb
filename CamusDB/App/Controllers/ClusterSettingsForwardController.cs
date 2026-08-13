
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Config;
using Microsoft.AspNetCore.Mvc;

namespace CamusDB.App.Controllers;

/// <summary>
/// Internal node-to-node endpoint that lands a forwarded cluster-setting change on the
/// settings-partition leader — the receiving half of <see cref="HttpClusterSettingsForwarder"/>.
/// Authenticated by the shared node secret via the <c>/internal/</c> middleware prefix rule.
///
/// <para>The action begins with an explicit leader check and answers 503/not-leader when this
/// node does not lead the settings partition, so a stale forward is bounced back to the sender's
/// retry loop rather than re-forwarded — a forwarded request can never chain across nodes. No
/// operation-id replay cache is needed: setting changes are last-writer-wins by Raft commit
/// order, so a retried forward that applies twice converges to the same state.</para>
/// </summary>
[ApiController]
public sealed class ClusterSettingsForwardController : ControllerBase
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    private readonly ILogger<ICamusDB> logger;

    public ClusterSettingsForwardController(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    [HttpPost]
    [Route("/internal/cluster-settings/apply")]
    public async Task<JsonResult> Apply()
    {
        ClusterSettingsForwardRequest? request;

        try
        {
            request = await JsonSerializer.DeserializeAsync<ClusterSettingsForwardRequest>(
                Request.Body, JsonOptions).ConfigureAwait(false);
        }
        catch (JsonException e)
        {
            return Failed(CamusDBErrorCodes.InvalidInput, $"Malformed cluster-settings request: {e.Message}", 400);
        }

        if (request is null || string.IsNullOrWhiteSpace(request.Key))
            return Failed(CamusDBErrorCodes.InvalidInput, "Cluster-settings request requires a key", 400);

        ClusterSettingsService? settings = HttpContext.RequestServices.GetService<ClusterSettingsService>();
        if (settings is null)
            return Failed(CamusDBErrorCodes.InvalidInternalOperation, "Cluster settings are not configured on this node", 500);

        try
        {
            bool applied = await settings.TryApplyAsLeaderAsync(
                new ClusterSettingChange(request.Key, request.Value), HttpContext.RequestAborted).ConfigureAwait(false);

            if (!applied)
                return new JsonResult(new ClusterSettingsForwardResponse { Status = "not-leader" })
                {
                    StatusCode = StatusCodes.Status503ServiceUnavailable,
                };

            return new JsonResult(new ClusterSettingsForwardResponse { Status = "ok" });
        }
        catch (CamusDBException e)
        {
            return Failed(e.Code, e.Message, 500);
        }
        catch (Exception e)
        {
            logger.LogError("Forwarded cluster-setting change for '{Key}' failed: {Message}", request.Key, e.Message);
            return Failed(CamusDBErrorCodes.InvalidInternalOperation, e.Message, 500);
        }
    }

    private static JsonResult Failed(string code, string message, int statusCode)
        => new(new ClusterSettingsForwardResponse { Status = "failed", Code = code, Message = message })
        {
            StatusCode = statusCode,
        };
}
