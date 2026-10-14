/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.AspNetCore.Mvc;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Diagnostics;
using CamusDB.App.Models;
using CamusDB.App.Services;

namespace CamusDB.App.Controllers;

/// <summary>
/// <c>GET /v1/admin/stats</c>: the receiving node's disk, CPU and memory — the REST twin of the gRPC
/// <c>GetNodeStats</c> call, built by the same <see cref="NodeStatsCollector"/>. See
/// <c>docs/node-stats.md</c>.
///
/// <para><b>Authorization.</b> With authentication on, the authentication middleware first requires a
/// valid bearer token (this route is deliberately not on its anonymous list beside <c>/ping</c> and
/// <c>/health</c>: the answer names filesystem paths and capacity). The caller must then be a
/// superuser or hold MONITOR on <c>*.*</c>; anyone else gets <c>403</c> with
/// <see cref="CamusDBErrorCodes.InsufficientPrivilege"/>. With authentication off, anyone may read it,
/// as with the dashboard.</para>
///
/// <para>The route does not start with <c>/execute</c>, so a monitor's poll is not counted as
/// foreground load (see <see cref="ForegroundRequestGaugeMiddleware"/>).</para>
/// </summary>
[ApiController]
public sealed class AdminStatsController : CommandsController
{
    private readonly NodeStatsCollector nodeStats;

    public AdminStatsController(
        CommandExecutor executor,
        HttpTransactionCoordinator transactions,
        ILogger<ICamusDB> logger,
        CamusDBOptions options,
        NodeStatsCollector nodeStats)
        : base(executor, transactions, logger, options)
    {
        this.nodeStats = nodeStats;
    }

    [HttpGet]
    [Route("/v1/admin/stats")]
    public async Task<JsonResult> GetStats()
    {
        try
        {
            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);
            NodeStatsCollector.EnsureAuthorized(principal, options.AuthenticationEnabled);

            return new JsonResult(AdminStatsResponse.From(nodeStats.Collect()));
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);
            return new JsonResult(new AdminStatsResponse("failed") { Code = e.Code, Message = e.Message })
            {
                StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code),
            };
        }
        catch (Exception e)
        {
            string message = LogUnclassifiedFailure(e);
            return new JsonResult(new AdminStatsResponse("failed") { Code = UnclassifiedErrorCode, Message = message })
            {
                StatusCode = StatusCodes.Status500InternalServerError,
            };
        }
    }
}
