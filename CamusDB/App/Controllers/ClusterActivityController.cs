/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Diagnostics;
using Microsoft.AspNetCore.Mvc;

namespace CamusDB.App.Controllers;

/// <summary>
/// Internal endpoints that answer a peer's <c>SHOW CLUSTER QUERIES</c>, <c>SHOW CLUSTER
/// CONNECTIONS</c> and forwarded <c>CANCEL QUERY</c> with this node's own rows. Authenticated
/// exclusively by the node secret — the <c>/internal/</c> middleware rule — which is what lets this
/// node trust the <see cref="ActivityViewer"/> in the body and filter its rows by it.
///
/// <para>Every endpoint answers node-local data only. A peer never asks this node to fan out in turn,
/// so a request cannot loop around the cluster.</para>
/// </summary>
[ApiController]
public sealed class ClusterActivityController : ControllerBase
{
    private readonly CommandExecutor executor;

    public ClusterActivityController(CommandExecutor executor)
    {
        this.executor = executor;
    }

    [HttpPost]
    [Route(ClusterActivityWire.QueriesPath)]
    public async Task<ActionResult<List<QueryActivityRow>>> ListQueries([FromBody] ClusterActivityWire.ListRequest request)
        => await executor.QueryActivityService
            .ListQueriesAsync(cluster: false, request.Viewer, HttpContext.RequestAborted).ConfigureAwait(false);

    [HttpPost]
    [Route(ClusterActivityWire.ConnectionsPath)]
    public async Task<ActionResult<List<ConnectionActivityRow>>> ListConnections([FromBody] ClusterActivityWire.ListRequest request)
        => await executor.QueryActivityService
            .ListConnectionsAsync(cluster: false, request.Viewer, HttpContext.RequestAborted).ConfigureAwait(false);

    [HttpPost]
    [Route(ClusterActivityWire.CancelPath)]
    public ActionResult<ClusterActivityWire.CancelResponse> Cancel([FromBody] ClusterActivityWire.CancelRequest request)
        => new ClusterActivityWire.CancelResponse(executor.QueryActivity.TryCancel(request.QueryId, request.Viewer));
}
