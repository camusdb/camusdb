/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Diagnostics;
using Microsoft.AspNetCore.Mvc;

namespace CamusDB.App.Controllers;

/// <summary>
/// Internal endpoints that answer a peer's <c>SHOW CLUSTER QUERIES</c>, <c>SHOW CLUSTER
/// CONNECTIONS</c>, <c>SHOW CLUSTER TRANSACTIONS</c>, forwarded <c>CANCEL QUERY</c> and forwarded
/// <c>KILL TRANSACTION</c> with this node's own rows. Authenticated
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

    [HttpPost]
    [Route(ClusterActivityWire.TransactionsPath)]
    public async Task<ActionResult<List<TransactionActivityRow>>> ListTransactions([FromBody] ClusterActivityWire.ListRequest request)
        => await executor.QueryActivityService
            .ListTransactionsAsync(cluster: false, request.Viewer, HttpContext.RequestAborted).ConfigureAwait(false);

    /// <summary>
    /// Kills one of this node's transactions for a peer. The kill does not observe the request token: a
    /// kill that reached its owner runs to its outcome even when the asking node stopped waiting. A
    /// rollback that failed answers with its error in the body, so the asking node reports it instead of
    /// reporting this node as unreachable.
    /// </summary>
    [HttpPost]
    [Route(ClusterActivityWire.KillTransactionPath)]
    public async Task<ActionResult<ClusterActivityWire.KillTransactionResponse>> KillTransaction([FromBody] ClusterActivityWire.KillTransactionRequest request)
    {
        try
        {
            TransactionKillOutcome outcome = await executor.QueryActivityService
                .KillLocalTransactionAsync(request.TransactionId, request.Viewer, request.KilledBy).ConfigureAwait(false);
            return new ClusterActivityWire.KillTransactionResponse(outcome);
        }
        catch (CamusDBException e)
        {
            return new ClusterActivityWire.KillTransactionResponse(TransactionKillOutcome.Finalizing, e.Code, e.Message);
        }
    }
}
