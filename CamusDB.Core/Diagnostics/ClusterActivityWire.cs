/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// The JSON bodies of the <c>/internal/activity/*</c> endpoints. They are shared by the HTTP
/// transport and the host controller so the two ends cannot spell a field differently.
/// </summary>
public static class ClusterActivityWire
{
    /// <summary>The path of the peer endpoint that lists running statements.</summary>
    public const string QueriesPath = "/internal/activity/queries";

    /// <summary>The path of the peer endpoint that lists open connections.</summary>
    public const string ConnectionsPath = "/internal/activity/connections";

    /// <summary>The path of the peer endpoint that cancels one statement.</summary>
    public const string CancelPath = "/internal/activity/cancel";

    /// <summary>The path of the peer endpoint that lists open explicit transactions.</summary>
    public const string TransactionsPath = "/internal/activity/transactions";

    /// <summary>The path of the peer endpoint that kills one transaction.</summary>
    public const string KillTransactionPath = "/internal/activity/kill-transaction";

    /// <summary>The body of a list request: who asks, so the peer can filter its rows.</summary>
    public sealed record ListRequest(ActivityViewer Viewer);

    /// <summary>The body of a cancel request.</summary>
    public sealed record CancelRequest(string QueryId, ActivityViewer Viewer);

    /// <summary>The body of a cancel response.</summary>
    public sealed record CancelResponse(QueryCancelOutcome Outcome);

    /// <summary>The body of a kill request. <paramref name="KilledBy"/> names the caller for the owner's audit log.</summary>
    public sealed record KillTransactionRequest(string TransactionId, ActivityViewer Viewer, TransactionKiller KilledBy);

    /// <summary>
    /// The body of a kill response. A rollback that failed on the owner answers with its error code and
    /// message rather than an HTTP error, so the caller can tell the owner's answer from a node that did
    /// not answer.
    /// </summary>
    public sealed record KillTransactionResponse(TransactionKillOutcome Outcome, string? ErrorCode = null, string? ErrorMessage = null);
}
