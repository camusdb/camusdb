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

    /// <summary>The body of a list request: who asks, so the peer can filter its rows.</summary>
    public sealed record ListRequest(ActivityViewer Viewer);

    /// <summary>The body of a cancel request.</summary>
    public sealed record CancelRequest(string QueryId, ActivityViewer Viewer);

    /// <summary>The body of a cancel response.</summary>
    public sealed record CancelResponse(QueryCancelOutcome Outcome);
}
