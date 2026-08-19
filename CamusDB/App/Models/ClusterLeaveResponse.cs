/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Verdict of a graceful decommission request. <c>Left</c> means the removal is committed and the
/// process can be stopped; <c>Drained</c> reports whether the node's replicas were evacuated first.
/// A non-final outcome carries <c>Retryable</c> so an orchestrator can branch without parsing the
/// reason text.
/// </summary>
public sealed class ClusterLeaveResponse
{
    public bool Left { get; set; }

    public bool Drained { get; set; }

    public string Outcome { get; set; } = "";

    public long MembershipVersion { get; set; }

    public bool Retryable { get; set; }

    public string Reason { get; set; } = "";
}
