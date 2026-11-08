/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Outcome of <c>POST /v1/cluster/split-range</c>. <c>Success</c> means the range map now holds one more
/// range of the key space, with <c>NewPartitionId</c> serving the upper half. Otherwise <c>Status</c> is
/// Kahuna's own outcome name (<c>NotLeader</c>, <c>NoRange</c>, <c>BelowMinRangeSize</c>, an indeterminate
/// <c>QuiesceFailed</c> or <c>TransferFailed</c>, and so on) or <c>Refused</c> when the request never
/// reached the splitter, and <c>Reason</c> says why. <c>Determinate</c> is false when the map may still
/// change after the call: a caller re-reads the ranges before concluding anything. <c>DescriptorsBefore</c>
/// is the range count the node saw before it tried, so a caller that retries after an indeterminate answer
/// can tell a split that landed from one that did not.
/// </summary>
public sealed class ClusterSplitRangeResponse
{
    public bool Success { get; set; }

    public string Status { get; set; } = "";

    public bool Determinate { get; set; } = true;

    public string KeySpace { get; set; } = "";

    public string? SplitKey { get; set; }

    public int KeysSeen { get; set; }

    public int DescriptorsBefore { get; set; }

    public int NewPartitionId { get; set; }

    public string? LeaderHint { get; set; }

    public string? Reason { get; set; }
}
