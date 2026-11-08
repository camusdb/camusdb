/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Body of <c>POST /v1/cluster/split-range</c>: which relation's key space to divide. <c>Index</c> null
/// names the row space; a name (or the primary index's aliases <c>primary</c> and <c>&lt;table&gt;_pkey</c>)
/// names one of the readable indexes. The split key is not a parameter: the node picks the space's
/// median key, see <c>RangeSplitKeyChooser</c>. <c>MaxKeys</c> bounds the scan that finds it; a space
/// with more keys is refused.
/// </summary>
public sealed class ClusterSplitRangeRequest
{
    public string Database { get; set; } = "";

    public string Table { get; set; } = "";

    public string? Index { get; set; }

    public int MaxKeys { get; set; } = 1_000_000;
}
