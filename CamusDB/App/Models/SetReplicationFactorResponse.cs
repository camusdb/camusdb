/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Outcome of a per-partition replication-factor override. Only the meta-partition leader accepts
/// the mutation: a follower refuses with the reason so the caller can retry against the leader
/// instead of receiving an opaque failure.
/// </summary>
public sealed class SetReplicationFactorResponse
{
    public bool Success { get; set; }

    public string Status { get; set; } = "";

    public long Generation { get; set; }

    public string? Reason { get; set; }
}
