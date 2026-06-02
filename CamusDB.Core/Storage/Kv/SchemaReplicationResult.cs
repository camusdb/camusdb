/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Storage.Kv;

public enum SchemaReplicationOutcome
{
    Committed = 0,
    NotLeader = 1,
    Timeout = 2,
    Failed = 3
}

public sealed class SchemaReplicationResult
{
    public SchemaReplicationOutcome Outcome { get; }

    public int PartitionId { get; }

    public long LogIndex { get; }

    public string? Leader { get; }

    public string? Status { get; }

    public bool Committed => Outcome == SchemaReplicationOutcome.Committed;

    public SchemaReplicationResult(
        SchemaReplicationOutcome outcome,
        int partitionId,
        long logIndex = 0,
        string? leader = null,
        string? status = null
    )
    {
        Outcome = outcome;
        PartitionId = partitionId;
        LogIndex = logIndex;
        Leader = leader;
        Status = status;
    }
}
