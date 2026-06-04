
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Durable record of an in-flight coordinator job. Written to the KV store before
/// the first step and deleted after the last. If the coordinator node crashes or
/// loses leadership mid-sequence the new leader reads the persisted jobs and resumes
/// from where the previous leader left off (D2 leader-change resume).
/// </summary>
public sealed class PersistedCoordinatorJob
{
    public string TableName { get; set; } = "";

    public string ElementName { get; set; } = "";

    public SchemaElementState TargetState { get; set; }

    /// <summary>
    /// Set when the column must still be created (add sequence that may not yet
    /// have reached the first <c>DeleteOnly</c> step). Null when the column already
    /// exists and only state transitions remain.
    /// </summary>
    public ColumnType? ColumnType { get; set; }

    public bool ColumnNotNull { get; set; }

    public ColumnValue? ColumnDefault { get; set; }
}
