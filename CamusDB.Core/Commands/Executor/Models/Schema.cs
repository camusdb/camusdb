
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// In-memory schema of a single database: the monotonic version counter and the live table
/// set. This is the local materialization of the replicated state machine — it is advanced
/// by <c>CatalogsManager.ApplySchemaDelta</c> as committed <see cref="Catalogs.Models.SchemaChangeLogEntry"/>
/// deltas are applied. See <c>docs/distributed-schema-architecture.md</c> §7.1.
/// </summary>
public sealed class Schema : IDisposable
{
    /// <summary>Monotonic per-database schema version. Bumped by each applied delta.</summary>
    public long SchemaVersion { get; set; }

    /// <summary>Live tables keyed by name. Renaming swaps the key; the table's immutable Id is unchanged.</summary>
    public Dictionary<string, TableSchema> Tables { get; set; } = new();

    /// <summary>Serializes schema validation and apply so deltas are applied one at a time.</summary>
    public SemaphoreSlim Semaphore { get; } = new(1, 1);

    public void Dispose()
    {
        Semaphore?.Dispose();
    }
}
