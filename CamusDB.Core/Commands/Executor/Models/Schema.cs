
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

    /// <summary>
    /// Serializes schema validation and apply so deltas are applied one at a time.
    /// Acquire via <see cref="AcquireLockAsync"/> and release via <see cref="ReleaseLock"/>
    /// so the depth counter stays in sync for §3.1 assertions.
    /// </summary>
    public SemaphoreSlim Semaphore { get; } = new(1, 1);

    // H1 §3.1: tracks how many callers currently hold Schema.Semaphore.
    // Zero means nobody holds it; non-zero flags a §3.1 invariant violation when
    // a replicated KV write is attempted. Interlocked for thread safety across
    // the async continuations that may resume on different threads.
    private int _lockDepth;

    /// <summary>
    /// Number of callers currently holding <see cref="Semaphore"/>. Used by §3.1
    /// assertions to detect replicated KV writes while the schema lock is held.
    /// </summary>
    public int LockDepth => Volatile.Read(ref _lockDepth);

    /// <summary>
    /// Acquires <see cref="Semaphore"/> and increments the depth counter.
    /// Always pair with <see cref="ReleaseLock"/> in a finally block.
    /// </summary>
    public async Task AcquireLockAsync()
    {
        await Semaphore.WaitAsync().ConfigureAwait(false);
        Interlocked.Increment(ref _lockDepth);
    }

    /// <summary>
    /// Decrements the depth counter then releases <see cref="Semaphore"/>.
    /// Decrement-before-release so <see cref="LockDepth"/> returns 0 only after
    /// the lock is fully relinquished from this holder's perspective.
    /// </summary>
    public void ReleaseLock()
    {
        Interlocked.Decrement(ref _lockDepth);
        Semaphore.Release();
    }

    public void Dispose()
    {
        Semaphore?.Dispose();
    }
}
