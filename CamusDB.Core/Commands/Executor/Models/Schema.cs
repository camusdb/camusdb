
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
/// deltas are applied. See the architecture documentation.
/// </summary>
public sealed class Schema : IDisposable
{
    /// <summary>Monotonic per-database schema version. Bumped by each applied delta.</summary>
    public long SchemaVersion { get; set; }

    /// <summary>
    /// Live tables keyed by name. Renaming swaps the key; the table's immutable Id is unchanged.
    /// The comparer is <see cref="StringComparer.OrdinalIgnoreCase"/> so table names match
    /// case-insensitively regardless of the case the user wrote in SQL, while the stored
    /// <see cref="TableSchema.Name"/> preserves the original case the table was created with.
    /// </summary>
    public Dictionary<string, TableSchema> Tables { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Live non-materialized views keyed by name, with the same case-insensitive comparer and the
    /// same rename-swaps-the-key rule as <see cref="Tables"/>.
    ///
    /// <para>Materialized views are deliberately <b>not</b> here — they are real relations and live
    /// in <see cref="Tables"/> with <c>Kind == RelationKind.MaterializedView</c>. Consequently this
    /// map alone never answers "does this name exist"; use
    /// <see cref="RequireRelationNameAvailable"/> or <see cref="TryResolveRelation"/>, which consult
    /// both.</para>
    /// </summary>
    public Dictionary<string, ViewSchema> Views { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Serializes schema validation and apply so deltas are applied one at a time.
    /// Acquire via <see cref="AcquireLockAsync"/> and release via <see cref="ReleaseLock"/>
    /// so the depth counter stays in sync for the lock-depth assertions.
    /// </summary>
    public SemaphoreSlim Semaphore { get; } = new(1, 1);

    // Tracks how many callers currently hold Schema.Semaphore.
    // Zero means nobody holds it; non-zero flags an invariant violation when
    // a replicated KV write is attempted. Interlocked for thread safety across
    // the async continuations that may resume on different threads.
    private int _lockDepth;

    /// <summary>
    /// Number of callers currently holding <see cref="Semaphore"/>. Used by the
    /// lock-depth assertions to detect replicated KV writes while the schema lock is held.
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

    /// <summary>
    /// What a relation name currently resolves to, if anything. Tables, materialized views and
    /// views share one namespace, so a single lookup that consults only one map is always a latent
    /// bug — this is the one place that knows all three live together.
    /// </summary>
    public bool TryResolveRelation(string name, out TableSchema? table, out ViewSchema? view)
    {
        if (Tables.TryGetValue(name, out TableSchema? foundTable))
        {
            table = foundTable;
            view = null;
            return true;
        }

        if (Views.TryGetValue(name, out ViewSchema? foundView))
        {
            table = null;
            view = foundView;
            return true;
        }

        table = null;
        view = null;
        return false;
    }

    /// <summary>
    /// Throws if <paramref name="name"/> is already taken by a table, a materialized view, or a
    /// view. Tables, materialized views and views share one namespace — PostgreSQL's <c>pg_class</c>
    /// rule — so <b>every</b> relation-creating DDL path must call this; checking only the map you
    /// are about to insert into lets a view shadow a table (or the reverse), after which name
    /// resolution silently prefers whichever map is consulted first.
    /// </summary>
    /// <remarks>
    /// This is a check-then-act: the act — inserting into <see cref="Tables"/> or
    /// <see cref="Views"/> — happens in the schema delta that follows. The caller must therefore
    /// hold <see cref="Semaphore"/> across both, or two concurrent creations of the same name both
    /// pass the check and the second silently overwrites the first.
    /// </remarks>
    public void RequireRelationNameAvailable(string name)
    {
        if (Tables.TryGetValue(name, out TableSchema? existingTable))
            throw new CamusDBException(
                existingTable.IsMaterializedView ? CamusDBErrorCodes.ViewAlreadyExists : CamusDBErrorCodes.TableAlreadyExists,
                $"Relation '{name}' already exists");

        if (Views.ContainsKey(name))
            throw new CamusDBException(
                CamusDBErrorCodes.ViewAlreadyExists,
                $"Relation '{name}' already exists");
    }

    public void Dispose()
    {
        Semaphore?.Dispose();
    }
}
