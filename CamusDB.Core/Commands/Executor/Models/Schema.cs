
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics.CodeAnalysis;
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
        // A name carrying the reserved prefix would shadow the reference a stored view body uses to
        // name a relation, so a body could be made to read something other than what it was bound to.
        if (StoredRelationRef.IsReservedRelationName(name))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Relation name '{name}' starts with '{StoredRelationRef.Prefix}', which is reserved: " +
                "stored view definitions use it to refer to a relation by its immutable id");

        if (Tables.TryGetValue(name, out TableSchema? existingTable))
            throw new CamusDBException(
                existingTable.IsMaterializedView ? CamusDBErrorCodes.ViewAlreadyExists : CamusDBErrorCodes.TableAlreadyExists,
                $"Relation '{name}' already exists");

        if (Views.ContainsKey(name))
            throw new CamusDBException(
                CamusDBErrorCodes.ViewAlreadyExists,
                $"Relation '{name}' already exists");
    }

    // Published as one finished dictionary that is never mutated after publication, so a reader
    // holding no lock always sees a complete index rather than one being filled in.
    private volatile Dictionary<string, string>? relationNamesById;

    /// <summary>
    /// The name a relation id currently answers to. This is the reverse of every other lookup here,
    /// and it exists because a stored view body names its relations by immutable id — see
    /// <see cref="Catalogs.Models.StoredRelationRef"/>.
    /// </summary>
    /// <remarks>
    /// Reads a published snapshot and never walks <see cref="Tables"/> or <see cref="Views"/>
    /// itself. That is the point: those dictionaries are mutated in place by an applying delta, and
    /// enumerating one while it is being written throws, so only the writer — which holds
    /// <see cref="Semaphore"/> — may walk them. See <see cref="RebuildRelationNameIndex"/>.
    /// </remarks>
    public bool TryGetRelationNameById(string relationId, [MaybeNullWhen(false)] out string name)
    {
        Dictionary<string, string>? index = relationNamesById;

        if (index is null)
        {
            // Only reachable before the first build — a database is indexed as its schema is loaded,
            // and every delta re-indexes. Guarded because this is the one path that could walk the
            // live maps without the lock.
            index = BuildRelationNameIndexDefensively();
            relationNamesById = index;
        }

        return index.TryGetValue(relationId, out name!);
    }

    /// <summary>
    /// Re-indexes relation ids to names. Must be called while holding <see cref="Semaphore"/>, with
    /// the schema's dictionaries in their final post-mutation state.
    /// </summary>
    /// <remarks>
    /// Called after a delta's mutations and <b>before</b> <see cref="SchemaVersion"/> advances, and
    /// again whenever the maps are replaced wholesale by a schema load. Ordering it before the
    /// version bump keeps a lock-free reader's worst case to the staleness it already lives with on
    /// a <see cref="Tables"/> lookup — the pre-delta name — rather than a torn or missing index.
    /// </remarks>
    public void RebuildRelationNameIndex()
    {
        Dictionary<string, string> names = new(Tables.Count + Views.Count, StringComparer.Ordinal);

        // Tables and materialized views alike: a materialized view is a relation and a body may read
        // one. Views second, and neither can overwrite the other — ids come from one sequence, so a
        // collision would mean the sequence handed the same id out twice.
        foreach (TableSchema table in Tables.Values)
        {
            if (table.Id is { Length: > 0 } id && table.Name is { Length: > 0 } tableName)
                names[id] = tableName;
        }

        foreach (ViewSchema view in Views.Values)
        {
            if (view.Id is { Length: > 0 } id && view.Name is { Length: > 0 } viewName)
                names[id] = viewName;
        }

        relationNamesById = names;
    }

    private Dictionary<string, string> BuildRelationNameIndexDefensively()
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                RebuildRelationNameIndex();
                return relationNamesById!;
            }
            catch (InvalidOperationException) when (attempt < 2)
            {
                // A delta mutated a map mid-walk. Retrying is enough: the writer holds the lock for
                // the length of one apply, not for anything unbounded.
            }
        }
    }

    public void Dispose()
    {
        Semaphore?.Dispose();
    }
}
