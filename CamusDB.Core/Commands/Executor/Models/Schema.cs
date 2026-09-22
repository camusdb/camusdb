
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
/// by <c>SchemaDeltaApplier.ApplySchemaDelta</c> as committed <see cref="Catalogs.Models.SchemaChangeLogEntry"/>
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
    /// Live sequences keyed by name, with the same case-insensitive comparer and the same
    /// rename-swaps-the-key rule as <see cref="Tables"/>.
    ///
    /// <para>Sequences share the relation namespace with tables and views, as they do in PostgreSQL,
    /// so <see cref="RequireRelationNameAvailable"/> consults this map too. They are <b>not</b>
    /// relations for any other purpose: nothing selects from a sequence, and
    /// <see cref="TryResolveRelation"/> deliberately does not return one.</para>
    /// </summary>
    public Dictionary<string, SequenceSchema> Sequences { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    // Published as one finished dictionary that is never mutated after publication, for the same
    // reason relationNamesById is: the map it indexes is mutated in place by an applying delta,
    // and enumerating one while it is being written throws.
    private volatile Dictionary<string, SequenceSchema>? sequencesById;

    /// <summary>
    /// Finds a sequence by its immutable id. Used where a name would be the wrong key: a column
    /// default records the id, and a rename must not break it.
    /// </summary>
    /// <remarks>
    /// <para>Answered from a published index rather than by walking <see cref="Sequences"/>. The
    /// walk is on the insert path — every statement with a sequence-backed default resolves every
    /// such default by id before it can reserve — so it made each identity table's inserts slower
    /// in proportion to how many <em>unrelated</em> sequences the database held.</para>
    ///
    /// <para>Walking the live map was also a read of a dictionary that a concurrent schema delta
    /// may be mutating in place. The index removes both problems at once, which is why it is a
    /// published snapshot and not a lock.</para>
    /// </remarks>
    public SequenceSchema? FindSequenceById(string sequenceId)
    {
        Dictionary<string, SequenceSchema>? index = sequencesById;

        if (index is null)
        {
            index = BuildSequenceIdIndexDefensively();
            sequencesById = index;
        }

        return index.TryGetValue(sequenceId, out SequenceSchema? sequence) ? sequence : null;
    }

    /// <summary>
    /// Re-indexes sequence ids. Must be called while holding <see cref="Semaphore"/>, with
    /// <see cref="Sequences"/> in its final post-mutation state — the same contract
    /// <see cref="RebuildRelationNameIndex"/> has, and it is rebuilt from the same places.
    /// </summary>
    public void RebuildSequenceIdIndex()
    {
        Dictionary<string, SequenceSchema> index = new(Sequences.Count, StringComparer.Ordinal);

        foreach (SequenceSchema sequence in Sequences.Values)
        {
            if (sequence.Id is { Length: > 0 } id)
                index[id] = sequence;
        }

        sequencesById = index;
    }

    private Dictionary<string, SequenceSchema> BuildSequenceIdIndexDefensively()
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                RebuildSequenceIdIndex();
                return sequencesById!;
            }
            catch (InvalidOperationException) when (attempt < 2)
            {
                // A delta mutated the map mid-walk. Retrying is enough: the writer holds the lock
                // for the length of one apply, not for anything unbounded.
            }
        }
    }

    /// <summary>
    /// Serializes schema validation and apply so deltas are applied one at a time.
    /// Acquire via <see cref="AcquireLockAsync"/> and release via <see cref="ReleaseLock"/>
    /// so the hold is recorded for the schema-lock assertions.
    /// </summary>
    public SemaphoreSlim Semaphore { get; } = new(1, 1);

    // Tracks how many callers currently hold Schema.Semaphore, across every flow in the process.
    // Zero means nobody holds it. Interlocked for thread safety across the async continuations
    // that may resume on different threads.
    private int _lockDepth;

    // The lock hold of the current async flow, if it has one. The value is a mutable holder rather
    // than a flag: an AsyncLocal assignment made inside an async method is undone when that method
    // returns, so the holder is published synchronously by AcquireLockAsync, in the caller's own
    // context, and only its Held field changes afterwards. Flows forked while the lock is held see
    // the same holder, and so see the release too.
    private readonly AsyncLocal<LockHold?> _currentHold = new();

    private sealed class LockHold
    {
        public volatile bool Held;
    }

    /// <summary>
    /// Number of callers currently holding <see cref="Semaphore"/>, in <b>any</b> flow. Diagnostic
    /// only: on a node that runs DDL concurrently it is routinely non-zero while another operation
    /// applies its delta, so it says nothing about the caller. Invariant checks must use
    /// <see cref="IsHeldByCurrentFlow"/>.
    /// </summary>
    public int LockDepth => Volatile.Read(ref _lockDepth);

    /// <summary>
    /// True while the current async flow — the method that acquired the lock, its callees, and
    /// anything it forked meanwhile — holds <see cref="Semaphore"/>. This is what the
    /// "no replicated write under the schema lock" assertions check: the deadlock they guard
    /// against is a holder waiting on a write that needs its own lock. A different operation
    /// holding the lock for the length of an in-memory apply is normal and harmless.
    /// </summary>
    public bool IsHeldByCurrentFlow => _currentHold.Value is { Held: true };

    /// <summary>
    /// Acquires <see cref="Semaphore"/> and records the hold for the current flow.
    /// Always pair with <see cref="ReleaseLock"/> in a finally block of the same method.
    /// </summary>
    /// <remarks>
    /// Deliberately not an async method: the flow's holder must be published in the caller's
    /// execution context, which an async method would restore on return.
    /// </remarks>
    public Task AcquireLockAsync()
    {
        LockHold hold = new();
        _currentHold.Value = hold;
        return AcquireLockCoreAsync(hold);
    }

    private async Task AcquireLockCoreAsync(LockHold hold)
    {
        await Semaphore.WaitAsync().ConfigureAwait(false);
        Interlocked.Increment(ref _lockDepth);
        hold.Held = true;
    }

    /// <summary>
    /// Clears the flow's hold and the depth counter, then releases <see cref="Semaphore"/>.
    /// Cleared before release so neither reports a hold after the lock is relinquished from this
    /// holder's perspective.
    /// </summary>
    public void ReleaseLock()
    {
        if (_currentHold.Value is { } hold)
        {
            hold.Held = false;
            _currentHold.Value = null;
        }

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

        // Sequences share the namespace too, so a table cannot be created over a sequence's name
        // and a sequence cannot be created over a table's. Letting them collide would make
        // `nextval('x')` and `SELECT … FROM x` name different objects that answer to one word.
        if (Sequences.ContainsKey(name))
            throw new CamusDBException(
                CamusDBErrorCodes.SequenceAlreadyExists,
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
