
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Storage.Kv;

using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Represents a descriptor to access a table
/// </summary>
public sealed class TableDescriptor
{
    /// <summary>
    /// Unique identifier of table
    /// </summary>
    public string Id { get; }

    /// <summary>
    /// Name of the table
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Pointer to the table schema
    /// </summary>
    public TableSchema Schema { get; }

    /// <summary>
    /// KV-backed store for row and index data
    /// </summary>
    public KvTableStore Store { get; }

    /// <summary>
    /// Global monotonic counter backing <see cref="IndexSetGeneration"/>. Process-wide (static)
    /// so a generation value can never repeat across descriptor rebuilds of the same table —
    /// a per-instance counter would restart at zero after descriptor eviction and could
    /// coincidentally match a stale cached value.
    /// </summary>
    private static long globalIndexSetGeneration;

    private readonly object indexMutationLock = new();

    private Dictionary<string, TableIndexSchema> indexes = new(StringComparer.OrdinalIgnoreCase);

    private long indexSetGeneration = Interlocked.Increment(ref globalIndexSetGeneration);

    /// <summary>
    /// Indexes on the table, keyed by index name. Uses <see cref="StringComparer.OrdinalIgnoreCase"/>
    /// so an index referenced in SQL (e.g. a <c>FORCE INDEX</c> hint or <c>DROP INDEX</c>) matches
    /// regardless of the case written, while the stored name preserves the case it was created with.
    ///
    /// The returned dictionary is an immutable snapshot published by copy-on-write: online DDL
    /// (index add/publish/drop) runs concurrently with query planning, and planners enumerate this
    /// dictionary lock-free — an in-place <c>Add</c>/<c>Remove</c> during a concurrent
    /// <c>foreach</c> throws and a concurrent resize is undefined behavior. NEVER mutate it in
    /// place from engine code; use <see cref="MutateIndexes"/>, which clones, mutates the clone,
    /// and swaps the reference. Capture the property once per operation when consistency across
    /// multiple lookups matters.
    /// </summary>
    public Dictionary<string, TableIndexSchema> Indexes => Volatile.Read(ref indexes);

    /// <summary>
    /// Monotonically increasing generation of the index set, bumped on every
    /// <see cref="MutateIndexes"/> swap. Used by the plan cache as an invalidation signal for
    /// index DDL, which deliberately does not bump <see cref="TableSchema.Version"/> (indexes
    /// are not part of row encoding). Values are process-unique, never reused across
    /// descriptor rebuilds.
    /// </summary>
    public long IndexSetGeneration => Volatile.Read(ref indexSetGeneration);

    /// <summary>
    /// Applies <paramref name="mutate"/> to a clone of the current index map and atomically
    /// publishes the clone, so concurrent lock-free readers always observe a complete snapshot
    /// (old or new, never a mid-mutation dictionary). Serialized against other mutations;
    /// bumps <see cref="IndexSetGeneration"/> after the swap.
    /// </summary>
    public void MutateIndexes(Action<Dictionary<string, TableIndexSchema>> mutate)
    {
        lock (indexMutationLock)
        {
            Dictionary<string, TableIndexSchema> next = new(indexes, StringComparer.OrdinalIgnoreCase);
            mutate(next);
            Volatile.Write(ref indexes, next);
            Volatile.Write(ref indexSetGeneration, Interlocked.Increment(ref globalIndexSetGeneration));
        }
    }

    /// <summary>
    /// Compiled positional row codecs, one per stored schema-history version encountered. Built lazily
    /// (a build may await lazy schema-history loading) and cached because a <see cref="CompiledRowCodec"/>
    /// is an immutable pure function of one version's column layout, so it is safe to share across every
    /// row and thread that reads/writes rows of that version. Keyed by the stored version so rows written
    /// under an older schema decode against the exact historical layout.
    /// </summary>
    private readonly ConcurrentDictionary<int, CompiledRowCodec> rowCodecs = new();

    public TableDescriptor(string id, string name, TableSchema schema, KvTableStore store)
    {
        Id = id;
        Name = name;
        Schema = schema;
        Store = store;
    }

    /// <summary>
    /// Returns the <see cref="CompiledRowCodec"/> for <paramref name="schemaVersion"/>, building it from
    /// that version's column layout on first use and caching it. Racing callers may both build (the codec
    /// is immutable and value-equal, so the loser's instance is simply discarded by
    /// <see cref="ConcurrentDictionary{TKey,TValue}.GetOrAdd(TKey,TValue)"/>). Pass the current
    /// transaction's read timestamp so a not-yet-loaded historical layout can be fetched.
    /// </summary>
    internal async ValueTask<CompiledRowCodec> GetRowCodecAsync(HLCTimestamp txId, int schemaVersion)
    {
        if (rowCodecs.TryGetValue(schemaVersion, out CompiledRowCodec? cached))
            return cached;

        TableSchemaHistory history = await Schema.GetSchemaHistoryAsync(txId, schemaVersion).ConfigureAwait(false);
        CompiledRowCodec codec = CompiledRowCodec.Build(schemaVersion, history.Columns!);
        return rowCodecs.GetOrAdd(schemaVersion, codec);
    }
}
