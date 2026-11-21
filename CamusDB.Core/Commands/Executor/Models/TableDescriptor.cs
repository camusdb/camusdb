
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

    private Dictionary<string, TableIndexSchema> allIndexes = new(StringComparer.OrdinalIgnoreCase);

    private Dictionary<string, TableIndexSchema> indexes = new(StringComparer.OrdinalIgnoreCase);

    private TableIndexSchema[] invertedIndexes = [];

    private Dictionary<string, BoundIndexExpression> expressionKeys = new(StringComparer.OrdinalIgnoreCase);

    private long indexSetGeneration = Interlocked.Increment(ref globalIndexSetGeneration);

    /// <summary>
    /// The ordered indexes on the table (unique and non-unique), keyed by index name. Uses
    /// <see cref="StringComparer.OrdinalIgnoreCase"/> so an index referenced in SQL (e.g. a
    /// <c>FORCE INDEX</c> hint or <c>DROP INDEX</c>) matches regardless of the case written, while the
    /// stored name preserves the case it was created with.
    ///
    /// <para><b>A GIN index is not here.</b> Every planner, join, seek and foreign-key path reads this
    /// map and treats each entry as an ordered key over its columns. A GIN index
    /// (<see cref="IndexType.Inverted"/>) holds one entry per path and value of a document, so it is
    /// left out, and those paths cannot pick it by mistake. The write paths and the DDL, which must
    /// see every index, read <see cref="AllIndexes"/> or <see cref="InvertedIndexes"/>.</para>
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
    /// Every index on the table, GIN indexes included, keyed by index name: what DDL name checks,
    /// DROP INDEX, DROP TABLE and SHOW INDEXES must see. The same copy-on-write snapshot rules as
    /// <see cref="Indexes"/>.
    /// </summary>
    public Dictionary<string, TableIndexSchema> AllIndexes => Volatile.Read(ref allIndexes);

    /// <summary>The GIN indexes on the table, in no particular order (a snapshot, like <see cref="Indexes"/>).</summary>
    public IReadOnlyList<TableIndexSchema> InvertedIndexes => Volatile.Read(ref invertedIndexes);

    /// <summary>
    /// The expression key parts of the ordered indexes, keyed by hidden key name
    /// (<see cref="BoundIndexExpression.KeyName"/>): what the planner matches a WHERE term against, and
    /// how a planner helper finds the type of a key position that is not a column. Two indexes over one
    /// expression share an entry. Published with <see cref="Indexes"/> by <see cref="MutateIndexes"/>,
    /// so a reader that captured one snapshot of each sees matching sets; empty for a table without
    /// expression indexes. Never mutate it.
    /// </summary>
    public IReadOnlyDictionary<string, BoundIndexExpression> ExpressionKeys => Volatile.Read(ref expressionKeys);

    /// <summary>
    /// The result type of the expression whose hidden key name is <paramref name="keyName"/>, or null
    /// when the name is not one. A helper that types the key positions of an index reads this before
    /// its fallback, so an expression position never decodes with the type of a missing column.
    /// </summary>
    public ColumnType? ExpressionKeyType(string keyName) =>
        ExpressionKeys.TryGetValue(keyName, out BoundIndexExpression? expression) ? expression.Type : null;

    /// <summary>
    /// The column named <paramref name="name"/>, or, for a hidden key name, the expression's
    /// <see cref="BoundIndexExpression.KeyColumn"/>; null when neither exists. For the planner passes that
    /// type a comparison by the name it compares.
    /// </summary>
    public TableColumnSchema? FindColumnOrExpressionKey(string name)
    {
        List<TableColumnSchema>? columns = Schema.Columns;
        if (columns is not null)
        {
            for (int i = 0; i < columns.Count; i++)
            {
                if (string.Equals(columns[i].Name, name, StringComparison.OrdinalIgnoreCase))
                    return columns[i];
            }
        }

        return ExpressionKeys.TryGetValue(name, out BoundIndexExpression? expression) ? expression.KeyColumn : null;
    }

    /// <summary>
    /// Monotonically increasing generation of the index set, bumped on every
    /// <see cref="MutateIndexes"/> swap. Used by the plan cache as an invalidation signal for
    /// index DDL, which deliberately does not bump <see cref="TableSchema.Version"/> (indexes
    /// are not part of row encoding). Values are process-unique, never reused across
    /// descriptor rebuilds.
    /// </summary>
    public long IndexSetGeneration => Volatile.Read(ref indexSetGeneration);

    /// <summary>
    /// Applies <paramref name="mutate"/> to a clone of the map of every index (GIN indexes included)
    /// and atomically publishes it, with the ordered-only <see cref="Indexes"/> and the
    /// <see cref="InvertedIndexes"/> derived from it, so concurrent lock-free readers always observe a
    /// complete snapshot (old or new, never a mid-mutation dictionary). Serialized against other
    /// mutations; bumps <see cref="IndexSetGeneration"/> after the swap.
    /// </summary>
    public void MutateIndexes(Action<Dictionary<string, TableIndexSchema>> mutate)
    {
        lock (indexMutationLock)
        {
            Dictionary<string, TableIndexSchema> next = new(allIndexes, StringComparer.OrdinalIgnoreCase);
            mutate(next);

            Dictionary<string, TableIndexSchema> ordered = new(next.Count, StringComparer.OrdinalIgnoreCase);
            Dictionary<string, BoundIndexExpression> expressions = new(StringComparer.OrdinalIgnoreCase);
            List<TableIndexSchema> inverted = [];
            foreach (KeyValuePair<string, TableIndexSchema> entry in next)
            {
                if (entry.Value.Type == IndexType.Inverted)
                    inverted.Add(entry.Value);
                else
                {
                    ordered[entry.Key] = entry.Value;

                    if (entry.Value.Expressions is { } bound)
                    {
                        foreach (BoundIndexExpression? expression in bound)
                        {
                            if (expression is not null)
                                expressions.TryAdd(expression.KeyName, expression);
                        }
                    }
                }
            }

            Volatile.Write(ref expressionKeys, expressions);
            Volatile.Write(ref invertedIndexes, inverted.ToArray());
            Volatile.Write(ref indexes, ordered);
            Volatile.Write(ref allIndexes, next);
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
