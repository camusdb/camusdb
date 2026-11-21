
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
    /// Every expression key part that an ordered index of this descriptor has had, keyed by hidden key
    /// name (<see cref="BoundIndexExpression.KeyName"/>): how a planner pass that has only a name finds
    /// the type of a comparison on a key position that is not a column. Two indexes over one expression
    /// share an entry. Empty for a table that never had an expression index. Never mutate it.
    ///
    /// <para><b>Grow-only.</b> <see cref="MutateIndexes"/> adds entries and never removes one, so a
    /// DROP INDEX leaves its expression here. A hidden key name is a hash of the stored text and the
    /// result type, so one name has one type for the life of the descriptor. A plan that captured an
    /// index before a DROP INDEX therefore still finds its type by name. A reader of <see cref="Indexes"/>
    /// and of this map makes two reads, which can see two generations; with a grow-only map that
    /// difference cannot hide a type. The planner matches WHERE terms against the expressions of the
    /// indexes it captured, never against this map, so a dropped expression gets no new comparison.</para>
    /// </summary>
    public IReadOnlyDictionary<string, BoundIndexExpression> ExpressionKeys => Volatile.Read(ref expressionKeys);

    /// <summary>
    /// The result type of the expression whose hidden key name is <paramref name="keyName"/>, or null
    /// when the name is not one. For a pass that has only a name; a pass that has the index reads
    /// <see cref="IndexKeyType"/>.
    /// </summary>
    public ColumnType? ExpressionKeyType(string keyName) =>
        ExpressionKeys.TryGetValue(keyName, out BoundIndexExpression? expression) ? expression.Type : null;

    /// <summary>
    /// The type of key position <paramref name="position"/> of <paramref name="index"/>: the result
    /// type of the expression at an expression position, otherwise the type of the column; null when
    /// the table has no such column. The expression type comes from <paramref name="index"/> itself, so
    /// a scan decodes the keys of the index its plan captured with that index's types, whatever DDL
    /// changed the table's current indexes since.
    /// </summary>
    public ColumnType? IndexKeyType(TableIndexSchema index, int position)
    {
        if (index.ExpressionAt(position) is { } expression)
            return expression.Type;

        string name = index.Columns[position];
        List<TableColumnSchema>? columns = Schema.Columns;
        if (columns is not null)
        {
            for (int i = 0; i < columns.Count; i++)
            {
                if (string.Equals(columns[i].Name, name, StringComparison.OrdinalIgnoreCase))
                    return columns[i].Type;
            }
        }

        // A hidden key name on an index whose bound expressions are not at hand.
        return ExpressionKeyType(name);
    }

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

            // Grow-only (see ExpressionKeys): it starts from the entries already published. A newer
            // binding of a name (after a column rename) replaces the older one; both have one type.
            Dictionary<string, BoundIndexExpression> expressions = new(expressionKeys, StringComparer.OrdinalIgnoreCase);
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
                                expressions[expression.KeyName] = expression;
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
