
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
    /// Indexes on the table, keyed by index name. Uses <see cref="StringComparer.OrdinalIgnoreCase"/>
    /// so an index referenced in SQL (e.g. a <c>FORCE INDEX</c> hint or <c>DROP INDEX</c>) matches
    /// regardless of the case written, while the stored name preserves the case it was created with.
    /// </summary>
    public Dictionary<string, TableIndexSchema> Indexes { get; } = new(StringComparer.OrdinalIgnoreCase);

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
