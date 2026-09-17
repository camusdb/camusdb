/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.Catalogs.Models;
using Kommander.Time;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Tells a <see cref="KvTableStore"/> read which compressed or out-of-line cells to resolve before
/// the row bytes are returned. A cell that is not resolved keeps its storage-form mark, and a decoder
/// that later reads it fails with <see cref="CamusDBErrorCodes.LargeValueNotResolved"/> instead of
/// returning pointer bytes as a value.
///
/// <para><b>A null fetch means resolve everything.</b> That is the default of every store read, so a
/// caller that passes nothing gets rows any decoder can read, whatever it later projects. Only a
/// caller that decodes with a known required-column set passes <see cref="Columns"/> with that same
/// set; this is how a query that does not name a large column avoids fetching it. The planner's
/// <c>ScanRequiredColumns</c> uses null for "decode everything", and <see cref="Columns"/> maps that
/// null to this default, never to "fetch nothing".</para>
///
/// <para><see cref="Raw"/> returns the stored bytes untouched, marks included. The delete and update
/// paths use it to read which keys a row points at without fetching the values.</para>
/// </summary>
public sealed class LargeValueFetch
{
    private readonly TableSchema? schema;
    private readonly IReadOnlySet<string>? requiredColumns;
    private readonly ConcurrentDictionary<int, bool[]> requiredByVersion = new();

    /// <summary>True for the <see cref="Raw"/> instance: no cell is resolved.</summary>
    internal bool IsRaw { get; }

    /// <summary>Return stored bytes untouched, with every storage-form mark intact.</summary>
    public static LargeValueFetch Raw { get; } = new(null, null, raw: true);

    private LargeValueFetch(TableSchema? schema, IReadOnlySet<string>? requiredColumns, bool raw)
    {
        this.schema = schema;
        this.requiredColumns = requiredColumns;
        IsRaw = raw;
    }

    /// <summary>
    /// Resolve only the cells of columns named in <paramref name="requiredColumns"/>, matched by
    /// column name exactly as the row decoder matches them. Returns null — resolve everything — when
    /// <paramref name="requiredColumns"/> is null. The caller must decode with the same set.
    /// </summary>
    public static LargeValueFetch? Columns(TableSchema schema, IReadOnlySet<string>? requiredColumns)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return requiredColumns is null ? null : new LargeValueFetch(schema, requiredColumns, raw: false);
    }

    /// <summary>
    /// Per variable ordinal of the stored layout of <paramref name="storedVersion"/>, whether the cell
    /// must be resolved. The layout may be a historical one, so it is loaded through the schema history
    /// and cached per version for the life of this fetch.
    /// </summary>
    internal async ValueTask<bool[]> RequiredVariableOrdinalsAsync(HLCTimestamp txId, int storedVersion)
    {
        if (requiredByVersion.TryGetValue(storedVersion, out bool[]? cached))
            return cached;

        TableSchemaHistory history = await schema!.GetSchemaHistoryAsync(txId, storedVersion).ConfigureAwait(false);
        List<TableColumnSchema> columns = history.Columns ?? [];

        List<bool> required = new(columns.Count);
        foreach (TableColumnSchema column in columns)
        {
            if (TableColumnSchema.SupportsStorageStrategy(column.Type))
                required.Add(requiredColumns!.Contains(column.Name));
        }

        bool[] mask = required.ToArray();
        return requiredByVersion.GetOrAdd(storedVersion, mask);
    }
}
