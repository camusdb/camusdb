
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Owning row representation backed by an ordinal-indexed <c>ColumnValue[]</c> rather than a
/// per-row <c>Dictionary&lt;string,ColumnValue&gt;</c>.  One <see cref="RowLayout"/> is shared
/// by every row emitted by a scan or join node; <see cref="Values"/> is the per-row array that
/// owns the decoded cell values.
///
/// <para>
/// <b>Ownership:</b> rows escape into sort/hash/join/spill buffers, so <see cref="Values"/> is
/// always a privately owned array — never a slice of a pooled buffer. The row is the sole owner
/// for its lifetime.
/// </para>
///
/// <para>
/// <b>Migration adapter:</b> <see cref="QueryRow"/> implements
/// <see cref="IReadOnlyDictionary{TKey,TValue}"/> by routing through
/// <see cref="RowLayout.IndexOf"/>. This lets existing consumers that read a row by string key
/// continue to compile and behave correctly while the pipeline is being migrated incrementally
/// to ordinal access. The adapter is a temporary bridge — once all consumers are on ordinals it
/// will be removed.
/// </para>
/// </summary>
public sealed class QueryRow : IReadOnlyDictionary<string, ColumnValue>
{
    /// <summary>The KV row identifier. <see cref="ObjectIdValue"/> default for synthetic rows (joins, aggregates).</summary>
    public ObjectIdValue RowId { get; }

    /// <summary>The plan-bound layout that maps column names to positions in <see cref="Values"/>.</summary>
    public RowLayout Layout { get; }

    /// <summary>Column values in ordinal order as defined by <see cref="Layout"/>.</summary>
    public ColumnValue[] Values { get; }

    public QueryRow(ObjectIdValue rowId, RowLayout layout, ColumnValue[] values)
    {
        RowId = rowId;
        Layout = layout;
        Values = values;
    }

    // ── IReadOnlyDictionary<string, ColumnValue> adapter ──────────────────────

    /// <inheritdoc/>
    public int Count => Layout.Count;

    /// <inheritdoc/>
    public IEnumerable<string> Keys => Layout.OutputNames;

    /// <inheritdoc/>
    IEnumerable<ColumnValue> IReadOnlyDictionary<string, ColumnValue>.Values => EnumerateValues();

    private IEnumerable<ColumnValue> EnumerateValues()
    {
        for (int i = 0; i < Values.Length; i++)
            yield return Values[i];
    }

    /// <inheritdoc/>
    public ColumnValue this[string key]
    {
        get
        {
            int ordinal = Layout.IndexOf(key);
            if (ordinal < 0)
                throw new KeyNotFoundException($"Column '{key}' not found in row layout.");
            return Values[ordinal];
        }
    }

    /// <inheritdoc/>
    public bool ContainsKey(string key) => Layout.IndexOf(key) >= 0;

    /// <inheritdoc/>
    public bool TryGetValue(string key, out ColumnValue value)
    {
        int ordinal = Layout.IndexOf(key);
        if (ordinal < 0)
        {
            value = default!;
            return false;
        }
        value = Values[ordinal];
        return true;
    }

    /// <inheritdoc/>
    public IEnumerator<KeyValuePair<string, ColumnValue>> GetEnumerator()
    {
        for (int i = 0; i < Layout.Count; i++)
            yield return new KeyValuePair<string, ColumnValue>(Layout.OutputNames[i], Values[i]);
    }

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
