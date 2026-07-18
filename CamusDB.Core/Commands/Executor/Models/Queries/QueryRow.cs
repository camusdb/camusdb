
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Owning row representation backed by an ordinal-indexed array plus a shared <see cref="RowLayout"/>.
/// One layout is shared by every row a scan or join node emits; the per-row array owns the cell values.
///
/// <para>
/// <b>Two backings.</b> A row is built either from decoded <see cref="ValueSlot"/>s (the scan/decode
/// path) or from ready <see cref="ColumnValue"/>s (synthetic producers: projection, join, merge,
/// aggregate). Slot-backed rows convert to <see cref="ColumnValue"/> <b>lazily, per accessed cell</b>
/// (cached), so a row that is filtered out — or a cell that is never read — never pays for a
/// <see cref="ColumnValue"/>. This is the whole point of the slot path: decode allocates one
/// <c>ValueSlot[]</c> and zero per-cell objects, and only the cells that actually escape materialize.
/// Synthetic producers already hold <see cref="ColumnValue"/>s, so they use the eager backing with no
/// slot round-trip.
/// </para>
///
/// <para>
/// <b>Values is the whole-row escape hatch.</b> <see cref="Values"/> returns the full
/// <see cref="ColumnValue"/> array; for a slot-backed row that forces materialization of every cell, so
/// hot paths should prefer <see cref="GetColumnValue(int)"/> (per-cell) or the dictionary adapter.
/// </para>
///
/// <para>
/// <b>Ownership:</b> rows escape into sort/hash/join/spill buffers, so the backing array is always
/// privately owned — never a slice of a pooled buffer. The row is the sole owner for its lifetime.
/// Not thread-safe: a row is produced and consumed on a single pipeline path, so the lazy cache needs
/// no synchronization.
/// </para>
///
/// <para>
/// <b>Migration adapter:</b> <see cref="QueryRow"/> implements
/// <see cref="IReadOnlyDictionary{TKey,TValue}"/> by routing through <see cref="RowLayout.IndexOf"/>,
/// materializing per cell. This lets consumers that read a row by string key keep working while the
/// pipeline migrates to ordinal access.
/// </para>
/// </summary>
public sealed class QueryRow : IReadOnlyDictionary<string, ColumnValue>
{
    /// <summary>The KV row identifier. <see cref="ObjectIdValue"/> default for synthetic rows (joins, aggregates).</summary>
    public ObjectIdValue RowId { get; }

    /// <summary>The plan-bound layout that maps column names to positions.</summary>
    public RowLayout Layout { get; }

    // Exactly one primary backing is non-null at construction:
    //   _slots != null  → decode path; _eager is filled lazily (whole-row) or _cellCache per-cell.
    //   _slots == null  → eager path; _eager holds the values directly.
    private readonly ValueSlot[]? _slots;
    private ColumnValue[]? _eager;
    private ColumnValue?[]? _cellCache;

    /// <summary>Eager backing — synthetic producers that already hold <see cref="ColumnValue"/>s.</summary>
    public QueryRow(ObjectIdValue rowId, RowLayout layout, ColumnValue[] values)
    {
        RowId = rowId;
        Layout = layout;
        _eager = values;
        _slots = null;
    }

    private QueryRow(ObjectIdValue rowId, RowLayout layout, ValueSlot[] slots, bool _)
    {
        RowId = rowId;
        Layout = layout;
        _slots = slots;
        _eager = null;
    }

    /// <summary>
    /// Slot backing — the decode path. A static factory (not a constructor overload) so it never
    /// collides with the eager <see cref="ColumnValue"/> constructor for untyped arguments. Internal
    /// because <see cref="ValueSlot"/> is internal; cells materialize to <see cref="ColumnValue"/>
    /// lazily on access.
    /// </summary>
    internal static QueryRow FromSlots(ObjectIdValue rowId, RowLayout layout, ValueSlot[] slots)
        => new(rowId, layout, slots, false);

    /// <summary>
    /// The cell at <paramref name="ordinal"/> as a <see cref="ColumnValue"/>. For a slot-backed row this
    /// materializes (and caches) just that cell — the per-cell path hot consumers should use so unread
    /// cells stay unmaterialized.
    /// </summary>
    public ColumnValue GetColumnValue(int ordinal)
    {
        if (_eager is not null)
            return _eager[ordinal];

        _cellCache ??= new ColumnValue?[Layout.Count];
        return _cellCache[ordinal] ??= _slots![ordinal].ToColumnValue();
    }

    /// <summary>
    /// Orders this row's cell <paramref name="ordinal"/> against <paramref name="other"/>'s cell
    /// <paramref name="otherOrdinal"/> with <see cref="ColumnValue.CompareTo"/> semantics. When both
    /// rows are slot-backed the comparison runs directly on the <see cref="ValueSlot"/>s — no
    /// <see cref="ColumnValue"/> is materialized for a sort key — otherwise it falls back to per-cell
    /// materialization. Used by ORDER BY so sorting a decoded result set allocates nothing per key.
    /// </summary>
    internal int CompareCellTo(int ordinal, QueryRow other, int otherOrdinal)
        => _slots is not null && _eager is null && other._slots is not null && other._eager is null
            ? _slots[ordinal].CompareTo(other._slots[otherOrdinal])
            : GetColumnValue(ordinal).CompareTo(other.GetColumnValue(otherOrdinal));

    /// <summary>
    /// Column values in ordinal order. For a slot-backed row this materializes <b>every</b> cell (and
    /// caches the full array) — the whole-row escape hatch; prefer <see cref="GetColumnValue(int)"/>.
    /// </summary>
    public ColumnValue[] Values
    {
        get
        {
            if (_eager is not null)
                return _eager;

            ColumnValue[] full = new ColumnValue[Layout.Count];
            if (_cellCache is null)
            {
                for (int i = 0; i < full.Length; i++)
                    full[i] = _slots![i].ToColumnValue();
            }
            else
            {
                for (int i = 0; i < full.Length; i++)
                    full[i] = _cellCache[i] ??= _slots![i].ToColumnValue();
            }

            // Promote to the eager backing so repeated whole-row access is free and the cache is dropped.
            _eager = full;
            _cellCache = null;
            return full;
        }
    }

    /// <summary>
    /// Returns a row with the same values under a different <see cref="RowLayout"/> (column re-qualify),
    /// preserving the slot backing so no materialization happens. Used when a scan row is requalified
    /// (e.g. join input aliasing).
    /// </summary>
    public QueryRow WithLayout(RowLayout layout)
        => _slots is not null && _eager is null
            ? FromSlots(RowId, layout, _slots)
            : new QueryRow(RowId, layout, Values);

    // ── IReadOnlyDictionary<string, ColumnValue> adapter (per-cell materialization) ──

    /// <inheritdoc/>
    public int Count => Layout.Count;

    /// <inheritdoc/>
    public IEnumerable<string> Keys => Layout.OutputNames;

    /// <inheritdoc/>
    IEnumerable<ColumnValue> IReadOnlyDictionary<string, ColumnValue>.Values => EnumerateValues();

    private IEnumerable<ColumnValue> EnumerateValues()
    {
        for (int i = 0; i < Layout.Count; i++)
            yield return GetColumnValue(i);
    }

    /// <inheritdoc/>
    public ColumnValue this[string key]
    {
        get
        {
            int ordinal = Layout.IndexOf(key);
            if (ordinal < 0)
                throw new KeyNotFoundException($"Column '{key}' not found in row layout.");
            return GetColumnValue(ordinal);
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
        value = GetColumnValue(ordinal);
        return true;
    }

    /// <inheritdoc/>
    public IEnumerator<KeyValuePair<string, ColumnValue>> GetEnumerator()
    {
        for (int i = 0; i < Layout.Count; i++)
            yield return new KeyValuePair<string, ColumnValue>(Layout.OutputNames[i], GetColumnValue(i));
    }

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
