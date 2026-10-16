
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Owning row representation backed by an ordinal-indexed array plus a shared <see cref="RowLayout"/>.
/// One layout is shared by every row a scan or join node emits; the per-row array owns the cell values.
///
/// <para>
/// <b>Four backings.</b> A row is built from ready <see cref="ColumnValue"/>s (synthetic producers:
/// projection, merge, aggregate — eager), from decoded <see cref="ValueSlot"/>s (the slot decode
/// path), from a borrowed <see cref="RowView"/> over the raw KV bytes (the zero-copy decode path), or
/// from a pair of join input rows (<see cref="FromPair"/>: a joined row that reads each cell from the
/// left or right input on demand, so a pair the <c>ON</c> rejects never copies its cells).
/// Both decode backings convert to <see cref="ColumnValue"/> <b>lazily, per accessed cell</b> (cached),
/// so a row that is filtered out — or a cell that is never read — never pays for a
/// <see cref="ColumnValue"/>. The borrowed backing goes further: it allocates no <c>ValueSlot[]</c> at
/// all and decodes an unread cell not even to a <see cref="ValueSlot"/>, reading straight from the
/// borrowed bytes; a fixed scalar it does read allocates nothing. Synthetic producers already hold
/// <see cref="ColumnValue"/>s, so they use the eager backing with no round-trip.
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

    // Exactly one primary backing is chosen at construction:
    //   _eager   != null → eager path; _eager holds ready ColumnValues directly.
    //   _slots   != null → decode path; cells materialize lazily from ValueSlots (cached per cell).
    //   _rowView != null → borrowed path; cells materialize lazily straight from the row's KV bytes,
    //                      with no up-front ValueSlot[] — a filtered-out or unread cell decodes nothing.
    // Slot- and view-backed rows may promote to _eager on a whole-row read.
    private readonly ValueSlot[]? _slots;
    private readonly RowView? _rowView;
    // For a view-backed row: output ordinal → stored (codec) ordinal, or -1 for a column injected as a
    // default (added after the row was written); injected cells are pre-placed in _cellCache.
    private readonly int[]? _outputToStored;
    private ColumnValue[]? _eager;
    private ColumnValue?[]? _cellCache;
    // Pair backing: a joined row over its two input rows. _pairMap says, for each output ordinal, which
    // input holds the cell and where; the inputs are owned rows the join retains anyway.
    private readonly JoinPairMap? _pairMap;
    private readonly IReadOnlyDictionary<string, ColumnValue>? _pairLeft;
    private readonly IReadOnlyDictionary<string, ColumnValue>? _pairRight;

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

    private QueryRow(ObjectIdValue rowId, RowLayout layout, RowView view, int[] outputToStored, ColumnValue?[]? cellCache)
    {
        RowId = rowId;
        Layout = layout;
        _rowView = view;
        _outputToStored = outputToStored;
        _cellCache = cellCache;
        _slots = null;
        _eager = null;
    }

    /// <summary>
    /// Borrowed backing — the zero-copy decode path. The row holds a <see cref="RowView"/> over the KV
    /// bytes and decodes each cell on demand (mapping output ordinal → stored ordinal via
    /// <paramref name="outputToStored"/>), so no <c>ValueSlot[]</c> is allocated and a cell that is never
    /// read decodes nothing. Columns added after the row was written (<paramref name="injectedDefaults"/>,
    /// stored ordinal -1) are pre-placed in the per-cell cache. Internal because <see cref="RowView"/> is
    /// internal; value-identical to the slot/eager backings for the same row.
    /// </summary>
    internal static QueryRow FromRowView(
        ObjectIdValue rowId,
        RowLayout layout,
        RowView view,
        int[] outputToStored,
        (int Ordinal, ColumnValue Value)[] injectedDefaults)
    {
        ColumnValue?[]? cache = null;
        if (injectedDefaults.Length > 0)
        {
            cache = new ColumnValue?[layout.Count];
            for (int i = 0; i < injectedDefaults.Length; i++)
                cache[injectedDefaults[i].Ordinal] = injectedDefaults[i].Value;
        }
        return new QueryRow(rowId, layout, view, outputToStored, cache);
    }

    private QueryRow(RowLayout layout, JoinPairMap map, IReadOnlyDictionary<string, ColumnValue> left, IReadOnlyDictionary<string, ColumnValue> right)
    {
        RowId = default;
        Layout = layout;
        _pairMap = map;
        _pairLeft = left;
        _pairRight = right;
        _slots = null;
        _eager = null;
    }

    /// <summary>
    /// Pair backing — a joined row over its left and right input rows. No cell is copied at
    /// construction: <see cref="GetColumnValue"/> reads a cell from the input that holds it, through
    /// <paramref name="map"/>, and caches it. A join evaluates its <c>ON</c> on this row, so a rejected
    /// pair costs one small object instead of a value array and a read of every input cell, and an
    /// accepted pair flows on as the same row, so a later projection reads only the cells it needs.
    ///
    /// <para>The inputs must not change while the row lives; join inputs are owned rows (or rows the
    /// join retains in a hash table or spool), so they do not. The shape is checked here by count,
    /// and by key when a cell is read (<see cref="JoinPairMap"/>): a row whose keys diverge from the
    /// layout throws instead of returning a wrong cell.</para>
    /// </summary>
    internal static QueryRow FromPair(
        RowLayout layout,
        JoinPairMap map,
        IReadOnlyDictionary<string, ColumnValue> left,
        IReadOnlyDictionary<string, ColumnValue> right)
    {
        map.EnsureShape(layout, left, right);
        return new QueryRow(layout, map, left, right);
    }

    /// <summary>
    /// True when this row is slot-backed (lazy per-cell materialization); false when eager
    /// (<see cref="ColumnValue"/><c>[]</c>). Reflects the decode path chosen for the producing scan —
    /// see <see cref="CamusDB.Core.Storage.Kv.RowEncoder.RowDecodeState.SlotBackedDecode"/>. Exposed for
    /// tests and diagnostics; consumers read values through <see cref="GetColumnValue"/> regardless.
    /// </summary>
    internal bool IsSlotBacked => _slots is not null;

    /// <summary>
    /// True when this row is borrowed-backed (a <see cref="RowView"/> over raw KV bytes, lazy per-cell,
    /// no <c>ValueSlot[]</c>) and has not yet been promoted to the eager backing. Exposed for tests and
    /// diagnostics; consumers read values through <see cref="GetColumnValue"/> regardless of backing.
    /// </summary>
    internal bool IsBorrowedBacked => _rowView is not null && _eager is null;

    /// <summary>
    /// Estimated heap bytes this row keeps alive, for the byte cap of a retaining operator
    /// (<see cref="RowMemoryEstimator"/>). Reads only what the backing already holds, so it decodes and
    /// materializes nothing:
    /// <list type="bullet">
    /// <item>eager — the value array and each value with its payload;</item>
    /// <item>slot — the slot array and each slot's string, bytes or element payload;</item>
    /// <item>borrowed — the whole KV buffer the view references, which stays alive with the row;</item>
    /// <item>pair — both input rows, charged in full although other pair rows can share them.</item>
    /// </list>
    /// The shared <see cref="Layout"/> and a borrowed row's output-to-stored map are charged to no row.
    /// Cells materialized from the backing (the per-cell cache, or the array of a promoted row) add one
    /// value object each. A cell decoded from a borrowed view also adds its payload, because the
    /// decode copies the bytes into a new string or array; a slot or pair cell shares its payload with
    /// the backing, which is already charged.
    /// </summary>
    internal long EstimateRetainedBytes()
    {
        long total = RowMemoryEstimator.QueryRowBytes;
        bool hasBacking = _slots is not null || _rowView is not null || _pairMap is not null;
        bool copiesPayload = _rowView is not null;

        if (_eager is not null)
        {
            total += RowMemoryEstimator.ArrayHeaderBytes + (long)RowMemoryEstimator.ReferenceBytes * _eager.Length;

            for (int i = 0; i < _eager.Length; i++)
                total += !hasBacking || copiesPayload
                    ? RowMemoryEstimator.EstimateValue(_eager[i])
                    : RowMemoryEstimator.ColumnValueBytes;

            // A promoted row still holds its original backing, charged below.
            if (!hasBacking)
                return total;
        }

        if (_cellCache is not null)
        {
            total += RowMemoryEstimator.ArrayHeaderBytes + (long)RowMemoryEstimator.ReferenceBytes * _cellCache.Length;

            for (int i = 0; i < _cellCache.Length; i++)
            {
                if (_cellCache[i] is { } cell)
                    total += copiesPayload ? RowMemoryEstimator.EstimateValue(cell) : RowMemoryEstimator.ColumnValueBytes;
            }
        }

        if (_slots is not null)
        {
            total += RowMemoryEstimator.ArrayHeaderBytes + (long)RowMemoryEstimator.ValueSlotBytes * _slots.Length;

            for (int i = 0; i < _slots.Length; i++)
                total += RowMemoryEstimator.SlotPayloadBytes(_slots[i]);
        }
        else if (_rowView is { } view)
        {
            total += RowMemoryEstimator.BackingBufferBytes(view.Storage);
        }
        else if (_pairMap is not null)
        {
            total += RowMemoryEstimator.EstimateRow(_pairLeft!) + RowMemoryEstimator.EstimateRow(_pairRight!);
        }

        return total;
    }

    /// <summary>
    /// The cell at <paramref name="ordinal"/> as a <see cref="ColumnValue"/>. For a slot-backed row this
    /// materializes (and caches) just that cell — the per-cell path hot consumers should use so unread
    /// cells stay unmaterialized.
    /// </summary>
    public ColumnValue GetColumnValue(int ordinal)
    {
        if (_eager is not null)
            return _eager[ordinal];

        // A pair row caches its cells too, although its inputs hold them: a cell read through a pair
        // walks the chain of pairs beneath it, and a sort or a filter reads the same cell many times.
        // Measured on the Join Order Benchmark, a pair row without the cache allocated 2 to 5 percent
        // less and ran one query at half the speed.
        _cellCache ??= new ColumnValue?[Layout.Count];
        if (_cellCache[ordinal] is { } cached)
            return cached;

        if (_rowView is { } view)
        {
            int stored = _outputToStored![ordinal];
            // Injected (added-after-write) columns have stored == -1 and were pre-placed in the cache, so
            // reaching here with stored < 0 means a genuinely absent value → NULL.
            return _cellCache[ordinal] = stored < 0 ? ColumnValue.Null : view.GetSlot(stored).ToColumnValue();
        }

        if (_pairMap is { } pairMap)
            return _cellCache[ordinal] = pairMap.Resolve(ordinal, _pairLeft!, _pairRight!);

        return _cellCache[ordinal] = _slots![ordinal].ToColumnValue();
    }

    /// <summary>
    /// Exposes a borrowed cell's raw UTF-8 bytes without materializing a managed <see cref="string"/> —
    /// the seam operators use for byte-native string comparison. Succeeds only for a borrowed-backed row
    /// whose cell at <paramref name="ordinal"/> is a non-NULL <see cref="ColumnType.String"/> that has not
    /// already been materialized into the cache; the returned span is valid only for the row's borrowed
    /// lifetime, so the caller must consume it immediately. Returns <see langword="false"/> in every other
    /// case (eager/slot backing, injected default, NULL, non-string column, or already-cached cell), and
    /// the caller falls back to <see cref="GetColumnValue"/>.
    /// </summary>
    internal bool TryGetUtf8(int ordinal, out ReadOnlySpan<byte> utf8)
    {
        utf8 = default;

        if (_rowView is not { } view || _eager is not null)
            return false;
        if (_cellCache is not null && _cellCache[ordinal] is not null)
            return false;

        int stored = _outputToStored![ordinal];
        if (stored < 0 || !view.Codec.IsStringColumn(stored) || view.IsNull(stored))
            return false;

        utf8 = view.GetVariableSlice(stored);
        return true;
    }

    /// <summary>
    /// Exposes the cell at <paramref name="ordinal"/> as a <see cref="ValueSlot"/> without materializing
    /// a <see cref="ColumnValue"/> — the seam response sinks use to serialize a cell they read exactly
    /// once, so the projected cell of a slot- or view-backed row never allocates on its way to the wire.
    /// Succeeds only when serving the slot is genuinely cheaper: the row must not be eager (already
    /// <see cref="ColumnValue"/>s) and the cell must not already be cached (reuse beats re-decoding).
    /// A view-backed injected-default cell (stored ordinal -1, absent from the cache) also returns
    /// <see langword="false"/>; the caller falls back to <see cref="GetColumnValue"/>, which reports it
    /// as NULL. Deliberately does <b>not</b> populate the per-cell cache: a sink reads each cell once,
    /// and caching would allocate the cache array for nothing.
    /// </summary>
    internal bool TryGetSlot(int ordinal, out ValueSlot slot)
    {
        if (_eager is null && (_cellCache is null || _cellCache[ordinal] is null))
        {
            if (_slots is not null)
            {
                slot = _slots[ordinal];
                return true;
            }

            if (_rowView is { } view)
            {
                int stored = _outputToStored![ordinal];
                if (stored >= 0)
                {
                    slot = view.GetSlot(stored);
                    return true;
                }
            }
        }

        slot = ValueSlot.Null;
        return false;
    }

    /// <summary>
    /// Orders this row's cell <paramref name="ordinal"/> against <paramref name="other"/>'s cell
    /// <paramref name="otherOrdinal"/> with <see cref="ColumnValue.CompareTo"/> semantics. When both
    /// rows are slot-backed the comparison runs directly on the <see cref="ValueSlot"/>s — no
    /// <see cref="ColumnValue"/> is materialized for a sort key — otherwise it falls back to per-cell
    /// materialization. Used by ORDER BY so sorting a decoded result set allocates nothing per key.
    /// </summary>
    internal int CompareCellTo(int ordinal, QueryRow other, int otherOrdinal)
    {
        if (_slots is not null && _eager is null && other._slots is not null && other._eager is null)
            return _slots[ordinal].CompareTo(other._slots[otherOrdinal]);

        // Both borrowed: compare on the decoded ValueSlots straight from the bytes, materializing no
        // ColumnValue for the sort key. Injected (stored < 0) cells fall through to the general path.
        if (_rowView is { } v1 && _eager is null && other._rowView is { } v2 && other._eager is null)
        {
            int s1 = _outputToStored![ordinal];
            int s2 = other._outputToStored![otherOrdinal];
            if (s1 >= 0 && s2 >= 0)
                return v1.GetSlot(s1).CompareTo(v2.GetSlot(s2));
        }

        return GetColumnValue(ordinal).CompareTo(other.GetColumnValue(otherOrdinal));
    }

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

            // Unified over the slot and borrowed backings: GetColumnValue reads each cell from whichever
            // backing this row has (and its cache), so the whole-row materialization is backing-agnostic.
            ColumnValue[] full = new ColumnValue[Layout.Count];
            for (int i = 0; i < full.Length; i++)
                full[i] = GetColumnValue(i);

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
    {
        if (ReferenceEquals(layout, Layout))
            return this;

        if (_slots is not null && _eager is null)
            return FromSlots(RowId, layout, _slots);

        // Requalification keeps the same columns/ordinals, so the borrowed view and output→stored map stay
        // valid; clone the cache so the two rows never share mutable per-cell state.
        if (_rowView is { } view && _eager is null)
            return new QueryRow(RowId, layout, view, _outputToStored!, (ColumnValue?[]?)_cellCache?.Clone());

        return new QueryRow(RowId, layout, Values);
    }

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
