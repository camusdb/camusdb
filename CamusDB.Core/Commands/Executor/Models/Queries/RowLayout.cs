
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Frozen;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Immutable, plan-bound descriptor that maps column names to ordinal positions in a row's
/// <c>ColumnValue[]</c>. One <see cref="RowLayout"/> is built once per scan/join node and
/// shared across every row the node produces — ordinal resolution therefore happens once per
/// plan rather than once per row per column access (the per-row hashing cost of the old
/// <c>Dictionary&lt;string,ColumnValue&gt;</c>).
///
/// <para>
/// For single-table scans the names are the bare column names as they appear in the schema.
/// For join output the names include both the qualified <c>{alias}.{column}</c> form and,
/// for names that are unique across all join sources, an additional bare-name alias pointing
/// at the same ordinal — matching the naming contract documented in <see cref="BoundRow"/>.
/// </para>
///
/// <para>
/// The name→ordinal index is a <see cref="FrozenDictionary{TKey,TValue}"/> built once in the
/// constructor; subsequent <see cref="IndexOf"/> calls pay only a single dictionary lookup
/// with no per-lookup allocation.
/// </para>
/// </summary>
public sealed class RowLayout
{
    /// <summary>
    /// Physical column names, one per <c>Values[]</c> slot. Index <c>i</c> corresponds to
    /// <c>Values[i]</c> in a row. Alias names (see the aliasing constructor) are resolvable via
    /// <see cref="IndexOf"/> but do <b>not</b> appear here and do not add a slot.
    /// </summary>
    public string[] OutputNames { get; }

    private readonly FrozenDictionary<string, int> nameToOrdinal;

    /// <summary>
    /// Number of physical column slots (= <see cref="OutputNames"/>.Length = a row's
    /// <c>Values.Length</c>). Alias names are not counted.
    /// </summary>
    public int Count => OutputNames.Length;

    /// <summary>
    /// Constructs a layout from an ordered sequence of physical column names. Each name maps to
    /// its own <c>Values[]</c> slot. If the same name appears more than once, only the first
    /// occurrence is registered (<see cref="Dictionary{TKey,TValue}.TryAdd"/> semantics); the
    /// binder rejects genuinely ambiguous bare references before execution, so a well-formed
    /// single-source layout never contains real duplicates.
    /// </summary>
    public RowLayout(IEnumerable<string> names) : this(names, null)
    {
    }

    /// <summary>
    /// Constructs a layout with physical column names plus additional resolution-only
    /// <paramref name="aliases"/>. Each alias maps an extra name onto the ordinal of an existing
    /// physical slot — a bare <c>{column}</c> pointing at the same ordinal as its qualified
    /// <c>{alias}.{column}</c> form for join output — <b>without</b> adding a slot or duplicating
    /// the value. Aliases are reachable through <see cref="IndexOf"/> / a row's
    /// <c>TryGetValue</c>, but are intentionally excluded from <see cref="OutputNames"/>,
    /// <see cref="Count"/>, and row enumeration; the join/projection layer that owns the output
    /// key set decides what a serialized row exposes.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when an alias points at an ordinal outside the physical slot range.
    /// </exception>
    public RowLayout(IEnumerable<string> names, IEnumerable<KeyValuePair<string, int>>? aliases)
    {
        OutputNames = names.ToArray();

        var builder = new Dictionary<string, int>(OutputNames.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < OutputNames.Length; i++)
            builder.TryAdd(OutputNames[i], i);

        if (aliases is not null)
        {
            foreach (KeyValuePair<string, int> alias in aliases)
            {
                if (alias.Value < 0 || alias.Value >= OutputNames.Length)
                    throw new ArgumentOutOfRangeException(
                        nameof(aliases),
                        $"Alias '{alias.Key}' points at ordinal {alias.Value}, outside the physical slot range [0, {OutputNames.Length}).");

                builder.TryAdd(alias.Key, alias.Value);
            }
        }

        nameToOrdinal = builder.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Returns the ordinal (0-based index into a row's <c>Values[]</c>) for <paramref name="name"/>,
    /// or <c>-1</c> if the name is not present in this layout.
    /// </summary>
    public int IndexOf(string name) =>
        nameToOrdinal.TryGetValue(name, out int ordinal) ? ordinal : -1;

    /// <summary>Returns the physical column name at <paramref name="ordinal"/>.</summary>
    /// <exception cref="IndexOutOfRangeException">Thrown when <paramref name="ordinal"/> is out of range.</exception>
    public string NameAt(int ordinal) => OutputNames[ordinal];

    /// <summary>
    /// Factory for the single-table case: the layout's names are just the bare column names in
    /// the order they appear in the schema.
    /// </summary>
    public static RowLayout ForColumns(IEnumerable<string> names) => new(names);
}
