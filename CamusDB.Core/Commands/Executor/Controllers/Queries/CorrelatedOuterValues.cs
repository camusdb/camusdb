/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Reads, from one outer row, the value of each outer name that a correlated subquery reads. The values
/// come in the order of <see cref="Names"/>, one value for each distinct name.
///
/// <para>The body form (<see cref="CorrelatedSubqueryBody"/>) passes these values to its run as hidden
/// parameters, and <see cref="CorrelatedResultCache{T}"/> uses the same values as its key. So the key holds
/// exactly what the body can read of the outer row: two rows with the same values give the body the same
/// input.</para>
///
/// <para><b>The reads.</b> A name resolves, by the outer resolver, to a key of a row that holds every outer
/// column under its bare and its qualified key, and that key comes from a fixed list of keys of the outer row
/// (<see cref="CorrelatedRowMerger.OuterRowKeysByMergedKey"/>).
/// <see cref="Create"/> computes that list for each name, so each row reads only the cells of the names, in
/// place of a merged row of every outer column under two keys. A name that no key of the row fills is an
/// unknown column, as it is for the merged row. For a <see cref="QueryRow"/> the keys resolve to one slot for
/// each name, one time for each <see cref="RowLayout"/>.</para>
///
/// <para>The object is shared by the executions of a cached statement, so the per-row path writes nothing
/// but the slot cache, which it replaces as a whole.</para>
/// </summary>
internal sealed class CorrelatedOuterValues
{
    private readonly NodeAst[] names;

    private readonly IReadOnlyList<BoundTableSource> outerSources;

    /// <summary>For each name, the keys of an outer row that keys its cells by the bare column name when one source is in scope.</summary>
    private readonly string[][] readsOfSourceRow;

    /// <summary>For each name, the keys of an outer row of a join, which keys each cell as <c>{alias}.{column}</c>.</summary>
    private readonly string[][] readsOfQualifiedRow;

    /// <summary>The slot of each name in the last row layout read, see <see cref="SlotsFor"/>.</summary>
    private LayoutSlots? lastSlots;

    /// <summary>One reference node for each distinct outer name, in the order of the values.</summary>
    public IReadOnlyList<NodeAst> Names => names;

    /// <summary>The outer table sources that the names resolve to.</summary>
    public IReadOnlyList<BoundTableSource> OuterTables => outerSources;

    private CorrelatedOuterValues(
        NodeAst[] names,
        IReadOnlyList<BoundTableSource> outerSources,
        string[][] readsOfSourceRow,
        string[][] readsOfQualifiedRow)
    {
        this.names = names;
        this.outerSources = outerSources;
        this.readsOfSourceRow = readsOfSourceRow;
        this.readsOfQualifiedRow = readsOfQualifiedRow;
    }

    /// <summary>
    /// Builds the reader for <paramref name="outerReferences"/>, one value for each distinct name (case does
    /// not matter, as for a column name). Each name is checked against the outer sources here.
    /// </summary>
    public static CorrelatedOuterValues Create(
        IReadOnlyList<NodeAst> outerReferences,
        IReadOnlyList<BoundTableSource> outerSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        QueryRowNameResolver outerRowNames)
    {
        HashSet<string> seen = new(StringComparer.OrdinalIgnoreCase);
        List<NodeAst> names = new(outerReferences.Count);
        List<string> mergedKeys = new(outerReferences.Count);

        foreach (NodeAst reference in outerReferences)
        {
            string name = reference.yytext!;

            if (!seen.Add(name))
                continue;

            // Throws for an unknown or ambiguous name.
            mergedKeys.Add(outerRowNames.ResolveRowLookupKey(name));
            names.Add(reference);
        }

        return new CorrelatedOuterValues(
            [.. names],
            outerSources,
            ReadsOf(mergedKeys, CorrelatedRowMerger.OuterRowKeysByMergedKey(outerSources, outerDerivedSources, rowKeysAreQualified: false)),
            ReadsOf(mergedKeys, CorrelatedRowMerger.OuterRowKeysByMergedKey(outerSources, outerDerivedSources, rowKeysAreQualified: true)));
    }

    private static string[][] ReadsOf(List<string> mergedKeys, Dictionary<string, List<string>> outerRowKeys)
    {
        string[][] reads = new string[mergedKeys.Count][];

        for (int i = 0; i < mergedKeys.Count; i++)
            reads[i] = outerRowKeys.TryGetValue(mergedKeys[i], out List<string>? keys) ? [.. keys] : [];

        return reads;
    }

    /// <summary>The value of each name in <paramref name="outerRow"/>, in the order of <see cref="Names"/>.</summary>
    /// <param name="rowKeysAreQualified">True for a row of a join, which keys each cell as
    /// <c>{alias}.{column}</c>, see <see cref="CorrelatedRowMerger.OuterRowKeysByMergedKey"/>.</param>
    public ColumnValue[] For(IReadOnlyDictionary<string, ColumnValue> outerRow, bool rowKeysAreQualified = false)
    {
        string[][] reads = rowKeysAreQualified ? readsOfQualifiedRow : readsOfSourceRow;
        ColumnValue[] values = new ColumnValue[names.Length];

        if (outerRow is QueryRow queryRow)
        {
            int[] slots = SlotsFor(queryRow.Layout, reads);

            for (int i = 0; i < slots.Length; i++)
            {
                if (slots[i] < 0)
                    throw UnknownColumn(i);

                values[i] = queryRow.GetColumnValue(slots[i]);
            }

            return values;
        }

        for (int i = 0; i < reads.Length; i++)
            values[i] = Read(outerRow, reads[i]) ?? throw UnknownColumn(i);

        return values;
    }

    private static ColumnValue? Read(IReadOnlyDictionary<string, ColumnValue> outerRow, string[] keys)
    {
        foreach (string key in keys)
        {
            if (outerRow.TryGetValue(key, out ColumnValue? value) && value is not null)
                return value;
        }

        return null;
    }

    /// <summary>
    /// The slot of each name in a row of <paramref name="layout"/>, or -1 when no key of the name is in it. The
    /// last result is kept, because the rows of one operator share one layout. Concurrent executions of a
    /// cached statement can replace it; each one reads a complete entry, so a race costs a new resolution only.
    /// </summary>
    private int[] SlotsFor(RowLayout layout, string[][] reads)
    {
        LayoutSlots? cached = Volatile.Read(ref lastSlots);

        if (cached is not null && ReferenceEquals(cached.Layout, layout) && ReferenceEquals(cached.Reads, reads))
            return cached.Slots;

        int[] slots = new int[reads.Length];

        for (int i = 0; i < reads.Length; i++)
        {
            slots[i] = -1;

            foreach (string key in reads[i])
            {
                int slot = layout.IndexOf(key);

                if (slot >= 0)
                {
                    slots[i] = slot;
                    break;
                }
            }
        }

        Volatile.Write(ref lastSlots, new LayoutSlots(layout, reads, slots));
        return slots;
    }

    private CamusDBException UnknownColumn(int name) =>
        new(CamusDBErrorCodes.UnknownColumn, "Unknown column: " + names[name].yytext!);

    /// <summary>The slots of the names in one row layout, for one form of row keys.</summary>
    private sealed record LayoutSlots(RowLayout Layout, string[][] Reads, int[] Slots);
}
