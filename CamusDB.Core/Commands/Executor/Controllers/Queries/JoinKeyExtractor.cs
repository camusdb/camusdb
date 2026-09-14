/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Reads the equi-join key values out of a row, and orders two extracted keys.
///
/// <para>Every join strategy keys its rows the same way, so the rules live in one place: a row
/// whose key column is absent or NULL is excluded (SQL inner-join NULL semantics), and two keys
/// order by column type first so a cross-type key never throws. Hash-shaped callers fill a
/// reusable scratch array; the merge join needs an owning copy it can hold across a key run.</para>
/// </summary>
internal static class JoinKeyExtractor
{
    /// <summary>
    /// Extracts the join-key values of a row into a caller-owned scratch array.
    /// Returns false when any key column is absent or NULL (the row is excluded — SQL inner-join
    /// NULL semantics). <paramref name="destination"/> must have exactly
    /// <paramref name="keyColumns"/>.Count slots and is overwritten on every call, so the caller
    /// must finish reading it (hash-table lookup, partition routing, span comparison) before the
    /// next extraction. Hash-table code paths pass the filled span to the alternate lookup of
    /// <see cref="CompositeColumnValueComparer"/>; an owned copy is made only on bucket insert.
    /// </summary>
    internal static bool TryExtractKeyInto(
        IReadOnlyDictionary<string, ColumnValue> row,
        IReadOnlyList<string> keyColumns,
        ColumnValue[] destination)
    {
        for (int i = 0; i < keyColumns.Count; i++)
        {
            if (!row.TryGetValue(keyColumns[i], out ColumnValue? v) || v.Type == ColumnType.Null)
                return false;

            destination[i] = v;
        }

        return true;
    }

    /// <summary>
    /// Extracts the merge-join key values from a row into a freshly allocated array.
    /// Returns null when any key column is absent or NULL (the row is excluded).
    /// The merge-join callers retain the returned array across iterations (current-key runs),
    /// so this owning form must not be replaced with the scratch-filling
    /// <see cref="TryExtractKeyInto"/> there.
    /// </summary>
    internal static ColumnValue[]? ExtractMergeKey(
        IReadOnlyDictionary<string, ColumnValue> row,
        IReadOnlyList<string> keyColumns)
    {
        ColumnValue[] key = new ColumnValue[keyColumns.Count];
        return TryExtractKeyInto(row, keyColumns, key) ? key : null;
    }

    /// <summary>
    /// Lexicographic comparison of two parallel key arrays using <see cref="ColumnValue.CompareTo"/>.
    /// Both arrays must have the same length (guaranteed by construction).
    ///
    /// Compares by <c>Type</c> ordinal first: <see cref="ColumnValue.CompareTo"/> throws on mismatched
    /// types, so a cross-type equi-join key (e.g. <c>a.intCol = b.strCol</c>) would otherwise crash the
    /// merge. Ordering by type first makes mismatched types sort deterministically and never
    /// compare-equal — so such a join simply yields no matches, matching the hash-join comparer's
    /// graceful "different type ⇒ not equal" behaviour. (NULL keys are already excluded upstream.)
    /// </summary>
    internal static int CompareMergeKeys(ColumnValue[] x, ColumnValue[] y)
    {
        for (int i = 0; i < x.Length; i++)
        {
            int typeCmp = ((int)x[i].Type).CompareTo((int)y[i].Type);
            if (typeCmp != 0) return typeCmp;

            int c = x[i].CompareTo(y[i]);
            if (c != 0) return c;
        }

        return 0;
    }
}
