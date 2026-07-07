
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Merges scanned/joined rows using qualified keys per <see cref="BoundRow"/> rules.
/// </summary>
internal static class QueryRowMerger
{
    public static Dictionary<string, ColumnValue> QualifyRow(
        IReadOnlyDictionary<string, ColumnValue> row,
        string alias)
    {
        Dictionary<string, ColumnValue> qualified = new(row.Count);

        foreach (KeyValuePair<string, ColumnValue> entry in row)
        {
            string key = IsQualifiedKey(entry.Key)
                ? entry.Key
                : QueryRowNameResolver.FormatQualifiedKey(alias, entry.Key);

            qualified[key] = entry.Value;
        }

        return qualified;
    }

    public static Dictionary<string, ColumnValue> MergeRows(
        IReadOnlyDictionary<string, ColumnValue> leftRow,
        IReadOnlyDictionary<string, ColumnValue> rightRow,
        string rightAlias)
    {
        Dictionary<string, ColumnValue> merged = new(leftRow);

        foreach (KeyValuePair<string, ColumnValue> entry in rightRow)
        {
            string key = IsQualifiedKey(entry.Key)
                ? entry.Key
                : QueryRowNameResolver.FormatQualifiedKey(rightAlias, entry.Key);

            if (merged.ContainsKey(key))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Column collision on merged join row key '{key}'");
            }

            merged[key] = entry.Value;
        }

        return merged;
    }

    internal static bool IsQualifiedKey(string key) => key.Contains('.');

    /// <summary>
    /// Builds the <see cref="RowLayout"/> for a merged join output row, replacing the per-row
    /// <see cref="Dictionary{TKey,TValue}"/> that <see cref="MergeRows"/> allocates.
    /// <para>
    /// Physical slot names are the already-qualified left keys followed by the right keys
    /// qualified to <c>{rightAlias}.{column}</c> — the same key set <see cref="MergeRows"/>
    /// would produce. Bare column aliases (e.g. <c>"id"</c> → ordinal of <c>"u.id"</c>) are
    /// added for every bare name that appears in exactly one join source, so that expression
    /// evaluation can resolve bare references in the ON predicate without a qualified prefix.
    /// </para>
    /// <para>
    /// Call this once per join node (lazily from the first row pair) and reuse for all subsequent
    /// rows — the schema is fixed for the lifetime of the plan.
    /// </para>
    /// </summary>
    /// <exception cref="CamusDBException">
    /// Thrown when a right-side key collides with an existing left-side key after qualification,
    /// matching the behaviour of <see cref="MergeRows"/>.
    /// </exception>
    public static RowLayout BuildJoinLayout(
        IReadOnlyDictionary<string, ColumnValue> leftQualified,
        IReadOnlyDictionary<string, ColumnValue> rightRow,
        string rightAlias)
    {
        List<string> physicalNames = new(leftQualified.Count + rightRow.Count);

        // Left keys are already qualified (QualifyRow was applied); add them as-is.
        foreach (string key in leftQualified.Keys)
            physicalNames.Add(key);

        // Right keys: qualify bare names, leave already-qualified names unchanged.
        HashSet<string> leftKeySet = new(leftQualified.Keys, StringComparer.Ordinal);
        foreach (string key in rightRow.Keys)
        {
            string qualKey = IsQualifiedKey(key) ? key : QueryRowNameResolver.FormatQualifiedKey(rightAlias, key);
            if (leftKeySet.Contains(qualKey))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Column collision on merged join row key '{qualKey}'");
            physicalNames.Add(qualKey);
        }

        // Bare aliases: for each bare name that appears in exactly one physical slot (after
        // stripping the qualifier), add a bare→ordinal alias so ordinal lookup works for
        // unqualified column references in ON predicates and projections.
        Dictionary<string, int> bareCounts = new(physicalNames.Count, StringComparer.Ordinal);
        for (int i = 0; i < physicalNames.Count; i++)
        {
            string name = physicalNames[i];
            string bare = IsQualifiedKey(name) ? name[(name.IndexOf('.') + 1)..] : name;
            bareCounts[bare] = bareCounts.GetValueOrDefault(bare, 0) + 1;
        }

        List<KeyValuePair<string, int>> aliases = [];
        for (int i = 0; i < physicalNames.Count; i++)
        {
            string name = physicalNames[i];
            if (!IsQualifiedKey(name))
                continue; // bare physical slots don't need an alias
            string bare = name[(name.IndexOf('.') + 1)..];
            if (bareCounts.GetValueOrDefault(bare, 0) == 1)
                aliases.Add(new KeyValuePair<string, int>(bare, i));
        }

        return new RowLayout(physicalNames, aliases);
    }

    /// <summary>
    /// Merges <paramref name="leftQualified"/> and <paramref name="rightRow"/> into a
    /// <see cref="QueryRow"/> using the pre-built <paramref name="joinLayout"/>.
    /// <para>
    /// Values from the left row (already qualified) are placed at the ordinals determined by
    /// <see cref="RowLayout.IndexOf"/> — O(1) frozen-dictionary lookup per column. Values from
    /// the right row are qualified to <c>{rightAlias}.{column}</c> before the same ordinal
    /// lookup. No per-row dictionary allocation is performed.
    /// </para>
    /// <para>
    /// The <paramref name="joinLayout"/> must have been built by
    /// <see cref="BuildJoinLayout"/> from the same schema — i.e. from a row pair whose key sets
    /// match the current pair. In the steady state every row pair shares the same schema (the
    /// plan is fixed), so building the layout once from the first pair and reusing it is correct.
    /// </para>
    /// <para>
    /// Fail-fast guard: if a later row pair has an extra key (unmapped by the layout) or is
    /// missing a key (leaving a slot unfilled), a <see cref="CamusDBException"/> is thrown rather
    /// than emitting a half-null row. This surfaces real bugs early — a silent null slot would
    /// produce deferred NREs or wrong results far from the join. When LEFT JOIN is added, that
    /// code should build a null-padded partial row explicitly instead of relying on this path,
    /// and bypass the slot-count check for the null side.
    /// </para>
    /// </summary>
    /// <exception cref="CamusDBException">
    /// Thrown when any key from the input rows is absent from <paramref name="joinLayout"/> (extra
    /// key), when two keys resolve to the same slot (collision), or when the total slots filled does
    /// not equal <see cref="RowLayout.Count"/> (missing key).
    /// </exception>
    public static QueryRow MergeRowsAsQueryRow(
        IReadOnlyDictionary<string, ColumnValue> leftQualified,
        IReadOnlyDictionary<string, ColumnValue> rightRow,
        string rightAlias,
        RowLayout joinLayout)
    {
        ColumnValue[] values = new ColumnValue[joinLayout.Count];
        int placements = 0;

        foreach (KeyValuePair<string, ColumnValue> entry in leftQualified)
        {
            int ord = joinLayout.IndexOf(entry.Key);
            if (ord < 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Join row shape diverged from join layout: unexpected left key '{entry.Key}'");
            // Write-once: two keys mapping to the same slot means the pair's shape (or a
            // late-appearing collision) diverges from the layout — counting writes alone would
            // let an overwrite mask a different unfilled slot.
            if (values[ord] is not null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Join row shape diverged from join layout: duplicate placement at '{entry.Key}'");
            values[ord] = entry.Value;
            placements++;
        }

        foreach (KeyValuePair<string, ColumnValue> entry in rightRow)
        {
            string key = IsQualifiedKey(entry.Key)
                ? entry.Key
                : QueryRowNameResolver.FormatQualifiedKey(rightAlias, entry.Key);
            int ord = joinLayout.IndexOf(key);
            if (ord < 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Join row shape diverged from join layout: unexpected right key '{key}'");
            if (values[ord] is not null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Join row shape diverged from join layout: duplicate placement at '{key}'");
            values[ord] = entry.Value;
            placements++;
        }

        if (placements != joinLayout.Count)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Join row shape diverged from join layout: expected {joinLayout.Count} columns, placed {placements}");

        return new QueryRow(default(ObjectIdValue), joinLayout, values);
    }
}