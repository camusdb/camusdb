
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
        Dictionary<string, ColumnValue> qualified = new(row.Count, StringComparer.OrdinalIgnoreCase);

        foreach (KeyValuePair<string, ColumnValue> entry in row)
        {
            string key = IsQualifiedKey(entry.Key)
                ? entry.Key
                : QueryRowNameResolver.FormatQualifiedKey(alias, entry.Key);

            qualified[key] = entry.Value;
        }

        return qualified;
    }

    /// <summary>
    /// Builds a <see cref="RowLayout"/> whose physical names are those of
    /// <paramref name="sourceLayout"/> prefixed with <paramref name="alias"/>
    /// (e.g. <c>"col"</c> → <c>"alias.col"</c>). Names that are already qualified
    /// (contain a dot) are kept unchanged.
    /// <para>
    /// Ordinals in the returned layout correspond to the same positions as in
    /// <paramref name="sourceLayout"/>, so a <see cref="QueryRow"/> can be re-wrapped
    /// with the new layout without copying its <c>Values</c> array — see
    /// <see cref="QualifyRowAsQueryRow"/>.
    /// </para>
    /// </summary>
    public static RowLayout BuildQualifiedLayout(RowLayout sourceLayout, string alias)
    {
        string[] names = sourceLayout.OutputNames;
        List<string> qualifiedNames = new(names.Length);
        foreach (string name in names)
            qualifiedNames.Add(IsQualifiedKey(name) ? name : QueryRowNameResolver.FormatQualifiedKey(alias, name));
        return RowLayout.ForColumns(qualifiedNames);
    }

    /// <summary>
    /// Returns a <see cref="QueryRow"/> that shares <paramref name="source"/>'s
    /// <c>Values</c> array but exposes its columns through <paramref name="qualifiedLayout"/>,
    /// eliminating the per-row <see cref="Dictionary{TKey,TValue}"/> allocation of
    /// <see cref="QualifyRow"/>.
    /// <para>
    /// <paramref name="qualifiedLayout"/> must be built from <paramref name="source"/>'s
    /// own layout via <see cref="BuildQualifiedLayout"/> so that ordinal positions are
    /// identical; the returned row's <c>Values[i]</c> is the same object as the
    /// source's <c>Values[i]</c> — no copy is performed.
    /// </para>
    /// <para>
    /// Build the layout once per join node (lazily, from the first left row encountered)
    /// and reuse it for every subsequent row — the schema is fixed for the lifetime of
    /// the plan.
    /// </para>
    /// </summary>
    public static QueryRow QualifyRowAsQueryRow(QueryRow source, RowLayout qualifiedLayout) =>
        // WithLayout preserves the slot backing (no whole-row materialization) when the source is a
        // freshly decoded scan row; requalifying only changes column names, not values.
        source.WithLayout(qualifiedLayout);

    /// <summary>
    /// Precomputes a mapping from each key in <paramref name="rightRow"/> — exactly as
    /// the key appears in the row, whether bare or already qualified — to its ordinal in
    /// <paramref name="joinLayout"/>.
    /// <para>
    /// Build this once per join node (lazily from the first row pair) and pass to
    /// <see cref="MergeRowsAsQueryRow(IReadOnlyDictionary{string,ColumnValue},IReadOnlyDictionary{string,ColumnValue},RowLayout,Dictionary{string,int})"/>
    /// to avoid per-row <see cref="QueryRowNameResolver.FormatQualifiedKey"/> and
    /// <see cref="RowLayout.IndexOf"/> calls for right-side columns.
    /// </para>
    /// <para>
    /// The key set of right rows is fixed for the lifetime of the plan (same table schema),
    /// so a single precomputed map is correct for all rows from the same source.
    /// </para>
    /// </summary>
    public static Dictionary<string, int> BuildRightKeyOrdinalMap(
        IReadOnlyDictionary<string, ColumnValue> rightRow,
        string rightAlias,
        RowLayout joinLayout)
    {
        Dictionary<string, int> map = new(rightRow.Count, StringComparer.OrdinalIgnoreCase);
        foreach (string key in rightRow.Keys)
        {
            string qualKey = IsQualifiedKey(key) ? key : QueryRowNameResolver.FormatQualifiedKey(rightAlias, key);
            map[key] = joinLayout.IndexOf(qualKey);
        }
        return map;
    }

    public static Dictionary<string, ColumnValue> MergeRows(
        IReadOnlyDictionary<string, ColumnValue> leftRow,
        IReadOnlyDictionary<string, ColumnValue> rightRow,
        string rightAlias)
    {
        Dictionary<string, ColumnValue> merged = new(leftRow, StringComparer.OrdinalIgnoreCase);

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
        HashSet<string> leftKeySet = new(leftQualified.Keys, StringComparer.OrdinalIgnoreCase);
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
        Dictionary<string, int> bareCounts = new(physicalNames.Count, StringComparer.OrdinalIgnoreCase);
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

    /// <summary>
    /// Fast-path variant of
    /// <see cref="MergeRowsAsQueryRow(IReadOnlyDictionary{string,ColumnValue},IReadOnlyDictionary{string,ColumnValue},string,RowLayout)"/>
    /// that uses a precomputed right-key → layout-ordinal map (from
    /// <see cref="BuildRightKeyOrdinalMap"/>) in place of per-row
    /// <see cref="QueryRowNameResolver.FormatQualifiedKey"/> and
    /// <see cref="RowLayout.IndexOf"/> calls for right-side columns.
    /// <para>
    /// Use at sites where <paramref name="rightKeyOrdinalMap"/> was built once from the
    /// first row pair and the right row's key set is stable across iterations (same table
    /// schema — true for any fixed query plan).
    /// </para>
    /// </summary>
    public static QueryRow MergeRowsAsQueryRow(
        IReadOnlyDictionary<string, ColumnValue> leftQualified,
        IReadOnlyDictionary<string, ColumnValue> rightRow,
        RowLayout joinLayout,
        Dictionary<string, int> rightKeyOrdinalMap)
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
            if (values[ord] is not null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Join row shape diverged from join layout: duplicate placement at '{entry.Key}'");
            values[ord] = entry.Value;
            placements++;
        }

        foreach (KeyValuePair<string, ColumnValue> entry in rightRow)
        {
            if (!rightKeyOrdinalMap.TryGetValue(entry.Key, out int ord) || ord < 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Join row shape diverged from join layout: unexpected right key '{entry.Key}'");
            if (values[ord] is not null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Join row shape diverged from join layout: duplicate placement at '{entry.Key}'");
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