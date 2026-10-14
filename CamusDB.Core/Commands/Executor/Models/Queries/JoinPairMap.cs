/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// For each column of a join's output layout, which input row holds the cell and where: the left
/// (qualified) row or the right row, by ordinal when the input is a <see cref="QueryRow"/> of the
/// layout the map was built from, else by key. Built once per join execution from the first pair,
/// and read by every pair-backed row (<see cref="QueryRow.FromPair"/>) the join produces.
///
/// <para><b>The shape guard.</b> The eager merge checked every pair: an input key missing from the
/// layout, a key placed twice, or a slot left empty threw, so a join bug surfaced at once instead
/// of as a wrong cell far away. A pair row keeps that guarantee in two steps: the build checks the
/// first pair's keys exactly, every later pair is checked by key count
/// (<see cref="EnsureShape"/>), and a cell read by key throws when the input lacks the key.</para>
///
/// <para>Immutable after <see cref="Build"/>; one instance is shared by every row of one join
/// execution.</para>
/// </summary>
internal sealed class JoinPairMap
{
    private readonly RowLayout layout;
    private readonly bool[] fromRight;
    private readonly int[] sourceOrdinal;
    private readonly string[] sourceKey;
    private readonly RowLayout? leftLayout;
    private readonly RowLayout? rightLayout;
    private readonly int leftCount;
    private readonly int rightCount;

    private JoinPairMap(
        RowLayout layout,
        bool[] fromRight,
        int[] sourceOrdinal,
        string[] sourceKey,
        RowLayout? leftLayout,
        RowLayout? rightLayout,
        int leftCount,
        int rightCount)
    {
        this.layout = layout;
        this.fromRight = fromRight;
        this.sourceOrdinal = sourceOrdinal;
        this.sourceKey = sourceKey;
        this.leftLayout = leftLayout;
        this.rightLayout = rightLayout;
        this.leftCount = leftCount;
        this.rightCount = rightCount;
    }

    /// <summary>The output layout this map was built for.</summary>
    public RowLayout Layout => layout;

    /// <summary>
    /// Maps every output column of <paramref name="joinLayout"/> to its input. A left key is already
    /// qualified, so it names its output column directly; a right key goes through
    /// <paramref name="rightKeyOrdinalMap"/> (<c>QueryRowMerger.BuildRightKeyOrdinalMap</c>). Throws
    /// when the pair's keys do not cover the layout exactly once.
    /// </summary>
    public static JoinPairMap Build(
        RowLayout joinLayout,
        IReadOnlyDictionary<string, ColumnValue> left,
        IReadOnlyDictionary<string, ColumnValue> right,
        Dictionary<string, int> rightKeyOrdinalMap)
    {
        int width = joinLayout.Count;
        bool[] fromRight = new bool[width];
        int[] sourceOrdinal = new int[width];
        string[] sourceKey = new string[width];
        bool[] placed = new bool[width];
        int placements = 0;

        int leftIndex = 0;
        foreach (string key in left.Keys)
        {
            Place(joinLayout.IndexOf(key), key, false, leftIndex++, "left");
        }

        int rightIndex = 0;
        foreach (string key in right.Keys)
        {
            int ordinal = rightKeyOrdinalMap.TryGetValue(key, out int mapped) ? mapped : -1;
            Place(ordinal, key, true, rightIndex++, "right");
        }

        if (placements != width)
            throw Diverged($"expected {width} columns, placed {placements}");

        return new JoinPairMap(
            joinLayout, fromRight, sourceOrdinal, sourceKey,
            (left as QueryRow)?.Layout, (right as QueryRow)?.Layout,
            leftIndex, rightIndex);

        void Place(int ordinal, string key, bool isRight, int index, string side)
        {
            if (ordinal < 0)
                throw Diverged($"unexpected {side} key '{key}'");

            if (placed[ordinal])
                throw Diverged($"duplicate placement at '{key}'");

            placed[ordinal] = true;
            fromRight[ordinal] = isRight;
            sourceOrdinal[ordinal] = index;
            sourceKey[ordinal] = key;
            placements++;
        }
    }

    /// <summary>
    /// Checks a later pair against the shape the map was built from: the same layout, and the same
    /// number of keys on each side. A key that differs is caught when its cell is read.
    /// </summary>
    public void EnsureShape(RowLayout joinLayout, IReadOnlyDictionary<string, ColumnValue> left, IReadOnlyDictionary<string, ColumnValue> right)
    {
        if (!ReferenceEquals(joinLayout, layout))
            throw Diverged("the pair map was built for another layout");

        if (left.Count != leftCount || right.Count != rightCount)
            throw Diverged($"expected {leftCount}+{rightCount} columns, got {left.Count}+{right.Count}");
    }

    /// <summary>The cell at output <paramref name="ordinal"/>, read from the input that holds it.</summary>
    public ColumnValue Resolve(int ordinal, IReadOnlyDictionary<string, ColumnValue> left, IReadOnlyDictionary<string, ColumnValue> right)
    {
        bool isRight = fromRight[ordinal];
        IReadOnlyDictionary<string, ColumnValue> source = isRight ? right : left;
        RowLayout? sourceLayout = isRight ? rightLayout : leftLayout;

        if (sourceLayout is not null && source is QueryRow row && ReferenceEquals(row.Layout, sourceLayout))
            return row.GetColumnValue(sourceOrdinal[ordinal]);

        if (source.TryGetValue(sourceKey[ordinal], out ColumnValue? value))
            return value;

        throw Diverged($"{(isRight ? "right" : "left")} row has no key '{sourceKey[ordinal]}'");
    }

    private static CamusDBException Diverged(string detail) =>
        new(CamusDBErrorCodes.InvalidInternalOperation, "Join row shape diverged from join layout: " + detail);
}
