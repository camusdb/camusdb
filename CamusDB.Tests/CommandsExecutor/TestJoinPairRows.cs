/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A join builds each candidate pair as a pair-backed row (<see cref="JoinRowPairer"/>): it holds
/// its two input rows and reads a cell only when something asks for it. The values must be the
/// ones the eager merge produced, a cell read must touch only the input that holds it, and a later
/// pair whose shape diverges from the layout must throw, as the eager merge did.
/// </summary>
[TestFixture]
public sealed class TestJoinPairRows
{
    /// <summary>A right row that counts every cell read, to prove which input a read touches.</summary>
    private sealed class CountingRow : IReadOnlyDictionary<string, ColumnValue>
    {
        private readonly Dictionary<string, ColumnValue> inner;

        public CountingRow(Dictionary<string, ColumnValue> inner) => this.inner = inner;

        public int Reads { get; private set; }

        public ColumnValue this[string key] { get { Reads++; return inner[key]; } }

        public IEnumerable<string> Keys => inner.Keys;

        public IEnumerable<ColumnValue> Values { get { Reads += inner.Count; return inner.Values; } }

        public int Count => inner.Count;

        public bool ContainsKey(string key) => inner.ContainsKey(key);

        public bool TryGetValue(string key, out ColumnValue value)
        {
            Reads++;
            return inner.TryGetValue(key, out value!);
        }

        public IEnumerator<KeyValuePair<string, ColumnValue>> GetEnumerator() { Reads += inner.Count; return inner.GetEnumerator(); }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private static Dictionary<string, ColumnValue> Left(string id, string email) => new()
    {
        { "u.id",    new(ColumnType.Id, id) },
        { "u.email", new(ColumnType.String, email) },
    };

    private static Dictionary<string, ColumnValue> Right(string title, string userId) => new()
    {
        { "title",   new(ColumnType.String, title) },
        { "user_id", new(ColumnType.Id, userId) },
    };

    private static (RowLayout Layout, Dictionary<string, int> RightMap) LayoutOf(
        IReadOnlyDictionary<string, ColumnValue> left, IReadOnlyDictionary<string, ColumnValue> right)
    {
        RowLayout layout = QueryRowMerger.BuildJoinLayout(left, right, "p");
        return (layout, QueryRowMerger.BuildRightKeyOrdinalMap(right, "p", layout));
    }

    [Test]
    public void PairRow_HasTheValuesOfTheEagerMerge()
    {
        Dictionary<string, ColumnValue> left = Left("abc", "a@example.com");
        Dictionary<string, ColumnValue> right = Right("Post A", "abc");
        (RowLayout layout, Dictionary<string, int> rightMap) = LayoutOf(left, right);

        QueryRow eager = QueryRowMerger.MergeRowsAsQueryRow(left, right, "p", layout);
        QueryRow pair = new JoinRowPairer().Pair(left, right, layout, rightMap);

        Assert.AreEqual(eager.Values.Select(v => v.ToString()), pair.Values.Select(v => v.ToString()));
        Assert.AreEqual("Post A", pair["p.title"].StrValue);
        Assert.AreEqual("abc", pair["u.id"].StrValue);
        Assert.AreEqual(eager.Keys, pair.Keys);
    }

    [Test]
    public void PairRow_OverQueryRowInputs_ReadsByOrdinal()
    {
        // Both inputs as QueryRows over shared layouts, as a join scan produces them.
        Dictionary<string, ColumnValue> leftDict = Left("abc", "a@example.com");
        Dictionary<string, ColumnValue> rightDict = Right("Post A", "abc");
        RowLayout leftLayout = new(leftDict.Keys.ToList());
        RowLayout rightLayout = new(rightDict.Keys.ToList());
        QueryRow left = new(default, leftLayout, leftDict.Values.ToArray());
        QueryRow right = new(default, rightLayout, rightDict.Values.ToArray());
        (RowLayout layout, Dictionary<string, int> rightMap) = LayoutOf(left, right);

        JoinRowPairer pairer = new();
        QueryRow first = pairer.Pair(left, right, layout, rightMap);

        // A second pair over the same layouts reads its own inputs.
        QueryRow left2 = new(default, leftLayout, Left("xyz", "x@example.com").Values.ToArray());
        QueryRow right2 = new(default, rightLayout, Right("Post B", "xyz").Values.ToArray());
        QueryRow second = pairer.Pair(left2, right2, layout, rightMap);

        Assert.AreEqual("a@example.com", first["u.email"].StrValue);
        Assert.AreEqual("Post A", first["p.title"].StrValue);
        Assert.AreEqual("x@example.com", second["u.email"].StrValue);
        Assert.AreEqual("Post B", second["p.title"].StrValue);
    }

    [Test]
    public void ReadingALeftCell_DoesNotReadTheRightRow()
    {
        Dictionary<string, ColumnValue> left = Left("abc", "a@example.com");
        CountingRow right = new(Right("Post A", "abc"));
        (RowLayout layout, Dictionary<string, int> rightMap) = LayoutOf(left, right);
        int readsForLayout = right.Reads;

        QueryRow pair = new JoinRowPairer().Pair(left, right, layout, rightMap);

        Assert.AreEqual("abc", pair["u.id"].StrValue);
        Assert.AreEqual(readsForLayout, right.Reads, "a left cell must not read the right row");

        Assert.AreEqual("Post A", pair["p.title"].StrValue);
        Assert.AreEqual(readsForLayout + 1, right.Reads, "a right cell reads that one cell");

        Assert.AreEqual("Post A", pair["p.title"].StrValue);
        Assert.AreEqual(readsForLayout + 1, right.Reads, "a cell read twice is read from the input once");
    }

    [Test]
    public void LaterPairWithAnExtraKey_Throws()
    {
        Dictionary<string, ColumnValue> left = Left("abc", "a@example.com");
        Dictionary<string, ColumnValue> right = Right("Post A", "abc");
        (RowLayout layout, Dictionary<string, int> rightMap) = LayoutOf(left, right);

        JoinRowPairer pairer = new();
        pairer.Pair(left, right, layout, rightMap);

        Dictionary<string, ColumnValue> wider = Right("Post B", "xyz");
        wider["author"] = new(ColumnType.String, "Alice");

        CamusDBException? ex = Assert.Throws<CamusDBException>(() => pairer.Pair(Left("xyz", "x@example.com"), wider, layout, rightMap));
        StringAssert.Contains("diverged", ex!.Message);
    }

    [Test]
    public void LaterPairWithADifferentKey_ThrowsWhenTheCellIsRead()
    {
        Dictionary<string, ColumnValue> left = Left("abc", "a@example.com");
        Dictionary<string, ColumnValue> right = Right("Post A", "abc");
        (RowLayout layout, Dictionary<string, int> rightMap) = LayoutOf(left, right);

        JoinRowPairer pairer = new();
        pairer.Pair(left, right, layout, rightMap);

        // Same key count, but "user_id" is replaced by "author".
        Dictionary<string, ColumnValue> renamed = new()
        {
            { "title",  new(ColumnType.String, "Post B") },
            { "author", new(ColumnType.String, "Alice") },
        };

        QueryRow pair = pairer.Pair(Left("xyz", "x@example.com"), renamed, layout, rightMap);

        Assert.AreEqual("Post B", pair["p.title"].StrValue);
        CamusDBException? ex = Assert.Throws<CamusDBException>(() => _ = pair["p.user_id"]);
        StringAssert.Contains("diverged", ex!.Message);
    }

    [Test]
    public void FirstPairThatDoesNotCoverTheLayout_Throws()
    {
        Dictionary<string, ColumnValue> left = Left("abc", "a@example.com");
        Dictionary<string, ColumnValue> right = Right("Post A", "abc");
        (RowLayout layout, Dictionary<string, int> rightMap) = LayoutOf(left, right);

        Dictionary<string, ColumnValue> narrow = new() { { "title", new(ColumnType.String, "Post A") } };

        CamusDBException? ex = Assert.Throws<CamusDBException>(() => new JoinRowPairer().Pair(left, narrow, layout, rightMap));
        StringAssert.Contains("diverged", ex!.Message);
    }
}
