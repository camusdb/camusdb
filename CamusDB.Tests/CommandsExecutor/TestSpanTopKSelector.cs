/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The per-span selection of a span-local top k (<see cref="SpanTopKSelector"/>): the first k values
/// in either direction, every row that ties with the k-th value, NULL ranked as the smallest value,
/// and the edge sizes.
/// </summary>
public sealed class TestSpanTopKSelector
{
    private static QueryResultRow Row(long group, long? value) => new(default, new Dictionary<string, ColumnValue>
    {
        { "g", new ColumnValue(ColumnType.Integer64, group) },
        { "m", value is { } v ? new ColumnValue(ColumnType.Integer64, v) : ColumnValue.Null },
    });

    private static List<long> Groups(List<QueryResultRow> rows) => rows.Select(r => r.Row["g"].LongValue).OrderBy(g => g).ToList();

    [Test]
    public void Empty_ReturnsEmpty()
    {
        Assert.IsEmpty(SpanTopKSelector.Select([], "m", descending: true, k: 3));
    }

    [Test]
    public void KZero_ReturnsEmpty()
    {
        Assert.IsEmpty(SpanTopKSelector.Select([Row(1, 5)], "m", descending: true, k: 0));
    }

    [Test]
    public void KAtOrPastTheRowCount_ReturnsEveryRow()
    {
        List<QueryResultRow> rows = [Row(1, 5), Row(2, 3)];

        Assert.AreEqual(2, SpanTopKSelector.Select(rows, "m", descending: true, k: 2).Count);
        Assert.AreEqual(2, SpanTopKSelector.Select(rows, "m", descending: true, k: 9).Count);
    }

    [Test]
    public void Descending_KeepsTheLargestValues()
    {
        List<QueryResultRow> rows = [Row(1, 5), Row(2, 9), Row(3, 1), Row(4, 7), Row(5, 3)];

        CollectionAssert.AreEqual(new[] { 2L, 4L }, Groups(SpanTopKSelector.Select(rows, "m", descending: true, k: 2)));
    }

    [Test]
    public void Ascending_KeepsTheSmallestValues()
    {
        List<QueryResultRow> rows = [Row(1, 5), Row(2, 9), Row(3, 1), Row(4, 7), Row(5, 3)];

        CollectionAssert.AreEqual(new[] { 3L, 5L }, Groups(SpanTopKSelector.Select(rows, "m", descending: false, k: 2)));
    }

    [Test]
    public void TiesWithTheKthValue_AreAllKept()
    {
        List<QueryResultRow> rows = [Row(1, 9), Row(2, 7), Row(3, 7), Row(4, 7), Row(5, 3)];

        CollectionAssert.AreEqual(new[] { 1L, 2L, 3L, 4L }, Groups(SpanTopKSelector.Select(rows, "m", descending: true, k: 2)));
    }

    [Test]
    public void Null_IsTheSmallestValue()
    {
        List<QueryResultRow> rows = [Row(1, null), Row(2, 4), Row(3, null), Row(4, 8)];

        CollectionAssert.AreEqual(new[] { 2L, 4L }, Groups(SpanTopKSelector.Select(rows, "m", descending: true, k: 2)));
        CollectionAssert.AreEqual(new[] { 1L, 3L }, Groups(SpanTopKSelector.Select(rows, "m", descending: false, k: 1)),
            "Ascending, NULL comes first, and the two NULL rows tie");
    }

    [Test]
    public void MissingValue_CountsAsNull()
    {
        QueryResultRow noValue = new(default, new Dictionary<string, ColumnValue> { { "g", new ColumnValue(ColumnType.Integer64, 1) } });
        List<QueryResultRow> rows = [noValue, Row(2, 4), Row(3, 6)];

        CollectionAssert.AreEqual(new[] { 2L, 3L }, Groups(SpanTopKSelector.Select(rows, "m", descending: true, k: 2)));
    }
}
