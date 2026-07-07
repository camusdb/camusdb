
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

public class TestQuerySorter
{
    [Test]
    public async Task SortResultset_ThreeOrderColumns_sortsByAllKeys()
    {
        QueryTicket ticket = MakeTicket(
            new QueryOrderBy("enabled", OrderType.Ascending),
            new QueryOrderBy("year", OrderType.Descending),
            new QueryOrderBy("name", OrderType.Ascending));

        List<QueryResultRow> rows =
        [
            Row(("enabled", false), ("year", 2020L), ("name", "b")),
            Row(("enabled", false), ("year", 2021L), ("name", "a")),
            Row(("enabled", true), ("year", 2022L), ("name", "z")),
            Row(("enabled", false), ("year", 2021L), ("name", "c")),
        ];

        QuerySorter sorter = new();
        List<QueryResultRow> sorted = await sorter.SortResultset(ticket, ToAsync(rows)).ToListAsync();

        Assert.AreEqual(4, sorted.Count);
        Assert.AreEqual("a", sorted[0].Row["name"].StrValue);
        Assert.AreEqual("c", sorted[1].Row["name"].StrValue);
        Assert.AreEqual("b", sorted[2].Row["name"].StrValue);
        Assert.AreEqual("z", sorted[3].Row["name"].StrValue);
    }

    [Test]
    public async Task SortResultset_AliasQualifiedOrderColumn_resolvesToBareRowKey()
    {
        // Single-table aliased query: `ORDER BY u.position`, but scan rows are keyed by the
        // bare column name "position". The sorter must fall back from the qualified name to the
        // bare key. This only manifests with non-empty result sets (0-row sorts never look up).
        QueryTicket ticket = MakeTicket(new QueryOrderBy("u.position", OrderType.Ascending));

        List<QueryResultRow> rows =
        [
            Row(("position", 3L)),
            Row(("position", 1L)),
            Row(("position", 2L)),
        ];

        QuerySorter sorter = new();
        List<QueryResultRow> sorted = await sorter.SortResultset(ticket, ToAsync(rows)).ToListAsync();

        Assert.AreEqual(3, sorted.Count);
        Assert.AreEqual(1L, sorted[0].Row["position"].LongValue);
        Assert.AreEqual(2L, sorted[1].Row["position"].LongValue);
        Assert.AreEqual(3L, sorted[2].Row["position"].LongValue);
    }

    [Test]
    public void SortResultset_MissingSortColumn_throws()
    {
        QueryTicket ticket = MakeTicket(new QueryOrderBy("missing", OrderType.Ascending));

        // Two rows are required: List.Sort on a single element makes zero comparisons,
        // so GetSortValue is never called and the missing column goes undetected on any path.
        // List.Sort wraps comparer exceptions in InvalidOperationException; unwrap to get the root cause.
        QuerySorter sorter = new();
        List<QueryResultRow> rows = [Row(("name", "robot-a")), Row(("name", "robot-b"))];

        // List.Sort wraps comparer exceptions in InvalidOperationException; unwrap the inner cause.
        Exception outer = Assert.CatchAsync(
            async () => await sorter.SortResultset(ticket, ToAsync(rows)).ToListAsync())!;

        CamusDBException? exception = outer as CamusDBException ?? outer.InnerException as CamusDBException;
        Assert.IsNotNull(exception, $"Expected CamusDBException (possibly wrapped); got: {outer}");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInternalOperation, exception!.Code);
        StringAssert.Contains("missing", exception.Message);
    }

    [Test]
    public async Task SortResultset_GroupedRowsWithoutIdColumn_preservesAllRows()
    {
        QueryTicket ticket = MakeTicket(
            new QueryOrderBy("role", OrderType.Ascending),
            new QueryOrderBy("cnt", OrderType.Descending));

        List<QueryResultRow> rows =
        [
            GroupedRow(("role", "member"), ("cnt", 2L)),
            GroupedRow(("role", "admin"), ("cnt", 5L)),
            GroupedRow(("role", "admin"), ("cnt", 3L)),
        ];

        QuerySorter sorter = new();
        List<QueryResultRow> sorted = await sorter.SortResultset(ticket, ToAsync(rows)).ToListAsync();

        Assert.AreEqual(3, sorted.Count);
        Assert.AreEqual("admin", sorted[0].Row["role"].StrValue);
        Assert.AreEqual(5, sorted[0].Row["cnt"].LongValue);
        Assert.AreEqual("admin", sorted[1].Row["role"].StrValue);
        Assert.AreEqual(3, sorted[1].Row["cnt"].LongValue);
        Assert.AreEqual("member", sorted[2].Row["role"].StrValue);
    }

    private static QueryTicket MakeTicket(params QueryOrderBy[] orderBy)
    {
        KvTransaction txn = new(Kommander.Time.HLCTimestamp.Zero, "query-sorter-test");

        return new QueryTicket(
            txnState: txn,
            databaseName: "db",
            tableName: "robots",
            index: null,
            projection: null,
            filters: null,
            where: null,
            orderBy: orderBy.ToList(),
            limit: null,
            offset: null,
            parameters: null);
    }

    /// <summary>
    /// Verifies that the sort comparator degrades safely to the dictionary path when two
    /// QueryRow inputs carry different RowLayout instances (same column names, different ordinal
    /// order). Without the ReferenceEquals guard the comparator would reuse ordinals from the
    /// first layout for rows whose Values[] is ordered differently, silently comparing wrong columns.
    /// </summary>
    [Test]
    public async Task SortResultset_MixedLayoutQueryRows_sortsByCorrectColumnValues()
    {
        // Layout A: [name=0, score=1]
        RowLayout layoutA = RowLayout.ForColumns(["name", "score"]);
        // Layout B: [score=0, name=1]  — column order reversed, same names
        RowLayout layoutB = RowLayout.ForColumns(["score", "name"]);

        // Build QueryRows whose Values[] match their layout's ordinal order.
        QueryResultRow MakeA(string name, long score) =>
            new(default, new QueryRow(default, layoutA, [new(ColumnType.String, name), new(ColumnType.Integer64, score)]));
        QueryResultRow MakeB(string name, long score) =>
            new(default, new QueryRow(default, layoutB, [new(ColumnType.Integer64, score), new(ColumnType.String, name)]));

        // Mix rows from both layouts; ORDER BY name ascending.
        List<QueryResultRow> rows = [MakeA("robot-c", 3L), MakeB("robot-a", 1L), MakeA("robot-b", 2L)];

        QueryTicket ticket = MakeTicket(new QueryOrderBy("name", OrderType.Ascending));
        QuerySorter sorter = new();
        List<QueryResultRow> sorted = await sorter.SortResultset(ticket, ToAsync(rows)).ToListAsync();

        Assert.AreEqual(3, sorted.Count);
        // Each row must resolve "name" correctly regardless of which layout it carries.
        Assert.AreEqual("robot-a", sorted[0].Row["name"].StrValue);
        Assert.AreEqual("robot-b", sorted[1].Row["name"].StrValue);
        Assert.AreEqual("robot-c", sorted[2].Row["name"].StrValue);
    }

    private static QueryResultRow Row(params (string name, object value)[] columns)
    {
        Dictionary<string, ColumnValue> row = new();

        foreach ((string name, object value) in columns)
            row[name] = ToColumnValue(value);

        return new QueryResultRow(default(ObjectIdValue), row);
    }

    private static QueryResultRow GroupedRow(params (string name, object value)[] columns) =>
        Row(columns);

    private static ColumnValue ToColumnValue(object value) =>
        value switch
        {
            string s => new(ColumnType.String, s),
            long l => new(ColumnType.Integer64, l),
            bool b => new(ColumnType.Bool, b),
            _ => throw new AssertionException($"Unsupported test value type: {value.GetType()}"),
        };

    private static async IAsyncEnumerable<QueryResultRow> ToAsync(IEnumerable<QueryResultRow> rows)
    {
        foreach (QueryResultRow row in rows)
            yield return row;

        await Task.CompletedTask;
    }
}
