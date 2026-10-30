/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>The helpers and the seed data the window-function fixtures share.</summary>
public abstract class WindowFunctionsTestBase : SharedNodeBaseTest
{
    protected static async Task ExecDdl(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    protected static async Task ExecNonQuery(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    protected static async Task<List<QueryResultRow>> ExecQuery(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    protected static async Task<CamusDBException> AssertFails(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql, string code, string? messagePart = null,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        }, sql);

        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.AreEqual(code, ex!.Code, sql + ": " + ex.Message);
        if (messagePart is not null)
            Assert.That(ex.Message, Does.Contain(messagePart), sql);
        return ex;
    }

    /// <summary>A cell as a long, or null for NULL.</summary>
    protected static long? L(QueryResultRow row, string column)
    {
        ColumnValue value = row.Row[column];
        if (value.Type == ColumnType.Null)
            return null;

        Assert.AreEqual(ColumnType.Integer64, value.Type, column);
        return value.LongValue;
    }

    /// <summary>A cell as a double, or null for NULL.</summary>
    protected static double? D(QueryResultRow row, string column)
    {
        ColumnValue value = row.Row[column];
        if (value.Type == ColumnType.Null)
            return null;

        Assert.AreEqual(ColumnType.Float64, value.Type, column);
        return value.FloatValue;
    }

    /// <summary>The rows keyed by their <c>id</c> column.</summary>
    protected static Dictionary<long, QueryResultRow> ById(List<QueryResultRow> rows) =>
        rows.ToDictionary(r => r.Row["id"].LongValue);

    /// <summary>
    /// <c>w</c> holds three partitions by <c>g</c>, with ties, a NULL value and a NULL partition key:
    /// <code>
    /// id  g     v
    /// 1   1     10
    /// 2   1     20
    /// 3   1     20
    /// 4   1     30
    /// 5   1     NULL
    /// 6   2     5      (a one-row partition)
    /// 7   NULL  7
    /// 8   NULL  9
    /// </code>
    /// NULL sorts first in an ascending ORDER BY, the engine's rule for every ORDER BY.
    /// </summary>
    protected static async Task SeedW(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ExecDdl(executor, database, dbname, "CREATE TABLE w (id int64 PRIMARY KEY, g int64 NULL, v int64 NULL)");
        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO w (id, g, v) VALUES (1, 1, 10), (2, 1, 20), (3, 1, 20), (4, 1, 30), (5, 1, NULL), "
            + "(6, 2, 5), (7, NULL, 7), (8, NULL, 9)");
    }
}

/// <summary>
/// Window functions end to end through <see cref="ExecuteSQLTicket"/>: every function, the default
/// frame and its peer rule, explicit frames, windows over a grouped query and a join, the derived-table
/// filter, LIMIT, and every refused form. Expected values are written out by hand from the fixture in
/// <see cref="WindowFunctionsTestBase.SeedW"/>, with PostgreSQL semantics except for NULL placement,
/// which follows the engine's ORDER BY.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestWindowFunctions : WindowFunctionsTestBase
{
    [Test]
    public async Task RankingFunctions_TiesNullPartitionAndOneRowPartition()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id, row_number() OVER (PARTITION BY g ORDER BY v, id) AS rn, "
            + "rank() OVER (PARTITION BY g ORDER BY v) AS rk, "
            + "dense_rank() OVER (PARTITION BY g ORDER BY v) AS dr, "
            + "percent_rank() OVER (PARTITION BY g ORDER BY v) AS pr, "
            + "cume_dist() OVER (PARTITION BY g ORDER BY v) AS cd, "
            + "ntile(2) OVER (PARTITION BY g ORDER BY v, id) AS nt "
            + "FROM w ORDER BY id");

        Assert.AreEqual(Enumerable.Range(1, 8).Select(i => (long)i), rows.Select(r => r.Row["id"].LongValue));

        long?[] rn = [2, 3, 4, 5, 1, 1, 1, 2];
        long?[] rk = [2, 3, 3, 5, 1, 1, 1, 2];
        long?[] dr = [2, 3, 3, 4, 1, 1, 1, 2];
        double?[] pr = [0.25, 0.5, 0.5, 1.0, 0.0, 0.0, 0.0, 1.0];
        double?[] cd = [0.4, 0.8, 0.8, 1.0, 0.2, 1.0, 0.5, 1.0];
        long?[] nt = [1, 1, 2, 2, 1, 1, 1, 2];

        Assert.AreEqual(rn, rows.Select(r => L(r, "rn")).ToArray(), "row_number");
        Assert.AreEqual(rk, rows.Select(r => L(r, "rk")).ToArray(), "rank");
        Assert.AreEqual(dr, rows.Select(r => L(r, "dr")).ToArray(), "dense_rank");
        Assert.AreEqual(pr, rows.Select(r => D(r, "pr")).ToArray(), "percent_rank");
        Assert.AreEqual(cd, rows.Select(r => D(r, "cd")).ToArray(), "cume_dist");
        Assert.AreEqual(nt, rows.Select(r => L(r, "nt")).ToArray(), "ntile");
    }

    [Test]
    public async Task OffsetFunctions_LagLeadWithOffsetAndDefault()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id, lag(v) OVER (PARTITION BY g ORDER BY id) AS lg, "
            + "lead(v) OVER (PARTITION BY g ORDER BY id) AS ld, "
            + "lag(v, 2, -1) OVER (PARTITION BY g ORDER BY id) AS lg2, "
            + "lead(v, 1, id) OVER (PARTITION BY g ORDER BY id) AS ldid "
            + "FROM w ORDER BY id");

        Assert.AreEqual(new long?[] { null, 10, 20, 20, 30, null, null, 7 }, rows.Select(r => L(r, "lg")).ToArray(), "lag");
        Assert.AreEqual(new long?[] { 20, 20, 30, null, null, null, 9, null }, rows.Select(r => L(r, "ld")).ToArray(), "lead");
        Assert.AreEqual(new long?[] { -1, -1, 10, 20, 20, -1, -1, -1 }, rows.Select(r => L(r, "lg2")).ToArray(), "lag(v, 2, -1)");
        // The default is read on the current row: the last row of each partition gets its own id.
        Assert.AreEqual(new long?[] { 20, 20, 30, null, 5, 6, 9, 8 }, rows.Select(r => L(r, "ldid")).ToArray(), "lead(v, 1, id)");
    }

    [Test]
    public async Task ValueFunctions_DefaultFrameEndsAtTheLastPeer()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id, first_value(v) OVER (PARTITION BY g ORDER BY id) AS fv, "
            + "last_value(v) OVER (PARTITION BY g ORDER BY id) AS lv, "
            + "last_value(v) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lvall, "
            + "nth_value(v, 2) OVER (PARTITION BY g ORDER BY id) AS nv, "
            + "last_value(id) OVER (PARTITION BY g ORDER BY v) AS lastpeer "
            + "FROM w ORDER BY id");

        Assert.AreEqual(new long?[] { 10, 10, 10, 10, 10, 5, 7, 7 }, rows.Select(r => L(r, "fv")).ToArray(), "first_value");
        Assert.AreEqual(new long?[] { 10, 20, 20, 30, null, 5, 7, 9 }, rows.Select(r => L(r, "lv")).ToArray(), "last_value");
        Assert.AreEqual(new long?[] { null, null, null, null, null, 5, 9, 9 }, rows.Select(r => L(r, "lvall")).ToArray(), "last_value whole");
        Assert.AreEqual(new long?[] { null, 20, 20, 20, 20, null, null, 9 }, rows.Select(r => L(r, "nv")).ToArray(), "nth_value");

        // ids 2 and 3 tie on v, so with the default frame both see the last of the two peers.
        Dictionary<long, QueryResultRow> byId = ById(rows);
        Assert.AreEqual(1, L(byId[1], "lastpeer"));
        Assert.AreEqual(L(byId[2], "lastpeer"), L(byId[3], "lastpeer"), "peers share last_value");
        Assert.That(L(byId[2], "lastpeer"), Is.EqualTo(2).Or.EqualTo(3));
        Assert.AreEqual(4, L(byId[4], "lastpeer"));
    }

    [Test]
    public async Task RunningAggregates_TiedKeysShareTheValue()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id, sum(v) OVER (PARTITION BY g ORDER BY v) AS s, "
            + "count(v) OVER (PARTITION BY g ORDER BY v) AS c, "
            + "count(*) OVER (PARTITION BY g ORDER BY v) AS cs, "
            + "sum(v) OVER (PARTITION BY g) AS total, "
            + "max(v) OVER () AS mx "
            + "FROM w ORDER BY id");

        Assert.AreEqual(new long?[] { 10, 50, 50, 80, null, 5, 7, 16 }, rows.Select(r => L(r, "s")).ToArray(), "running sum");
        Assert.AreEqual(new long?[] { 1, 3, 3, 4, 0, 1, 1, 2 }, rows.Select(r => L(r, "c")).ToArray(), "running count(v)");
        Assert.AreEqual(new long?[] { 2, 4, 4, 5, 1, 1, 1, 2 }, rows.Select(r => L(r, "cs")).ToArray(), "running count(*)");
        Assert.AreEqual(new long?[] { 80, 80, 80, 80, 80, 5, 16, 16 }, rows.Select(r => L(r, "total")).ToArray(), "partition sum");
        Assert.That(rows.Select(r => L(r, "mx")), Is.All.EqualTo(30), "max over everything");
    }

    [Test]
    public async Task SlidingRowsFrame_SumAvgMinMax()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        const string frame = "OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)";

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            $"SELECT id, sum(v) {frame} AS s, avg(v) {frame} AS a, min(v) {frame} AS mn, max(v) {frame} AS mx, "
            + $"count(v) {frame} AS c FROM w WHERE g = 1 ORDER BY id");

        // v by id in partition 1: 10, 20, 20, 30, NULL.
        Assert.AreEqual(new long?[] { 30, 50, 80, 70, 50 }, rows.Select(r => L(r, "s")).ToArray(), "sum");
        Assert.AreEqual(new long?[] { 10, 10, 10, 20, 20 }, rows.Select(r => L(r, "mn")).ToArray(), "min");
        Assert.AreEqual(new long?[] { 20, 20, 30, 30, 30 }, rows.Select(r => L(r, "mx")).ToArray(), "max");
        Assert.AreEqual(new long?[] { 2, 3, 4, 3, 2 }, rows.Select(r => L(r, "c")).ToArray(), "count");

        double?[] averages = rows.Select(r => D(r, "a")).ToArray();
        double[] expected = [15.0, 50.0 / 3, 20.0, 70.0 / 3, 25.0];

        for (int i = 0; i < expected.Length; i++)
            Assert.AreEqual(expected[i], averages[i]!.Value, 1e-9, "avg row " + i);
    }

    [Test]
    public async Task FloatSums_AreTheExactlyRoundedSumOfTheFrame()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, database, db, "CREATE TABLE f (id int64 PRIMARY KEY, x float64)");
        await ExecNonQuery(executor, database, db,
            "INSERT INTO f (id, x) VALUES (1, 0.1), (2, 0.2), (3, 0.3), (4, 0.4), (5, 0.5)");

        // The mixed column holds an int64 on the odd rows and a float64 on the even rows, so a frame
        // of two rows always mixes the types and its result type follows the plain SUM rule.
        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id, sum(x) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS suffix, "
            + "sum(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS slide, "
            + "sum(x) OVER (ORDER BY id) AS running, "
            + "avg(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS slideavg, "
            + "sum(CASE WHEN id % 2 = 0 THEN x ELSE id END) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS mixed "
            + "FROM f ORDER BY id");

        double[] x = [0.1, 0.2, 0.3, 0.4, 0.5];
        double[] mixedCells = [1, 0.2, 3, 0.4, 5];

        for (int i = 0; i < 5; i++)
        {
            int slideStart = Math.Max(0, i - 1);
            int slideEnd = Math.Min(4, i + 1);

            // Every frame is the exact sum of its values rounded once, so its value does not depend on
            // the order of the additions. 0.1 + 0.2 + 0.3 is 0.6 here and 0.6000000000000001 in row order.
            Assert.AreEqual(ExactSumOracle.Sum(x[i..]), D(rows[i], "suffix"), "suffix " + i);
            Assert.AreEqual(ExactSumOracle.Sum(x[slideStart..(slideEnd + 1)]), D(rows[i], "slide"), "slide " + i);
            Assert.AreEqual(ExactSumOracle.Sum(x[..(i + 1)]), D(rows[i], "running"), "running " + i);
            Assert.AreEqual(ExactSumOracle.Sum(x[slideStart..(slideEnd + 1)]) / (slideEnd - slideStart + 1), D(rows[i], "slideavg"), "slideavg " + i);

            if (i > 0)
                Assert.AreEqual(ExactSumOracle.Sum(mixedCells[(i - 1)..(i + 1)]), D(rows[i], "mixed"), "mixed " + i);
        }

        Assert.AreEqual(0.6, D(rows[2], "running"));

        // The first frame of the mixed column holds the int64 1 alone, so the window sums it as an
        // int64; the column is declared float64, so the derived table widens that cell to 1.0 and the
        // column has one type on every row.
        Assert.AreEqual(1.0, D(rows[0], "mixed"));
    }

    [Test]
    public async Task Cancellation_StopsThePartitionComputation()
    {
        (_, DatabaseDescriptor database, _) = await CreateDatabase();

        using CancellationTokenSource cancellation = new();

        // Fewer rows than the input loop's check interval, so only the computation of the partition
        // can see the cancel, which the input raises as its last row goes in.
        const int rowCount = 100;

        static async IAsyncEnumerable<QueryResultRow> Rows(int count, CancellationTokenSource toCancel)
        {
            for (int i = 0; i < count; i++)
            {
                await Task.Yield();

                if (i == count - 1)
                    toCancel.Cancel();

                yield return new QueryResultRow(new ObjectIdValue(), new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
                {
                    ["~win_in0"] = new ColumnValue(ColumnType.Integer64, i),
                });
            }
        }

        NodeAst callAst = SQLParserProcessor.Parse("SELECT sum(x) OVER () FROM t").leftAst!;
        WindowFunctionCall call = new(
            "~win0",
            WindowFunctionKind.Sum,
            [new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "~win_in0")],
            [],
            [],
            WindowFrameSpec.Default,
            callAst);
        DerivedColumnSchema[] columns = [new("~win_in0", ColumnType.Integer64), new("~win0", ColumnType.Integer64)];

        IAsyncEnumerable<QueryResultRow> output = WindowOperator.Apply(
            Rows(rowCount, cancellation),
            [call],
            columns,
            QueryExecutionContext.For(database, cancellation.Token),
            new QuerySorter(),
            parameters: null,
            stats: null,
            cancellation.Token);

        int emitted = 0;

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (QueryResultRow _ in output)
                emitted++;
        });

        Assert.AreEqual(0, emitted, "no row leaves before the partition is computed");
    }

    [Test]
    public async Task EmptyInput_ReturnsNoRows()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id, row_number() OVER (ORDER BY id) AS rn, sum(v) OVER () AS s FROM w WHERE id > 100");

        Assert.IsEmpty(rows);
    }

    [Test]
    public async Task TwoSpecs_FinalOrderByOnAWindowColumn()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id, row_number() OVER (ORDER BY id) AS up, row_number() OVER (ORDER BY id DESC) AS down "
            + "FROM w ORDER BY down");

        Assert.AreEqual(new long[] { 8, 7, 6, 5, 4, 3, 2, 1 }, rows.Select(r => r.Row["id"].LongValue));
        Assert.AreEqual(new long?[] { 8, 7, 6, 5, 4, 3, 2, 1 }, rows.Select(r => L(r, "up")).ToArray());
        Assert.AreEqual(new long?[] { 1, 2, 3, 4, 5, 6, 7, 8 }, rows.Select(r => L(r, "down")).ToArray());
    }

    [Test]
    public async Task OrderByAWindowCallThatIsNotSelected()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id FROM w ORDER BY row_number() OVER (ORDER BY id DESC)");

        Assert.AreEqual(new long[] { 8, 7, 6, 5, 4, 3, 2, 1 }, rows.Select(r => r.Row["id"].LongValue));
        Assert.AreEqual(1, rows[0].Row.Count, "no hidden column leaks into the result");
    }

    [Test]
    public async Task WindowOverAGroupedQuery()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT g, sum(v) AS s, rank() OVER (ORDER BY sum(v) DESC) AS r, "
            + "sum(sum(v)) OVER () AS grand FROM w WHERE g IS NOT NULL GROUP BY g ORDER BY g");

        Assert.AreEqual(new long?[] { 1, 2 }, rows.Select(r => L(r, "g")).ToArray());
        Assert.AreEqual(new long?[] { 80, 5 }, rows.Select(r => L(r, "s")).ToArray());
        Assert.AreEqual(new long?[] { 1, 2 }, rows.Select(r => L(r, "r")).ToArray());
        Assert.AreEqual(new long?[] { 85, 85 }, rows.Select(r => L(r, "grand")).ToArray());
    }

    [Test]
    public async Task WindowInAJoinQuery()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);
        await ExecDdl(executor, database, db, "CREATE TABLE names (g int64 PRIMARY KEY, label string)");
        await ExecNonQuery(executor, database, db, "INSERT INTO names (g, label) VALUES (1, 'one'), (2, 'two')");

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT w.id, n.label, row_number() OVER (PARTITION BY n.label ORDER BY w.id DESC) AS rn "
            + "FROM w JOIN names n ON w.g = n.g ORDER BY w.id");

        Assert.AreEqual(new long[] { 1, 2, 3, 4, 5, 6 }, rows.Select(r => r.Row["id"].LongValue));
        Assert.AreEqual(new long?[] { 5, 4, 3, 2, 1, 1 }, rows.Select(r => L(r, "rn")).ToArray());
        Assert.AreEqual("two", rows[5].Row["label"].StrValue);
    }

    [Test]
    public async Task StarWithAWindow_ShowsNoHiddenColumn()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT *, row_number() OVER (ORDER BY v DESC, id) AS rn FROM w ORDER BY id");

        Assert.AreEqual(8, rows.Count);
        CollectionAssert.AreEquivalent(new[] { "id", "g", "v", "rn" }, rows[0].Row.Keys.ToArray());
        Assert.AreEqual(4, L(rows[0], "rn"));
    }

    [Test]
    public async Task TopNPerGroup_ThroughADerivedTable()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id, rn FROM (SELECT id, g, row_number() OVER (PARTITION BY g ORDER BY id) AS rn FROM w) d "
            + "WHERE rn <= 2 ORDER BY id");

        Assert.AreEqual(new long[] { 1, 2, 6, 7, 8 }, rows.Select(r => r.Row["id"].LongValue));
        Assert.AreEqual(new long?[] { 1, 2, 1, 1, 2 }, rows.Select(r => L(r, "rn")).ToArray());

        // A filter on the partition key gives the rows the same filter inside the inner query gives.
        List<QueryResultRow> outer = await ExecQuery(executor, database, db,
            "SELECT id, rn FROM (SELECT id, g, row_number() OVER (PARTITION BY g ORDER BY id) AS rn FROM w) d "
            + "WHERE g = 1 ORDER BY id");
        List<QueryResultRow> inner = await ExecQuery(executor, database, db,
            "SELECT id, row_number() OVER (PARTITION BY g ORDER BY id) AS rn FROM w WHERE g = 1 ORDER BY id");

        Assert.AreEqual(inner.Select(r => L(r, "rn")), outer.Select(r => L(r, "rn")));
    }

    [Test]
    public async Task OuterLimit_CutsTheFullResult()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        List<QueryResultRow> limited = await ExecQuery(executor, database, db,
            "SELECT id, row_number() OVER (ORDER BY id DESC) AS rn FROM w ORDER BY id LIMIT 3");
        List<QueryResultRow> full = await ExecQuery(executor, database, db,
            "SELECT id, row_number() OVER (ORDER BY id DESC) AS rn FROM w ORDER BY id");

        Assert.AreEqual(full.Take(3).Select(r => L(r, "rn")), limited.Select(r => L(r, "rn")));
        Assert.AreEqual(new long?[] { 8, 7, 6 }, limited.Select(r => L(r, "rn")).ToArray());

        List<QueryResultRow> unordered = await ExecQuery(executor, database, db,
            "SELECT id, count(*) OVER () AS c FROM w LIMIT 2");

        Assert.AreEqual(2, unordered.Count);
        Assert.That(unordered.Select(r => L(r, "c")), Is.All.EqualTo(8), "LIMIT must not cut the window's input");
    }

    [Test]
    public async Task ExpressionsAroundWindowCalls()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id, id * 100 + row_number() OVER (ORDER BY id) AS mixed, "
            + "coalesce(lag(v) OVER (ORDER BY id), 0) AS prev, "
            + "v - avg(v) OVER (PARTITION BY g) AS delta "
            + "FROM w WHERE g = 1 ORDER BY id");

        Assert.AreEqual(new long?[] { 101, 202, 303, 404, 505 }, rows.Select(r => L(r, "mixed")).ToArray());
        Assert.AreEqual(new long?[] { 0, 10, 20, 20, 30 }, rows.Select(r => L(r, "prev")).ToArray());
        Assert.AreEqual(-10.0, D(rows[0], "delta"));
        Assert.IsNull(D(rows[4], "delta"));
    }

    [Test]
    public async Task RefusedForms_GiveNamedErrors()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        string invalid = CamusDBErrorCodes.InvalidInput;

        await AssertFails(executor, database, db, "SELECT id FROM w WHERE row_number() OVER () > 1", invalid, "WHERE");
        await AssertFails(executor, database, db, "SELECT g FROM w GROUP BY row_number() OVER ()", invalid, "GROUP BY");
        await AssertFails(executor, database, db, "SELECT g, count(*) FROM w GROUP BY g HAVING rank() OVER () > 1", invalid, "HAVING");
        await AssertFails(executor, database, db, "SELECT w.id FROM w JOIN w AS x ON w.id = row_number() OVER ()", invalid, "JOIN");
        await AssertFails(executor, database, db, "SELECT sum(row_number() OVER ()) OVER () FROM w", invalid, "nested");
        await AssertFails(executor, database, db, "SELECT rank() OVER (ORDER BY row_number() OVER ()) FROM w", invalid, "nested");
        await AssertFails(executor, database, db, "SELECT sum(v) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w", invalid, "RANGE with an offset");
        await AssertFails(executor, database, db, "SELECT sum(v) OVER (ORDER BY v GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w", invalid, "GROUPS");
        await AssertFails(executor, database, db, "SELECT sum(v) OVER (ORDER BY v ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW) FROM w", invalid, "EXCLUDE");
        await AssertFails(executor, database, db, "SELECT sum(v) OVER win FROM w", invalid, "Named windows");
        await AssertFails(executor, database, db, "SELECT upper(v) OVER () FROM w", invalid, "not a window function");
        await AssertFails(executor, database, db, "SELECT lag() OVER () FROM w", invalid, "takes 1 to 3");
        await AssertFails(executor, database, db, "SELECT ntile(0) OVER (ORDER BY id) FROM w", invalid, "greater than zero");
        await AssertFails(executor, database, db, "SELECT nth_value(v, 0) OVER (ORDER BY id) FROM w", invalid, "greater than zero");
        await AssertFails(executor, database, db, "SELECT sum(v) OVER (ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM w", invalid, "cannot start at CURRENT ROW");
        await AssertFails(executor, database, db, "SELECT sum(v) OVER (ROWS UNBOUNDED FOLLOWING) FROM w", invalid, "UNBOUNDED FOLLOWING");
        await AssertFails(executor, database, db, "SELECT lag(v, 1, 'x') OVER (ORDER BY id) FROM w", invalid, "default of lag");
        await AssertFails(executor, database, db, "SELECT DISTINCT * FROM w ORDER BY row_number() OVER ()", invalid, "DISTINCT");
    }

    [Test]
    public async Task WindowWordsStayUsableAsNames()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, db,
            "CREATE TABLE rows (id int64 PRIMARY KEY, range int64, current int64, partition int64, preceding int64, "
            + "following int64, unbounded int64, groups int64, exclude int64)");
        await ExecNonQuery(executor, database, db,
            "INSERT INTO rows (id, range, current, partition, preceding, following, unbounded, groups, exclude) "
            + "VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9)");

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT range, current, partition, preceding, following, unbounded, groups, exclude, "
            + "sum(range) OVER (PARTITION BY partition ORDER BY current ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s "
            + "FROM rows");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(2, L(rows[0], "s"));
        Assert.AreEqual(9, L(rows[0], "exclude"));

        // OVER is reserved: as a column name it is a syntax error.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecDdl(executor, database, db, "CREATE TABLE t_over (id int64 PRIMARY KEY, over int64)"))!;
        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, ex.Code);
    }

    [Test]
    public async Task Placeholders_AsFrameOffsetAndArguments()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@n"] = new ColumnValue(ColumnType.Integer64, 1),
            ["@k"] = new ColumnValue(ColumnType.Integer64, 2),
        };

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN @n PRECEDING AND CURRENT ROW) AS s, "
            + "lag(v, @k) OVER (ORDER BY id) AS lg FROM w WHERE g = 1 ORDER BY id", parameters);

        Assert.AreEqual(new long?[] { 10, 30, 40, 50, 30 }, rows.Select(r => L(r, "s")).ToArray());
        Assert.AreEqual(new long?[] { null, null, 10, 20, 20 }, rows.Select(r => L(r, "lg")).ToArray());
    }

    [Test]
    public async Task FrameOffsets_PastThePartition_ReachItsEnds()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);
        await ExecDdl(executor, database, db, "CREATE TABLE f (id int64 PRIMARY KEY, x float64)");
        await ExecNonQuery(executor, database, db, "INSERT INTO f (id, x) VALUES (1, 0.5), (2, 0.25), (3, 0.125)");

        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@far"] = new ColumnValue(ColumnType.Integer64, long.MaxValue),
            ["@back"] = new ColumnValue(ColumnType.Integer64, long.MinValue),
        };

        // An offset at the top of the integer range, as a literal and as a placeholder, reaches the end
        // of the partition: the sum row + offset must not wrap into a negative index, which once made
        // every frame after the first row empty.
        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id, first_value(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 9223372036854775807 FOLLOWING) AS fv, "
            + "last_value(id) OVER (ORDER BY id ROWS BETWEEN @far PRECEDING AND @far FOLLOWING) AS lv, "
            + "count(*) OVER (ORDER BY id ROWS BETWEEN @far PRECEDING AND @far FOLLOWING) AS c, "
            + "nth_value(v, 9223372036854775807) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS nv, "
            + "lag(v, @far) OVER (ORDER BY id) AS lg, lead(v, @far, -1) OVER (ORDER BY id) AS ld, "
            + "lag(v, @back) OVER (ORDER BY id) AS lgb, lead(v, @back) OVER (ORDER BY id) AS ldb "
            + "FROM w WHERE g = 1 ORDER BY id", parameters);

        Assert.AreEqual(new long?[] { 10, 20, 20, 30, null }, rows.Select(r => L(r, "fv")).ToArray(), "first_value of a frame that starts at the current row");
        Assert.That(rows.Select(r => L(r, "lv")), Is.All.EqualTo(5), "last_value of the whole partition");
        Assert.That(rows.Select(r => L(r, "c")), Is.All.EqualTo(5), "count of the whole partition");
        Assert.That(rows.Select(r => L(r, "nv")), Is.All.Null, "nth_value past the frame");
        Assert.That(rows.Select(r => L(r, "lg")), Is.All.Null, "lag past the partition");
        Assert.That(rows.Select(r => L(r, "ld")), Is.All.EqualTo(-1), "lead past the partition reads the default");
        Assert.That(rows.Select(r => L(r, "lgb")), Is.All.Null, "lag by long.MinValue");
        Assert.That(rows.Select(r => L(r, "ldb")), Is.All.Null, "lead by long.MinValue");

        // The float path reads each frame's bounds directly instead of sliding, so it needs its own check.
        List<QueryResultRow> floats = await ExecQuery(executor, database, db,
            "SELECT id, sum(x) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 9223372036854775807 FOLLOWING) AS s FROM f ORDER BY id");

        Assert.AreEqual(new double?[] { 0.875, 0.375, 0.125 }, floats.Select(r => D(r, "s")).ToArray());
    }

    [Test]
    public async Task LagLeadDefault_OfAnotherNumericType_WidensTheResult()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);
        await ExecDdl(executor, database, db, "CREATE TABLE f (id int64 PRIMARY KEY, x float64)");
        await ExecNonQuery(executor, database, db, "INSERT INTO f (id, x) VALUES (1, 0.5), (2, 0.25)");

        // An int64 value with a float default gives a Float64 column on every row, so an ORDER BY on
        // it compares one type. Before, only the default cell was Float64 and the sort threw.
        QuerySchemaHolder holder = new();
        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db,
            "SELECT id, lag(v, 1, 0.5) OVER (ORDER BY id) AS l, lead(v, 1, 2.5) OVER (ORDER BY id) AS d, "
            + "lag(v, 1, v * 0.5) OVER (ORDER BY id) AS e FROM w WHERE g = 1 ORDER BY l, id", null), schemaOut: holder);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(new long[] { 1, 2, 3, 4, 5 }, rows.Select(r => r.Row["id"].LongValue));
        Assert.AreEqual(new double?[] { 0.5, 10, 20, 20, 30 }, rows.Select(r => D(r, "l")).ToArray(), "lag with a float literal default");
        Assert.AreEqual(new double?[] { 20, 20, 30, null, 2.5 }, rows.Select(r => D(r, "d")).ToArray(), "lead with a float literal default");
        Assert.AreEqual(new double?[] { 5, 10, 20, 20, 30 }, rows.Select(r => D(r, "e")).ToArray(), "lag with a float expression default");
        Assert.AreEqual(ColumnType.Float64, holder.Schema.Single(c => c.Name == "l").Type);
        Assert.AreEqual(ColumnType.Float64, holder.Schema.Single(c => c.Name == "d").Type);
        Assert.AreEqual(ColumnType.Float64, holder.Schema.Single(c => c.Name == "e").Type);

        // A placeholder default is typed when the statement runs: an int64 default under a float value
        // is widened to the column; a float default under an int64 value would widen the column itself,
        // which is fixed by then, so it is refused, as a default of another kind is.
        List<QueryResultRow> narrower = await ExecQuery(executor, database, db,
            "SELECT id, lag(x, 1, @d) OVER (ORDER BY id) AS l FROM f ORDER BY id",
            new Dictionary<string, ColumnValue> { ["@d"] = new ColumnValue(ColumnType.Integer64, 1) });

        Assert.AreEqual(new double?[] { 1.0, 0.5 }, narrower.Select(r => D(r, "l")).ToArray());

        await AssertFails(executor, database, db, "SELECT id, lag(v, 1, @d) OVER (ORDER BY id) AS l FROM w",
            CamusDBErrorCodes.InvalidInput, "wider",
            new Dictionary<string, ColumnValue> { ["@d"] = new ColumnValue(ColumnType.Float64, 0.5) });
        await AssertFails(executor, database, db, "SELECT id, lag(v, 1, @d) OVER (ORDER BY id) AS l FROM w",
            CamusDBErrorCodes.InvalidInput, "does not match",
            new Dictionary<string, ColumnValue> { ["@d"] = new ColumnValue(ColumnType.String, "x") });
    }
}
