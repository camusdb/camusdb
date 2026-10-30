/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Window functions through every consumer that builds a query: views, CTAS, INSERT … SELECT,
/// subqueries and set-operation operands; the refusals outside a query (UPDATE SET, CHECK,
/// FOR UPDATE); EXPLAIN; the statement shape; the parser, the renderer and the wire codec.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestWindowFunctionsIntegration : WindowFunctionsTestBase
{
    [Test]
    public async Task View_TopNPerGroupAndColumnTypes()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        await ExecDdl(executor, database, db,
            "CREATE VIEW ranked AS SELECT id, g, v, row_number() OVER (PARTITION BY g ORDER BY id DESC) AS rn, "
            + "percent_rank() OVER (ORDER BY id) AS pr, lag(v) OVER (ORDER BY id) AS prev FROM w");

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id FROM ranked WHERE rn = 1 ORDER BY id");
        Assert.AreEqual(new long[] { 5, 6, 8 }, rows.Select(r => r.Row["id"].LongValue));

        List<QueryResultRow> star = await ExecQuery(executor, database, db, "SELECT * FROM ranked ORDER BY id");
        CollectionAssert.AreEquivalent(new[] { "id", "g", "v", "rn", "pr", "prev" }, star[0].Row.Keys.ToArray());

        List<QueryResultRow> columns = await ExecQuery(executor, database, db, "SHOW COLUMNS FROM ranked");
        Dictionary<string, string> types = columns.ToDictionary(c => c.Row["Field"].StrValue!, c => c.Row["Type"].StrValue!);
        Assert.AreEqual(6, types.Count, string.Join(",", types.Keys));
        StringAssert.Contains("int", types["rn"].ToLowerInvariant());
        StringAssert.Contains("float", types["pr"].ToLowerInvariant());
        StringAssert.Contains("int", types["prev"].ToLowerInvariant());

        List<QueryResultRow> shown = await ExecQuery(executor, database, db, "SHOW CREATE VIEW ranked");
        string body = shown[0].Row["create view"].StrValue!;
        StringAssert.Contains("OVER (PARTITION BY", body);
        StringAssert.Contains("ORDER BY", body);

        // The stored body re-parses: a second view from the rendered text gives the same rows.
        string select = body[body.IndexOf("SELECT", System.StringComparison.OrdinalIgnoreCase)..];
        List<QueryResultRow> again = await ExecQuery(executor, database, db, select + " ORDER BY 1");
        Assert.AreEqual(8, again.Count);
    }

    [Test]
    public async Task View_RenamedColumnInAWindowSpecIsFollowed()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        await ExecDdl(executor, database, db,
            "CREATE VIEW firsts AS SELECT id, row_number() OVER (PARTITION BY g ORDER BY v) AS rn FROM w");
        await ExecDdl(executor, database, db, "ALTER TABLE w RENAME COLUMN g TO grp");

        List<QueryResultRow> rows = await ExecQuery(executor, database, db, "SELECT id FROM firsts WHERE rn = 1 ORDER BY id");
        Assert.AreEqual(new long[] { 5, 6, 7 }, rows.Select(r => r.Row["id"].LongValue));
    }

    [Test]
    public async Task CreateTableAsSelect_AndInsertSelect()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        await ExecDdl(executor, database, db,
            "CREATE TABLE ranked AS SELECT id, rank() OVER (ORDER BY v DESC) AS r, cume_dist() OVER (ORDER BY id) AS cd FROM w");

        List<QueryResultRow> rows = await ExecQuery(executor, database, db, "SELECT id, r, cd FROM ranked ORDER BY id");
        Assert.AreEqual(new long?[] { 4, 2, 2, 1, 8, 7, 6, 5 }, rows.Select(r => L(r, "r")).ToArray());
        Assert.AreEqual(0.125, D(rows[0], "cd"));

        await ExecDdl(executor, database, db, "CREATE TABLE copy (id int64 PRIMARY KEY, rn int64)");
        await ExecNonQuery(executor, database, db,
            "INSERT INTO copy (id, rn) SELECT id, row_number() OVER (ORDER BY id DESC) FROM w");

        List<QueryResultRow> copied = await ExecQuery(executor, database, db, "SELECT id, rn FROM copy ORDER BY id");
        Assert.AreEqual(new long?[] { 8, 7, 6, 5, 4, 3, 2, 1 }, copied.Select(r => L(r, "rn")).ToArray());
    }

    [Test]
    public async Task Subqueries_RunTheWindow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        // lag(v) over partition 1 by id is {NULL, 10, 20, 20, 30}. A rewrite that read w.v directly
        // would also match 5, 7 and 9.
        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id FROM w WHERE v IN (SELECT lag(v) OVER (ORDER BY id) FROM w WHERE g = 1) ORDER BY id");
        Assert.AreEqual(new long[] { 1, 2, 3, 4 }, rows.Select(r => r.Row["id"].LongValue));

        List<QueryResultRow> scalar = await ExecQuery(executor, database, db,
            "SELECT id FROM w WHERE id = (SELECT max(rn) - 1 FROM (SELECT row_number() OVER () AS rn FROM w) d)");
        Assert.AreEqual(new long[] { 7 }, scalar.Select(r => r.Row["id"].LongValue));

        // rank() over v is 1, 2, 3, 4, 5, 6, 6, 8: the tie skips 7.
        List<QueryResultRow> exists = await ExecQuery(executor, database, db,
            "SELECT id FROM w WHERE EXISTS (SELECT 1 FROM (SELECT rank() OVER (ORDER BY v) AS r FROM w) d WHERE d.r = 8)");
        Assert.AreEqual(8, exists.Count);

        List<QueryResultRow> missing = await ExecQuery(executor, database, db,
            "SELECT id FROM w WHERE EXISTS (SELECT 1 FROM (SELECT rank() OVER (ORDER BY v) AS r FROM w) d WHERE d.r = 7)");
        Assert.IsEmpty(missing);
    }

    [Test]
    public async Task SetOperationOperands_EachRunTheirOwnWindow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT id, row_number() OVER (ORDER BY id) AS rn FROM w WHERE g = 2 "
            + "UNION ALL SELECT id, row_number() OVER (ORDER BY id DESC) FROM w WHERE g IS NULL ORDER BY 1");

        Assert.AreEqual(new long[] { 6, 7, 8 }, rows.Select(r => r.Row["id"].LongValue));
        Assert.AreEqual(new long?[] { 1, 2, 1 }, rows.Select(r => L(r, "rn")).ToArray());
    }

    [Test]
    public async Task OutsideAQuery_WindowsAreRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException update = Assert.ThrowsAsync<CamusDBException>(() =>
            executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "UPDATE w SET v = row_number() OVER () WHERE id = 1", null)))!;
        await database.Transactions.RollbackIfNotCompletedAsync(tx);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, update.Code, update.Message);
        StringAssert.Contains("Window functions", update.Message);

        CamusDBException check = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecDdl(executor, database, db, "CREATE TABLE c (id int64 PRIMARY KEY, x int64 CHECK (x > rank() OVER ()))"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, check.Code, check.Message);
        StringAssert.Contains("window", check.Message);

        CamusDBException syntax = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecDdl(executor, database, db, "CREATE TABLE d (id int64 PRIMARY KEY, x int64 DEFAULT row_number() OVER ())"))!;
        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, syntax.Code, syntax.Message);

        KvTransaction lockTx = await database.Transactions.BeginAsync();
        CamusDBException forUpdate = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(lockTx, db, "SELECT id, row_number() OVER (ORDER BY id) FROM w FOR UPDATE", null));
            await cursor.ToListAsync();
        })!;
        await database.Transactions.RollbackIfNotCompletedAsync(lockTx);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, forUpdate.Code, forUpdate.Message);
    }

    [Test]
    public async Task Explain_ShowsTheWindowNodeAndAnalyzeCounters()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        const string query = "SELECT id, row_number() OVER (PARTITION BY g ORDER BY id DESC) AS rn, "
            + "sum(v) OVER (PARTITION BY g ORDER BY id DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s, "
            + "rank() OVER (ORDER BY v) AS r FROM w";

        List<QueryResultRow> plan = await ExecQuery(executor, database, db, "EXPLAIN " + query);
        QueryResultRow window = plan.Single(r => r.Row["node"].StrValue == "window");
        string detail = window.Row["detail"].StrValue!;

        StringAssert.Contains("partition=[g] order=[id DESC] functions=[row_number(), sum(v) rows between 1 preceding and current row]", detail);
        StringAssert.Contains("partition=[] order=[v] functions=[rank()]", detail);
        StringAssert.DoesNotContain("partitions=", detail, "plain EXPLAIN runs nothing");

        List<QueryResultRow> analyzed = await ExecQuery(executor, database, db, "EXPLAIN (ANALYZE) " + query);
        string analyzedDetail = analyzed.Single(r => r.Row["node"].StrValue == "window").Row["detail"].StrValue!;

        // Group one has three partitions of the 8 rows, group two one partition of the 8 rows.
        StringAssert.Contains("partitions=4 rows=16 spilled=0", analyzedDetail);
    }

    [Test]
    public async Task Caches_DoNotMixTwoSpecsOrTwoParameterValues()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with { PlanCacheEnabled = true });
        await SeedW(executor, database, db);

        for (int round = 0; round < 2; round++)
        {
            List<QueryResultRow> up = await ExecQuery(executor, database, db,
                "SELECT id, row_number() OVER (ORDER BY id) AS rn FROM w ORDER BY id");
            List<QueryResultRow> down = await ExecQuery(executor, database, db,
                "SELECT id, row_number() OVER (ORDER BY id DESC) AS rn FROM w ORDER BY id");

            Assert.AreEqual(new long?[] { 1, 2, 3, 4, 5, 6, 7, 8 }, up.Select(r => L(r, "rn")).ToArray(), "round " + round);
            Assert.AreEqual(new long?[] { 8, 7, 6, 5, 4, 3, 2, 1 }, down.Select(r => L(r, "rn")).ToArray(), "round " + round);
        }

        const string sliding = "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN @n PRECEDING AND CURRENT ROW) AS s FROM w WHERE g = 1 ORDER BY id";

        List<QueryResultRow> zero = await ExecQuery(executor, database, db, sliding,
            new Dictionary<string, ColumnValue> { ["@n"] = new ColumnValue(ColumnType.Integer64, 0) });
        List<QueryResultRow> one = await ExecQuery(executor, database, db, sliding,
            new Dictionary<string, ColumnValue> { ["@n"] = new ColumnValue(ColumnType.Integer64, 1) });

        Assert.AreEqual(new long?[] { 10, 20, 20, 30, null }, zero.Select(r => L(r, "s")).ToArray());
        Assert.AreEqual(new long?[] { 10, 30, 40, 50, 30 }, one.Select(r => L(r, "s")).ToArray());
    }

    [Test]
    public async Task ResultSchema_NamesAndTypesWithoutHiddenColumns()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);
        await ExecDdl(executor, database, db, "CREATE TABLE names (g int64 PRIMARY KEY, label string)");
        await ExecNonQuery(executor, database, db, "INSERT INTO names (g, label) VALUES (1, 'one'), (2, 'two')");

        async Task<(IReadOnlyList<DerivedColumnSchema> Schema, List<QueryResultRow> Rows)> Run(string sql)
        {
            QuerySchemaHolder holder = new();
            KvTransaction tx = await database.Transactions.BeginAsync();
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db, sql, null), schemaOut: holder);
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return (holder.Schema, rows);
        }

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) = await Run(
            "SELECT *, row_number() OVER (ORDER BY id) AS rn, percent_rank() OVER (ORDER BY id) AS pr FROM w ORDER BY id");

        Assert.AreEqual(new[] { "id", "g", "v", "rn", "pr" }, schema.Select(c => c.Name).ToArray());
        Assert.AreEqual(ColumnType.Integer64, schema[3].Type);
        Assert.AreEqual(ColumnType.Float64, schema[4].Type);
        Assert.That(schema.Select(c => c.Name), Has.None.StartsWith("~"));
        Assert.AreEqual(8, rows.Count);

        // A * over a join keeps the same column names with and without a window beside it.
        (IReadOnlyList<DerivedColumnSchema> plainSchema, List<QueryResultRow> plainRows) = await Run(
            "SELECT * FROM w JOIN names n ON w.g = n.g ORDER BY w.id");
        (IReadOnlyList<DerivedColumnSchema> windowSchema, List<QueryResultRow> windowRows) = await Run(
            "SELECT *, row_number() OVER (ORDER BY w.id) AS rn FROM w JOIN names n ON w.g = n.g ORDER BY w.id");

        Assert.AreEqual(plainSchema.Select(c => c.Name).Append("rn").ToArray(), windowSchema.Select(c => c.Name).ToArray());
        Assert.AreEqual(plainRows.Count, windowRows.Count);
        CollectionAssert.AreEquivalent(plainRows[0].Row.Keys.Append("rn").ToArray(), windowRows[0].Row.Keys.ToArray());
        Assert.AreEqual(new long?[] { 1, 2, 3, 4, 5, 6 }, windowRows.Select(r => L(r, "rn")).ToArray());
    }

    [Test]
    public void Shape_DiffersByWindowSpecOnly()
    {
        SelectQueryCreator creator = new();

        string Shape(string sql) => QueryShapeComputer.Compute(creator.CreateSelectQuery(SQLParserProcessor.Parse(sql)));

        string asc = Shape("SELECT id, row_number() OVER (ORDER BY v) FROM w");
        string desc = Shape("SELECT id, row_number() OVER (ORDER BY v DESC) FROM w");
        string framed = Shape("SELECT id, sum(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w");
        string framedOther = Shape("SELECT id, sum(v) OVER (ORDER BY v ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) FROM w");
        string framedUnbounded = Shape("SELECT id, sum(v) OVER (ORDER BY v ROWS UNBOUNDED PRECEDING) FROM w");

        Assert.AreNotEqual(asc, desc, "a direction is structure");
        Assert.AreEqual(framed, framedOther, "a frame offset is a literal");
        Assert.AreNotEqual(framed, framedUnbounded, "a frame bound kind is structure");
    }

    [Test]
    public void Rewrite_ShapeOfTheDerivedTable()
    {
        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(
            "SELECT id, v + 1 AS next, rank() OVER (PARTITION BY g ORDER BY v) AS r, rank() OVER (PARTITION BY g ORDER BY v) AS r2 "
            + "FROM w WHERE id > 0 ORDER BY r LIMIT 5"));

        DerivedTableSource derived = (DerivedTableSource)query.Source;
        Assert.AreEqual(DerivedTableSource.WindowDerivedAlias, derived.Alias);
        Assert.AreEqual(1, derived.Windows!.Count, "identical calls share one result");
        Assert.AreEqual(new[] { "~win_in2" }, derived.Windows[0].PartitionColumns);
        Assert.IsNotNull(derived.Query.Where, "WHERE stays in the inner query");
        Assert.IsNull(query.Where);
        Assert.IsNotNull(query.Limit, "LIMIT stays in the outer query");
        Assert.IsNull(derived.Query.Limit);
        Assert.AreEqual(new[] { "id", "next", "r", "r2" }, query.Projections.Select(p => p.OutputName));
    }

    [Test]
    public void Parser_FramesRenderAndRoundTrip()
    {
        string[] calls =
        [
            "row_number() OVER ()",
            "rank() OVER (PARTITION BY a, b ORDER BY c DESC, d ASC)",
            "sum(x) OVER (ORDER BY c ROWS 3 PRECEDING)",
            "sum(x) OVER (ORDER BY c ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)",
            "avg(x) OVER (PARTITION BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
            "min(x) OVER (ROWS BETWEEN CURRENT ROW AND @n FOLLOWING)",
            "lag(x, 2, 0) OVER (ORDER BY c)",
        ];

        foreach (string call in calls)
        {
            NodeAst select = SQLParserProcessor.Parse($"SELECT {call} FROM t");
            NodeAst window = select.leftAst!;
            Assert.AreEqual(NodeType.ExprWindowCall, window.nodeType, call);

            string rendered = ViewBodyRenderer.RenderExpression(window);
            NodeAst reparsed = SQLParserProcessor.Parse($"SELECT {rendered} FROM t").leftAst!;
            Assert.IsTrue(QueryAstComparer.AreEquivalent(window, reparsed), call + " => " + rendered);

            NodeAst decoded = NodeAstWireCodec.Deserialize(NodeAstWireCodec.Serialize(window));
            Assert.IsTrue(QueryAstComparer.AreEquivalent(window, decoded), "wire codec: " + call);
        }

        NodeAst ordered = SQLParserProcessor.Parse("SELECT a FROM t ORDER BY row_number() OVER (ORDER BY b), a");
        Assert.IsNotNull(ordered.extendedTwo);
    }

    [Test]
    public async Task VolatileExpressions_AreNotSharedBetweenOccurrences()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedW(executor, database, db);

        // Two gen_uuid_v4() items stay two calls per row beside a window, as they are without one, and a
        // volatile PARTITION BY key is its own call too, so every row is its own partition.
        List<QueryResultRow> rows = await ExecQuery(executor, database, db,
            "SELECT gen_uuid_v4() AS a, gen_uuid_v4() AS b, row_number() OVER (PARTITION BY gen_uuid_v4()) AS rn FROM w");

        Assert.AreEqual(8, rows.Count);
        Assert.That(rows.Select(r => r.Row["a"].CompareTo(r.Row["b"]) == 0), Is.All.False, "a and b are two values on every row");
        Assert.AreEqual(8, rows.Select(r => r.Row["a"].ToGuid()).Distinct().Count(), "a differs from row to row");
        Assert.That(rows.Select(r => L(r, "rn")), Is.All.EqualTo(1), "each row is its own partition");

        // The rewrite keeps one inner column per volatile occurrence and still shares a deterministic one.
        SelectQueryCreator creator = new();
        DerivedTableSource Derived(string sql) => (DerivedTableSource)creator.CreateSelectQuery(SQLParserProcessor.Parse(sql)).Source;

        Assert.AreEqual(3, Derived("SELECT gen_uuid_v4() AS a, gen_uuid_v4() AS b, row_number() OVER (PARTITION BY gen_uuid_v4()) FROM w").Query.Projections.Count);
        Assert.AreEqual(1, Derived("SELECT v + 1 AS a, v + 1 AS b, row_number() OVER (PARTITION BY v + 1) FROM w").Query.Projections.Count);

        // Two window calls over a volatile argument are two computations; over a column they are one.
        Assert.AreEqual(2, Derived("SELECT sum(random()) OVER () AS a, sum(random()) OVER () AS b FROM w").Windows!.Count);
        Assert.AreEqual(1, Derived("SELECT sum(v) OVER () AS a, sum(v) OVER () AS b FROM w").Windows!.Count);
    }
}
