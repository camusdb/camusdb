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
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An equality join whose left side is an expression of the left row (<c>sd.id = CASE WHEN s.side = 0
/// THEN l.right_sidedef ELSE l.left_sidedef END</c>, <c>t.f = n.n * 0.5</c>) runs as a hash join keyed
/// on the computed value. Each test compares the result with the same join written so that the
/// conjunct is no key (<c>NOT (a &lt;&gt; b)</c> in place of <c>a = b</c>, the same truth value for every
/// pair, NULL included), which runs as a nested loop. This
/// fixture runs in cluster mode on a shared node; <see cref="TestJoinExpressionKeysStandalone"/> runs
/// it in standalone mode.
/// </summary>
[NonParallelizable]
internal class TestJoinExpressionKeys : SharedNodeBaseTest
{
    private static async Task Ddl(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Dml(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Query(
        CommandExecutor executor, DatabaseDescriptor database, string db, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static string Text(ColumnValue v) => v.Type switch
    {
        ColumnType.Null => "NULL",
        ColumnType.Integer64 => v.LongValue.ToString(),
        ColumnType.Float64 => v.FloatValue.ToString("R"),
        _ => v.StrValue ?? "",
    };

    private static List<string> Lines(List<QueryResultRow> rows) =>
        rows.Select(r => string.Join("|", r.Row.Values.Select(Text))).ToList();

    /// <summary>The detail of every hash join in the plan of <paramref name="sql"/>, one per line.</summary>
    private static async Task<string> HashJoinDetail(
        CommandExecutor executor, DatabaseDescriptor database, string db, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        List<QueryResultRow> plan = await Query(executor, database, db, "EXPLAIN " + sql, parameters);
        return string.Join("\n", plan.Where(r => r.Row["node"].StrValue == "hash-join").Select(r => r.Row["detail"].StrValue));
    }

    private static async Task<string> Plan(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        List<QueryResultRow> plan = await Query(executor, database, db, "EXPLAIN " + sql);
        return string.Join("\n", plan.Select(r => r.Row["node"].StrValue + " " + r.Row["detail"].StrValue));
    }

    /// <summary>
    /// A small copy of the DoomSQL geometry: a seg names a linedef and a side, a linedef names the
    /// sidedef of each side, and a left sidedef can be absent (NULL). Sidedefs above 99 do not exist.
    /// </summary>
    private async Task<(string Db, DatabaseDescriptor Database, CommandExecutor Executor)> Seeded(CamusDBOptions? options = null)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options ?? Options);
        await Ddl(executor, database, db, "CREATE TABLE linedef (id INT64 PRIMARY KEY, right_sidedef INT64 NULL, left_sidedef INT64 NULL)");
        await Ddl(executor, database, db, "CREATE TABLE sidedef (id INT64 PRIMARY KEY, tex STRING)");
        await Ddl(executor, database, db, "CREATE TABLE seg (id INT64 PRIMARY KEY, linedef_id INT64, side INT64)");
        await Dml(executor, database, db,
            "INSERT INTO linedef (id, right_sidedef, left_sidedef) " +
            "SELECT g, g * 2, CASE WHEN g % 3 = 0 THEN NULL ELSE g * 2 + 1 END FROM generate_series(0, 59) g");
        await Dml(executor, database, db, "INSERT INTO sidedef (id, tex) SELECT g, 'T' || CAST(g AS STRING) FROM generate_series(0, 99) g");
        await Dml(executor, database, db, "INSERT INTO seg (id, linedef_id, side) SELECT g, g % 60, g % 2 FROM generate_series(0, 149) g");
        return (db, database, executor);
    }

    private const string SideKey = "CASE WHEN s.side = 0 THEN l.right_sidedef ELSE l.left_sidedef END";

    private const string SidedefJoin =
        "SELECT s.id, sd.id AS sd_id, sd.tex FROM seg s " +
        "JOIN linedef l ON l.id = s.linedef_id " +
        "JOIN sidedef sd ON sd.id = " + SideKey + " " +
        "ORDER BY s.id";

    /// <summary>
    /// Inner join rows of <see cref="SidedefJoin"/>. Side 0 names sidedef 2 * linedef, present for a
    /// linedef up to 49: 65 of the 75 even segs. Side 1 names 2 * linedef + 1, absent for a linedef
    /// divisible by 3 and above 49: 44 of the 75 odd segs.
    /// </summary>
    private const int SidedefJoinRows = 65 + 44;

    /// <summary><paramref name="sql"/> with <c>a = b</c> written <c>NOT (a &lt;&gt; b)</c>, which no join keys.</summary>
    private static string Oracle(string sql, string a, string b)
    {
        string key = $"{a} = {b}";
        Assert.That(sql, Does.Contain(key));
        return sql.Replace(key, $"NOT ({a} <> {b})");
    }

    private async Task AssertSameAsNestedLoop(
        CommandExecutor executor, DatabaseDescriptor database, string db, string sql, string a, string b, int expectedRows)
    {
        string oracleSql = Oracle(sql, a, b);

        List<string> hashed = Lines(await Query(executor, database, db, sql));
        List<string> plain = Lines(await Query(executor, database, db, oracleSql));

        CollectionAssert.AreEqual(plain, hashed, sql);
        Assert.That(hashed, Has.Count.EqualTo(expectedRows), sql);

        StringAssert.DoesNotContain("(", await HashJoinDetail(executor, database, db, oracleSql),
            "the oracle joins on no expression key: " + oracleSql);
    }

    [Test]
    public async Task ACaseOfLeftColumnsIsAHashKey()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await AssertSameAsNestedLoop(executor, database, db, SidedefJoin, "sd.id", SideKey, SidedefJoinRows);

        string detail = await HashJoinDetail(executor, database, db, SidedefJoin);
        StringAssert.Contains("on=(" + SideKey + ")=id", detail, await Plan(executor, database, db, SidedefJoin));
    }

    [Test]
    public async Task ExplainAnalyzeOfTheExpressionJoinCountsThePairsItKeeps()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db, "EXPLAIN (ANALYZE) " + SidedefJoin);
        QueryResultRow join = rows.Single(r => r.Row["node"].StrValue == "hash-join" && r.Row["detail"].StrValue!.Contains("(CASE"));

        Assert.AreEqual((long)SidedefJoinRows, join.Row["actual_rows"].LongValue);
    }

    [Test]
    public async Task AKeyOfAColumnAndAnExpressionHashesOnBoth()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await Ddl(executor, database, db, "CREATE TABLE texture_column (id INT64 PRIMARY KEY, texture_id INT64, u INT64, px INT64)");
        await Ddl(executor, database, db, "CREATE TABLE pixel (id INT64 PRIMARY KEY, texture_id INT64, u INT64, width INT64)");
        await Dml(executor, database, db,
            "INSERT INTO texture_column (id, texture_id, u, px) SELECT g, g / 8, g % 8, g * 10 FROM generate_series(0, 31) g");
        await Dml(executor, database, db,
            "INSERT INTO pixel (id, texture_id, u, width) SELECT g, g % 4, (g % 13) - 6, 8 FROM generate_series(0, 99) g");

        const string wrapped = "CASE WHEN p.u < 0 THEN p.u + p.width ELSE p.u END";
        const string sql =
            "SELECT p.id, tc.px FROM pixel p JOIN texture_column tc " +
            "ON tc.texture_id = p.texture_id AND tc.u = " + wrapped + " ORDER BY p.id";

        // Every pixel has u in -6 .. 6, which wraps into 0 .. 7: one texture column each.
        await AssertSameAsNestedLoop(executor, database, db, sql, "tc.u", wrapped, 100);

        // The join-order search may put either table on the left; the expression is a key either way.
        string detail = await HashJoinDetail(executor, database, db, sql);
        StringAssert.Contains("texture_id=", detail, await Plan(executor, database, db, sql));
        StringAssert.Contains("(" + wrapped + ")", detail);
    }

    [Test]
    public async Task AnExpressionOfTheRightSideKeysTheBuild()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // linedef is the right side: the key is computed from each right row.
        const string key = "CASE WHEN l.id % 2 = 0 THEN l.right_sidedef ELSE l.left_sidedef END";
        const string sql = "SELECT sd.id, sd.tex FROM sidedef sd LEFT JOIN linedef l ON sd.id = " + key + " ORDER BY sd.id";

        // Of the 100 sidedefs, 42 are named (see the build-left test); the other 58 are padded.
        await AssertSameAsNestedLoop(executor, database, db, sql, "sd.id", key, 100);

        string detail = await HashJoinDetail(executor, database, db, sql);
        // EXPLAIN writes % as mod().
        StringAssert.Contains("on=sd.id=(CASE WHEN mod(l.id, 2) = 0 THEN", detail, await Plan(executor, database, db, sql));
        StringAssert.Contains("kind=left-outer", detail);
    }

    [Test]
    public async Task ALeftJoinPadsALeftRowWhoseKeyIsNullOrMissing()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        string sql = SidedefJoin.Replace("JOIN sidedef sd", "LEFT JOIN sidedef sd");
        await AssertSameAsNestedLoop(executor, database, db, sql, "sd.id", SideKey, 150);

        List<QueryResultRow> rows = await Query(executor, database, db, sql);
        Assert.That(rows.Count(r => r.Row["sd_id"].Type == ColumnType.Null), Is.EqualTo(150 - SidedefJoinRows));
        StringAssert.Contains("kind=left-outer", await HashJoinDetail(executor, database, db, sql));
    }

    [Test]
    public async Task ArithmeticOfAnotherNumericTypeKeysAFloatColumn()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await Ddl(executor, database, db, "CREATE TABLE numbers (n INT64 PRIMARY KEY)");
        await Ddl(executor, database, db, "CREATE TABLE t (id INT64 PRIMARY KEY, f FLOAT64)");
        await Dml(executor, database, db, "INSERT INTO numbers (n) SELECT g FROM generate_series(0, 39) g");
        await Dml(executor, database, db, "INSERT INTO t (id, f) SELECT g, g * 0.25 FROM generate_series(0, 79) g");

        // n * 0.5 is a FLOAT64 for an INT64 n. It equals f = id * 0.25 for id = 2 * n, so every n up
        // to 39 matches one row of t.
        const string sql = "SELECT n.n, t.id FROM numbers n JOIN t ON t.f = n.n * 0.5 ORDER BY n.n";
        await AssertSameAsNestedLoop(executor, database, db, sql, "t.f", "n.n * 0.5", 40);
        StringAssert.Contains("(n.n * 0.5)=f", await HashJoinDetail(executor, database, db, sql));
    }

    [Test]
    public async Task AnExpressionThatIsNoKeyKeepsTheNestedLoopAndItsRows()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await Ddl(executor, database, db, "CREATE TABLE numbers (n INT64 PRIMARY KEY)");
        await Ddl(executor, database, db, "CREATE TABLE t (id INT64 PRIMARY KEY, k INT64, j INT64)");
        await Dml(executor, database, db, "INSERT INTO numbers (n) SELECT g FROM generate_series(0, 19) g");
        await Dml(executor, database, db, "INSERT INTO t (id, k, j) SELECT g, g % 30, g % 3 FROM generate_series(0, 59) g");

        Dictionary<string, ColumnValue> parameters = new() { ["@p"] = new ColumnValue(ColumnType.Integer64, 2) };

        (string Sql, string A, string B, Dictionary<string, ColumnValue>? Parameters)[] cases =
        [
            // FLOAT64 against an INT64 column: the hash table would call 2.0 and 2 different.
            ("SELECT n.n, t.id FROM numbers n JOIN t ON t.k = n.n * 1.0 ORDER BY n.n, t.id", "t.k", "n.n * 1.0", null),
            // A parameter: the cached plan is reused with other values.
            ("SELECT n.n, t.id FROM numbers n JOIN t ON t.k = n.n + @p ORDER BY n.n, t.id", "t.k", "n.n + @p", parameters),
            // One side reads both tables.
            ("SELECT n.n, t.id FROM numbers n JOIN t ON t.k = n.n + t.j ORDER BY n.n, t.id", "t.k", "n.n + t.j", null),
            // A volatile call.
            ("SELECT n.n, t.id FROM numbers n JOIN t ON t.k = n.n + CAST(random() * 0 AS INT64) ORDER BY n.n, t.id",
                "t.k", "n.n + CAST(random() * 0 AS INT64)", null),
        ];

        foreach ((string sql, string a, string b, Dictionary<string, ColumnValue>? caseParameters) in cases)
        {
            List<string> rows = Lines(await Query(executor, database, db, sql, caseParameters));
            List<string> oracle = Lines(await Query(executor, database, db, Oracle(sql, a, b), caseParameters));

            CollectionAssert.AreEqual(oracle, rows, sql);
            Assert.That(rows, Is.Not.Empty, sql);
            Assert.That(await HashJoinDetail(executor, database, db, sql, caseParameters), Is.Empty, sql);
        }
    }

    [Test]
    public async Task ASmallLeftSideIsBuiltAndKeyedByItsExpression()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        const string key = "CASE WHEN l.id % 2 = 0 THEN l.right_sidedef ELSE l.left_sidedef END";
        const string sql = "SELECT l.id, sd.tex FROM linedef l JOIN sidedef sd ON sd.id = " + key + " ORDER BY l.id";

        List<string> baseline = Lines(await Query(executor, database, db, Oracle(sql, "sd.id", key)));

        executor.Statistics.SeedRowCountForTesting(database, await database.TableDescriptors["linedef"], 1);
        executor.Statistics.SeedRowCountForTesting(database, await database.TableDescriptors["sidedef"], 100_000);

        string detail = await HashJoinDetail(executor, database, db, sql);
        StringAssert.Contains("build=l", detail, await Plan(executor, database, db, sql));
        StringAssert.Contains("(CASE", detail);

        // Even ids name 2 * id, present up to id 48: 25. Odd ids name 2 * id + 1, absent when id is
        // divisible by 3 and above 49: 17.
        List<string> built = Lines(await Query(executor, database, db, sql));
        CollectionAssert.AreEqual(baseline, built);
        Assert.That(built, Has.Count.EqualTo(42));
    }

    [Test]
    public async Task TheGraceJoinPartitionsRowsByTheExpression()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await Seeded(Options with { SpillEnabled = true, ForceSpillThresholdRows = 2 });

        await AssertSameAsNestedLoop(executor, database, db, SidedefJoin, "sd.id", SideKey, SidedefJoinRows);

        List<QueryResultRow> rows = await Query(executor, database, db, "EXPLAIN (ANALYZE) " + SidedefJoin);
        string detail = string.Join("\n", rows.Where(r => r.Row["node"].StrValue == "hash-join").Select(r => r.Row["detail"].StrValue));
        Assert.That(detail, Does.Contain("(CASE"), detail);
        Assert.That(detail, Does.Contain("ran=grace-hash"), detail);

        // The same with the expression on the build side.
        const string key = "CASE WHEN l.id % 2 = 0 THEN l.right_sidedef ELSE l.left_sidedef END";
        const string rightSql = "SELECT sd.id, sd.tex FROM sidedef sd JOIN linedef l ON sd.id = " + key + " ORDER BY sd.id";
        await AssertSameAsNestedLoop(executor, database, db, rightSql, "sd.id", key, 42);
    }

    [Test]
    public async Task AnErrorInTheKeyExpressionIsRaised()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        const string sql = "SELECT s.id FROM seg s JOIN sidedef sd ON sd.id = 10 / (s.side - s.side)";
        Assert.That(await HashJoinDetail(executor, database, db, sql), Does.Contain("(10 /"));

        Assert.ThrowsAsync<CamusDBException>(async () => await Query(executor, database, db, sql));
    }
}

/// <summary>The cases of <see cref="TestJoinExpressionKeys"/> in standalone mode.</summary>
[NonParallelizable]
internal sealed class TestJoinExpressionKeysStandalone : TestJoinExpressionKeys
{
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: false);
    }
}
