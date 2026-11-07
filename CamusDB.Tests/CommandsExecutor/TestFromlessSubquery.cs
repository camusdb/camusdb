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

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A SELECT with no FROM as a subquery, a derived table and a stored view body. Each expected result in
/// <see cref="Statement"/> was measured on PostgreSQL 16.11 in the C locale with the same rows. A result
/// renders as <c>t</c> or <c>f</c>, a NULL renders as an empty cell, cells join with <c>|</c>, rows join with
/// <c> ; </c>, and an expected value that starts with <c>CADB</c> is an error code where PostgreSQL also gave
/// an error.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestFromlessSubquery : BaseTest
{
    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Bool => value.BoolValue ? "t" : "f",
        ColumnType.Null => "",
        ColumnType.String => value.StrValue!,
        ColumnType.Integer64 => value.LongValue.ToString(),
        _ => throw new ArgumentException(value.Type.ToString()),
    };

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        if (sql.StartsWith("CREATE", StringComparison.Ordinal))
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        else
            _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<string> Run(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            return string.Join(" ; ", rows.Select(row => string.Join("|", row.Row.Values.Select(Render))));
        }
        catch (CamusDBException e)
        {
            return e.Code;
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    /// <summary>The error code of a statement, or an empty string when it succeeds and commits.</summary>
    private static async Task<string> TryCommit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        try
        {
            await Commit(database, executor, dbname, sql);
            return "";
        }
        catch (CamusDBException e)
        {
            return e.Code;
        }
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> CreateTables(CamusDBOptions? options = null)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            options is null ? await CreateDatabase() : await CreateDatabase(options);

        await Commit(database, executor, dbname, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, s STRING)");
        await Commit(database, executor, dbname, "CREATE TABLE em (id INT64 PRIMARY KEY, s STRING)");
        await Commit(database, executor, dbname, "CREATE SEQUENCE sq");
        await Commit(database, executor, dbname, "INSERT INTO t (id, v, s) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')");

        return (dbname, database, executor);
    }

    [TestCase("SELECT (SELECT 1)", "1")]
    [TestCase("SELECT EXISTS (SELECT 1)", "t")]
    [TestCase("SELECT NOT EXISTS (SELECT 1)", "f")]
    [TestCase("SELECT 1 IN (SELECT 1)", "t")]
    [TestCase("SELECT 2 IN (SELECT 1)", "f")]
    [TestCase("SELECT 1 NOT IN (SELECT 1)", "f")]
    [TestCase("SELECT id FROM t WHERE id IN (SELECT 1)", "1")]
    [TestCase("SELECT id FROM t WHERE id NOT IN (SELECT 1) ORDER BY id", "2 ; 3")]
    [TestCase("SELECT id FROM t WHERE EXISTS (SELECT 1) ORDER BY id", "1 ; 2 ; 3")]
    [TestCase("SELECT id FROM t WHERE NOT EXISTS (SELECT 1)", "")]
    [TestCase("SELECT id FROM t WHERE id = (SELECT 1)", "1")]
    [TestCase("SELECT id, (SELECT 5) FROM t ORDER BY id", "1|5 ; 2|5 ; 3|5")]
    [TestCase("SELECT id FROM t WHERE id = ANY (SELECT 1)", "1")]
    [TestCase("SELECT id FROM t WHERE id > ALL (SELECT 1) ORDER BY id", "2 ; 3")]
    [TestCase("SELECT x FROM (SELECT 5 AS x) d", "5")]
    [TestCase("SELECT d.x + 1 FROM (SELECT 5 AS x, 'a' AS y) d", "6")]
    [TestCase("SELECT d.y FROM (SELECT 5 AS x, 'a' AS y) d", "a")]
    [TestCase("SELECT * FROM (SELECT 5 AS x) d", "5")]
    [TestCase("SELECT x FROM (SELECT 5) d", "CADB0404")]
    [TestCase("SELECT t.id, d.x FROM t JOIN (SELECT 1 AS x) d ON t.id = d.x", "1|1")]
    [TestCase("SELECT t.id FROM t, (SELECT 1 AS x) d WHERE t.id = d.x", "1")]
    [TestCase("SELECT t.id, d.x FROM t LEFT JOIN (SELECT 1 AS x) d ON t.id = d.x ORDER BY t.id", "1|1 ; 2| ; 3|")]
    [TestCase("SELECT x FROM (SELECT 5 AS x WHERE TRUE) d", "5")]
    [TestCase("SELECT x FROM (SELECT 5 AS x WHERE FALSE) d", "")]
    [TestCase("SELECT x FROM (SELECT 5 AS x LIMIT 0) d", "")]
    [TestCase("SELECT x FROM (SELECT 5 AS x LIMIT 1 OFFSET 1) d", "")]
    [TestCase("SELECT x FROM (SELECT 5 AS x LIMIT 1) d", "5")]
    [TestCase("SELECT (SELECT 1 WHERE TRUE)", "1")]
    [TestCase("SELECT (SELECT 1 WHERE FALSE)", "")]
    [TestCase("SELECT (SELECT 1 WHERE FALSE) IS NULL", "t")]
    [TestCase("SELECT EXISTS (SELECT 1 WHERE TRUE)", "t")]
    [TestCase("SELECT EXISTS (SELECT 1 WHERE FALSE)", "f")]
    [TestCase("SELECT 1 IN (SELECT 1 WHERE FALSE)", "f")]
    [TestCase("SELECT (SELECT 1 WHERE 1)", "CADB0400")]
    [TestCase("SELECT (SELECT 1 LIMIT 0)", "")]
    [TestCase("SELECT EXISTS (SELECT 1 LIMIT 0)", "f")]
    [TestCase("SELECT (SELECT *)", "CADB0400")]
    [TestCase("SELECT * FROM (SELECT *) d", "CADB0400")]
    [TestCase("SELECT EXISTS (SELECT *)", "CADB0400")]
    [TestCase("SELECT 1 IN (SELECT *)", "CADB0400")]
    [TestCase("SELECT id, (SELECT t.id) FROM t ORDER BY id", "CADB0400")]
    [TestCase("SELECT id, (SELECT id) FROM t ORDER BY id", "CADB0404")]
    [TestCase("SELECT (SELECT nosuch)", "CADB0404")]
    [TestCase("SELECT EXISTS (SELECT nosuch)", "CADB0404")]
    [TestCase("SELECT x FROM (SELECT nosuch AS x) d", "CADB0404")]
    [TestCase("SELECT id FROM em WHERE id IN (SELECT nosuch)", "CADB0404")]
    [TestCase("SELECT (SELECT 1 / 0)", "CADB0400")]
    [TestCase("SELECT EXISTS (SELECT 1 / 0)", "t")]
    [TestCase("SELECT (SELECT 1 / 0 WHERE FALSE)", "CADB0400")]
    [TestCase("SELECT x FROM (SELECT 1 / 0 AS x) d", "CADB0400")]
    [TestCase("SELECT id FROM em WHERE (SELECT 1 / 0) = 1", "CADB0400")]
    [TestCase("SELECT (SELECT (SELECT 1))", "1")]
    [TestCase("SELECT x FROM (SELECT (SELECT 5) AS x) d", "5")]
    [TestCase("SELECT EXISTS (SELECT 1 WHERE EXISTS (SELECT 1))", "t")]
    [TestCase("SELECT (SELECT 1, 2)", "CADB0400")]
    [TestCase("SELECT 1 IN (SELECT 1, 2)", "CADB0400")]
    [TestCase("SELECT (SELECT 1) + 1", "2")]
    [TestCase("SELECT (SELECT 'a') || 'b'", "ab")]
    [TestCase("SELECT (SELECT NULL) IS NULL", "t")]
    [TestCase("SELECT 1 = (SELECT 'a')", "CADB0400")]
    [TestCase("SELECT id FROM t WHERE (SELECT 'a')", "CADB0400")]
    [TestCase("SELECT (SELECT 1 UNION SELECT 1)", "1")]
    [TestCase("SELECT x FROM (SELECT 1 AS x UNION SELECT 2) d ORDER BY x", "1 ; 2")]
    [TestCase("SELECT CASE WHEN EXISTS (SELECT 1) THEN 1 ELSE 0 END", "1")]
    [TestCase("SELECT COALESCE((SELECT 1), 2)", "1")]
    [TestCase("SELECT COUNT(*) FROM t HAVING COUNT(*) > (SELECT 1)", "3")]
    [TestCase("SELECT SUM((SELECT 1)) FROM t", "3")]
    [TestCase("SELECT t.id FROM t JOIN em ON t.id = (SELECT 1)", "")]
    [TestCase("SELECT (SELECT ROW_NUMBER() OVER ())", "1")]
    [TestCase("SELECT (SELECT DISTINCT 1)", "1")]
    [TestCase("SELECT x FROM (SELECT DISTINCT 5 AS x) d", "5")]
    [TestCase("SELECT (SELECT NULL)", "")]
    [TestCase("SELECT x FROM (SELECT NULL AS x) d", "")]
    [TestCase("SELECT x IS NULL FROM (SELECT NULL AS x) d", "t")]
    [TestCase("SELECT x || 'a' FROM (SELECT 'b' AS x) d", "ba")]
    [TestCase("SELECT x FROM (SELECT 1 AS x) d WHERE x = 1", "1")]
    [TestCase("SELECT x FROM (SELECT 1 AS x) d WHERE x = 2", "")]
    [TestCase("SELECT x + 1 AS y FROM (SELECT 1 AS x) d ORDER BY y", "2")]
    [TestCase("SELECT x FROM (SELECT 'a' AS x) d WHERE x + 1 = 2", "CADB0400")]
    [TestCase("SELECT x FROM (SELECT 1 AS x) d WHERE x", "CADB0400")]
    [TestCase("SELECT COUNT(*) FROM (SELECT 5 AS x) d", "1")]
    [TestCase("SELECT COUNT(*) FROM (SELECT 5 AS x WHERE FALSE) d", "0")]
    [TestCase("SELECT SUM(x) FROM (SELECT 5 AS x) d", "5")]
    [TestCase("SELECT SUM(x) FROM (SELECT 5 AS x WHERE FALSE) d", "")]
    [TestCase("SELECT x, COUNT(*) FROM (SELECT 5 AS x) d GROUP BY x", "5|1")]
    [TestCase("SELECT y FROM (SELECT x AS y FROM (SELECT 5 AS x) a) b", "5")]
    [TestCase("SELECT a.x, b.y FROM (SELECT 1 AS x) a, (SELECT 2 AS y) b", "1|2")]
    [TestCase("SELECT a.x FROM (SELECT 1 AS x) a JOIN (SELECT 1 AS x) b ON a.x = b.x", "1")]
    [TestCase("SELECT a.x, b.x FROM (SELECT 1 AS x) a LEFT JOIN (SELECT 2 AS x) b ON a.x = b.x", "1|")]
    [TestCase("SELECT t.id, b.x FROM (SELECT 1 AS x) b RIGHT JOIN t ON t.id = b.x ORDER BY t.id", "1|1 ; 2| ; 3|")]
    [TestCase("SELECT ROW_NUMBER() OVER () FROM (SELECT 5 AS x) d", "1")]
    [TestCase("SELECT DISTINCT x FROM (SELECT 5 AS x) d", "5")]
    [TestCase("SELECT x FROM (SELECT 5 AS x) d UNION SELECT 6 ORDER BY 1", "5 ; 6")]
    [TestCase("SELECT x FROM (SELECT 5 AS x) d LIMIT 0", "")]
    [TestCase("SELECT id FROM t WHERE EXISTS (SELECT 1 LIMIT 0)", "")]
    [TestCase("SELECT id FROM t WHERE id IN (SELECT 1 LIMIT 0)", "")]
    [TestCase("SELECT id FROM t WHERE id IN (SELECT 1 WHERE EXISTS (SELECT 1 FROM t))", "1")]
    [TestCase("SELECT (SELECT 1 WHERE EXISTS (SELECT 1 FROM em))", "")]
    [TestCase("SELECT x.id FROM t x WHERE EXISTS (SELECT 1 FROM t y WHERE y.id = x.id AND x.id = 1)", "1")]
    [TestCase("SELECT x.id FROM t x WHERE x.id IN (SELECT x.id FROM t y)", "1 ; 2 ; 3")]
    [TestCase("SELECT id FROM em WHERE EXISTS (SELECT 1 WHERE em.id = 1)", "")]
    public async Task Statement(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo(expected));
    }

    /// <summary>
    /// The one-row source is a planned form only, and it must be the same object for one parse tree: the
    /// bound-query cache keys on the planned tree, so a new tree for each run would never hit that cache.
    /// </summary>
    [Test]
    public void OneRowSource_IsTheSameForOneParseTree()
    {
        NodeAst select = SQLParserProcessor.Parse("SELECT 5 AS x");

        Assert.That(select.rightAst, Is.Null);
        Assert.That(SelectQueryCreator.WithOneRowSource(select), Is.SameAs(SelectQueryCreator.WithOneRowSource(select)));
        Assert.That(select.rightAst, Is.Null, "the tree of the parser stays as written");
    }

    [Test]
    public async Task Parameters_EachRunReadsItsOwnValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        foreach ((string sql, string name, ColumnValue value, string expected) in new[]
        {
            ("SELECT x FROM (SELECT @p AS x) d", "@p", new ColumnValue(ColumnType.Integer64, 1), "1"),
            ("SELECT x FROM (SELECT @p AS x) d", "@p", new ColumnValue(ColumnType.Integer64, 2), "2"),
            ("SELECT x FROM (SELECT @p AS x) d", "@p", new ColumnValue(ColumnType.String, "a"), "a"),
            ("SELECT (SELECT @p)", "@p", new ColumnValue(ColumnType.Integer64, 3), "3"),
            ("SELECT (SELECT @p)", "@p", new ColumnValue(ColumnType.Integer64, 4), "4"),
            ("SELECT id FROM t WHERE id IN (SELECT @p)", "@p", new ColumnValue(ColumnType.Integer64, 1), "1"),
            ("SELECT id FROM t WHERE id IN (SELECT @p)", "@p", new ColumnValue(ColumnType.Integer64, 2), "2"),
            ("SELECT id FROM t WHERE id = (SELECT @p)", "@p", new ColumnValue(ColumnType.Integer64, 3), "3"),
            ("SELECT id FROM t WHERE id = 1 AND EXISTS (SELECT 1 WHERE @p)", "@p", new ColumnValue(ColumnType.Bool, true), "1"),
            ("SELECT id FROM t WHERE id = 1 AND EXISTS (SELECT 1 WHERE @p)", "@p", new ColumnValue(ColumnType.Bool, false), ""),
        })
        {
            Dictionary<string, ColumnValue> parameters = new() { [name] = value };

            Assert.That(await Run(database, executor, dbname, sql, parameters), Is.EqualTo(expected), sql);
        }
    }

    /// <summary>
    /// A write statement reaches a subquery through its own path, so each form must change the rows that
    /// PostgreSQL changes.
    /// </summary>
    [Test]
    public async Task Writes_ReadTheOneRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "UPDATE t SET v = (SELECT 9) WHERE id = 1");
        Assert.That(await Run(database, executor, dbname, "SELECT v FROM t WHERE id = 1"), Is.EqualTo("9"));

        await Commit(database, executor, dbname, "UPDATE t SET v = (SELECT 9 WHERE FALSE) WHERE id = 1");
        Assert.That(await Run(database, executor, dbname, "SELECT v FROM t WHERE id = 1"), Is.EqualTo(""));

        await Commit(database, executor, dbname, "UPDATE t SET v = 5 WHERE id = (SELECT 2)");
        Assert.That(await Run(database, executor, dbname, "SELECT v FROM t WHERE id = 2"), Is.EqualTo("5"));

        await Commit(database, executor, dbname, "INSERT INTO t (id, v, s) VALUES (9, (SELECT 9), 'x')");
        Assert.That(await Run(database, executor, dbname, "SELECT v, s FROM t WHERE id = 9"), Is.EqualTo("9|x"));

        await Commit(database, executor, dbname, "INSERT INTO t (id, v, s) SELECT x, x, 'y' FROM (SELECT 8 AS x) d");
        Assert.That(await Run(database, executor, dbname, "SELECT v, s FROM t WHERE id = 8"), Is.EqualTo("8|y"));

        await Commit(database, executor, dbname, "DELETE FROM t WHERE id IN (SELECT 9)");
        Assert.That(await Run(database, executor, dbname, "SELECT id FROM t WHERE id = 9"), Is.EqualTo(""));

        await Commit(database, executor, dbname, "DELETE FROM t WHERE NOT EXISTS (SELECT 1)");
        Assert.That(await Run(database, executor, dbname, "SELECT COUNT(*) FROM t"), Is.EqualTo("4"));

        await Commit(database, executor, dbname, "DELETE FROM t WHERE EXISTS (SELECT 1)");
        Assert.That(await Run(database, executor, dbname, "SELECT COUNT(*) FROM t"), Is.EqualTo("0"));
    }

    [Test]
    public async Task CreateTableAs_ReadsADerivedTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE TABLE tb AS SELECT x FROM (SELECT 5 AS x) d");

        Assert.That(await Run(database, executor, dbname, "SELECT x FROM tb"), Is.EqualTo("5"));
    }

    [TestCase("CREATE VIEW v AS SELECT 5 AS x", "SELECT x FROM v", "5")]
    [TestCase("CREATE VIEW v AS SELECT 5 AS x", "SELECT * FROM v", "5")]
    [TestCase("CREATE VIEW v AS SELECT 5 AS x", "SELECT v.x, t.id FROM t JOIN v ON t.id = v.x - 4", "5|1")]
    [TestCase("CREATE VIEW v AS SELECT 5 AS x WHERE FALSE", "SELECT x FROM v", "")]
    [TestCase("CREATE VIEW v AS SELECT 5 AS x LIMIT 0", "SELECT x FROM v", "")]
    [TestCase("CREATE VIEW v AS SELECT 1 AS a, 'b' AS b", "SELECT a, b FROM v", "1|b")]
    [TestCase("CREATE VIEW v AS SELECT (SELECT 7) AS x", "SELECT x FROM v", "7")]
    [TestCase("CREATE VIEW v AS SELECT id FROM t WHERE id IN (SELECT 1)", "SELECT id FROM v", "1")]
    [TestCase("CREATE VIEW v AS SELECT x FROM (SELECT 5 AS x) d", "SELECT x FROM v", "5")]
    [TestCase("CREATE VIEW v AS SELECT ROW_NUMBER() OVER () AS r", "SELECT r FROM v", "1")]
    [TestCase("CREATE VIEW v AS SELECT 5 AS a, 'x' AS b, TRUE AS d, NULL AS e", "SELECT a + 1, b || 'y', NOT d, e IS NULL FROM v", "6|xy|f|t")]
    public async Task View_WithNoFrom_CanBeRead(string create, string read, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, create);

        Assert.That(await Run(database, executor, dbname, read), Is.EqualTo(expected));
    }

    /// <summary>A body that CREATE VIEW refuses leaves no view, so no statement can find one that it cannot read.</summary>
    [TestCase("CREATE VIEW v AS SELECT *", "CADB0400")]
    [TestCase("CREATE VIEW v AS SELECT COUNT(*) AS c", "CADB0400")]
    [TestCase("CREATE VIEW v AS SELECT (SELECT COUNT(*)) AS c", "CADB0400")]
    [TestCase("CREATE VIEW v AS SELECT 5 AS x ORDER BY 1", "CADB0400")]
    [TestCase("CREATE VIEW v AS SELECT nosuch AS n", "CADB0404")]
    [TestCase("CREATE VIEW v AS SELECT 'a' + 1 AS n", "CADB0400")]
    [TestCase("CREATE VIEW v AS SELECT 1 AS a, 2 AS a", "CADB0400")]
    [TestCase("CREATE VIEW v AS SELECT nextval('sq') AS n", "CADB0547")]
    [TestCase("CREATE VIEW v AS SELECT (SELECT nextval('sq')) AS n", "CADB0547")]
    [TestCase("CREATE VIEW v AS SELECT id FROM t WHERE id = (SELECT nextval('sq'))", "CADB0547")]
    public async Task View_ThatCannotBeRead_IsRefused(string create, string code)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await TryCommit(database, executor, dbname, create), Is.EqualTo(code));
        Assert.That(await Run(database, executor, dbname, "SELECT * FROM v"), Is.EqualTo("CADB0011"));
    }

    /// <summary>
    /// CREATE VIEW binds a body with no FROM and does not evaluate it, so a division by zero in the body
    /// fails when a statement reads the view, as in PostgreSQL. A view over that view fails the same way.
    /// </summary>
    [Test]
    public async Task View_ABodyThatFails_FailsWhenRead()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW b1 AS SELECT 1 / 0 AS x");
        await Commit(database, executor, dbname, "CREATE VIEW b2 AS SELECT x FROM b1");

        Assert.That(await Run(database, executor, dbname, "SELECT x FROM b1"), Is.EqualTo("CADB0400"));
        Assert.That(await Run(database, executor, dbname, "SELECT x FROM b2"), Is.EqualTo("CADB0400"));
    }

    /// <summary>The stored body holds the statement as written and not a value from CREATE, so each read sees the rows of that read.</summary>
    [Test]
    public async Task View_WithASubquery_ReadsTheRowsOfEachRead()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW c AS SELECT (SELECT COUNT(*) FROM t) AS n");
        Assert.That(await Run(database, executor, dbname, "SELECT n FROM c"), Is.EqualTo("3"));

        await Commit(database, executor, dbname, "INSERT INTO t (id, v, s) VALUES (4, 40, 'd')");
        Assert.That(await Run(database, executor, dbname, "SELECT n FROM c"), Is.EqualTo("4"));
    }

    [Test]
    public async Task View_SurvivesAReopen_AndKeepsItsText()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW v AS SELECT 5 AS x");
        await Commit(database, executor, dbname, "CREATE VIEW w AS SELECT x + 1 AS y FROM v");
        Assert.That(await Run(database, executor, dbname, "SHOW CREATE VIEW v"), Does.Contain("SELECT 5 AS x"));

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        Assert.That(await Run(reopened, executor, dbname, "SELECT y FROM w"), Is.EqualTo("6"));
        Assert.That(await Run(reopened, executor, dbname, "SHOW CREATE VIEW v"), Does.Contain("SELECT 5 AS x"));
    }

    [Test]
    public async Task View_CanBeReplaced_WithTheSameShape()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW v AS SELECT id AS x FROM t WHERE id = 1");
        await Commit(database, executor, dbname, "CREATE OR REPLACE VIEW v AS SELECT 5 AS x");
        Assert.That(await Run(database, executor, dbname, "SELECT x FROM v"), Is.EqualTo("5"));

        await Commit(database, executor, dbname, "CREATE OR REPLACE VIEW v AS SELECT id AS x FROM t WHERE id = 2");
        Assert.That(await Run(database, executor, dbname, "SELECT x FROM v"), Is.EqualTo("2"));
    }

    /// <summary>A materialized view takes its columns from the bound form and its rows from the statement form, so the two must agree on each type.</summary>
    [Test]
    public async Task MaterializedView_KeepsTheTypes_AndRefreshes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE MATERIALIZED VIEW m AS SELECT 5 AS a, 'x' AS b, TRUE AS d");
        Assert.That(await Run(database, executor, dbname, "SELECT a + 1, b || 'y', NOT d FROM m"), Is.EqualTo("6|xy|f"));

        await Commit(database, executor, dbname, "CREATE MATERIALIZED VIEW c AS SELECT (SELECT COUNT(*) FROM t) AS n");
        Assert.That(await Run(database, executor, dbname, "SELECT n FROM c"), Is.EqualTo("3"));

        await Commit(database, executor, dbname, "INSERT INTO t (id, v, s) VALUES (4, 40, 'd')");
        await Commit(database, executor, dbname, "REFRESH MATERIALIZED VIEW c");
        Assert.That(await Run(database, executor, dbname, "SELECT n FROM c"), Is.EqualTo("4"));

        Assert.That(await TryCommit(database, executor, dbname, "CREATE MATERIALIZED VIEW z AS SELECT 1 / 0 AS x"), Is.EqualTo("CADB0400"));
    }

    /// <summary>
    /// A statement with no FROM reserves its sequence values before it evaluates anything, so a nested call
    /// is refused and cannot skip a value when a later clause decides that it does not run. The top-level
    /// form draws one value, as before.
    /// </summary>
    [TestCase("SELECT (SELECT nextval('sq'))")]
    [TestCase("SELECT (SELECT (SELECT nextval('sq')))")]
    [TestCase("SELECT (SELECT nextval('sq') LIMIT 0)")]
    [TestCase("SELECT (SELECT nextval('sq') WHERE FALSE)")]
    [TestCase("SELECT EXISTS (SELECT nextval('sq'))")]
    [TestCase("SELECT 1 IN (SELECT nextval('sq'))")]
    [TestCase("SELECT x FROM (SELECT nextval('sq') AS x) d")]
    [TestCase("SELECT id, (SELECT nextval('sq')) FROM t")]
    [TestCase("SELECT id FROM t WHERE id = (SELECT nextval('sq'))")]
    [TestCase("SELECT id FROM t WHERE id IN (SELECT nextval('sq'))")]
    [TestCase("SELECT id FROM t WHERE EXISTS (SELECT nextval('sq'))")]
    public async Task Sequence_InASubqueryWithNoFrom_IsRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo("CADB0547"));
    }

    [TestCase("UPDATE t SET v = (SELECT nextval('sq')) WHERE id = 1")]
    [TestCase("INSERT INTO t (id, v, s) VALUES (9, (SELECT nextval('sq')), 'x')")]
    [TestCase("DELETE FROM t WHERE id = (SELECT nextval('sq'))")]
    public async Task Sequence_InASubqueryOfAWrite_IsRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await TryCommit(database, executor, dbname, sql), Is.EqualTo("CADB0547"));
    }

    /// <summary>
    /// A correlated reference in a subquery with no FROM is not supported yet, and the error must say so or
    /// name the reference. The same scalar form with a FROM gives the same error.
    /// </summary>
    [TestCase("SELECT x.id FROM t x WHERE EXISTS (SELECT 1 WHERE x.id = 1)", "no FROM clause")]
    [TestCase("SELECT x.id FROM t x WHERE x.id IN (SELECT 1 WHERE x.id = 1)", "Correlated IN")]
    [TestCase("SELECT x.id FROM t x WHERE (SELECT x.id) = 1", "Unknown alias 'x'")]
    [TestCase("SELECT x.id FROM t x WHERE x.id IN (SELECT x.id)", "Unknown alias 'x'")]
    [TestCase("SELECT id FROM t WHERE (SELECT v) = 10", "Unknown column: v")]
    public async Task Correlated_InASubqueryWithNoFrom_NamesTheProblem(string sql, string message)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();
        KvTransaction tx = await database.Transactions.BeginAsync();

        CamusDBException? error = Assert.CatchAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await cursor.ToListAsync();
        });

        await database.Transactions.RollbackAsync(tx);
        Assert.That(error!.Message, Does.Contain(message));
    }

    /// <summary>The join operators read a one-row source through different paths, and each must give the same rows.</summary>
    [Test]
    public async Task Join_GivesTheSameRowsOnEveryPlanArm()
    {
        foreach (CamusDBOptions options in new[]
        {
            Options,
            Options with { HashJoinMaxBuildRows = 0 },
            Options with { SpillEnabled = true, ForceSpillThresholdRows = 1 },
            Options with { PlanCacheEnabled = true },
            Options with { BoundQueryCacheEnabled = false },
            Options with { CostBasedJoinOrderEnabled = false, CostBasedAccessPathEnabled = false },
        })
        {
            (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables(options);

            for (int run = 0; run < 2; run++)
            {
                Assert.That(await Run(database, executor, dbname, "SELECT t.id, d.x FROM t JOIN (SELECT 1 AS x) d ON t.id = d.x"), Is.EqualTo("1|1"));
                Assert.That(await Run(database, executor, dbname, "SELECT t.id, d.x FROM t LEFT JOIN (SELECT 1 AS x) d ON t.id = d.x ORDER BY t.id"), Is.EqualTo("1|1 ; 2| ; 3|"));
                Assert.That(await Run(database, executor, dbname, "SELECT t.id FROM t, (SELECT 2 AS x) d WHERE t.id = d.x"), Is.EqualTo("2"));
                Assert.That(await Run(database, executor, dbname, "SELECT id FROM t WHERE id IN (SELECT 3)"), Is.EqualTo("3"));
                Assert.That(await Run(database, executor, dbname, "SELECT id FROM t WHERE id NOT IN (SELECT 3) ORDER BY id"), Is.EqualTo("1 ; 2"));
            }
        }
    }
}
