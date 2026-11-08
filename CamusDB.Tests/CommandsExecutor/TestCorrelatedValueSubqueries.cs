/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A correlated scalar, <c>IN</c>, <c>NOT IN</c> or quantified subquery runs for each outer row, in the WHERE
/// and the select list of a SELECT, in the WHERE of an UPDATE or a DELETE, and in the SET values of an UPDATE.
/// A correlated <c>EXISTS</c> runs in the select list and the SET values too. Before, each form was refused
/// with <c>CADB0533</c>. Each expected value was measured on PostgreSQL 16.11 with the same rows.
///
/// <para>The tables: <c>r(id, v, s)</c> = (1, 0, 'a'), (2, 2, 'b'), (3, 0, NULL); <c>r2(id, w)</c> = (1, 10),
/// (2, 20); <c>d(id, v)</c> = (1, 1), (2, 2), (3, 3); <c>e(id, s)</c> is empty; the view <c>vw</c> is
/// <c>SELECT id, w FROM r2</c>. A boolean renders as <c>t</c> or <c>f</c>, as psql prints it.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestCorrelatedValueSubqueries : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE r (id INT64 PRIMARY KEY, v INT64, s STRING)", ddl: true);
        await Commit(database, executor, dbname, "INSERT INTO r (id, v, s) VALUES (1, 0, 'a'), (2, 2, 'b'), (3, 0, NULL)");
        await Commit(database, executor, dbname, "CREATE TABLE r2 (id INT64 PRIMARY KEY, w INT64)", ddl: true);
        await Commit(database, executor, dbname, "INSERT INTO r2 (id, w) VALUES (1, 10), (2, 20)");
        await Commit(database, executor, dbname, "CREATE TABLE d (id INT64 PRIMARY KEY, v INT64)", ddl: true);
        await Commit(database, executor, dbname, "INSERT INTO d (id, v) VALUES (1, 1), (2, 2), (3, 3)");
        await Commit(database, executor, dbname, "CREATE TABLE e (id INT64 PRIMARY KEY, s STRING)", ddl: true);
        await Commit(database, executor, dbname, "CREATE VIEW vw AS SELECT id, w FROM r2", ddl: true);

        return (dbname, database, executor);
    }

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, bool ddl = false)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        if (ddl)
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        else
            _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

        await database.Transactions.CommitAsync(tx);
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "",
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Float64 => value.FloatValue.ToString("R", CultureInfo.InvariantCulture),
        ColumnType.Bool => value.BoolValue ? "t" : "f",
        _ => value.StrValue ?? "",
    };

    /// <summary>Each row as its columns joined with <c>|</c>, the rows sorted and joined with <c> ; </c>.</summary>
    private static async Task<List<string>> ReadRows(CommandExecutor executor, KvTransaction tx, string dbname, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
        List<QueryResultRow> rows = await cursor.ToListAsync();

        return rows
            .Select(r => string.Join("|", r.Row.Values.Select(Render)))
            .OrderBy(r => r, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>The rows of <paramref name="sql"/>, or the code and message of the error.</summary>
    private static async Task<string> Query(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            return string.Join(" ; ", await ReadRows(executor, tx, dbname, sql, parameters));
        }
        catch (CamusDBException e)
        {
            return e.Code + ": " + e.Message;
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    /// <summary>
    /// Runs <paramref name="write"/> and then <paramref name="read"/> in one transaction, and rolls back.
    /// Returns the rows of the read, or the code and message of the error.
    /// </summary>
    private static async Task<string> WriteThenRead(DatabaseDescriptor database, CommandExecutor executor, string dbname, string write, string read)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, write, null));
            return string.Join(" ; ", await ReadRows(executor, tx, dbname, read, null));
        }
        catch (CamusDBException e)
        {
            return e.Code + ": " + e.Message;
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    [TestCase("SELECT id, (SELECT w FROM r2 WHERE r2.id = r.id) FROM r", "1|10 ; 2|20 ; 3|", TestName = "SelectList_Scalar")]
    [TestCase("SELECT id, (SELECT w FROM r2 WHERE w = v * 10) FROM r", "1| ; 2|20 ; 3|", TestName = "SelectList_ABareOuterName")]
    [TestCase("SELECT id, (SELECT COUNT(*) FROM d WHERE d.v <= r.id) FROM r", "1|1 ; 2|2 ; 3|3", TestName = "SelectList_AnAggregateBody")]
    [TestCase("SELECT id, EXISTS (SELECT 1 FROM r2 WHERE r2.id = r.id) FROM r", "1|t ; 2|t ; 3|f", TestName = "SelectList_Exists")]
    [TestCase("SELECT id, EXISTS (SELECT 1 FROM r2 WHERE w = v + 10) FROM r", "1|t ; 2|f ; 3|t", TestName = "SelectList_ExistsWithABareOuterName")]
    [TestCase("SELECT id, id IN (SELECT d.v FROM d WHERE d.v > r.v) FROM r", "1|t ; 2|f ; 3|t", TestName = "SelectList_In")]
    [TestCase("SELECT id, 'a' NOT IN (SELECT s FROM r i WHERE i.id >= r.id) FROM r", "1|f ; 2| ; 3|", TestName = "SelectList_NotInWithANull")]
    [TestCase("SELECT id, id < ALL (SELECT v FROM d WHERE d.id > r.id) FROM r", "1|t ; 2|t ; 3|t", TestName = "SelectList_AllOverNoRow")]
    [TestCase("SELECT id, 2 = ANY (SELECT d.v FROM d WHERE d.id <= r.id) FROM r", "1|f ; 2|t ; 3|t", TestName = "SelectList_Any")]
    [TestCase("SELECT id, (SELECT MAX(w) FROM r2 WHERE r2.id <= r.id) + id FROM r", "1|11 ; 2|22 ; 3|23", TestName = "SelectList_InAnExpression")]
    [TestCase("SELECT a.id, (SELECT w FROM r2 WHERE r2.id = b.v) FROM r a JOIN d b ON a.id = b.id", "1|10 ; 2|20 ; 3|", TestName = "SelectList_OverAJoin")]
    [TestCase("SELECT DISTINCT (SELECT COUNT(*) FROM r2 WHERE r2.id = r.id) FROM r", "0 ; 1", TestName = "SelectList_Distinct")]
    [TestCase("SELECT id, (SELECT w FROM r2 WHERE r2.id = r.id) FROM r ORDER BY id DESC LIMIT 2", "2|20 ; 3|", TestName = "SelectList_OrderByAndLimit")]
    [TestCase("SELECT id, (SELECT (SELECT MAX(v) FROM d WHERE d.v < r2.w / 5) FROM r2 WHERE r2.id = r.id) FROM r", "1|1 ; 2|3 ; 3|", TestName = "SelectList_NestedReadsTheBody")]
    [TestCase("SELECT id, (SELECT COUNT(*) FROM r2 WHERE r2.id IN (SELECT d.id FROM d WHERE d.v <= r.id)) FROM r", "1|1 ; 2|2 ; 3|2", TestName = "SelectList_NestedReadsTheOuterRow")]
    [TestCase("SELECT id, COALESCE((SELECT w FROM r2 WHERE r2.id = r.id), -1) FROM r", "1|10 ; 2|20 ; 3|-1", TestName = "SelectList_Coalesce")]
    [TestCase("SELECT id, (ARRAY[(SELECT w FROM r2 WHERE r2.id = r.id), 1])[1] FROM r", "1|10 ; 2|20 ; 3|", TestName = "SelectList_AnArrayElement")]
    [TestCase("SELECT id, CASE WHEN v = 0 THEN (SELECT w FROM r2 WHERE r2.id = r.id) ELSE 0 END FROM r", "1|10 ; 2|0 ; 3|", TestName = "SelectList_Case")]
    [TestCase("SELECT x FROM (SELECT (SELECT w FROM r2 WHERE r2.id = r.id) AS x FROM r) t WHERE x IS NOT NULL", "10 ; 20", TestName = "SelectList_InADerivedTable")]
    [TestCase("SELECT id, (SELECT k FROM (SELECT w AS k FROM r2 WHERE r2.id = r.id) x) FROM r", "1|10 ; 2|20 ; 3|", TestName = "SelectList_ABareNameOverADerivedTableThatReadsTheOuterRow")]
    [TestCase("SELECT id, (SELECT w FROM (SELECT * FROM r2 WHERE r2.id = r.id) x) FROM r", "1|10 ; 2|20 ; 3|", TestName = "SelectList_ABareNameOverAStarThatReadsTheOuterRow")]
    [TestCase("SELECT id, (SELECT id FROM (SELECT * FROM r2 WHERE r2.w = r.v * 5) x) FROM r", "1| ; 2|1 ; 3|", TestName = "SelectList_ABareNameOfBothQueriesIsTheDerivedColumn")]
    [TestCase("SELECT id, CASE WHEN v = 2 THEN (SELECT w FROM r2 WHERE w > r.v * 6) ELSE 0 END FROM r", "1|0 ; 2|20 ; 3|0", TestName = "SelectList_AnErrorInABranchThatDoesNotRun")]
    [TestCase("SELECT id, COALESCE(id, (SELECT w FROM r2 WHERE w > r.v)) FROM r", "1|1 ; 2|2 ; 3|3", TestName = "SelectList_AnErrorAfterTheFirstValue")]
    [TestCase("SELECT id, (SELECT w FROM r2 WHERE r2.id = r.id) AS q FROM r WHERE id < 3", "1|10 ; 2|20", TestName = "SelectList_WithAnAlias")]
    [TestCase("SELECT id, (SELECT w FROM vw WHERE vw.id = r.id) FROM r", "1|10 ; 2|20 ; 3|", TestName = "SelectList_AView")]
    [TestCase("SELECT id, (SELECT g FROM generate_series(1, 3) AS g WHERE g = r.id + 1) FROM r", "1|2 ; 2|3 ; 3|", TestName = "SelectList_ASeries")]
    [TestCase("SELECT id, (SELECT w FROM r2 WHERE r2.id = r.id UNION SELECT 99 WHERE r.id = 3) FROM r", "1|10 ; 2|20 ; 3|99", TestName = "SelectList_ASetOperation")]
    [TestCase("SELECT id, (SELECT SUM(w) FROM r2 HAVING SUM(w) > r.id * 10) FROM r", "1|30 ; 2|30 ; 3|", TestName = "SelectList_Having")]
    [TestCase("SELECT id, (SELECT rank() OVER (ORDER BY w) FROM r2 WHERE r2.id = r.id) FROM r", "1|1 ; 2|1 ; 3|", TestName = "SelectList_AWindowInTheBody")]
    [TestCase("SELECT id, (SELECT MAX(rk) FROM (SELECT rank() OVER (ORDER BY w) AS rk FROM r2) x) FROM r", "1|2 ; 2|2 ; 3|2", TestName = "SelectList_AnUncorrelatedWindowInADerivedTable")]
    [TestCase("SELECT id, (SELECT MAX(rk) FROM (SELECT rank() OVER (ORDER BY w) AS rk FROM r2 WHERE r2.id <= r.id) x) FROM r", "1|1 ; 2|2 ; 3|2", TestName = "SelectList_ACorrelatedWindowInADerivedTable")]
    [TestCase("SELECT id, (SELECT w FROM r2 WHERE w >= r.v ORDER BY w LIMIT 1 OFFSET 1) FROM r", "1|20 ; 2|20 ; 3|20", TestName = "SelectList_LimitAndOffset")]
    [TestCase("SELECT id, rank() OVER (ORDER BY id), (SELECT w FROM r2 WHERE r2.id = r.id) FROM r", "1|1|10 ; 2|2|20 ; 3|3|", TestName = "SelectList_BesideAWindowFunction")]
    [TestCase("SELECT id, rank() OVER (ORDER BY (SELECT COUNT(*) FROM d WHERE d.v >= r.id)) FROM r", "1|3 ; 2|2 ; 3|1", TestName = "SelectList_InTheOrderOfAWindow")]
    [TestCase("SELECT id, (SELECT w FROM r2 WHERE r2.id = r.id) + rank() OVER (ORDER BY id) FROM r", "1|11 ; 2|22 ; 3|", TestName = "SelectList_WithAWindowFunction")]
    public async Task ASubqueryInTheSelectList_RunsForEachOuterRow(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expected, await Query(database, executor, dbname, sql), sql);
    }

    [TestCase("SELECT id FROM r WHERE (SELECT w FROM r2 WHERE r2.id = r.id) = 20", "2", TestName = "Where_Scalar")]
    [TestCase("SELECT id FROM r WHERE v < (SELECT COUNT(*) FROM d WHERE d.v > r.id)", "1", TestName = "Where_AnAggregateBody")]
    [TestCase("SELECT id FROM r WHERE id IN (SELECT r2.id FROM r2 WHERE r2.w > r.v * 6)", "1 ; 2", TestName = "Where_In")]
    [TestCase("SELECT id FROM r WHERE id NOT IN (SELECT r2.id FROM r2 WHERE r2.w > r.v * 6)", "3", TestName = "Where_NotIn")]
    [TestCase("SELECT id FROM r WHERE 'z' NOT IN (SELECT s FROM r i WHERE i.id >= r.id)", "", TestName = "Where_NotInWithANull")]
    [TestCase("SELECT id FROM r WHERE 'z' NOT IN (SELECT s FROM r i WHERE i.id > r.id + 5)", "1 ; 2 ; 3", TestName = "Where_NotInOverNoRow")]
    [TestCase("SELECT id FROM r WHERE id >= ALL (SELECT d.v FROM d WHERE d.v <= r.id + v)", "3", TestName = "Where_AllABareNameIsTheNearestColumn")]
    [TestCase("SELECT id FROM r WHERE 20 = ANY (SELECT w FROM r2 WHERE r2.id <= r.id)", "2 ; 3", TestName = "Where_Any")]
    [TestCase("SELECT id FROM r WHERE id = 3 OR (SELECT w FROM r2 WHERE r2.id = r.id) = 10", "1 ; 3", TestName = "Where_UnderAnOr")]
    [TestCase("SELECT a.id FROM r a JOIN d b ON a.id = b.id WHERE (SELECT COUNT(*) FROM r2 WHERE r2.id <= b.v) = 2", "2 ; 3", TestName = "Where_OverAJoin")]
    [TestCase("SELECT id FROM r WHERE id IN (SELECT id FROM d WHERE d.v = r.v)", "2", TestName = "Where_InWithAnIndexedInnerColumn")]
    [TestCase("SELECT id FROM r WHERE (SELECT w FROM r2 WHERE r2.id = r.id) IN (SELECT w FROM r2 WHERE r2.id >= r.id)", "1 ; 2", TestName = "Where_InWithACorrelatedTestedValue")]
    [TestCase("SELECT id FROM r WHERE CASE WHEN v = 2 THEN (SELECT w FROM r2 WHERE w > r.v * 6) = 20 ELSE FALSE END", "2", TestName = "Where_AnErrorInABranchThatDoesNotRun")]
    [TestCase("SELECT id FROM r WHERE (SELECT COUNT(*) FROM r2 WHERE w > r.v * 6) = 1 AND EXISTS (SELECT 1 FROM d WHERE d.id = r.id)", "2", TestName = "Where_BesideAnExists")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM (SELECT rank() OVER (ORDER BY w) AS rk FROM r2 WHERE r2.id <= r.id) x WHERE rk = 2)", "2 ; 3", TestName = "Where_AnExistsOverAWindowInADerivedTable")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT k FROM (SELECT id AS k FROM r2 WHERE r2.id = o.id) x)", "1 ; 2", TestName = "Where_AnExistsWithABareNameOverADerivedTable")]
    public async Task ASubqueryInTheWhere_RunsForEachOuterRow(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expected, await Query(database, executor, dbname, sql), sql);
    }

    /// <summary>A statement parameter reaches the body and the outer query alike.</summary>
    [Test]
    public async Task AStatementParameter_ReachesTheBody()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Dictionary<string, ColumnValue> parameters = new() { ["@m"] = new ColumnValue(ColumnType.Integer64, 15) };

        Assert.AreEqual(
            "1| ; 2|20 ; 3|",
            await Query(database, executor, dbname, "SELECT id, (SELECT w FROM r2 WHERE r2.id = r.id AND w > @m) FROM r", parameters));
    }

    /// <summary>
    /// The binding records the result type of a <c>COALESCE</c> from the types of its arguments, and the type of
    /// a correlated scalar is one of them, so each row is a FLOAT64, also a row where the subquery gives an INT64.
    /// PostgreSQL gives a numeric column.
    /// </summary>
    [Test]
    public async Task ACoalesce_KeepsTheTypeOfTheWholeExpression()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, dbname, "SELECT COALESCE((SELECT w FROM r2 WHERE r2.id = r.id), 0.5) AS x FROM r", null));

            List<ColumnValue> values = (await cursor.ToListAsync()).Select(r => r.Row.Values.Single()).ToList();

            Assert.That(values.Select(v => v.Type), Is.All.EqualTo(ColumnType.Float64));
            Assert.That(values.Select(v => v.FloatValue).OrderBy(v => v), Is.EqualTo(new[] { 0.5, 10.0, 20.0 }));
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    /// <summary>A scalar subquery that returns a second row fails, as in PostgreSQL.</summary>
    [TestCase("SELECT id, (SELECT w FROM r2 WHERE w > r.v) FROM r", TestName = "TooManyRows_InTheSelectList")]
    [TestCase("SELECT id FROM r WHERE (SELECT w FROM r2 WHERE w > r.v) = 10", TestName = "TooManyRows_InTheWhere")]
    [TestCase("SELECT id FROM r WHERE v = 2 AND (SELECT w FROM r2 WHERE w > r.v) = 20", TestName = "TooManyRows_AfterAnAnd")]
    public async Task ASecondRow_FailsTheStatement(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual("CADB0400: Scalar subquery returned more than one row", await Query(database, executor, dbname, sql), sql);
    }

    /// <summary>
    /// The body and the expression around it are checked when the statement binds, so each error appears over
    /// an empty outer table too, as in PostgreSQL.
    /// </summary>
    [TestCase("SELECT id FROM e WHERE (SELECT w FROM r2 WHERE r2.id = e.id) = e.s", "CADB0400", TestName = "Error_AComparisonWithTheColumnType")]
    [TestCase("SELECT id FROM e WHERE e.s IN (SELECT w FROM r2 WHERE r2.id = e.id)", "CADB0400", TestName = "Error_AnInWithTheColumnType")]
    [TestCase("SELECT id FROM e WHERE e.s NOT IN (SELECT w FROM r2 WHERE r2.id = e.id)", "CADB0400", TestName = "Error_ANotInWithTheColumnType")]
    [TestCase("SELECT id FROM e WHERE id = ANY (SELECT s FROM r i WHERE i.id = e.id)", "CADB0400", TestName = "Error_AnAnyWithTheColumnType")]
    [TestCase("SELECT (SELECT w FROM r2 WHERE r2.id = e.id) + e.s FROM e", "CADB0400", TestName = "Error_AnArithmeticWithTheColumnType")]
    [TestCase("SELECT (SELECT w, id FROM r2 WHERE r2.id = e.id) FROM e", "CADB0400", TestName = "Error_TwoColumns")]
    [TestCase("SELECT (SELECT nope FROM r2 WHERE r2.id = e.id) FROM e", "CADB0404", TestName = "Error_AnUnknownColumn")]
    public async Task AMistake_FailsWithNoOuterRow(string sql, string code)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        string result = await Query(database, executor, dbname, sql);

        Assert.That(result, Does.StartWith(code + ":"), sql);
    }

    /// <summary>
    /// The WHERE of a DELETE or an UPDATE and the SET values of an UPDATE run a correlated subquery for each row of
    /// the target table. The subquery reads the rows as the statement found them, also when it reads the target
    /// table: a row that the statement changes is still as it was for the subquery of a later row.
    /// </summary>
    [TestCase("UPDATE r SET v = (SELECT w FROM r2 WHERE r2.id = r.id) WHERE id > 0", "SELECT id, v FROM r", "1|10 ; 2|20 ; 3|", TestName = "Update_AScalarSetValue")]
    [TestCase("UPDATE d SET v = (SELECT MAX(i.v) FROM d i WHERE i.id < d.id) WHERE id > 0", "SELECT id, v FROM d", "1| ; 2|1 ; 3|2", TestName = "Update_TheSetValueReadsTheTargetAsItWas")]
    [TestCase("UPDATE r SET s = CASE WHEN EXISTS (SELECT 1 FROM r2 WHERE r2.id = r.id) THEN 'y' ELSE 'n' END WHERE id > 0", "SELECT id, s FROM r", "1|y ; 2|y ; 3|n", TestName = "Update_AnExistsInASetValue")]
    [TestCase("UPDATE r SET v = CASE WHEN id IN (SELECT d.v - 1 FROM d WHERE d.id > r.id) THEN 1 ELSE 0 END WHERE id > 0", "SELECT id, v FROM r", "1|1 ; 2|1 ; 3|0", TestName = "Update_AnInInASetValue")]
    [TestCase("UPDATE r SET s = 'w' WHERE (SELECT w FROM r2 WHERE r2.id = r.id) = 10", "SELECT id, s FROM r", "1|w ; 2|b ; 3|", TestName = "Update_AScalarInTheWhere")]
    [TestCase("UPDATE d SET v = v + (SELECT COUNT(*) FROM d i WHERE i.v > d.v) WHERE id > 0", "SELECT id, v FROM d", "1|3 ; 2|3 ; 3|3", TestName = "Update_TheSetValueReadsTheRowToo")]
    [TestCase("UPDATE r SET v = CASE WHEN id = 2 THEN (SELECT w FROM r2 WHERE w > r.v * 6) ELSE v END WHERE id > 0", "SELECT id, v FROM r", "1|0 ; 2|20 ; 3|0", TestName = "Update_AnErrorInABranchThatDoesNotRun")]
    [TestCase("UPDATE r SET v = (SELECT w FROM r2 WHERE r2.id = r.id) WHERE id IN (SELECT r2.id FROM r2 WHERE r2.w = r.v * 10 + 10)", "SELECT id, v FROM r", "1|10 ; 2|2 ; 3|0", TestName = "Update_InTheWhereAndInASetValue")]
    [TestCase("DELETE FROM r WHERE v = (SELECT COUNT(*) FROM r2 WHERE r2.id < r.id)", "SELECT id FROM r", "2 ; 3", TestName = "Delete_AScalarInTheWhere")]
    [TestCase("DELETE FROM d WHERE v IN (SELECT i.v + 1 FROM d i WHERE i.id < d.id)", "SELECT id FROM d", "1", TestName = "Delete_TheInReadsTheTargetAsItWas")]
    [TestCase("INSERT INTO e (id, s) SELECT id, (SELECT CAST(w AS STRING) FROM r2 WHERE r2.id = r.id) FROM r", "SELECT id, s FROM e", "1|10 ; 2|20 ; 3|", TestName = "InsertSelect_ASubqueryInTheSelectList")]
    public async Task AWrite_RunsTheSubqueryForEachRow(string write, string read, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expected, await WriteThenRead(database, executor, dbname, write, read), write);
    }

    /// <summary>A SET value that returns a second row fails, and the update writes nothing.</summary>
    [Test]
    public async Task ASecondRowInASetValue_FailsTheUpdate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(
            "CADB0400: Scalar subquery returned more than one row",
            await WriteThenRead(database, executor, dbname, "UPDATE r SET v = (SELECT w FROM r2 WHERE w > r.v) WHERE id > 0", "SELECT id, v FROM r"));

        Assert.AreEqual("1|0 ; 2|2 ; 3|0", await Query(database, executor, dbname, "SELECT id, v FROM r"));
    }

    /// <summary>A SET value whose type the column does not accept fails with no row to update, as in PostgreSQL.</summary>
    [Test]
    public async Task AWrongSetValueType_FailsWithNoRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE TABLE f (id INT64 PRIMARY KEY, n INT64)", ddl: true);

        string result = await WriteThenRead(database, executor, dbname, "UPDATE f SET n = (SELECT s FROM r i WHERE i.id = f.id) WHERE id > 0", "SELECT id FROM f");

        Assert.That(result, Does.StartWith("CADB0401:"));
    }

    /// <summary>A view whose select list holds a correlated subquery runs it for each row of the view.</summary>
    [Test]
    public async Task AView_RunsItsSelectListSubquery()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW vq AS SELECT id, (SELECT w FROM r2 WHERE r2.id = r.id) AS w FROM r", ddl: true);

        Assert.AreEqual("1|10 ; 2|20 ; 3|", await Query(database, executor, dbname, "SELECT id, w FROM vq"));
    }

    /// <summary>
    /// <c>CREATE TABLE … AS SELECT</c> and <c>CREATE MATERIALIZED VIEW</c> store the value of a correlated
    /// select-list subquery for each row, in a column of the subquery's type. PostgreSQL gives the rows
    /// 1|10, 2|20, 3| and the column type bigint for both.
    /// </summary>
    [TestCase("CREATE TABLE c1 AS SELECT id, (SELECT w FROM r2 WHERE r2.id = r.id) AS w FROM r", "c1")]
    [TestCase("CREATE MATERIALIZED VIEW c1 AS SELECT id, (SELECT w FROM r2 WHERE r2.id = r.id) AS w FROM r", "c1")]
    public async Task ACreatedRelation_StoresTheValueOfEachRow(string ddl, string name)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, ddl, ddl: true);

        Assert.AreEqual("1|10 ; 2|20 ; 3|", await Query(database, executor, dbname, $"SELECT id, w FROM {name}"));

        TableSchema schema = (await executor.OpenDatabase(dbname)).Schema.Tables[name];
        Assert.AreEqual(ColumnType.Integer64, schema.Columns!.Single(c => c.Name == "w").Type);
    }

    /// <summary>
    /// A refresh runs the correlated subquery again for each row, so the view reads the new values.
    /// PostgreSQL gives 1|10, 2|20, 3| before the refresh and 1|10, 2|30, 3|40 after it.
    /// </summary>
    [Test]
    public async Task AMaterializedViewRefresh_RunsTheSubqueryAgain()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname,
            "CREATE MATERIALIZED VIEW c1 AS SELECT id, (SELECT w FROM r2 WHERE r2.id = r.id) AS w FROM r", ddl: true);
        await Commit(database, executor, dbname, "UPDATE r2 SET w = 30 WHERE id = 2");
        await Commit(database, executor, dbname, "INSERT INTO r2 (id, w) VALUES (3, 40)");

        Assert.AreEqual("1|10 ; 2|20 ; 3|", await Query(database, executor, dbname, "SELECT id, w FROM c1"));

        await Commit(database, executor, dbname, "REFRESH MATERIALIZED VIEW c1");

        Assert.AreEqual("1|10 ; 2|30 ; 3|40", await Query(database, executor, dbname, "SELECT id, w FROM c1"));
    }

    /// <summary>
    /// The clauses that have no per-row stage refuse a correlated subquery, with the clause in the message.
    /// PostgreSQL runs each of these; each is a listed difference.
    /// </summary>
    [TestCase("SELECT v FROM r GROUP BY v HAVING (SELECT COUNT(*) FROM d WHERE d.v > r.v) > 0", "HAVING", TestName = "Refused_Having")]
    [TestCase("SELECT a.id FROM r a JOIN d b ON b.id = (SELECT MIN(r2.id) FROM r2 WHERE r2.w > a.v * 6)", "a JOIN ON clause", TestName = "Refused_JoinOn")]
    [TestCase("SELECT v, (SELECT COUNT(*) FROM d WHERE d.v > r.v) FROM r GROUP BY v", "the select list of a query that groups or aggregates", TestName = "Refused_GroupedSelectList")]
    [TestCase("SELECT COUNT(*), (SELECT COUNT(*) FROM d WHERE d.v > MAX(r.v)) FROM r", "the select list of a query that groups or aggregates", TestName = "Refused_AggregateSelectList")]
    public async Task AClauseWithNoPerRowStage_RefusesIt(string sql, string clause)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        string result = await Query(database, executor, dbname, sql);

        Assert.That(result, Does.StartWith("CADB0533:").And.Contain("not supported in " + clause + " yet"), sql);
    }

    /// <summary>ON CONFLICT DO UPDATE has no locate scan, so a correlated SET value is refused and nothing is written.</summary>
    [Test]
    public async Task OnConflictDoUpdate_RefusesACorrelatedSetValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        string result = await WriteThenRead(
            database,
            executor,
            dbname,
            "INSERT INTO r (id, v, s) VALUES (1, 5, 'z') ON CONFLICT (id) DO UPDATE SET v = (SELECT w FROM r2 WHERE r2.id = r.id)",
            "SELECT id, v FROM r");

        Assert.That(result, Does.StartWith("CADB0533:").And.Contain("ON CONFLICT DO UPDATE"));
    }
}
