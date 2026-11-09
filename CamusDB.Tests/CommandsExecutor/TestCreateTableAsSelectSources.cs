
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
/// <c>CREATE TABLE … AS SELECT</c> and <c>INSERT … SELECT</c> read a view as a source, and a scalar subquery
/// column that is NULL because its table is empty keeps the declared type of the subquery column. Each expected
/// value was measured on PostgreSQL 16.11 with the same rows.
///
/// <para>The tables: <c>t(id, v)</c> = (1, 0), (2, 2); <c>r2(id, w)</c> = (1, 10); <c>em(id, v)</c> is empty.
/// The views: <c>c</c> = <c>SELECT id, v FROM t</c>; <c>cc</c> = <c>SELECT id, v FROM c WHERE v &gt; 0</c>, a view
/// over a view; <c>cs</c> has a correlated subquery column; <c>cu</c> has an uncorrelated subquery column; the
/// materialized view <c>cm</c> = <c>SELECT id, v * 10 AS v FROM t</c>.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestCreateTableAsSelectSources : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)", ddl: true);
        await Commit(database, executor, dbname, "INSERT INTO t (id, v) VALUES (1, 0), (2, 2)");
        await Commit(database, executor, dbname, "CREATE TABLE r2 (id INT64 PRIMARY KEY, w INT64)", ddl: true);
        await Commit(database, executor, dbname, "INSERT INTO r2 (id, w) VALUES (1, 10)");
        await Commit(database, executor, dbname, "CREATE TABLE em (id INT64 PRIMARY KEY, v INT64)", ddl: true);
        await Commit(database, executor, dbname, "CREATE VIEW c AS SELECT id, v FROM t", ddl: true);
        await Commit(database, executor, dbname, "CREATE VIEW cc AS SELECT id, v FROM c WHERE v > 0", ddl: true);
        await Commit(database, executor, dbname, "CREATE VIEW cs AS SELECT id, (SELECT w FROM r2 WHERE r2.id = t.id) AS w FROM t", ddl: true);
        await Commit(database, executor, dbname, "CREATE VIEW cu AS SELECT id, (SELECT MAX(w) FROM r2) AS w FROM t", ddl: true);
        await Commit(database, executor, dbname, "CREATE MATERIALIZED VIEW cm AS SELECT id, v * 10 AS v FROM t", ddl: true);

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

    /// <summary>Runs <paramref name="sql"/> and commits it. Returns "ok", or the code and message of the error.</summary>
    private static async Task<string> TryCommit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, bool ddl = false)
    {
        try
        {
            await Commit(database, executor, dbname, sql, ddl);
            return "ok";
        }
        catch (CamusDBException e)
        {
            return e.Code + ": " + e.Message;
        }
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "",
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        _ => value.StrValue ?? "",
    };

    /// <summary>Each row as its columns joined with <c>|</c>, the rows sorted and joined with <c> ; </c>.</summary>
    private static async Task<string> Query(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();

            return string.Join(" ; ", rows
                .Select(r => string.Join("|", r.Row.Values.Select(Render)))
                .OrderBy(r => r, StringComparer.Ordinal));
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    private static async Task<ColumnType> ColumnTypeOf(CommandExecutor executor, string dbname, string table, string column)
    {
        TableSchema schema = (await executor.OpenDatabase(dbname)).Schema.Tables[table];
        return schema.Columns!.Single(c => c.Name == column).Type;
    }

    /// <summary>
    /// A view of each form is a source that the statement reads. PostgreSQL copies the rows that the view gives.
    /// Before, each form failed with <c>CADB0525 'c' is a view and cannot be written to</c>.
    /// </summary>
    [TestCase("SELECT id, v FROM c", "SELECT id, v FROM z", "1|0 ; 2|2", TestName = "CreateTableAs_FromAView")]
    [TestCase("SELECT id, v FROM cc", "SELECT id, v FROM z", "2|2", TestName = "CreateTableAs_FromAViewOverAView")]
    [TestCase("SELECT id, w FROM cs", "SELECT id, w FROM z", "1|10 ; 2|", TestName = "CreateTableAs_FromAViewWithACorrelatedSubqueryColumn")]
    [TestCase("SELECT id, w FROM cu", "SELECT id, w FROM z", "1|10 ; 2|10", TestName = "CreateTableAs_FromAViewWithAnUncorrelatedSubqueryColumn")]
    [TestCase("SELECT id, v FROM cm", "SELECT id, v FROM z", "1|0 ; 2|20", TestName = "CreateTableAs_FromAMaterializedView")]
    [TestCase("SELECT c.id, c.v, r2.w FROM c JOIN r2 ON r2.id = c.id", "SELECT id, v, w FROM z", "1|0|10", TestName = "CreateTableAs_FromAViewJoinedToATable")]
    [TestCase("SELECT id, v FROM t WHERE id IN (SELECT id FROM c WHERE v > 0)", "SELECT id, v FROM z", "2|2", TestName = "CreateTableAs_WithAViewInASubquery")]
    public async Task CreateTableAs_CopiesTheRowsOfAViewSource(string select, string read, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual("ok", await TryCommit(database, executor, dbname, "CREATE TABLE z AS " + select, ddl: true));
        Assert.AreEqual(expected, await Query(database, executor, dbname, read));
    }

    /// <summary>
    /// <c>WITH NO DATA</c> from a view creates the table with the view's columns and no rows, as in PostgreSQL.
    /// </summary>
    [Test]
    public async Task CreateTableAs_WithNoData_FromAView_CreatesAnEmptyTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual("ok", await TryCommit(database, executor, dbname, "CREATE TABLE z AS SELECT id, v FROM c WITH NO DATA", ddl: true));
        Assert.AreEqual("", await Query(database, executor, dbname, "SELECT id, v FROM z"));
        Assert.AreEqual(ColumnType.Integer64, await ColumnTypeOf(executor, dbname, "z", "v"));
    }

    /// <summary>
    /// <c>INSERT … SELECT</c> reads a view as its source. PostgreSQL inserts the 2 rows.
    /// </summary>
    [Test]
    public async Task InsertSelect_FromAView_CopiesItsRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE TABLE ins (id INT64 PRIMARY KEY, v INT64)", ddl: true);

        Assert.AreEqual("ok", await TryCommit(database, executor, dbname, "INSERT INTO ins (id, v) SELECT id, v FROM c"));
        Assert.AreEqual("1|0 ; 2|2", await Query(database, executor, dbname, "SELECT id, v FROM ins"));
    }

    /// <summary>
    /// A write to a view is still refused: CamusDB has no updatable views. This differs from PostgreSQL, where a
    /// simple view is updatable. The view and its table keep their rows.
    /// </summary>
    [Test]
    public async Task InsertIntoAView_IsStillRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        string result = await TryCommit(database, executor, dbname, "INSERT INTO c (id, v) VALUES (9, 9)");

        StringAssert.StartsWith(CamusDBErrorCodes.ViewNotUpdatable + ": 'c' is a view and cannot be written to", result);
        Assert.AreEqual("1|0 ; 2|2", await Query(database, executor, dbname, "SELECT id, v FROM t"));
    }

    /// <summary>
    /// A scalar subquery over an empty table gives NULL in each row, and the column takes the declared type of
    /// the subquery column. PostgreSQL stores a <c>bigint</c> column with a NULL in each row. Before, CamusDB
    /// refused the statement because the NULL value had no type.
    /// </summary>
    [TestCase("CREATE TABLE a AS SELECT id, (SELECT MAX(v) FROM em) AS x FROM t", "SELECT id, x FROM a", "1| ; 2|", TestName = "CreateTableAs_AnAggregateOverAnEmptyTable_StoresAnInt64Column")]
    [TestCase("CREATE TABLE a AS SELECT x FROM (SELECT (SELECT MAX(v) FROM em) AS x FROM t) d", "SELECT COUNT(*), COUNT(x) FROM a", "2|0", TestName = "CreateTableAs_ThroughADerivedTable_StoresAnInt64Column")]
    [TestCase("CREATE TABLE a AS SELECT (SELECT v FROM em WHERE id = 1) AS x", "SELECT COUNT(*), COUNT(x) FROM a", "1|0", TestName = "CreateTableAs_ARowSubqueryWithNoRow_StoresAnInt64Column")]
    public async Task CreateTableAs_AnEmptyScalarSubquery_KeepsTheDeclaredType(string ddl, string read, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual("ok", await TryCommit(database, executor, dbname, ddl, ddl: true));
        Assert.AreEqual(expected, await Query(database, executor, dbname, read));
        Assert.AreEqual(ColumnType.Integer64, await ColumnTypeOf(executor, dbname, "a", "x"));
    }

    /// <summary>
    /// <c>INSERT … SELECT</c> of a scalar subquery over an empty table writes NULL in each row, as in PostgreSQL.
    /// </summary>
    [Test]
    public async Task InsertSelect_AnEmptyScalarSubquery_WritesNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE TABLE ins (id INT64 PRIMARY KEY, x INT64)", ddl: true);

        Assert.AreEqual("ok", await TryCommit(database, executor, dbname, "INSERT INTO ins (id, x) SELECT id, (SELECT MAX(v) FROM em) FROM t"));
        Assert.AreEqual("1| ; 2|", await Query(database, executor, dbname, "SELECT id, x FROM ins"));
    }

    /// <summary>
    /// A NULL literal has no type, so a column of a NULL literal is still refused, also through a subquery.
    /// PostgreSQL gives such a column the type <c>text</c>; CamusDB keeps its rule that a NULL literal takes the
    /// type that its use needs, and a new column gives it none.
    /// </summary>
    [TestCase("CREATE TABLE a AS SELECT NULL AS x", TestName = "CreateTableAs_ANullLiteral_IsStillRefused")]
    [TestCase("CREATE TABLE a AS SELECT id, (SELECT NULL) AS x FROM t", TestName = "CreateTableAs_ASubqueryOfANullLiteral_IsStillRefused")]
    public async Task CreateTableAs_AnUntypedNull_IsStillRefused(string ddl)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        string result = await TryCommit(database, executor, dbname, ddl, ddl: true);

        Assert.AreNotEqual("ok", result);
        Assert.IsFalse((await executor.OpenDatabase(dbname)).Schema.Tables.ContainsKey("a"));
    }
}
