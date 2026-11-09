
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
/// A bare name in ORDER BY matches the output names of the select list first, and an input column only when
/// no output has the name; a name that two outputs with different expressions have is refused as ambiguous.
/// A scalar subquery column takes the name of its single inner column. Each expected result was measured on
/// PostgreSQL 16.11 with the same rows.
///
/// <para>The tables: <c>r(id, v)</c> = (1, 30), (2, 10), (3, 20) and <c>r2(id)</c> = (7). <c>v</c> and
/// <c>id</c> sort the rows in different orders, so the order of the result shows which column the sort
/// used.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestOrderByOutputNames : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE r (id INT64 PRIMARY KEY, v INT64)", ddl: true);
        await Commit(database, executor, dbname, "INSERT INTO r (id, v) VALUES (1, 30), (2, 10), (3, 20)");
        await Commit(database, executor, dbname, "CREATE TABLE r2 (id INT64 PRIMARY KEY)", ddl: true);
        await Commit(database, executor, dbname, "INSERT INTO r2 (id) VALUES (7)");

        return (dbname, database, executor);
    }

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, bool ddl = false)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        if (ddl)
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        else
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

        await database.Transactions.CommitAsync(tx);
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "",
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        _ => value.StrValue ?? "",
    };

    /// <summary>
    /// The column names a client sees, joined with <c>|</c>, and the rows in the order the query returned
    /// them, each row as its columns joined with <c>|</c>, the rows joined with <c> ; </c>.
    /// </summary>
    private static async Task<(string Names, string Rows)> Query(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            QuerySchemaHolder schema = new();
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, dbname, sql, null), schemaOut: schema);
            List<QueryResultRow> rows = await cursor.ToListAsync();

            return (
                string.Join("|", schema.Schema.Select(column => column.Name)),
                string.Join(" ; ", rows.Select(row => string.Join("|", row.Row.Values.Select(Render)))));
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    private static async Task<string> Rows(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql) =>
        (await Query(database, executor, dbname, sql)).Rows;

    private static async Task<string> Names(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql) =>
        (await Query(database, executor, dbname, sql)).Names;

    [TestCase("SELECT id, (SELECT 5 AS id) FROM r ORDER BY id", "id")]
    [TestCase("SELECT id, (SELECT r2.id FROM r2 LIMIT 1) FROM r ORDER BY id", "id")]
    [TestCase("SELECT v AS id, (SELECT 5 AS id) FROM r ORDER BY id", "id")]
    [TestCase("SELECT v, (SELECT 5 AS v) FROM r ORDER BY v", "v")]
    [TestCase("SELECT id, (SELECT r2.id FROM r2 WHERE r2.id > r.id) FROM r ORDER BY id", "id")]
    public async Task Subquery_ColumnNamedLikeAnotherOutput_IsAmbiguous(string sql, string name)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () => await Rows(database, executor, dbname, sql))!;

        Assert.That(e.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        Assert.That(e.Message, Is.EqualTo($"ORDER BY \"{name}\" is ambiguous"));
    }

    [TestCase("SELECT id, v AS id FROM r ORDER BY id")]
    [TestCase("SELECT id, v AS id FROM r ORDER BY id DESC")]
    [TestCase("SELECT r.id, r.v AS id FROM r ORDER BY id")]
    [TestCase("SELECT v AS id, id FROM r ORDER BY id")]
    [TestCase("SELECT DISTINCT v AS id, id FROM r ORDER BY id")]
    [TestCase("SELECT id, v AS ID FROM r ORDER BY Id")]
    [TestCase("SELECT *, v AS id FROM r ORDER BY id")]
    [TestCase("SELECT v AS id, id FROM r GROUP BY v, id ORDER BY id")]
    [TestCase("SELECT COUNT(*) AS id, MAX(v) AS id FROM r ORDER BY id")]
    public async Task Name_OfTwoOutputsWithDifferentExpressions_IsAmbiguous(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () => await Rows(database, executor, dbname, sql))!;

        Assert.That(e.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        Assert.That(e.Message, Does.EndWith("is ambiguous"));
    }

    [Test]
    public async Task SetOperation_NameOfTwoOutputColumns_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Rows(database, executor, dbname, "SELECT 1 AS a, 2 AS a UNION SELECT 3, 4 ORDER BY a"))!;

        Assert.That(e.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        Assert.That(await Rows(database, executor, dbname, "SELECT 1 AS a, 2 AS b UNION SELECT 3, 4 ORDER BY a DESC"),
            Is.EqualTo("3|4 ; 1|2"));
    }

    [TestCase("SELECT id, id FROM r ORDER BY id", "1|1 ; 2|2 ; 3|3")]
    [TestCase("SELECT r.id, id FROM r ORDER BY id", "1|1 ; 2|2 ; 3|3")]
    [TestCase("SELECT v AS id, v AS id FROM r ORDER BY id", "10|10 ; 20|20 ; 30|30")]
    [TestCase("SELECT *, id FROM r ORDER BY id", "1|30|1 ; 2|10|2 ; 3|20|3")]
    public async Task Name_OfTwoOutputsWithOneExpression_IsNotAmbiguous(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Rows(database, executor, dbname, sql), Is.EqualTo(expected));
    }

    [TestCase("SELECT id, (SELECT 5 AS id) FROM r ORDER BY 1", "1|5 ; 2|5 ; 3|5")]
    [TestCase("SELECT id, (SELECT 5 AS id) FROM r ORDER BY r.id", "1|5 ; 2|5 ; 3|5")]
    [TestCase("SELECT id, v AS id FROM r ORDER BY id + 0", "1|30 ; 2|10 ; 3|20")]
    [TestCase("SELECT v AS id, id FROM r ORDER BY 2", "30|1 ; 10|2 ; 20|3")]
    [TestCase("SELECT v AS id, id FROM r ORDER BY 1", "10|2 ; 20|3 ; 30|1")]
    public async Task PositionQualifiedNameAndExpression_AreNotOutputNames(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Rows(database, executor, dbname, sql), Is.EqualTo(expected));
    }

    [TestCase("SELECT a.id FROM r AS a JOIN r AS b ON a.id = b.id ORDER BY id", "1 ; 2 ; 3")]
    [TestCase("SELECT a.id, b.v FROM r AS a JOIN r AS b ON a.id = b.id ORDER BY v", "2|10 ; 3|20 ; 1|30")]
    [TestCase("SELECT a.id FROM r AS a JOIN r AS b ON a.id = b.id GROUP BY a.id ORDER BY id", "1 ; 2 ; 3")]
    [TestCase("SELECT id AS x, v AS id FROM r ORDER BY id", "2|10 ; 3|20 ; 1|30")]
    [TestCase("SELECT v AS id FROM r ORDER BY id DESC", "30 ; 20 ; 10")]
    [TestCase("SELECT v, (SELECT 5 AS id) FROM r ORDER BY v", "10|5 ; 20|5 ; 30|5")]
    public async Task OutputName_MatchesBeforeAnInputColumn(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Rows(database, executor, dbname, sql), Is.EqualTo(expected));
    }

    [Test]
    public async Task Subquery_ThatIsTheOnlyOutputWithTheName_IsTheSortKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        // The subquery is a constant, so every order is valid; the statement is not refused and is not
        // sorted by the input column r.id.
        (string names, string rows) = await Query(database, executor, dbname, "SELECT (SELECT 5 AS id) FROM r ORDER BY id");

        Assert.That(names, Is.EqualTo("id"));
        Assert.That(rows, Is.EqualTo("5 ; 5 ; 5"));
    }

    [TestCase("SELECT id, (SELECT 5 AS id) FROM r ORDER BY r.id", "id|id")]
    [TestCase("SELECT v, (SELECT r2.id FROM r2) FROM r", "v|id")]
    [TestCase("SELECT (SELECT (SELECT 9 AS z)) FROM r", "z")]
    [TestCase("SELECT (SELECT 5 AS id) AS x FROM r", "x")]
    [TestCase("SELECT (SELECT 5 AS id)", "id")]
    [TestCase("SELECT id, (SELECT 5 AS z), row_number() OVER (ORDER BY id) FROM r", "id|z|2")]
    [TestCase("SELECT * FROM (SELECT id, (SELECT 5 AS id) FROM r) AS x", "id|id")]
    public async Task ScalarSubqueryColumn_TakesTheNameOfItsInnerColumn(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Names(database, executor, dbname, sql), Is.EqualTo(expected));
    }

    /// <summary>
    /// A subquery column whose inner column is an expression keeps its position as its name, because CamusDB
    /// names every expression column by its position. PostgreSQL names it <c>?column?</c>, or after the
    /// function (<c>max</c>).
    /// </summary>
    [TestCase("SELECT (SELECT 5), (SELECT 6) FROM r", "0|1")]
    [TestCase("SELECT (SELECT MAX(id) FROM r2), (SELECT r2.id + 1 FROM r2) FROM r", "0|1")]
    public async Task ScalarSubqueryColumn_OverAnExpression_KeepsItsPosition(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Names(database, executor, dbname, sql), Is.EqualTo(expected));
    }

    [Test]
    public async Task ScalarSubqueryColumn_IsReadByNameThroughADerivedTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Rows(database, executor, dbname, "SELECT x.id FROM (SELECT (SELECT 5 AS id) FROM r) AS x"),
            Is.EqualTo("5 ; 5 ; 5"));
    }

    [TestCase("CREATE TABLE c1 AS SELECT id, (SELECT 5 AS z) FROM r", "c1")]
    [TestCase("CREATE MATERIALIZED VIEW m1 AS SELECT id, (SELECT 5 AS z) FROM r", "m1")]
    [TestCase("CREATE VIEW w1 AS SELECT id, (SELECT 5 AS z) FROM r", "w1")]
    public async Task StoredDefinition_NamesTheSubqueryColumnAfterItsInnerColumn(string ddl, string relation)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, ddl, ddl: true);

        Assert.That(await Rows(database, executor, dbname, $"SELECT id, z FROM {relation} ORDER BY id"),
            Is.EqualTo("1|5 ; 2|5 ; 3|5"));
    }
}
