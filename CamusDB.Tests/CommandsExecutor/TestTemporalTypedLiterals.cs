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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The typed literals <c>DATE '…'</c> and <c>TIMESTAMP '…'</c> end to end: in INSERT values, in a WHERE
/// comparison with and without an index, as a column default, in a view body, and with malformed text.
/// The parser builds the same cast node as <c>CAST('…' AS DATE)</c>, so each case must behave as the
/// cast does. A timestamp literal takes the text form the cast takes, ISO-8601 UTC such as
/// <c>2024-02-29T10:30:00Z</c>.
/// </summary>
[NonParallelizable]
public sealed class TestTemporalTypedLiterals : SharedNodeBaseTest
{
    private static async Task ExecDdl(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        _ = await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    /// <summary>Creates <c>employees</c> with the six rows of the example INSERT and an index on <c>hired</c>.</summary>
    private async Task<(string, DatabaseDescriptor, CommandExecutor)> CreateEmployees()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname,
            "CREATE TABLE employees (id int64 PRIMARY KEY, name string, dept int64, manager int64, salary int64, hired date)");

        await ExecNonQuery(executor, database, dbname, """
            INSERT INTO employees VALUES
                (1, 'Ana',   10, NULL, 9000, DATE '2018-03-01'),
                (2, 'Luis',  10, 1,    6000, DATE '2020-07-15'),
                (3, 'Marta', 20, 1,    7000, DATE '2019-01-10'),
                (4, 'Pedro', 20, 3,    7000, DATE '2021-11-20'),
                (5, 'Sofía', 30, 1,    5000, DATE '2022-05-05'),
                (6, 'Jorge', NULL, 3,  4000, DATE '2023-02-01')
            """);

        return (dbname, database, executor);
    }

    private static long[] Ids(List<QueryResultRow> rows) => rows.Select(r => r.Row["id"].LongValue).ToArray();

    [Test]
    public async Task Insert_WritesTheDates()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateEmployees();

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT id, hired FROM employees ORDER BY id");

        Assert.That(rows.Select(r => r.Row["hired"].Type), Is.All.EqualTo(ColumnType.Date));
        Assert.That(rows.Select(r => r.Row["hired"].IsoValue).ToArray(), Is.EqualTo(new[]
        {
            "2018-03-01", "2020-07-15", "2019-01-10", "2021-11-20", "2022-05-05", "2023-02-01",
        }));
    }

    [Test]
    public async Task Where_SameRowsWithAndWithoutAnIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateEmployees();

        (string where, long[] expected)[] cases =
        [
            ("hired = DATE '2019-01-10'", [3]),
            ("hired >= DATE '2021-01-01'", [4, 5, 6]),
            ("hired >= DATE '2019-01-01' AND hired < DATE '2021-01-01'", [2, 3]),
            ("hired IN (DATE '2018-03-01', DATE '2023-02-01')", [1, 6]),
            ("hired < TIMESTAMP '2019-01-10T00:00:01Z'", [1, 3]),
        ];

        foreach ((string where, long[] expected) in cases)
            Assert.That(Ids(await ExecQuery(executor, database, dbname, $"SELECT id FROM employees WHERE {where} ORDER BY id")),
                Is.EqualTo(expected), "no index: " + where);

        await ExecDdl(executor, database, dbname, "CREATE INDEX employees_hired ON employees (hired)");

        foreach ((string where, long[] expected) in cases)
            Assert.That(Ids(await ExecQuery(executor, database, dbname, $"SELECT id FROM employees WHERE {where} ORDER BY id")),
                Is.EqualTo(expected), "index: " + where);
    }

    [Test]
    public async Task Select_GivesTheTypedValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT DATE '2024-02-29', TIMESTAMP '2024-02-29T10:30:00Z', DATETIME '2024-02-29T10:30:00Z', DATE '2024-02-29' + 1");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Date, rows[0].Row["0"].Type);
        Assert.AreEqual("2024-02-29", rows[0].Row["0"].IsoValue);
        Assert.AreEqual(ColumnType.DateTime, rows[0].Row["1"].Type);
        Assert.AreEqual("2024-02-29T10:30:00.0000000Z", rows[0].Row["1"].IsoValue);
        Assert.AreEqual(ColumnType.DateTime, rows[0].Row["2"].Type);
        Assert.AreEqual("2024-02-29T10:30:00.0000000Z", rows[0].Row["2"].IsoValue);
        Assert.AreEqual(ColumnType.Date, rows[0].Row["3"].Type);
        Assert.AreEqual("2024-03-01", rows[0].Row["3"].IsoValue);
    }

    [Test]
    public async Task ColumnDefault_WritesTheDate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname,
            "CREATE TABLE t (id int64 PRIMARY KEY, hired date DEFAULT(DATE '2020-01-01'), at datetime DEFAULT(TIMESTAMP '2020-01-01T10:00:00Z'))");
        await ExecNonQuery(executor, database, dbname, "INSERT INTO t (id) VALUES (1)");

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT hired, at FROM t");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Date, rows[0].Row["hired"].Type);
        Assert.AreEqual("2020-01-01", rows[0].Row["hired"].IsoValue);
        Assert.AreEqual(ColumnType.DateTime, rows[0].Row["at"].Type);
        Assert.AreEqual("2020-01-01T10:00:00.0000000Z", rows[0].Row["at"].IsoValue);
    }

    [Test]
    public async Task ViewBody_KeepsTheLiteral()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateEmployees();

        await ExecDdl(executor, database, dbname,
            "CREATE VIEW recent AS SELECT id FROM employees WHERE hired >= DATE '2021-01-01'");

        Assert.That(Ids(await ExecQuery(executor, database, dbname, "SELECT id FROM recent ORDER BY id")),
            Is.EqualTo(new long[] { 4, 5, 6 }));
    }

    [TestCase("SELECT DATE '2018-13-45'")]
    [TestCase("SELECT DATE 'yesterday'")]
    [TestCase("SELECT TIMESTAMP '2018-03-01T25:00:00Z'")]
    public async Task MalformedText_IsInvalidInput(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        }, sql);

        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code, sql);
    }
}
