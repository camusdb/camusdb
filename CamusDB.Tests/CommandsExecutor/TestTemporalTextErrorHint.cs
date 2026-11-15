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
/// Date and datetime text copied from a word processor often holds an en dash (U+2013) in place of
/// '-'. The two look the same in a terminal, so the parse error names the first non-ASCII character,
/// its code point and position, and the ASCII character to type when there is a clear match. CAST, a
/// typed literal, a comparison and an INSERT share one parse, so each must give the hint.
/// </summary>
[NonParallelizable]
public sealed class TestTemporalTextErrorHint : SharedNodeBaseTest
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

    private static async Task<string> QueryError(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        }, sql);

        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code, sql);
        return ex.Message;
    }

    private static async Task<string> NonQueryError(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(ticket), sql);

        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code, sql);
        return ex.Message;
    }

    private const string EnDashHint = "contains the non-ASCII character '–' (U+2013) at position 5, type '-' in its place";

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> CreateEmployees()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, database, dbname, "CREATE TABLE employees (id int64 PRIMARY KEY, name string, hired date, at datetime)");
        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO employees VALUES (1, 'Ana', '2018-03-01', '2018-03-01T10:00:00Z')");
        return (dbname, database, executor);
    }

    [Test]
    public async Task Comparison_NamesTheEnDash()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateEmployees();

        string message = await QueryError(executor, database, dbname, """
            SELECT name,
              CASE WHEN DATE_TRUNC('year', at) <= '2019–01–01' THEN 'Veteran' ELSE 'Recent' END AS tenure_group
            FROM employees
            """);

        Assert.That(message, Does.Contain("Cannot compare a datetime with '2019–01–01'"));
        Assert.That(message, Does.Contain(EnDashHint));

        message = await QueryError(executor, database, dbname, "SELECT id FROM employees WHERE hired = '2019–01–01'");
        Assert.That(message, Does.Contain("Cannot compare a date with"));
        Assert.That(message, Does.Contain(EnDashHint));
    }

    [TestCase("SELECT CAST('2019–01–01' AS date)")]
    [TestCase("SELECT CAST('2019–01–01' AS datetime)")]
    [TestCase("SELECT DATE '2019–01–01'")]
    [TestCase("SELECT TIMESTAMP '2019–01–01T10:00:00Z'")]
    public async Task Cast_NamesTheEnDash(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.That(await QueryError(executor, database, dbname, sql), Does.Contain(EnDashHint));
    }

    [Test]
    public async Task Insert_NamesTheEnDash()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateEmployees();

        string message = await NonQueryError(executor, database, dbname,
            "INSERT INTO employees VALUES (2, 'Luis', '2019–01–01', '2019-01-01T10:00:00Z')");

        Assert.That(message, Does.Contain(EnDashHint));
    }

    [Test]
    public async Task OtherLookalikes_NameTheAsciiCharacter()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // U+2212 minus sign in a date, and U+FF1A fullwidth colon in a time.
        Assert.That(await QueryError(executor, database, dbname, "SELECT DATE '2019−01-01'"),
            Does.Contain("'−' (U+2212) at position 5, type '-' in its place"));
        Assert.That(await QueryError(executor, database, dbname, "SELECT TIMESTAMP '2019-01-01T10：00:00Z'"),
            Does.Contain("'：' (U+FF1A) at position 14, type ':' in its place"));
    }

    [Test]
    public async Task UnmatchedNonAscii_NamesTheCharacterOnly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        string message = await QueryError(executor, database, dbname, "SELECT DATE '2019-01-0é'");

        Assert.That(message, Does.Contain("contains the non-ASCII character 'é' (U+00E9) at position 10"));
        Assert.That(message, Does.Not.Contain("in its place"));
    }

    [Test]
    public async Task AsciiOnlyText_HasNoHint()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        string message = await QueryError(executor, database, dbname, "SELECT DATE '2019-13-45'");

        Assert.That(message, Does.Contain("expected yyyy-MM-dd"));
        Assert.That(message, Does.Not.Contain("non-ASCII"));
    }
}
