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
/// A negative LIMIT or OFFSET fails the statement with the message of PostgreSQL. Before this rule a
/// negative count gave an answer: <c>LIMIT -1</c> returned no rows, <c>OFFSET -1</c> skipped none, and
/// <c>LIMIT 2 OFFSET -1</c> returned one row. A literal is refused when the statement binds, before any
/// row is read; a parameter is refused when the clause is read. Each expected result was measured on
/// PostgreSQL 16.11 with the same rows, unless the test says that CamusDB differs by decision.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestNegativeRowCounts : BaseTest
{
    private const string NegativeLimit = "CADB0400: LIMIT must not be negative";
    private const string NegativeOffset = "CADB0400: OFFSET must not be negative";

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE r (id INT64 PRIMARY KEY, v INT64)");
        await Commit(database, executor, dbname, "INSERT INTO r (id, v) VALUES (1, 0), (2, 2), (3, 0)");
        await Commit(database, executor, dbname, "CREATE TABLE r2 (id INT64 PRIMARY KEY, w INT64)");
        await Commit(database, executor, dbname, "INSERT INTO r2 (id, w) VALUES (1, 10), (2, 20)");
        await Commit(database, executor, dbname, "CREATE TABLE em (id INT64 PRIMARY KEY, v INT64)");
        await Commit(database, executor, dbname, "CREATE SEQUENCE sq");

        return (dbname, database, executor);
    }

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            if (sql.StartsWith("CREATE", StringComparison.Ordinal))
                await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
            else
                _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static Dictionary<string, ColumnValue> P(long value) => new() { ["@p"] = new ColumnValue(ColumnType.Integer64, value) };

    /// <summary>The sorted first cells of the rows of <paramref name="sql"/>, or the code and message of the error.</summary>
    private static async Task<string> Query(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();

            return string.Join(",", rows
                .Select(r => r.Row.Values.First().LongValue)
                .OrderBy(v => v)
                .Select(v => v.ToString(CultureInfo.InvariantCulture)));
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

    /// <summary>Runs a write and returns "written", or the code and message of the error.</summary>
    private static async Task<string> Write(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            if (sql.StartsWith("CREATE", StringComparison.Ordinal))
                await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, parameters));
            else
                _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));

            await database.Transactions.CommitAsync(tx);
            return "written";
        }
        catch (CamusDBException e)
        {
            await database.Transactions.RollbackAsync(tx);
            return e.Code + ": " + e.Message;
        }
    }

    [TestCase("SELECT id FROM r LIMIT -1", NegativeLimit)]
    [TestCase("SELECT id FROM em LIMIT -1", NegativeLimit)]
    [TestCase("SELECT id FROM r ORDER BY id LIMIT -1", NegativeLimit)]
    [TestCase("SELECT COUNT(*) FROM r LIMIT -1", NegativeLimit)]
    [TestCase("SELECT id FROM r OFFSET -1", NegativeOffset)]
    [TestCase("SELECT id FROM r ORDER BY id LIMIT 2 OFFSET -1", NegativeOffset)]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 LIMIT -1)", NegativeLimit)]
    [TestCase("SELECT id FROM em WHERE EXISTS (SELECT 1 FROM r2 LIMIT -1)", NegativeLimit)]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r2 i WHERE i.id = o.id LIMIT -1)", NegativeLimit)]
    [TestCase("SELECT (SELECT 1 FROM r LIMIT -1)", NegativeLimit)]
    [TestCase("SELECT id FROM r WHERE id IN (SELECT id FROM r2 LIMIT -1)", NegativeLimit)]
    [TestCase("SELECT x FROM (SELECT id AS x FROM r LIMIT -1) d", NegativeLimit)]
    [TestCase("SELECT id FROM r UNION SELECT id FROM r2 LIMIT -1", NegativeLimit)]
    [TestCase("SELECT 1 LIMIT -1", NegativeLimit)]
    [TestCase("SELECT 1 OFFSET -1", NegativeOffset)]
    public async Task ANegativeLiteral_IsRefused(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expected, await Query(database, executor, dbname, sql), sql);
    }

    [TestCase("SELECT id FROM r LIMIT @p", NegativeLimit)]
    [TestCase("SELECT id FROM r LIMIT 2 OFFSET @p", NegativeOffset)]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 LIMIT @p)", NegativeLimit)]
    [TestCase("SELECT id FROM em WHERE EXISTS (SELECT 1 FROM r2 LIMIT @p)", NegativeLimit)]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 OFFSET @p)", NegativeOffset)]
    [TestCase("SELECT 1 LIMIT @p", NegativeLimit)]
    public async Task ANegativeParameter_IsRefused(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expected, await Query(database, executor, dbname, sql, P(-1)), sql);
    }

    [TestCase("UPDATE r SET v = 1 WHERE id > 0 LIMIT @p", true)]
    [TestCase("DELETE FROM r WHERE id > 0 LIMIT @p", true)]
    [TestCase("UPDATE r SET v = 1 WHERE id > 0 LIMIT -1", false)]
    [TestCase("DELETE FROM r WHERE id > 0 LIMIT -1", false)]
    public async Task ANegativeCount_InAWrite_IsRefused_AndWritesNothing(string sql, bool withParameter)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(NegativeLimit, await Write(database, executor, dbname, sql, withParameter ? P(-1) : null), sql);
        Assert.AreEqual("1,2,3", await Query(database, executor, dbname, "SELECT id FROM r WHERE v IN (0, 2)"));
    }

    [TestCase("SELECT id FROM r LIMIT -0", "")]
    [TestCase("SELECT id FROM r LIMIT 0", "")]
    [TestCase("SELECT id FROM r OFFSET 0", "1,2,3")]
    [TestCase("SELECT id FROM r ORDER BY id LIMIT 2 OFFSET 1", "2,3")]
    [TestCase("SELECT id FROM r ORDER BY id LIMIT 9223372036854775807 OFFSET 1", "2,3")]
    public async Task ACountThatIsNotNegative_IsApplied(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expected, await Query(database, executor, dbname, sql), sql);
    }

    [Test]
    public async Task ARefusedFromlessSelect_DrawsNoSequenceValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(NegativeLimit, await Query(database, executor, dbname, "SELECT nextval('sq') LIMIT -1"));
        Assert.AreEqual("1", await Query(database, executor, dbname, "SELECT nextval('sq')"));
    }

    /// <summary>
    /// CamusDB differs by decision: PostgreSQL reads a LIMIT only when its node runs, so it accepts a
    /// view body with <c>LIMIT -1</c> and a correlated EXISTS with <c>LIMIT -1</c> that no outer row
    /// reaches. CamusDB refuses a negative literal when the statement binds, as it refuses other
    /// statement mistakes before any row is read.
    /// </summary>
    [Test]
    public async Task ANegativeLiteral_IsRefusedAtBind_WherePostgreSqlWaitsForARow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(NegativeLimit, await Write(database, executor, dbname, "CREATE VIEW c AS SELECT id FROM r LIMIT -1"));
        Assert.AreEqual(NegativeLimit, await Query(database, executor, dbname,
            "SELECT id FROM em o WHERE EXISTS (SELECT 1 FROM r2 i WHERE i.id = o.id LIMIT -1)"));
    }
}
