
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
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Tests for FROM-less SELECT (a projection list evaluated against a single synthetic row):
/// scalar/function expressions, aliases, parameters, LIMIT/OFFSET, and the rejected shapes
/// (SELECT *, aggregates, projection subqueries).
/// </summary>
[NonParallelizable]
public sealed class TestFromlessSelect : SharedNodeBaseTest
{
    private static async Task<List<QueryResultRow>> ExecQuery(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<CamusDBException> AssertThrows(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        return Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        })!;
    }

    [Test]
    public async Task Arithmetic_ReturnsSingleRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT 1 + 1");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Integer64, rows[0].Row["0"].Type);
        Assert.AreEqual(2L, rows[0].Row["0"].LongValue);
    }

    [Test]
    public async Task RegexpSplitToArray_TargetExample()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        // The motivating query, both with double-quoted (CamusDB string literal) and single-quoted forms.
        foreach (string sql in new[]
        {
            "SELECT regexp_split_to_array(\"one,two,three\", \",\")",
            "SELECT regexp_split_to_array('one,two,three', ',')",
        })
        {
            List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, sql);
            Assert.AreEqual(1, rows.Count, sql);
            ColumnValue cv = rows[0].Row["0"];
            Assert.AreEqual(ColumnType.Array, cv.Type, sql);
            Assert.AreEqual(3, cv.ArrayValues!.Count, sql);
            Assert.AreEqual("one", cv.ArrayValues[0].StrValue);
            Assert.AreEqual("two", cv.ArrayValues[1].StrValue);
            Assert.AreEqual("three", cv.ArrayValues[2].StrValue);
        }
    }

    [Test]
    public async Task ScalarFunction_ReturnsValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT upper('abc')");
        Assert.AreEqual("ABC", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task Alias_NamesTheColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT 41 + 1 AS answer");
        Assert.AreEqual(42L, rows[0].Row["answer"].LongValue);
    }

    [Test]
    public async Task MultipleProjections_OrdinalColumnNames()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT 1, 'a', 2 + 3");
        Assert.AreEqual(1L, rows[0].Row["0"].LongValue);
        Assert.AreEqual("a", rows[0].Row["1"].StrValue);
        Assert.AreEqual(5L, rows[0].Row["2"].LongValue);
    }

    [Test]
    public async Task Parameter_Binds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Dictionary<string, ColumnValue> parameters = new() { { "@p", new ColumnValue(ColumnType.Integer64, 99L) } };
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT @p", parameters);
        Assert.AreEqual(99L, rows[0].Row["0"].LongValue);
    }

    [Test]
    public async Task LimitZero_ReturnsNoRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT 1 LIMIT 0");
        Assert.AreEqual(0, rows.Count);
    }

    [Test]
    public async Task LimitOne_ReturnsTheRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT 1 LIMIT 1");
        Assert.AreEqual(1, rows.Count);
    }

    [Test]
    public async Task OffsetOne_SkipsTheRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT 1 LIMIT 1 OFFSET 1");
        Assert.AreEqual(0, rows.Count);
    }

    [Test]
    public async Task SelectStar_Rejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        CamusDBException ex = await AssertThrows(executor, database, dbname, "SELECT *");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("FROM"));
    }

    [Test]
    public async Task Aggregate_Rejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        CamusDBException ex = await AssertThrows(executor, database, dbname, "SELECT COUNT(*)");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("FROM"));
    }

    [Test]
    public async Task ProjectionSubquery_RejectedAsUnsupported()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        // A scalar subquery in a FROM-less projection is the existence-check idiom — a deliberate
        // later phase, rejected clearly rather than mis-evaluated.
        CamusDBException ex = await AssertThrows(executor, database, dbname, "SELECT (SELECT 1)");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("not yet supported"));
    }
}
