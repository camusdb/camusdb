
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
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

public class TestDivisionExpr : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "nums",
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id),
                new("a",     ColumnType.Integer64),
                new("b",     ColumnType.Integer64),
                new("fa",    ColumnType.Float64),
                new("fb",    ColumnType.Float64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        InsertTicket row = new(
            txnState: txn,
            databaseName: dbname,
            tableName: "nums",
            values: new()
            {
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "a",  new(ColumnType.Integer64, 10L) },
                    { "b",  new(ColumnType.Integer64, 4L) },
                    { "fa", new(ColumnType.Float64, 10.0) },
                    { "fb", new(ColumnType.Float64, 4.0) },
                }
            });

        await executor.Insert(row);
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> ExecuteSelect(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txn,
            database: dbname,
            sql: sql,
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    [Test]
    [NonParallelizable]
    public async Task Division_IntegerByInteger_TruncatesToInteger()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT a / b FROM nums");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(2L, result[0].Row["0"].LongValue);  // 10 / 4 = 2 (truncating)
    }

    [Test]
    [NonParallelizable]
    public async Task Division_FloatByFloat_ReturnsFloat()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT fa / fb FROM nums");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(ColumnType.Float64, result[0].Row["0"].Type);
        Assert.AreEqual(2.5, result[0].Row["0"].FloatValue, delta: 0.0001);
    }

    [Test]
    [NonParallelizable]
    public async Task Division_LiteralInteger_Works()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT 9 / 3 FROM nums");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(3L, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Division_ByZeroInteger_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteSelect(executor, database, dbname, "SELECT a / 0 FROM nums"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("zero", ex.Message.ToLowerInvariant());
    }

    [Test]
    [NonParallelizable]
    public async Task Division_ChainedWithAdd_Parses()
    {
        // (a / b) + 1  — tests operator precedence and chaining
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT a / b + 1 FROM nums");

        Assert.AreEqual(1, result.Count);
        // 10/4=2, +1=3
        Assert.AreEqual(3L, result[0].Row["0"].LongValue);
    }
}
