
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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

public class TestMathScalarFunctions : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupBasicTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("enabled", ColumnType.Bool),
                new("score", ColumnType.Float64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        double[] scores = [2.5, -2.5, 2.1, 1.235, -3.6];

        for (int i = 0; i < 5; i++)
        {
            InsertTicket insertTicket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, $"robot {i}") },
                        { "year", new(ColumnType.Integer64, 2000 + i) },
                        { "enabled", new(ColumnType.Bool, true) },
                        { "score", new(ColumnType.Float64, scores[i]) },
                    }
                });

            await executor.Insert(insertTicket);
        }

        await database.Transactions.CommitAsync(txnState);
        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> ExecuteSelect(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: sql,
            parameters: parameters);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    private static async Task<CamusDBException> AssertSelectThrows(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: sql,
            parameters: null);

        return Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        })!;
    }

    [Test]
    [NonParallelizable]
    public async Task Abs_IntegerInput_ReturnsInteger64()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname, "SELECT abs(-7) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(7, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Abs_FloatInput_ReturnsFloat64()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT abs(score) FROM robots WHERE year = 2001 LIMIT 1");

        Assert.AreEqual(ColumnType.Float64, result[0].Row["0"].Type);
        Assert.AreEqual(2.5, result[0].Row["0"].FloatValue, 1e-9);
    }

    [Test]
    [NonParallelizable]
    public async Task Abs_MinIntegerValue_ThrowsOverflow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            $"SELECT abs({long.MinValue}) FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'abs' integer overflow", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task Ceil_AliasCeiling_WorksOnFloat()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> ceil = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT ceil(score) FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> ceiling = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT ceiling(score) FROM robots WHERE year = 2000 LIMIT 1");

        Assert.AreEqual(3.0, ceil[0].Row["0"].FloatValue, 1e-9);
        Assert.AreEqual(3.0, ceiling[0].Row["0"].FloatValue, 1e-9);
    }

    [Test]
    [NonParallelizable]
    public async Task Floor_IntegerInput_StaysInteger64()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname, "SELECT floor(2001) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(2001, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Round_FloatWithoutScale_UsesAwayFromZero()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT round(score) FROM robots WHERE year = 2000 LIMIT 1");

        Assert.AreEqual(3.0, result[0].Row["0"].FloatValue, 1e-9);
    }

    [Test]
    [NonParallelizable]
    public async Task Round_WithScale_RoundsDecimalPlaces()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT round(score, 2) FROM robots WHERE year = 2003 LIMIT 1");

        Assert.AreEqual(1.24, result[0].Row["0"].FloatValue, 1e-9);
    }

    [Test]
    [NonParallelizable]
    public async Task Round_ScaleOverflow_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT round(score, 2147483648) FROM robots WHERE year = 2003 LIMIT 1");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'round' scale argument out of range", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task Sqrt_PositiveInput_ReturnsFloat64()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname, "SELECT sqrt(9) FROM robots LIMIT 1");

        Assert.AreEqual(3.0, result[0].Row["0"].FloatValue, 1e-9);
    }

    [Test]
    [NonParallelizable]
    public async Task Sqrt_NegativeInput_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(executor, database, dbname, "SELECT sqrt(-1) FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'sqrt' domain error", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task Pow_AliasPower_Works()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> pow = await ExecuteSelect(executor, database, dbname, "SELECT pow(2, 3) FROM robots LIMIT 1");
        List<QueryResultRow> power = await ExecuteSelect(executor, database, dbname, "SELECT power(2, 3) FROM robots LIMIT 1");

        Assert.AreEqual(8.0, pow[0].Row["0"].FloatValue, 1e-9);
        Assert.AreEqual(8.0, power[0].Row["0"].FloatValue, 1e-9);
    }

    [Test]
    [NonParallelizable]
    public async Task Mod_IntegerInputs_ReturnsInteger64()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname, "SELECT mod(10, 3) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(1, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Mod_DivisionByZero_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(executor, database, dbname, "SELECT mod(5, 0) FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'mod' division by zero", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task Mod_MinValueModMinusOne_ReturnsZero()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            $"SELECT mod({long.MinValue}, -1) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(0, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Sign_ReturnsInteger64()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> negative = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT sign(score) FROM robots WHERE year = 2001 LIMIT 1");
        List<QueryResultRow> zero = await ExecuteSelect(executor, database, dbname, "SELECT sign(0) FROM robots LIMIT 1");
        List<QueryResultRow> positive = await ExecuteSelect(executor, database, dbname, "SELECT sign(4) FROM robots LIMIT 1");

        Assert.AreEqual(-1, negative[0].Row["0"].LongValue);
        Assert.AreEqual(0, zero[0].Row["0"].LongValue);
        Assert.AreEqual(1, positive[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Random_ReturnsFloatInUnitInterval()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname, "SELECT random() FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Float64, result[0].Row["0"].Type);
        Assert.GreaterOrEqual(result[0].Row["0"].FloatValue, 0.0);
        Assert.Less(result[0].Row["0"].FloatValue, 1.0);
    }

    [Test]
    [NonParallelizable]
    public async Task NestedMathCalls_Work()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT abs(round(score)) FROM robots WHERE year = 2004 LIMIT 1");

        Assert.AreEqual(4.0, result[0].Row["0"].FloatValue, 1e-9);
    }

    [Test]
    [NonParallelizable]
    public async Task MathInWhere_FiltersRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT year FROM robots WHERE abs(year - 2002) <= 1 ORDER BY year");

        Assert.AreEqual(3, result.Count);
        Assert.AreEqual(2001, result[0].Row["year"].LongValue);
        Assert.AreEqual(2002, result[1].Row["year"].LongValue);
        Assert.AreEqual(2003, result[2].Row["year"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task MathInProjectionAlias_Works()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT abs(year - 2000) AS delta FROM robots ORDER BY year LIMIT 3");

        Assert.AreEqual(0, result[0].Row["delta"].LongValue);
        Assert.AreEqual(1, result[1].Row["delta"].LongValue);
        Assert.AreEqual(2, result[2].Row["delta"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task WrongArity_Sqrt_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(executor, database, dbname, "SELECT sqrt(1, 2) FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'sqrt' expects 1 argument(s) but received 2", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task WrongType_StringInput_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT abs(\"not-a-number\") FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'abs' expects argument 1 of type Integer64 or Float64 but received String", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task NullArgument_PropagatesNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname, "SELECT abs(NULL) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Null, result[0].Row["0"].Type);
    }
}
