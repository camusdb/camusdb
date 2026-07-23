
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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for the binary arithmetic operators (+ - * /) and their numeric promotion
/// rules: integer arithmetic stays integral, a Float64 operand widens the result to Float64, a
/// Float32 operand mixed with an integer keeps single precision, division by zero is an error, and
/// non-numeric operands are still rejected. Driven through SQL so the real evaluator path runs.
/// </summary>
[NonParallelizable]
public sealed class TestArithmeticOperators : SharedNodeBaseTest
{
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

    private static async Task<ColumnValue> ExecScalar(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, sql);
        Assert.AreEqual(1, rows.Count, sql);
        return rows[0].Row["0"];
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
    public async Task Float64_Arithmetic_ProducesFloat64()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        ColumnValue sum = await ExecScalar(executor, database, dbname, "SELECT 1.5 + 2.25");
        Assert.AreEqual(ColumnType.Float64, sum.Type);
        Assert.AreEqual(3.75, sum.FloatValue, 1e-12);

        ColumnValue sub = await ExecScalar(executor, database, dbname, "SELECT 5.5 - 2.25");
        Assert.AreEqual(ColumnType.Float64, sub.Type);
        Assert.AreEqual(3.25, sub.FloatValue, 1e-12);

        ColumnValue mult = await ExecScalar(executor, database, dbname, "SELECT 1.5 * 4.0");
        Assert.AreEqual(ColumnType.Float64, mult.Type);
        Assert.AreEqual(6.0, mult.FloatValue, 1e-12);

        ColumnValue div = await ExecScalar(executor, database, dbname, "SELECT 7.5 / 2.5");
        Assert.AreEqual(ColumnType.Float64, div.Type);
        Assert.AreEqual(3.0, div.FloatValue, 1e-12);
    }

    [Test]
    public async Task MixedIntegerAndFloat64_WidensToFloat64()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach ((string sql, double expected) in new[]
        {
            ("SELECT 1 + 0.5", 1.5),
            ("SELECT 0.5 + 1", 1.5),
            ("SELECT 3 - 0.5", 2.5),
            ("SELECT 0.5 - 3", -2.5),
            ("SELECT 3 * 0.5", 1.5),
            ("SELECT 0.5 * 3", 1.5),
        })
        {
            ColumnValue value = await ExecScalar(executor, database, dbname, sql);
            Assert.AreEqual(ColumnType.Float64, value.Type, sql);
            Assert.AreEqual(expected, value.FloatValue, 1e-12, sql);
        }
    }

    [Test]
    public async Task IntegerArithmetic_StaysIntegral()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach ((string sql, long expected) in new[]
        {
            ("SELECT 2 + 3", 5L),
            ("SELECT 2 - 3", -1L),
            ("SELECT 2 * 3", 6L),
            // Integer division truncates rather than promoting to a float.
            ("SELECT 7 / 2", 3L),
        })
        {
            ColumnValue value = await ExecScalar(executor, database, dbname, sql);
            Assert.AreEqual(ColumnType.Integer64, value.Type, sql);
            Assert.AreEqual(expected, value.LongValue, sql);
        }
    }

    [Test]
    public async Task Float32_Operand_KeepsSinglePrecisionResult()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // Float32 op Float32 and Float32 op Integer64 both stay Float32.
        ColumnValue f32 = await ExecScalar(executor, database, dbname, "SELECT to_float32(1.5) + to_float32(2.25)");
        Assert.AreEqual(ColumnType.Float32, f32.Type);
        Assert.AreEqual(3.75f, (float)f32.FloatValue);

        ColumnValue mixedInt = await ExecScalar(executor, database, dbname, "SELECT to_float32(1.5) * 2");
        Assert.AreEqual(ColumnType.Float32, mixedInt.Type);
        Assert.AreEqual(3.0f, (float)mixedInt.FloatValue);

        // A Float64 operand outranks Float32 and widens the result.
        ColumnValue widened = await ExecScalar(executor, database, dbname, "SELECT to_float32(1.5) + 2.25");
        Assert.AreEqual(ColumnType.Float64, widened.Type);
        Assert.AreEqual(3.75, widened.FloatValue, 1e-12);
    }

    [Test]
    public async Task DivisionByZero_IsRejectedForEveryNumericType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string sql in new[]
        {
            "SELECT 1 / 0",
            "SELECT 1.5 / 0.0",
            "SELECT 1.5 / 0",
            "SELECT 1 / 0.0",
            "SELECT 1.5 / to_float32(0.0)",
        })
        {
            CamusDBException ex = await AssertThrows(executor, database, dbname, sql);
            Assert.AreEqual("Division by zero", ex.Message, sql);
        }
    }

    [Test]
    public async Task NonNumericOperands_AreRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach ((string sql, string symbol) in new[]
        {
            ("SELECT \"a\" + 1", "+"),
            ("SELECT 1 - \"a\"", "-"),
            ("SELECT true * 2", "*"),
            ("SELECT \"a\" / 2", "/"),
        })
        {
            CamusDBException ex = await AssertThrows(executor, database, dbname, sql);
            Assert.IsTrue(
                ex.Message.StartsWith($"No matching signature for operator {symbol} for argument types:"),
                $"{sql} -> {ex.Message}");
        }
    }

    [Test]
    public async Task Float64Columns_AreAddedInProjectionAndFilter()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        ExecuteDDLSQLResult ddl = await executor.ExecuteDDLSQL(new(
            ddlTx, dbname,
            "CREATE TABLE prices (id STRING NOT NULL PRIMARY KEY, base FLOAT64 NOT NULL, tax FLOAT64 NOT NULL)",
            null));
        Assert.IsTrue(ddl.Success);

        KvTransaction insertTx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(insertTx, dbname,
            "INSERT INTO prices (id, base, tax) VALUES (\"p1\", 10.5, 2.25)", null));
        await database.Transactions.CommitAsync(insertTx);

        // Projection: Float64 + Float64 must evaluate rather than raise a signature error.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT base + tax FROM prices");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Float64, rows[0].Row["0"].Type);
        Assert.AreEqual(12.75, rows[0].Row["0"].FloatValue, 1e-12);

        // The same expression inside a predicate, which runs through the filter path.
        List<QueryResultRow> matched = await ExecQuery(executor, database, dbname,
            "SELECT id FROM prices WHERE base + tax > 12.0");
        Assert.AreEqual(1, matched.Count);

        List<QueryResultRow> unmatched = await ExecQuery(executor, database, dbname,
            "SELECT id FROM prices WHERE base + tax > 13.0");
        Assert.AreEqual(0, unmatched.Count);
    }
}
