
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

[NonParallelizable]
public class TestNullScalarFunctions : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "items",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("tag", ColumnType.String),         // nullable
                new("score", ColumnType.Integer64),    // nullable
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        // Row with non-null tag and score
        InsertTicket row1 = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "items",
            values: new()
            {
                new()
                {
                    { "id",    new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name",  new(ColumnType.String, "alpha") },
                    { "tag",   new(ColumnType.String, "present") },
                    { "score", new(ColumnType.Integer64, 42L) },
                }
            });

        // Row with null tag and null score
        InsertTicket row2 = new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "items",
            values: new()
            {
                new()
                {
                    { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "beta") },
                }
            });

        await executor.Insert(row1);
        await executor.Insert(row2);
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

    [Test]
    [NonParallelizable]
    public async Task Coalesce_NonNullColumn_ReturnsColumnValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT COALESCE(tag, 'fallback') FROM items WHERE name = 'alpha'");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(ColumnType.String, result[0].Row["0"].Type);
        Assert.AreEqual("present", result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Coalesce_NullColumn_ReturnsFallback()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT COALESCE(tag, 'fallback') FROM items WHERE name = 'beta'");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(ColumnType.String, result[0].Row["0"].Type);
        Assert.AreEqual("fallback", result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Coalesce_VariadicAllNull_ReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT COALESCE(NULL, NULL, NULL) FROM items LIMIT 1");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(ColumnType.Null, result[0].Row["0"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task Coalesce_VariadicPicksFirst()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT COALESCE(NULL, 'second', 'third') FROM items LIMIT 1");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual("second", result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Coalesce_NullableLiteralFirst_ReturnsSecond()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT COALESCE(NULL, 99) FROM items LIMIT 1");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(99L, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task IfNull_NonNullColumn_ReturnsColumnValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT IFNULL(tag, 'default') FROM items WHERE name = 'alpha'");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual("present", result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task IfNull_NullColumn_ReturnsFallback()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT IFNULL(tag, 'default') FROM items WHERE name = 'beta'");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual("default", result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Nvl_Alias_WorksIdenticallyToIfNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT NVL(tag, 'nvl-default') FROM items WHERE name = 'beta'");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual("nvl-default", result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Coalesce_IntegerColumn_NullableFallback()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT COALESCE(score, 0) FROM items ORDER BY name");

        Assert.AreEqual(2, result.Count);
        // alpha has score=42
        Assert.AreEqual(42L, result[0].Row["0"].LongValue);
        // beta has null score → 0
        Assert.AreEqual(0L, result[1].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Coalesce_MixedNumericAndString_ThrowsInvalidInput()
    {
        // Integer64 + String is an incompatible mix: inference rejects it so the declared
        // column type and the runtime value can never disagree.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await ExecuteSelect(executor, database, dbname,
                "SELECT COALESCE(score, 'fallback') FROM items WHERE name = 'alpha'"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("incompatible", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task Coalesce_MixedIdAndString_ThrowsInvalidInput()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await ExecuteSelect(executor, database, dbname,
                "SELECT COALESCE(id, 'fallback') FROM items WHERE name = 'alpha'"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("incompatible", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task Coalesce_MixedNumericAndString_InDerivedTable_ThrowsInvalidInput()
    {
        // The derived-table path calls InferReturnType statically at schema-build time,
        // so the rejection happens before any row is scanned.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            async () => await ExecuteSelect(executor, database, dbname,
                "SELECT c FROM (SELECT COALESCE(score, 'fallback') AS c FROM items) sub"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        StringAssert.Contains("incompatible", exception.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task Coalesce_NullFirstArg_ColumnSuppliesValue_TypeMatches()
    {
        // NULL + Integer64: inference skips the Null arg, infers Integer64 — no WiderType call.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT COALESCE(NULL, score) FROM items WHERE name = 'alpha'");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(42L, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Coalesce_MixedNumeric_IntegerAndFloat_CoercedToFloat()
    {
        // COALESCE(score, 3.5): inferred return type is Float64 (widest). When score is non-null
        // (Integer64), the evaluator must coerce it to Float64 so the runtime ColumnValue.Type
        // matches the schema-declared type. This prevents a type mismatch when COALESCE feeds a
        // derived table or nested query.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname,
            "SELECT COALESCE(score, 3.5) FROM items ORDER BY name");

        Assert.AreEqual(2, result.Count);

        // alpha: score=42 (Integer64) coerced to Float64
        Assert.AreEqual(ColumnType.Float64, result[0].Row["0"].Type);
        Assert.AreEqual(42.0, result[0].Row["0"].FloatValue, delta: 0.0001);

        // beta: score IS NULL → fallback 3.5 (Float64)
        Assert.AreEqual(ColumnType.Float64, result[1].Row["0"].Type);
        Assert.AreEqual(3.5, result[1].Row["0"].FloatValue, delta: 0.0001);
    }
}
