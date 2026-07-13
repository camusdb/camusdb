
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
public class TestCastScalarFunctions : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, string objectId)> SetupBasicTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();
        string objectId = ObjectIdGenerator.Generate().ToString();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("enabled", ColumnType.Bool),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        await executor.Insert(new InsertTicket(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            values: new()
            {
                new()
                {
                    { "id", new(ColumnType.Id, objectId) },
                    { "name", new(ColumnType.String, "Robot") },
                    { "year", new(ColumnType.Integer64, 42) },
                    { "enabled", new(ColumnType.Bool, true) },
                }
            }));

        await database.Transactions.CommitAsync(txnState);
        return (dbname, database, executor, objectId);
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
    public async Task ToString_FromInteger_ReturnsString()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor, database, dbname, "SELECT to_string(year) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.String, result[0].Row["0"].Type);
        Assert.AreEqual("42", result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task CastAsString_FromName_Works()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor, database, dbname, "SELECT CAST(name AS string) FROM robots LIMIT 1");

        Assert.AreEqual("Robot", result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task CastAsFloat64_FromInteger_Works()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor, database, dbname, "SELECT CAST(1 AS float64) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Float64, result[0].Row["0"].Type);
        Assert.AreEqual(1.0, result[0].Row["0"].FloatValue, 1e-9);
    }

    [Test]
    [NonParallelizable]
    public async Task NestedCast_Works()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT CAST(CAST(7 AS string) AS int64) FROM robots LIMIT 1");

        Assert.AreEqual(7, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task ToInt64_FromBool_ReturnsZeroOrOne()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor, database, dbname, "SELECT to_int64(enabled) FROM robots LIMIT 1");

        Assert.AreEqual(1, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task ToBool_FromString_Works()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor, database, dbname, "SELECT to_bool(\"TrUe\") FROM robots LIMIT 1");

        Assert.AreEqual(true, result[0].Row["0"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task ToId_FromString_Works()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, string objectId) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT to_id(@id) FROM robots LIMIT 1",
            parameters: new() { { "@id", new ColumnValue(ColumnType.String, objectId) } });

        Assert.AreEqual(objectId, result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task StrId_IsAliasForToId()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, string objectId) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT id FROM robots WHERE id = str_id(@id)",
            parameters: new() { { "@id", new ColumnValue(ColumnType.String, objectId) } });

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(objectId, result[0].Row["id"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task CastIdAlias_Works()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, string objectId) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT CAST(@id AS id) FROM robots LIMIT 1",
            parameters: new() { { "@id", new ColumnValue(ColumnType.String, objectId) } });

        Assert.AreEqual(objectId, result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task NullInput_ReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor, database, dbname, "SELECT to_string(NULL) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Null, result[0].Row["0"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task ToInt64_Overflow_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT to_int64(\"9223372036854775808\") FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task ToInt64_Float64UpperBoundOverflow_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT to_int64(pow(2, 63)) FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("integer overflow", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task ToId_InvalidHex_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT to_id(\"not-an-id\") FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("24-character lowercase hex", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task ToBool_InvalidString_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT to_bool(\"maybe\") FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("cannot convert string value 'maybe' to bool", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task CastToUuid_InvalidString_QuotesValueAndHint()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT CAST(\"not-a-uuid\" AS uuid) FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("the value 'not-a-uuid'", ex.Message);
        StringAssert.Contains("to Uuid", ex.Message);
        StringAssert.Contains("expected a valid UUID string", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task ToInt64_InvalidString_QuotesValueAndHint()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT to_int64(\"not-a-number\") FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("the value 'not-a-number'", ex.Message);
        StringAssert.Contains("expected a 64-bit integer", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task ToFloat64_InvalidString_QuotesValueAndHint()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT to_float64(\"not-a-number\") FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("the value 'not-a-number'", ex.Message);
        StringAssert.Contains("expected a number", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task CastUnknownTargetType_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT CAST(1 AS unknown_type) FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("unknown target type 'unknown_type'", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task CastInWhere_FiltersRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT name FROM robots WHERE CAST(year AS string) = \"42\"");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual("Robot", result[0].Row["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task CastInProjectionAlias_Works()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT CAST(year AS string) AS year_text FROM robots LIMIT 1");

        Assert.AreEqual("42", result[0].Row["year_text"].StrValue);
    }
}
