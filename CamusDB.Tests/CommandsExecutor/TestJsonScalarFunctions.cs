
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
public class TestJsonScalarFunctions : SharedNodeBaseTest
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
                new("payload", ColumnType.String),
                new("candidate", ColumnType.String),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        (string name, string? payload, string? candidate, long year)[] rows =
        [
            ("object-row", """{"name":"bot","tags":["a","b"],"meta":{"enabled":true,"count":3}}""", """{"meta":{"enabled":true}}""", 2000),
            ("array-row", "[1,2,3]", "[2]", 2001),
            ("string-row", "\"hello\"", null, 2002),
            ("number-row", "42", "42", 2003),
            ("bool-row", "true", null, 2004),
            ("null-row", "null", null, 2005),
            ("invalid-row", "not-json", null, 2006),
            ("missing-row", null, null, 2007),
            ("no-match-row", """{"name":"bot","tags":["a","b"],"meta":{"enabled":true,"count":3}}""", """{"missing":true}""", 2008),
            ("large-number-row", """{"value":1e400}""", null, 2009),
        ];

        foreach ((string name, string? payload, string? candidate, long year) in rows)
        {
            Dictionary<string, ColumnValue> row = new()
            {
                { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "name", new(ColumnType.String, name) },
                { "year", new(ColumnType.Integer64, year) },
            };

            if (payload is not null)
                row["payload"] = new ColumnValue(ColumnType.String, payload);

            if (candidate is not null)
                row["candidate"] = new ColumnValue(ColumnType.String, candidate);

            await executor.Insert(new InsertTicket(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                values: new() { row }));
        }

        await database.Transactions.CommitAsync(txnState);
        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> ExecuteSelect(
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
    public async Task JsonValid_DetectsValidAndInvalidJson()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> valid = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_valid(payload) FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> invalid = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_valid(payload) FROM robots WHERE year = 2006 LIMIT 1");

        Assert.AreEqual(ColumnType.Bool, valid[0].Row["0"].Type);
        Assert.IsTrue(valid[0].Row["0"].BoolValue);
        Assert.IsFalse(invalid[0].Row["0"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task JsonValid_NullInput_ReturnsFalse()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_valid(NULL) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Bool, result[0].Row["0"].Type);
        Assert.IsFalse(result[0].Row["0"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task JsonType_ReturnsTypeNamesOrNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> objectType = await ExecuteSelect(
            executor, database, dbname, "SELECT json_type(payload) FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> arrayType = await ExecuteSelect(
            executor, database, dbname, "SELECT json_type(payload) FROM robots WHERE year = 2001 LIMIT 1");
        List<QueryResultRow> stringType = await ExecuteSelect(
            executor, database, dbname, "SELECT json_type(payload) FROM robots WHERE year = 2002 LIMIT 1");
        List<QueryResultRow> numberType = await ExecuteSelect(
            executor, database, dbname, "SELECT json_type(payload) FROM robots WHERE year = 2003 LIMIT 1");
        List<QueryResultRow> boolType = await ExecuteSelect(
            executor, database, dbname, "SELECT json_type(payload) FROM robots WHERE year = 2004 LIMIT 1");
        List<QueryResultRow> nullType = await ExecuteSelect(
            executor, database, dbname, "SELECT json_type(payload) FROM robots WHERE year = 2005 LIMIT 1");
        List<QueryResultRow> invalid = await ExecuteSelect(
            executor, database, dbname, "SELECT json_type(payload) FROM robots WHERE year = 2006 LIMIT 1");

        Assert.AreEqual("object", objectType[0].Row["0"].StrValue);
        Assert.AreEqual("array", arrayType[0].Row["0"].StrValue);
        Assert.AreEqual("string", stringType[0].Row["0"].StrValue);
        Assert.AreEqual("number", numberType[0].Row["0"].StrValue);
        Assert.AreEqual("boolean", boolType[0].Row["0"].StrValue);
        Assert.AreEqual("null", nullType[0].Row["0"].StrValue);
        Assert.AreEqual(ColumnType.Null, invalid[0].Row["0"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task JsonExtract_ReturnsJsonTextAtPath()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> root = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_extract(payload, \"$\") FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> member = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_extract(payload, \"$.name\") FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> nested = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_extract(payload, \"$.meta.count\") FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> arrayIndex = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_extract(payload, \"$.tags[1]\") FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> missing = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_extract(payload, \"$.missing\") FROM robots WHERE year = 2000 LIMIT 1");

        StringAssert.StartsWith("{", root[0].Row["0"].StrValue);
        Assert.AreEqual("\"bot\"", member[0].Row["0"].StrValue);
        Assert.AreEqual("3", nested[0].Row["0"].StrValue);
        Assert.AreEqual("\"b\"", arrayIndex[0].Row["0"].StrValue);
        Assert.AreEqual(ColumnType.Null, missing[0].Row["0"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task JsonValue_ReturnsScalarColumnValues()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> stringValue = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_value(payload, \"$.name\") FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> numberValue = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_value(payload, \"$.meta.count\") FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> boolValue = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_value(payload, \"$.meta.enabled\") FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> objectValue = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_value(payload, \"$.meta\") FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> arrayValue = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_value(payload, \"$.tags\") FROM robots WHERE year = 2000 LIMIT 1");

        Assert.AreEqual(ColumnType.String, stringValue[0].Row["0"].Type);
        Assert.AreEqual("bot", stringValue[0].Row["0"].StrValue);
        Assert.AreEqual(ColumnType.Integer64, numberValue[0].Row["0"].Type);
        Assert.AreEqual(3, numberValue[0].Row["0"].LongValue);
        Assert.AreEqual(ColumnType.Bool, boolValue[0].Row["0"].Type);
        Assert.IsTrue(boolValue[0].Row["0"].BoolValue);
        Assert.AreEqual(ColumnType.Null, objectValue[0].Row["0"].Type);
        Assert.AreEqual(ColumnType.Null, arrayValue[0].Row["0"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task JsonValue_LargeNumber_ReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_value(payload, \"$.value\") FROM robots WHERE year = 2009 LIMIT 1");

        Assert.AreEqual(ColumnType.Null, result[0].Row["0"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task JsonArrayLength_ReturnsRootAndNestedLengths()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> rootArray = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_array_length(payload) FROM robots WHERE year = 2001 LIMIT 1");
        List<QueryResultRow> nestedArray = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_array_length(payload, \"$.tags\") FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> notArray = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_array_length(payload) FROM robots WHERE year = 2000 LIMIT 1");

        Assert.AreEqual(3, rootArray[0].Row["0"].LongValue);
        Assert.AreEqual(2, nestedArray[0].Row["0"].LongValue);
        Assert.AreEqual(ColumnType.Null, notArray[0].Row["0"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task JsonContains_MatchesStructuralContainment()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> objectSubset = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_contains(payload, candidate) FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> arraySubset = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_contains(payload, candidate) FROM robots WHERE year = 2001 LIMIT 1");
        List<QueryResultRow> scalarMatch = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_contains(payload, candidate) FROM robots WHERE year = 2003 LIMIT 1");
        List<QueryResultRow> noMatch = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT json_contains(payload, candidate) FROM robots WHERE year = 2008 LIMIT 1");

        Assert.IsTrue(objectSubset[0].Row["0"].BoolValue);
        Assert.IsTrue(arraySubset[0].Row["0"].BoolValue);
        Assert.IsTrue(scalarMatch[0].Row["0"].BoolValue);
        Assert.IsFalse(noMatch[0].Row["0"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task JsonFunctions_UnsupportedPath_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT json_extract(payload, \"$.tags[-1]\") FROM robots WHERE year = 2000 LIMIT 1");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'json_extract' unsupported JSON path", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task JsonFunctions_InWhereClause_FiltersRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT name FROM robots WHERE year = 2000 AND json_valid(payload) = true AND json_type(payload) = \"object\"");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual("object-row", result[0].Row["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task JsonFunctions_NullInput_ReturnsNullExceptJsonValid()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> type = await ExecuteSelect(
            executor, database, dbname, "SELECT json_type(NULL) FROM robots LIMIT 1");
        List<QueryResultRow> extract = await ExecuteSelect(
            executor, database, dbname, "SELECT json_extract(NULL, \"$\") FROM robots LIMIT 1");
        List<QueryResultRow> contains = await ExecuteSelect(
            executor, database, dbname, "SELECT json_contains(NULL, \"{}\") FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Null, type[0].Row["0"].Type);
        Assert.AreEqual(ColumnType.Null, extract[0].Row["0"].Type);
        Assert.AreEqual(ColumnType.Null, contains[0].Row["0"].Type);
    }
}
