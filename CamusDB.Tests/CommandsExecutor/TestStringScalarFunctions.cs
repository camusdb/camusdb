
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
public class TestStringScalarFunctions : SharedNodeBaseTest
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
                new("tag", ColumnType.String),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        (string name, string? tag, long year)[] rows =
        [
            ("  Alpha  ", "abc", 2000),
            ("beta", null, 2001),
            ("GAMMA", "prefix-match", 2002),
            ("", "empty-name", 2003),
        ];

        foreach ((string name, string? tag, long year) in rows)
        {
            Dictionary<string, ColumnValue> row = new()
            {
                { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "name", new(ColumnType.String, name) },
                { "year", new(ColumnType.Integer64, year) },
            };

            if (tag is not null)
                row["tag"] = new ColumnValue(ColumnType.String, tag);

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
    public async Task Length_ReturnsInteger64()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor, database, dbname, "SELECT length(\"hello\") FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Integer64, result[0].Row["0"].Type);
        Assert.AreEqual(5, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task LowerAndUpper_UseInvariantCasing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> lower = await ExecuteSelect(
            executor, database, dbname, "SELECT lower(name) FROM robots WHERE year = 2002 LIMIT 1");
        List<QueryResultRow> upper = await ExecuteSelect(
            executor, database, dbname, "SELECT upper(name) FROM robots WHERE year = 2002 LIMIT 1");

        Assert.AreEqual("gamma", lower[0].Row["0"].StrValue);
        Assert.AreEqual("GAMMA", upper[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TrimVariants_RemoveWhitespace()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> trim = await ExecuteSelect(
            executor, database, dbname, "SELECT trim(name) FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> ltrim = await ExecuteSelect(
            executor, database, dbname, "SELECT ltrim(name) FROM robots WHERE year = 2000 LIMIT 1");
        List<QueryResultRow> rtrim = await ExecuteSelect(
            executor, database, dbname, "SELECT rtrim(name) FROM robots WHERE year = 2000 LIMIT 1");

        Assert.AreEqual("Alpha", trim[0].Row["0"].StrValue);
        Assert.AreEqual("Alpha  ", ltrim[0].Row["0"].StrValue);
        Assert.AreEqual("  Alpha", rtrim[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Substring_OneBasedPositions()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> twoArg = await ExecuteSelect(
            executor, database, dbname, "SELECT substring(\"CamusDB\", 2) FROM robots LIMIT 1");
        List<QueryResultRow> threeArg = await ExecuteSelect(
            executor, database, dbname, "SELECT substring(\"CamusDB\", 2, 3) FROM robots LIMIT 1");

        Assert.AreEqual("amusDB", twoArg[0].Row["0"].StrValue);
        Assert.AreEqual("amu", threeArg[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Substring_StartBeyondStringLength_ReturnsEmptyString()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> twoArg = await ExecuteSelect(
            executor, database, dbname, "SELECT substring(\"abc\", 2147483648) FROM robots LIMIT 1");
        List<QueryResultRow> threeArg = await ExecuteSelect(
            executor, database, dbname, "SELECT substring(\"abc\", 2147483648, 1) FROM robots LIMIT 1");

        Assert.AreEqual("", twoArg[0].Row["0"].StrValue);
        Assert.AreEqual("", threeArg[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Replace_ReplacesAllOccurrences()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT replace(\"aba\", \"a\", \"z\") FROM robots LIMIT 1");

        Assert.AreEqual("zbz", result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Concat_IsVariadicAndCoercesScalars()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT concat(\"x\", 7, true) FROM robots LIMIT 1");

        Assert.AreEqual("x7true", result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Concat_TreatsNullArgumentsAsEmptyStrings()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT concat(\"a\", NULL, \"b\") FROM robots LIMIT 1");

        Assert.AreEqual("ab", result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Concat_AllNullArguments_ReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT concat(NULL) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Null, result[0].Row["0"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task StringPredicates_UseOrdinalComparison()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> contains = await ExecuteSelect(
            executor, database, dbname, "SELECT contains(\"CamusDB\", \"mus\") FROM robots LIMIT 1");
        List<QueryResultRow> startsWith = await ExecuteSelect(
            executor, database, dbname, "SELECT starts_with(tag, \"prefix\") FROM robots WHERE year = 2002 LIMIT 1");
        List<QueryResultRow> endsWith = await ExecuteSelect(
            executor, database, dbname, "SELECT ends_with(\"CamusDB\", \"DB\") FROM robots LIMIT 1");

        Assert.AreEqual(true, contains[0].Row["0"].BoolValue);
        Assert.AreEqual(true, startsWith[0].Row["0"].BoolValue);
        Assert.AreEqual(true, endsWith[0].Row["0"].BoolValue);
    }

    [Test]
    [NonParallelizable]
    public async Task NestedStringCalls_Work()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT upper(trim(name)) FROM robots WHERE year = 2000 LIMIT 1");

        Assert.AreEqual("ALPHA", result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task StringFunctionsInWhere_FilterRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT name FROM robots WHERE starts_with(lower(trim(name)), \"a\") ORDER BY year");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual("  Alpha  ", result[0].Row["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task StringFunctionsInProjectionAlias_Work()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT length(trim(name)) AS name_len FROM robots WHERE year = 2000 LIMIT 1");

        Assert.AreEqual(5, result[0].Row["name_len"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task EmptyStringInputs_Work()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT length(name) FROM robots WHERE year = 2003 LIMIT 1");

        Assert.AreEqual(0, result[0].Row["0"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task NullStringInput_PropagatesNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT lower(NULL) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Null, result[0].Row["0"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task Substring_InvalidPosition_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT substring(\"abc\", 0) FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'substring' expects argument 2 to be a 1-based position", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task Substring_InvalidLength_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        CamusDBException ex = await AssertSelectThrows(
            executor,
            database,
            dbname,
            "SELECT substring(\"abc\", 1, -1) FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'substring' expects argument 3 to be a non-negative length", ex.Message);
    }
}
