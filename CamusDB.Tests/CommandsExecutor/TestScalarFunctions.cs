
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
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

public class TestScalarFunctions : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupDatabase()
    {
        return await CreateDatabase();
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectIds)> SetupBasicTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        List<string> objectIds = new();

        for (int i = 0; i < 3; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();
            objectIds.Add(objectId);

            InsertTicket insertTicket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, objectId) },
                        { "name", new(ColumnType.String, $"some name {i}") },
                        { "year", new(ColumnType.Integer64, 2000 + i) },
                        { "enabled", new(ColumnType.Bool, i % 2 == 0) },
                    }
                });

            await executor.Insert(insertTicket);
        }

        await database.Transactions.CommitAsync(txnState);
        return (dbname, database, executor, objectIds);
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
    public async Task GenId_ReturnsUniqueObjectIds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        List<QueryResultRow> first = await ExecuteSelect(executor, database, dbname, "SELECT gen_id() FROM robots LIMIT 1");
        List<QueryResultRow> second = await ExecuteSelect(executor, database, dbname, "SELECT gen_id() FROM robots LIMIT 1");

        Assert.AreEqual(1, first.Count);
        Assert.AreEqual(1, second.Count);

        string firstId = first[0].Row["0"].StrValue!;
        string secondId = second[0].Row["0"].StrValue!;

        Assert.AreEqual(24, firstId.Length);
        Assert.AreEqual(24, secondId.Length);
        Assert.AreNotEqual(firstId, secondId);
    }

    [Test]
    [NonParallelizable]
    public async Task StrId_ConvertsStringParameterToId()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectIds) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT id, enabled FROM robots WHERE id = str_id(@id)",
            parameters: new() { { "@id", new ColumnValue(ColumnType.String, objectIds[0]) } });

        Assert.IsNotEmpty(result);
        Assert.AreEqual(objectIds[0], result[0].Row["id"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task StrId_AcceptsIdArgument()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> objectIds) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT str_id(id) FROM robots LIMIT 1");

        Assert.AreEqual(objectIds[0], result[0].Row["0"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task StrId_NullArgument_ReturnsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT str_id(NULL) FROM robots LIMIT 1");

        Assert.AreEqual(ColumnType.Null, result[0].Row["0"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task CurrentTimestamp_ReturnsIso8601UtcString()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor,
            database,
            dbname,
            "SELECT current_timestamp() FROM robots LIMIT 1");

        string timestamp = result[0].Row["0"].StrValue!;
        Assert.IsTrue(DateTimeOffset.TryParse(timestamp, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTimeOffset parsed));
        Assert.AreEqual(TimeSpan.Zero, parsed.Offset);
    }

    [Test]
    [NonParallelizable]
    public async Task Now_IsCaseInsensitiveAliasForCurrentTimestamp()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        List<QueryResultRow> lower = await ExecuteSelect(executor, database, dbname, "SELECT now() FROM robots LIMIT 1");
        List<QueryResultRow> upper = await ExecuteSelect(executor, database, dbname, "SELECT NOW() FROM robots LIMIT 1");

        foreach (string timestamp in new[] { lower[0].Row["0"].StrValue!, upper[0].Row["0"].StrValue! })
        {
            Assert.IsTrue(DateTimeOffset.TryParse(timestamp, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTimeOffset parsed));
            Assert.AreEqual(TimeSpan.Zero, parsed.Offset);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task UnknownFunction_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT does_not_exist(1) FROM robots",
            parameters: null);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        })!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidAstStmt, ex.Code);
        StringAssert.Contains("Function not found 'does_not_exist'", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task WrongArity_GenIdWithArgument_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT gen_id(1) FROM robots",
            parameters: null);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        })!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'gen_id' expects 0 argument(s) but received 1", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task WrongArity_AliasUsesCalledNameInError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT now(1) FROM robots",
            parameters: null);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        })!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'now' expects 0 argument(s) but received 1", ex.Message);
        StringAssert.DoesNotContain("current_timestamp", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task WrongArity_StrIdWithoutArgument_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT str_id() FROM robots",
            parameters: null);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        })!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'str_id' expects 1 argument(s) but received 0", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task WrongType_StrIdWithInteger_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txnState,
            database: dbname,
            sql: "SELECT str_id(123) FROM robots",
            parameters: null);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        })!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Function 'str_id' cannot convert Integer64 to Id", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public void PropagateNull_ReturnsNullWhenAnyArgumentIsNull()
    {
        ColumnValue? result = ScalarFunctionArguments.PropagateNull(
            new[]
            {
                new ColumnValue(ColumnType.String, "hello"),
                new ColumnValue(ColumnType.Null, 0),
            });

        Assert.IsNotNull(result);
        Assert.AreEqual(ColumnType.Null, result!.Type);
    }
}
