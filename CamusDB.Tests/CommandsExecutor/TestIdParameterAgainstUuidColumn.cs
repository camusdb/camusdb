/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.Grpc;
using ColumnType = CamusDB.Core.Catalogs.Models.ColumnType;
using CamusDB.Tests.Grpc;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A parameter typed as an object id (Id) against a <c>uuid</c> column, and the other pairs of a Uuid
/// or Id with a type that has no comparison rule with it.
///
/// <para>A client that binds a <see cref="Guid"/> as an Id parameter sends 36 characters of GUID text,
/// which is never an object id. Such a value could equal nothing, so <c>IN</c> returned no rows and
/// <c>NOT IN</c> returned every row with no error, while <c>=</c> escaped as a raw
/// <see cref="ArgumentException"/>. The engine now refuses an invalid Id parameter with
/// <see cref="CamusDBErrorCodes.InvalidInput"/> before it parses the statement, on every transport.</para>
///
/// <para>A valid object id against a uuid column is a different case: the pair has no comparison rule.
/// It is never equal under <c>=</c>, <c>IN</c>, <c>NOT IN</c> and <c>CASE</c>, and an ordering operator
/// on it is an <see cref="CamusDBErrorCodes.InvalidInput"/> error. The planner seeks an index for a
/// short list and scans for a long one, so each test walks the list size across that switch.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestIdParameterAgainstUuidColumn : BaseTest
{
    private static readonly int[] Percentages = [1, 2, 10, 25, 49, 50, 51, 75, 100];

    // ─── Invalid Id parameters ────────────────────────────────────────────────

    [Test]
    public async Task IdParameterHoldingGuidText_IsInvalidInput_ForEveryOperatorAndListSize()
    {
        (string db, CommandExecutor executor, List<string> ids) = await Seed(100, "uuid", UuidKey);

        foreach (int count in Percentages)
        {
            Dictionary<string, ColumnValue> parameters = IdParameters(ids.Take(count));
            string list = List(parameters);

            AssertGuidTextRejected(() => Count(executor, db, $"WHERE id IN {list}", parameters), $"IN, {count}");
            AssertGuidTextRejected(() => Count(executor, db, $"WHERE id NOT IN {list}", parameters), $"NOT IN, {count}");
        }

        Dictionary<string, ColumnValue> one = IdParameters(ids.Take(1));
        AssertGuidTextRejected(() => Count(executor, db, "WHERE id = @p0", one), "=");
        AssertGuidTextRejected(() => Count(executor, db, "WHERE id <> @p0", one), "<>");
        AssertGuidTextRejected(() => NonQuery(executor, db, "UPDATE users SET name = 'x' WHERE id = @p0", one), "UPDATE");
        AssertGuidTextRejected(() => NonQuery(executor, db, "DELETE FROM users WHERE id IN (@p0)", one), "DELETE");

        Assert.AreEqual(100, await Count(executor, db, "", null), "a refused statement changes no row");
    }

    [Test]
    public void InvalidIdParameter_IsRefusedByTheValidator_WithTheParameterName()
    {
        CommandValidator validator = new(Options);

        void Validate(Dictionary<string, ColumnValue> parameters) =>
            validator.Validate(new ExecuteSQLTicket(null!, "somedb", "SELECT 1", parameters));

        CamusDBException notHex = Assert.Throws<CamusDBException>(() =>
            Validate(new() { ["@who"] = new ColumnValue(ColumnType.Id, "0123456789abcdef0123456z") }))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, notHex.Code);
        StringAssert.Contains("'@who'", notHex.Message);
        StringAssert.DoesNotContain("UUID", notHex.Message, "only a value that parses as a UUID gets the uuid hint");

        // Upper-case hex is an object id: the row encoder accepts it, so it is made canonical, not refused.
        Dictionary<string, ColumnValue> upper = new()
        {
            ["@who"] = new ColumnValue(ColumnType.Id, "0123456789ABCDEF01234567"),
            ["@many"] = ColumnValue.FromArray(ColumnType.Id,
            [
                new ColumnValue(ColumnType.Id, "0123456789abcdef01234567"),
                new ColumnValue(ColumnType.Id, "ABCDEF0123456789ABCDEF01"),
            ]),
        };
        Validate(upper);
        Assert.AreEqual("0123456789abcdef01234567", upper["@who"].StrValue);
        Assert.AreEqual("abcdef0123456789abcdef01", upper["@many"].ArrayValues![1].StrValue);
        Assert.AreEqual(ColumnType.Id, upper["@many"].ArrayElementType);

        CamusDBException element = Assert.Throws<CamusDBException>(() =>
            Validate(new()
            {
                ["@ids"] = ColumnValue.FromArray(ColumnType.Id,
                [
                    new ColumnValue(ColumnType.Id, "0123456789abcdef01234567"),
                    new ColumnValue(ColumnType.Id, Guid.NewGuid().ToString()),
                ]),
            }))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, element.Code);
        StringAssert.Contains("'@ids'", element.Message);

        CamusDBException empty = Assert.Throws<CamusDBException>(() =>
            Validate(new() { ["@e"] = new ColumnValue(ColumnType.Id, "") }))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, empty.Code);

        CamusDBException longValue = Assert.Throws<CamusDBException>(() =>
            Validate(new() { ["@l"] = new ColumnValue(ColumnType.Id, new string('z', 10_000)) }))!;
        Assert.Less(longValue.Message.Length, 400, "the message shows only the start of a long value");

        Assert.DoesNotThrow(() => Validate(new()
        {
            ["@ok"] = new ColumnValue(ColumnType.Id, "0123456789abcdef01234567"),
            ["@s"] = new ColumnValue(ColumnType.String, Guid.NewGuid().ToString()),
            ["@u"] = ColumnValue.FromUuid(Guid.NewGuid()),
            ["@n"] = ColumnValue.Null,
        }));
    }

    [Test]
    public async Task ObjectIdKey_UpperCaseParameters_MatchOnSeekAndScan()
    {
        (string db, CommandExecutor executor, List<string> ids) = await Seed(100, "object_id", ObjectIdKey);

        // The index seek parses upper-case hex, and the scan compares text. Both must find the rows.
        foreach (int count in Percentages)
        {
            Dictionary<string, ColumnValue> parameters = IdParameters(ids.Take(count).Select(id => id.ToUpperInvariant()));
            string list = List(parameters);

            Assert.AreEqual(count, await Count(executor, db, $"WHERE id IN {list}", parameters), $"IN, {count}");
            Assert.AreEqual(100 - count, await Count(executor, db, $"WHERE id NOT IN {list}", parameters), $"NOT IN, {count}");
        }

        Dictionary<string, ColumnValue> one = IdParameters([ids[7].ToUpperInvariant()]);
        Assert.AreEqual(1, await Count(executor, db, "WHERE id = @p0", one), "=");
    }

    // ─── Valid object ids against a uuid column ───────────────────────────────

    [Test]
    public async Task UuidKey_ValidObjectIdParameters_AreNeverEqual_OnSeekAndScan()
    {
        (string db, CommandExecutor executor, _) = await Seed(100, "uuid", UuidKey);

        foreach (int count in Percentages)
        {
            Dictionary<string, ColumnValue> parameters = ObjectIdParameters(count);
            string list = List(parameters);

            Assert.AreEqual(0, await Count(executor, db, $"WHERE id IN {list}", parameters), $"IN, {count}");
            Assert.AreEqual(100, await Count(executor, db, $"WHERE id NOT IN {list}", parameters), $"NOT IN, {count}");
        }

        Dictionary<string, ColumnValue> one = ObjectIdParameters(1);
        Assert.AreEqual(0, await Count(executor, db, "WHERE id = @p0", one), "=");
        Assert.AreEqual(100, await Count(executor, db, "WHERE id <> @p0", one), "<>");
        Assert.AreEqual(0, await Count(executor, db, "WHERE @p0 = id", one), "= with the operands swapped");
    }

    [Test]
    public async Task UuidKey_MixedList_MatchesOnlyTheUuidItems_OnSeekAndScan()
    {
        (string db, CommandExecutor executor, List<string> ids) = await Seed(100, "uuid", UuidKey);

        foreach (int count in Percentages)
        {
            Dictionary<string, ColumnValue> parameters = [];
            int i = 0;
            foreach (string id in ids.Take(count))
                parameters[$"@p{i++}"] = ColumnValue.FromUuid(Guid.Parse(id));

            // The object id item can match no uuid row: the seek drops it, and the scan's filter
            // finds it equal to nothing.
            parameters["@oid"] = new ColumnValue(ColumnType.Id, "0123456789abcdef01234567");
            string list = List(parameters);

            Assert.AreEqual(count, await Count(executor, db, $"WHERE id IN {list}", parameters), $"IN, {count}");
            Assert.AreEqual(100 - count, await Count(executor, db, $"WHERE id NOT IN {list}", parameters), $"NOT IN, {count}");
            Assert.AreEqual(count, (await Query(executor, db, $"SELECT id FROM users WHERE id IN {list}", parameters)).Count, $"rows, {count}");
        }
    }

    [Test]
    public async Task UuidKey_OrderingAgainstObjectId_IsInvalidInput()
    {
        (string db, CommandExecutor executor, _) = await Seed(20, "uuid", UuidKey);

        Dictionary<string, ColumnValue> parameters = ObjectIdParameters(2);

        foreach (string where in new[] { "WHERE id < @p0", "WHERE id >= @p0", "WHERE id BETWEEN @p0 AND @p1" })
        {
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Count(executor, db, where, parameters))!;
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, where);
            StringAssert.Contains("Uuid, Id", ex.Message, where);
        }
    }

    [Test]
    public async Task UuidKey_SimpleCaseAgainstObjectId_IsNotAMatch()
    {
        (string db, CommandExecutor executor, _) = await Seed(20, "uuid", UuidKey);

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(
            executor, db, "SELECT CASE id WHEN @p0 THEN 1 ELSE 0 END AS m FROM users", ObjectIdParameters(1));

        Assert.AreEqual(20, rows.Count);
        Assert.IsTrue(rows.All(r => r["m"].LongValue == 0));
    }

    [Test]
    public async Task ObjectIdKey_UuidParameters_AreNeverEqual_OnSeekAndScan()
    {
        (string db, CommandExecutor executor, _) = await Seed(100, "object_id", ObjectIdKey);

        foreach (int count in Percentages)
        {
            Dictionary<string, ColumnValue> parameters = [];
            for (int i = 0; i < count; i++)
                parameters[$"@p{i}"] = ColumnValue.FromUuid(Guid.NewGuid());
            string list = List(parameters);

            Assert.AreEqual(0, await Count(executor, db, $"WHERE id IN {list}", parameters), $"IN, {count}");
            Assert.AreEqual(100, await Count(executor, db, $"WHERE id NOT IN {list}", parameters), $"NOT IN, {count}");
        }

        Dictionary<string, ColumnValue> one = new() { ["@p0"] = ColumnValue.FromUuid(Guid.NewGuid()) };
        Assert.AreEqual(0, await Count(executor, db, "WHERE id = @p0", one), "=");
        Assert.AreEqual(100, await Count(executor, db, "WHERE id <> @p0", one), "<>");
    }

    [Test]
    public async Task IntegerKey_UuidParameter_IsNotEqualAndDoesNotThrow()
    {
        (string db, CommandExecutor executor, _) = await Seed(20, "int64", i => (i + 1).ToString());

        Dictionary<string, ColumnValue> one = new() { ["@p0"] = ColumnValue.FromUuid(Guid.NewGuid()) };
        Assert.AreEqual(0, await Count(executor, db, "WHERE id = @p0", one), "=");
        Assert.AreEqual(0, await Count(executor, db, "WHERE id IN (@p0)", one), "IN");
        Assert.AreEqual(20, await Count(executor, db, "WHERE id NOT IN (@p0)", one), "NOT IN");
    }

    [Test]
    public async Task UuidKey_MixedList_SeeksOnlyTheUuidItems()
    {
        (string db, CommandExecutor executor, List<string> ids) = await Seed(100, "uuid", UuidKey);

        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@p0"] = ColumnValue.FromUuid(Guid.Parse(ids[0])),
            ["@p1"] = ColumnValue.FromUuid(Guid.Parse(ids[1])),
            ["@oid"] = new ColumnValue(ColumnType.Id, "0123456789abcdef01234567"),
        };

        // An index key uses the constant's encoding. An object id key on a uuid index addresses no
        // uuid entry by value, and on a non-unique index its prefix match could reach unrelated keys,
        // so the planner drops the item and seeks the uuid items only.
        List<IReadOnlyDictionary<string, ColumnValue>> plan = await Query(
            executor, db, "EXPLAIN SELECT id FROM users WHERE id IN (@p0, @p1, @oid)", parameters);

        IReadOnlyDictionary<string, ColumnValue>? seek = plan.FirstOrDefault(r => r["node"].StrValue == "index-in-list");
        Assert.IsNotNull(seek, "the uuid items still drive the index seek");
        StringAssert.Contains("values=2", seek!["detail"].StrValue);

        Assert.AreEqual(2, (await Query(executor, db, "SELECT id FROM users WHERE id IN (@p0, @p1, @oid)", parameters)).Count);
    }

    // ─── Transports ───────────────────────────────────────────────────────────

    [Test]
    public async Task Rest_InlineAndPrepared_InvalidIdParameterIsInvalidInput()
    {
        (CommandExecutor executor, HttpTransactionCoordinator coordinator) = ServiceExecutor();
        PreparedStatementRegistry registry = new(Options);

        try
        {
            ExecuteSQLController Sql(object body) =>
                new(executor, coordinator, registry, logger, Options) { ControllerContext = Context(body) };

            string db = "db" + Guid.NewGuid().ToString("n");
            await Sql(new { sql = $"CREATE DATABASE {db}" }).ExecuteSQLDDL();
            await Sql(new { databaseName = db, sql = "CREATE TABLE users (id uuid PRIMARY KEY NOT NULL, name string NULL)" }).ExecuteSQLDDL();

            object guidAsId = new { type = ColumnType.Id, strValue = Guid.NewGuid().ToString() };

            JsonResult inline = await Sql(new
            {
                databaseName = db,
                sql = "SELECT id FROM users WHERE id = @p0",
                parameters = new Dictionary<string, object> { ["@p0"] = guidAsId },
            }).ExecuteSQLQuery();

            ExecuteSQLQueryResponse inlineResponse = (ExecuteSQLQueryResponse)inline.Value!;
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, inlineResponse.Code, inlineResponse.Message);
            StringAssert.Contains("@p0", inlineResponse.Message);

            PreparedStatementsController statements =
                new(executor, coordinator, registry, logger, Options)
                {
                    ControllerContext = Context(new { databaseName = db, sql = "SELECT id FROM users WHERE id IN (@a, @b)" }),
                };
            PrepareStatementResponse prepared = (PrepareStatementResponse)(await statements.PrepareSQLStatement()).Value!;
            Assert.AreEqual("ok", prepared.Status, prepared.Message);

            JsonResult bound = await Sql(new
            {
                statementId = prepared.StatementId,
                positionalParameters = new[] { guidAsId, guidAsId },
            }).ExecuteSQLQuery();

            ExecuteSQLQueryResponse boundResponse = (ExecuteSQLQueryResponse)bound.Value!;
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, boundResponse.Code, boundResponse.Message);
        }
        finally
        {
            await executor.DisposeAsync();
        }
    }

    [Test]
    public async Task Grpc_InlineAndPrepared_InvalidIdParameterIsInvalidInput()
    {
        (CommandExecutor executor, HttpTransactionCoordinator coordinator) = ServiceExecutor();

        try
        {
            CamusSqlService service = new(executor, coordinator, logger, TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);

            string db = "db" + Guid.NewGuid().ToString("n");
            await service.ExecuteDdl(new SqlRequest { Database = "", Sql = $"CREATE DATABASE {db}" }, new TestServerCallContext());
            await service.ExecuteDdl(new SqlRequest { Database = db, Sql = "CREATE TABLE users (id uuid PRIMARY KEY NOT NULL, name string NULL)" }, new TestServerCallContext());

            Value guidAsId = new() { IdValue = Guid.NewGuid().ToString() };

            ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
            ObservingStreamWriter<BatchExecuteResponse> writer = new();
            Task server = service.BatchExecute(reader, writer, new TestServerCallContext());

            SqlRequest inline = new() { Database = db, Sql = "SELECT id FROM users WHERE id = @p0" };
            inline.Parameters["@p0"] = guidAsId;
            reader.Push(new BatchExecuteRequest { RequestId = 1, Kind = BatchStatementKind.Query, Request = inline });

            BatchExecuteResponse inlineReply = await Terminal(writer, 1);
            Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.Error, inlineReply.PayloadCase);
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, inlineReply.Error.Code, inlineReply.Error.Message);
            StringAssert.Contains("@p0", inlineReply.Error.Message);

            reader.Push(new BatchExecuteRequest
            {
                RequestId = 2,
                Kind = BatchStatementKind.Prepare,
                Request = new SqlRequest { Database = db, Sql = "DELETE FROM users WHERE id IN (@a)" },
            });
            BatchExecuteResponse prepared = await writer.WaitFor(m => m.RequestId == 2 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.PrepareReply);

            SqlRequest execute = new() { StatementId = prepared.PrepareReply.StatementId };
            execute.PositionalParameters.Add(guidAsId);
            reader.Push(new BatchExecuteRequest { RequestId = 3, Kind = BatchStatementKind.NonQuery, Request = execute });

            BatchExecuteResponse boundReply = await Terminal(writer, 3);
            Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.Error, boundReply.PayloadCase);
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, boundReply.Error.Code, boundReply.Error.Message);

            reader.Complete();
            await server;
        }
        finally
        {
            await executor.DisposeAsync();
        }
    }

    // ─── Harness ──────────────────────────────────────────────────────────────

    private static void AssertGuidTextRejected(Func<Task> statement, string label)
    {
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await statement())!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, label);
        StringAssert.Contains("'@p0'", ex.Message, label);
        StringAssert.Contains("uuid parameter", ex.Message, label);
    }

    private (CommandExecutor, HttpTransactionCoordinator) ServiceExecutor()
    {
        CommandExecutor executor = new(new CommandValidator(Options), new CatalogsManager(logger), logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        return (executor, new HttpTransactionCoordinator(executor));
    }

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private static Task<BatchExecuteResponse> Terminal(ObservingStreamWriter<BatchExecuteResponse> writer, int requestId) =>
        writer.WaitFor(m => m.RequestId == requestId && m.PayloadCase is
            BatchExecuteResponse.PayloadOneofCase.QueryComplete or
            BatchExecuteResponse.PayloadOneofCase.NonQuery or
            BatchExecuteResponse.PayloadOneofCase.Error);

    private static string List(Dictionary<string, ColumnValue> parameters) =>
        "(" + string.Join(", ", parameters.Keys) + ")";

    /// <summary>GUID text bound as an Id: what a client sends for a <see cref="Guid"/> with no uuid mapping.</summary>
    private static Dictionary<string, ColumnValue> IdParameters(IEnumerable<string> values)
    {
        Dictionary<string, ColumnValue> parameters = [];
        int i = 0;
        foreach (string value in values)
            parameters[$"@p{i++}"] = new ColumnValue(ColumnType.Id, value);
        return parameters;
    }

    private static Dictionary<string, ColumnValue> ObjectIdParameters(int count)
    {
        Dictionary<string, ColumnValue> parameters = [];
        for (int i = 0; i < count; i++)
            parameters[$"@p{i}"] = new ColumnValue(ColumnType.Id, (0x200000 + i).ToString("x24"));
        return parameters;
    }

    private static string UuidKey(int i) => Guid.NewGuid().ToString();

    private static string ObjectIdKey(int i) => (0x100000 + i).ToString("x24");

    private async Task<(string Db, CommandExecutor Executor, List<string> Ids)> Seed(int rows, string idType, Func<int, string> key)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        await Ddl(executor, db, $"CREATE TABLE users (id {idType} PRIMARY KEY NOT NULL, name string NULL)");

        List<string> ids = [];
        List<string> values = [];
        for (int i = 0; i < rows; i++)
        {
            string id = key(i);
            ids.Add(id);
            values.Add(idType == "int64" ? $"({id}, 'user{i}')" : $"('{id}', 'user{i}')");
        }

        await NonQuery(executor, db, "INSERT INTO users (id, name) VALUES " + string.Join(", ", values), null);

        // With statistics the planner takes the cost-based path, which a long-running server does.
        await Query(executor, db, "ANALYZE users", null);

        return (db, executor, ids);
    }

    private static async Task Ddl(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task NonQuery(CommandExecutor executor, string db, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));
            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<long> Count(CommandExecutor executor, string db, string fromTail, Dictionary<string, ColumnValue>? parameters)
    {
        List<IReadOnlyDictionary<string, ColumnValue>> rows =
            await Query(executor, db, $"SELECT COUNT(*) AS c FROM users {fromTail}", parameters);
        Assert.AreEqual(1, rows.Count);
        return rows[0]["c"].LongValue;
    }

    private static async Task<List<IReadOnlyDictionary<string, ColumnValue>>> Query(
        CommandExecutor executor, string db, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));

            List<IReadOnlyDictionary<string, ColumnValue>> result = [];
            await foreach (QueryResultRow row in rows)
                result.Add(row.Row);

            await database.Transactions.CommitAsync(tx);
            return result;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }
}
