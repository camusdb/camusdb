/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;

namespace CamusDB.Tests.Extensions;

/// <summary>
/// The extension statements through the real transports, on an engine that loads <c>Fixture.Good</c>:
/// <c>CALL</c> and trigger DDL over the REST non-query and DDL endpoints and over gRPC, including the
/// rollback a failing procedure must cause on the transport's own transaction.
/// </summary>
[NonParallelizable]
public sealed class TestExtensionTransports : ExtensionTestBase
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        defaults with { ExtensionAssemblies = [FixturePath("Fixture.Good")] };

    private static ControllerContext Context(string body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(body);
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        return new ControllerContext { HttpContext = http };
    }

    private ExecuteSQLController Controller(CommandExecutor executor, string db, string sql) =>
        new(executor, new HttpTransactionCoordinator(executor), new PreparedStatementRegistry(Options), logger, Options)
        {
            ControllerContext = Context(JsonSerializer.Serialize(new { databaseName = db, sql }))
        };

    private async Task<(string db, CommandExecutor executor)> SetupAsync()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE TABLE accounts (id OID PRIMARY KEY DEFAULT (gen_id()), name STRING, balance INT64)");
        await ExecNonQuery(executor, db, "INSERT INTO accounts (name, balance) VALUES ('alice', 100), ('bob', 0)");
        return (db, executor);
    }

    private static async Task<long> Balance(CommandExecutor executor, string db, string name) =>
        (await Scalar(executor, db, $"SELECT balance FROM accounts WHERE name = '{name}'")).LongValue;

    [Test]
    public async Task Rest_Call_CommitsAndReportsRows()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        JsonResult result = await Controller(executor, db, "CALL fx_transfer('alice', 'bob', 25)").ExecuteNonSQLQuery();

        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)result.Value!;
        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(2, response.Rows);
        Assert.AreEqual(75, await Balance(executor, db, "alice"));
    }

    [Test]
    public async Task Rest_Call_ThatThrows_RollsBack()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        JsonResult result = await Controller(executor, db, "CALL fx_transfer('alice', 'bob', 1000)").ExecuteNonSQLQuery();

        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)result.Value!;
        Assert.AreEqual("failed", response.Status);
        Assert.AreEqual(CamusDBErrorCodes.ProcedureFailed, response.Code);
        Assert.AreEqual(400, result.StatusCode);
        Assert.AreEqual(100, await Balance(executor, db, "alice"), "the transport's transaction rolled back");
    }

    [Test]
    public async Task Rest_TriggerDdl_OnTheDdlAndNonQueryEndpoints()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        JsonResult created = await Controller(executor, db,
            "CREATE TRIGGER guard ON accounts BEFORE UPDATE EXTERNAL NAME 'CamusDB.Fixtures.Good.GuardTrigger'").ExecuteSQLDDL();
        Assert.AreEqual(200, created.StatusCode ?? 200);

        // Schema DDL is also accepted by the non-query endpoint.
        JsonResult dropped = await Controller(executor, db, "DROP TRIGGER guard ON accounts").ExecuteNonSQLQuery();
        Assert.AreEqual("ok", ((ExecuteNonSQLQueryResponse)dropped.Value!).Status);

        JsonResult missing = await Controller(executor, db, "DROP TRIGGER guard ON accounts").ExecuteSQLDDL();
        Assert.AreEqual(404, missing.StatusCode);
    }

    [Test]
    public async Task Grpc_Call_CommitsAndUnknownProcedureIsNotFound()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        CamusSqlService service = new(executor, new HttpTransactionCoordinator(executor), logger,
            TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);

        NonQueryReply reply = await service.ExecuteNonQuery(
            new SqlRequest { Database = db, Sql = "CALL fx_transfer('alice', 'bob', 10)" }, new TestServerCallContext(default));
        Assert.AreEqual(2, reply.AffectedRows);
        Assert.AreEqual(90, await Balance(executor, db, "alice"));

        global::Grpc.Core.RpcException ex = Assert.ThrowsAsync<global::Grpc.Core.RpcException>(() => service.ExecuteNonQuery(
            new SqlRequest { Database = db, Sql = "CALL fx_nope()" }, new TestServerCallContext(default)))!;
        Assert.AreEqual(global::Grpc.Core.StatusCode.NotFound, ex.StatusCode);
    }
}
