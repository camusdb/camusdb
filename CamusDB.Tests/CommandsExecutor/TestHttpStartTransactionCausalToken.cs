/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;

using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The causal token on the HTTP <c>/start-transaction</c> and <c>/commit-transaction</c> endpoints,
/// driven through the real controllers with a <see cref="DefaultHttpContext"/>. The start controller
/// binds the token from the JSON body, so a token whose shape does not bind would be dropped with no
/// error and the transaction would start with no floor. These tests send the token in the shape the
/// server itself writes it, and check that the begun transaction carries it as its
/// statement-timestamp floor with all three HLC components. The commit of an explicit transaction
/// returns its token too, so a client can floor its next transaction on it.
///
/// <para>Serial: boots an embedded Kahuna node per test.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestHttpStartTransactionCausalToken : BaseTest
{
    /// <summary>The JSON options of an ASP.NET Core MVC app: camelCase names, as a client receives them.</summary>
    private static readonly JsonSerializerOptions WebJson = new(JsonSerializerDefaults.Web);

    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private PreparedStatementRegistry registry = null!;

    [SetUp]
    public void SetUpRest()
    {
        CommandValidator validator = new(Options);
        CatalogsManager catalogs = new(logger);
        executor = new(validator, catalogs, logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(executor);
        registry = new(Options);
    }

    [TearDown]
    public async Task TearDownRest()
    {
        try { await executor.DisposeAsync(); } catch { }
    }

    private static ControllerContext Context(string json)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(json);
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private ExecuteSQLController Sql(object body) =>
        new(executor, coordinator, registry, logger, Options) { ControllerContext = Context(JsonSerializer.Serialize(body)) };

    private TransactionsController Transactions(string json) =>
        new(executor, coordinator, logger, Options) { ControllerContext = Context(json) };

    private async Task<string> CreateDatabaseWithTableAsync()
    {
        string dbName = "db" + Guid.NewGuid().ToString("n");
        await Sql(new { sql = $"CREATE DATABASE {dbName}" }).ExecuteSQLDDL();
        await Sql(new { databaseName = dbName, sql = "CREATE TABLE robots (id oid PRIMARY KEY, year int64)" }).ExecuteSQLDDL();
        return dbName;
    }

    private async Task<StartTransactionResponse> StartAsync(string json)
    {
        JsonResult result = await Transactions(json).StartTransaction();
        StartTransactionResponse response = (StartTransactionResponse)result.Value!;
        Assert.That(response.Status, Is.EqualTo("ok"), response.Message);
        return response;
    }

    [Test]
    public async Task StartTransaction_CausalTokenFromAWriteResponse_BecomesTheStatementTimestampFloor()
    {
        string db = await CreateDatabaseWithTableAsync();

        JsonResult insert = await Sql(new
        {
            databaseName = db,
            sql = "INSERT INTO robots (id, year) VALUES (gen_id(), 1984)",
        }).ExecuteNonSQLQuery();

        ExecuteNonSQLQueryResponse written = (ExecuteNonSQLQueryResponse)insert.Value!;
        Assert.That(written.Status, Is.EqualTo("ok"), written.Message);
        Assert.That(written.CausalToken, Is.Not.Null, "an autocommit write answers with the token of its commit");

        // Send the token back in exactly the JSON the client received it in.
        string tokenJson = JsonSerializer.Serialize(written, WebJson);
        using JsonDocument writtenDoc = JsonDocument.Parse(tokenJson);
        string rawToken = writtenDoc.RootElement.GetProperty("causalToken").GetRawText();

        StartTransactionResponse started = await StartAsync(
            $$"""{"databaseName":"{{db}}","isolationLevel":"ReadCommitted","causalToken":{{rawToken}}}""");

        KvTransaction tx = coordinator.GetState(started.TxnIdPT, started.TxnIdCounter);
        try
        {
            Assert.That(tx.CausalFloor, Is.EqualTo(written.CausalToken!.Value));
            Assert.That(tx.TakesStatementSnapshot, Is.True);

            // A SELECT in the transaction reads at a statement timestamp at or above the floor, so it
            // sees the write whose token began the transaction.
            JsonResult query = await Sql(new
            {
                databaseName = db,
                txnIdPT = started.TxnIdPT,
                txnIdCounter = started.TxnIdCounter,
                sql = "SELECT year FROM robots",
            }).ExecuteSQLQuery();

            ExecuteSQLQueryResponse read = (ExecuteSQLQueryResponse)query.Value!;
            Assert.That(read.Status, Is.EqualTo("ok"), read.Message);
            Assert.That(read.Total, Is.EqualTo(1));
        }
        finally
        {
            await coordinator.RollbackAsync(tx, CancellationToken.None);
        }
    }

    [Test]
    public async Task CommitTransaction_ReturnsItsCausalToken_ThatFloorsTheNextTransaction()
    {
        string db = await CreateDatabaseWithTableAsync();

        StartTransactionResponse writer = await StartAsync(
            $$"""{"databaseName":"{{db}}","isolationLevel":"ReadCommitted"}""");

        JsonResult insert = await Sql(new
        {
            databaseName = db,
            txnIdPT = writer.TxnIdPT,
            txnIdCounter = writer.TxnIdCounter,
            sql = "INSERT INTO robots (id, year) VALUES (gen_id(), 1984)",
        }).ExecuteNonSQLQuery();

        ExecuteNonSQLQueryResponse written = (ExecuteNonSQLQueryResponse)insert.Value!;
        Assert.That(written.Status, Is.EqualTo("ok"), written.Message);

        JsonResult commit = await Transactions(JsonSerializer.Serialize(new
        {
            databaseName = db,
            txnIdPT = writer.TxnIdPT,
            txnIdCounter = writer.TxnIdCounter,
        }, WebJson)).CommitTransaction();

        CommitTransactionResponse committed = (CommitTransactionResponse)commit.Value!;
        Assert.That(committed.Status, Is.EqualTo("ok"), committed.Message);
        Assert.That(committed.CausalToken, Is.Not.Null, "the commit of a transaction that wrote answers with its token");

        // Send the commit's token back in exactly the JSON the client received it in.
        using JsonDocument committedDoc = JsonDocument.Parse(JsonSerializer.Serialize(committed, WebJson));
        string rawToken = committedDoc.RootElement.GetProperty("causalToken").GetRawText();

        StartTransactionResponse reader = await StartAsync(
            $$"""{"databaseName":"{{db}}","isolationLevel":"ReadCommitted","causalToken":{{rawToken}}}""");

        KvTransaction tx = coordinator.GetState(reader.TxnIdPT, reader.TxnIdCounter);
        try
        {
            Assert.That(tx.CausalFloor, Is.EqualTo(committed.CausalToken!.Value));

            JsonResult query = await Sql(new
            {
                databaseName = db,
                txnIdPT = reader.TxnIdPT,
                txnIdCounter = reader.TxnIdCounter,
                sql = "SELECT year FROM robots",
            }).ExecuteSQLQuery();

            ExecuteSQLQueryResponse read = (ExecuteSQLQueryResponse)query.Value!;
            Assert.That(read.Status, Is.EqualTo("ok"), read.Message);
            Assert.That(read.Total, Is.EqualTo(1));
        }
        finally
        {
            await coordinator.RollbackAsync(tx, CancellationToken.None);
        }
    }

    [Test]
    public async Task StartTransaction_HandWrittenCausalToken_KeepsAllThreeComponents()
    {
        string db = await CreateDatabaseWithTableAsync();

        // N is the node dimension of the HLC and takes part in its ordering: a token that loses it is
        // a different timestamp, so all three components must bind.
        StartTransactionResponse started = await StartAsync(
            $$$"""{"databaseName":"{{{db}}}","isolationLevel":"ReadCommitted","causalToken":{"n":3,"l":12345,"c":7}}""");

        KvTransaction tx = coordinator.GetState(started.TxnIdPT, started.TxnIdCounter);
        try
        {
            Assert.That(tx.CausalFloor, Is.EqualTo(new HLCTimestamp(3, 12345, 7)));
        }
        finally
        {
            await coordinator.RollbackAsync(tx, CancellationToken.None);
        }
    }

    [Test]
    public async Task StartTransaction_WithoutCausalToken_HasNoFloor()
    {
        string db = await CreateDatabaseWithTableAsync();

        StartTransactionResponse started = await StartAsync(
            $$"""{"databaseName":"{{db}}","isolationLevel":"ReadCommitted"}""");

        KvTransaction tx = coordinator.GetState(started.TxnIdPT, started.TxnIdCounter);
        try
        {
            Assert.That(tx.CausalFloor.IsNull(), Is.True, "no token means no floor");
            Assert.That(tx.TakesStatementSnapshot, Is.True);
        }
        finally
        {
            await coordinator.RollbackAsync(tx, CancellationToken.None);
        }
    }
}
