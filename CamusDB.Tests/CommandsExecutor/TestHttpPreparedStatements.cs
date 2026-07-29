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
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage of the REST prepared-statement surface, driven through the real controllers
/// with a <see cref="DefaultHttpContext"/>: prepare, execute positionally (buffered, streamed, and
/// inside an explicit transaction), close, and every refusal. Also pins the parts of the REST model
/// that have no gRPC counterpart — node-local handles, principal ownership, idle expiry, and caps —
/// since those exist precisely because HTTP has no session the server can trust.
///
/// <para>Toggles process-wide config statics for the cap and timeout cases, so
/// <c>[NonParallelizable]</c>.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestHttpPreparedStatements : BaseTest
{
    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private PreparedStatementRegistry registry = null!;

    private int savedIdleTimeout;
    private int savedPerPrincipal;
    private int savedGlobal;

    [SetUp]
    public void SetUpRest()
    {
        CommandValidator validator = new();
        CatalogsManager catalogs = new(logger);
        executor = new(validator, catalogs, logger,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(executor);
        registry = new();

        savedIdleTimeout = CamusConfig.PreparedStatementIdleTimeoutMs;
        savedPerPrincipal = CamusConfig.RestMaxPreparedStatementsPerPrincipal;
        savedGlobal = CamusConfig.RestMaxPreparedStatements;
    }

    [TearDown]
    public async Task TearDownRest()
    {
        CamusConfig.PreparedStatementIdleTimeoutMs = savedIdleTimeout;
        CamusConfig.RestMaxPreparedStatementsPerPrincipal = savedPerPrincipal;
        CamusConfig.RestMaxPreparedStatements = savedGlobal;
        try { await executor.DisposeAsync(); } catch { }
    }

    // ─── Harness ──────────────────────────────────────────────────────────────

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private ExecuteSQLController Sql(object body) =>
        new(executor, coordinator, registry, logger) { ControllerContext = Context(body) };

    private PreparedStatementsController Statements(object body) =>
        new(executor, coordinator, registry, logger) { ControllerContext = Context(body) };

    private async Task<string> CreateDatabaseWithTableAsync(string createTableSql)
    {
        string dbName = "db" + Guid.NewGuid().ToString("n");
        await Sql(new { sql = $"CREATE DATABASE {dbName}" }).ExecuteSQLDDL();
        await Sql(new { databaseName = dbName, sql = createTableSql }).ExecuteSQLDDL();
        return dbName;
    }

    private async Task<PrepareStatementResponse> PrepareAsync(string db, string sql)
    {
        JsonResult result = await Statements(new { databaseName = db, sql }).PrepareSQLStatement();
        return (PrepareStatementResponse)result.Value!;
    }

    private async Task<JsonResult> CloseAsync(string statementId)
        => await Statements(new { statementId }).CloseSQLStatement();

    private static object[] Values(params object[] values) => values;

    /// <summary>A parameter value in the same JSON shape a client sends for the named map.</summary>
    private static object Str(string v) => new { type = ColumnType.String, strValue = v };

    private static object Num(long v) => new { type = ColumnType.Integer64, longValue = v };

    // ─── Prepare ──────────────────────────────────────────────────────────────

    [Test]
    public async Task Prepare_ReturnsHandleAndParameterNamesInBindingOrder()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)");

        PrepareStatementResponse response = await PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");

        Assert.That(response.Status, Is.EqualTo("ok"), response.Message);
        Assert.That(response.StatementId, Is.Not.Null.And.Not.Empty);
        Assert.That(response.ParameterNames, Is.EqualTo(new[] { "@name", "@year" }));
    }

    [Test]
    public async Task Prepare_RejectsUnparsableSqlAtPrepareTime()
    {
        string db = await CreateDatabaseWithTableAsync("CREATE TABLE robots (id oid PRIMARY KEY)");

        PrepareStatementResponse response = await PrepareAsync(db, "SELECT FROM WHERE (");

        Assert.That(response.Status, Is.EqualTo("failed"));
        Assert.That(response.StatementId, Is.Null);
    }

    [Test]
    public async Task Prepare_RejectsDdl()
    {
        string db = await CreateDatabaseWithTableAsync("CREATE TABLE robots (id oid PRIMARY KEY)");

        JsonResult result = await Statements(new { databaseName = db, sql = "DROP TABLE robots" }).PrepareSQLStatement();
        PrepareStatementResponse response = (PrepareStatementResponse)result.Value!;

        Assert.That(response.Status, Is.EqualTo("failed"));
        Assert.That(response.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
    }

    // ─── Execute ──────────────────────────────────────────────────────────────

    [Test]
    public async Task PreparedNonQuery_InsertsWithPositionalValues()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)");

        PrepareStatementResponse prepared = await PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");

        foreach ((string name, long year) in new[] { ("optimus", 1984L), ("bumblebee", 1985L) })
        {
            JsonResult result = await Sql(new
            {
                statementId = prepared.StatementId,
                positionalParameters = Values(Str(name), Num(year)),
            }).ExecuteNonSQLQuery();

            ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)result.Value!;
            Assert.That(response.Status, Is.EqualTo("ok"), response.Message);
            Assert.That(response.Rows, Is.EqualTo(1));
        }

        // The values landed against the right names, not merely in the right count.
        ExecuteSQLQueryResponse rows = await QueryInlineAsync(db, "SELECT name FROM robots ORDER BY year");
        Assert.That(rows.Total, Is.EqualTo(2));
    }

    [Test]
    public async Task PreparedQuery_BindsByOrdinalAndReturnsRows()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)");

        PrepareStatementResponse insert = await PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");
        await Sql(new { statementId = insert.StatementId, positionalParameters = Values(Str("optimus"), Num(1984)) })
            .ExecuteNonSQLQuery();
        await Sql(new { statementId = insert.StatementId, positionalParameters = Values(Str("wall-e"), Num(2008)) })
            .ExecuteNonSQLQuery();

        PrepareStatementResponse select = await PrepareAsync(db, "SELECT name FROM robots WHERE year = @year");

        JsonResult result = await Sql(new
        {
            statementId = select.StatementId,
            positionalParameters = Values(Num(2008)),
        }).ExecuteSQLQuery();

        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;
        Assert.That(response.Status, Is.EqualTo("ok"), response.Message);
        Assert.That(response.Total, Is.EqualTo(1), "the WHERE must have bound @year to 2008");
    }

    [Test]
    public async Task PreparedQuery_StreamsOverTheNdjsonEndpoint()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)");

        PrepareStatementResponse insert = await PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");
        await Sql(new { statementId = insert.StatementId, positionalParameters = Values(Str("optimus"), Num(1984)) })
            .ExecuteNonSQLQuery();

        PrepareStatementResponse select = await PrepareAsync(db, "SELECT name FROM robots WHERE year = @year");
        ExecuteSQLController controller = Sql(new
        {
            statementId = select.StatementId,
            positionalParameters = Values(Num(1984)),
        });

        await controller.ExecuteSQLQueryStream();

        string body = ReadBody(controller);
        Assert.That(controller.Response.StatusCode, Is.EqualTo(200), body);
        Assert.That(body, Does.Contain("optimus"));
    }

    [Test]
    public async Task PreparedNonQuery_InsideAnExplicitTransaction_CommitsWithIt()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)");

        PrepareStatementResponse insert = await PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");

        Core.Transactions.KvTransaction tx = await coordinator.StartAsync(db);

        JsonResult result = await Sql(new
        {
            txnIdPT = tx.ClientId.L,
            txnIdCounter = tx.ClientId.C,
            statementId = insert.StatementId,
            positionalParameters = Values(Str("wall-e"), Num(2008)),
        }).ExecuteNonSQLQuery();

        Assert.That(((ExecuteNonSQLQueryResponse)result.Value!).Status, Is.EqualTo("ok"),
            ((ExecuteNonSQLQueryResponse)result.Value!).Message);

        await coordinator.CommitTrackedAsync(tx);

        ExecuteSQLQueryResponse rows = await QueryInlineAsync(db, "SELECT name FROM robots");
        Assert.That(rows.Total, Is.EqualTo(1));
    }

    // ─── Refusals ─────────────────────────────────────────────────────────────

    [Test]
    public async Task PreparedExecution_WithWrongValueCount_IsRefused()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)");

        PrepareStatementResponse prepared = await PrepareAsync(
            db, "SELECT name FROM robots WHERE year = @year AND name = @name");

        JsonResult result = await Sql(new
        {
            statementId = prepared.StatementId,
            positionalParameters = Values(Num(1984)),
        }).ExecuteSQLQuery();

        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;
        Assert.That(response.Status, Is.EqualTo("failed"));
        Assert.That(response.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
    }

    [Test]
    public async Task PreparedExecution_MixedWithInlineSql_IsRefused()
    {
        string db = await CreateDatabaseWithTableAsync("CREATE TABLE robots (id oid PRIMARY KEY, year int64)");
        PrepareStatementResponse prepared = await PrepareAsync(db, "SELECT id FROM robots WHERE year = @year");

        JsonResult result = await Sql(new
        {
            statementId = prepared.StatementId,
            sql = "SELECT id FROM robots",
            positionalParameters = Values(Num(1984)),
        }).ExecuteSQLQuery();

        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;
        Assert.That(response.Status, Is.EqualTo("failed"));
        Assert.That(response.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
    }

    [Test]
    public async Task UnknownHandle_Returns404SoAClientCanBranchOnStatusAlone()
    {
        await CreateDatabaseWithTableAsync("CREATE TABLE robots (id oid PRIMARY KEY, year int64)");

        JsonResult result = await Sql(new
        {
            statementId = "nosuch.handle",
            positionalParameters = Values(Num(1984)),
        }).ExecuteSQLQuery();

        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;
        Assert.That(response.Code, Is.EqualTo(CamusDBErrorCodes.UnknownPreparedStatement));
        Assert.That(result.StatusCode, Is.EqualTo(404));
    }

    [Test]
    public async Task UnknownHandle_OnTheStreamingEndpoint_FailsBeforeAnyStreamBytes()
    {
        await CreateDatabaseWithTableAsync("CREATE TABLE robots (id oid PRIMARY KEY, year int64)");

        ExecuteSQLController controller = Sql(new
        {
            statementId = "nosuch.handle",
            positionalParameters = Values(Num(1984)),
        });

        await controller.ExecuteSQLQueryStream();

        // Nothing was on the wire yet, so the failure is a normal JSON 404 rather than an in-band
        // trailer the client could only discover at the end of a 200 response.
        Assert.That(controller.Response.StatusCode, Is.EqualTo(404));
        Assert.That(ReadBody(controller), Does.Contain(CamusDBErrorCodes.UnknownPreparedStatement));
    }

    [Test]
    public async Task DdlEndpoint_RejectsAHandle()
    {
        string db = await CreateDatabaseWithTableAsync("CREATE TABLE robots (id oid PRIMARY KEY)");

        JsonResult result = await Sql(new { databaseName = db, statementId = "whatever" }).ExecuteSQLDDL();
        ExecuteDDLSQLResponse response = (ExecuteDDLSQLResponse)result.Value!;

        Assert.That(response.Status, Is.EqualTo("failed"));
        Assert.That(response.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
    }

    [Test]
    public async Task InlineRequest_WithPositionalValues_IsRefused()
    {
        string db = await CreateDatabaseWithTableAsync("CREATE TABLE robots (id oid PRIMARY KEY, year int64)");

        JsonResult result = await Sql(new
        {
            databaseName = db,
            sql = "SELECT id FROM robots WHERE year = @year",
            positionalParameters = Values(Num(1984)),
        }).ExecuteSQLQuery();

        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;
        Assert.That(response.Status, Is.EqualTo("failed"));
        Assert.That(response.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
    }

    // ─── Close ────────────────────────────────────────────────────────────────

    [Test]
    public async Task Close_FreesTheHandleAndIsIdempotent()
    {
        string db = await CreateDatabaseWithTableAsync("CREATE TABLE robots (id oid PRIMARY KEY, year int64)");
        PrepareStatementResponse prepared = await PrepareAsync(db, "SELECT id FROM robots WHERE year = @year");

        Assert.That(((CloseStatementResponse)(await CloseAsync(prepared.StatementId!)).Value!).Status, Is.EqualTo("ok"));
        Assert.That(((CloseStatementResponse)(await CloseAsync(prepared.StatementId!)).Value!).Status, Is.EqualTo("ok"),
            "closing twice must succeed — the requested end state already holds");
        Assert.That(((CloseStatementResponse)(await CloseAsync("never.existed")).Value!).Status, Is.EqualTo("ok"));

        JsonResult afterClose = await Sql(new
        {
            statementId = prepared.StatementId,
            positionalParameters = Values(Num(1984)),
        }).ExecuteSQLQuery();

        Assert.That(((ExecuteSQLQueryResponse)afterClose.Value!).Code,
            Is.EqualTo(CamusDBErrorCodes.UnknownPreparedStatement));
    }

    // ─── Registry model: ownership, expiry, caps ──────────────────────────────

    [Test]
    public void AHandleIsUnusableByAnotherPrincipalAndIndistinguishableFromNonexistent()
    {
        Principal alice = new("alice", isSuperuser: false, []);
        Principal bob = new("bob", isSuperuser: false, []);

        (string handle, _) = registry.Prepare(alice, "db", "SELECT 1");

        // Compared against a handle this node genuinely minted and then released: same shape, same
        // prefix, so the only difference under test is "exists but is not yours" vs "does not exist".
        (string released, _) = registry.Prepare(alice, "db", "SELECT 2");
        registry.Close(alice, released);

        CamusDBException stolen = Assert.Throws<CamusDBException>(() => registry.Resolve(bob, handle))!;
        CamusDBException missing = Assert.Throws<CamusDBException>(() => registry.Resolve(bob, released))!;

        Assert.That(stolen.Code, Is.EqualTo(CamusDBErrorCodes.UnknownPreparedStatement));
        Assert.That(stolen.Message, Is.EqualTo(missing.Message),
            "an ownership-specific error would confirm the handle exists");

        // Bob cannot close what he cannot see, either.
        Assert.That(registry.Close(bob, handle), Is.False);
        Assert.That(registry.Resolve(alice, handle), Is.Not.Null);
    }

    [Test]
    public async Task AnIdleHandleExpiresAndIsReportedAsUnknown()
    {
        CamusConfig.PreparedStatementIdleTimeoutMs = 1;

        (string handle, _) = registry.Prepare(null, "db", "SELECT 1");
        await Task.Delay(30);

        CamusDBException expired = Assert.Throws<CamusDBException>(() => registry.Resolve(null, handle))!;
        Assert.That(expired.Code, Is.EqualTo(CamusDBErrorCodes.UnknownPreparedStatement));
        Assert.That(registry.Count, Is.Zero, "resolving an expired handle must also drop it");
    }

    [Test]
    public async Task TheReaperDropsIdleHandlesAndKeepsTheOwnerCountHonest()
    {
        CamusConfig.PreparedStatementIdleTimeoutMs = 1;
        CamusConfig.RestMaxPreparedStatementsPerPrincipal = 2;

        registry.Prepare(null, "db", "SELECT 1");
        registry.Prepare(null, "db", "SELECT 2");
        await Task.Delay(30);

        Assert.That(registry.ReapExpired(), Is.EqualTo(2));
        Assert.That(registry.Count, Is.Zero);

        // If reaping had not decremented the per-owner counter, this would now fail at the cap.
        Assert.DoesNotThrow(() => registry.Prepare(null, "db", "SELECT 3"));
        Assert.DoesNotThrow(() => registry.Prepare(null, "db", "SELECT 4"));
    }

    [Test]
    public void OverThePerPrincipalCap_RefusesRatherThanEvictingALiveHandle()
    {
        CamusConfig.PreparedStatementIdleTimeoutMs = 600_000;
        CamusConfig.RestMaxPreparedStatementsPerPrincipal = 2;

        (string first, _) = registry.Prepare(null, "db", "SELECT 1");
        registry.Prepare(null, "db", "SELECT 2");

        CamusDBException tooMany = Assert.Throws<CamusDBException>(() => registry.Prepare(null, "db", "SELECT 3"))!;
        Assert.That(tooMany.Code, Is.EqualTo(CamusDBErrorCodes.PreparedStatementLimitExceeded));

        // The refusal must not have cost the caller a handle it still holds.
        Assert.That(registry.Resolve(null, first), Is.Not.Null);

        // Closing one makes room again.
        Assert.That(registry.Close(null, first), Is.True);
        Assert.DoesNotThrow(() => registry.Prepare(null, "db", "SELECT 3"));
    }

    [Test]
    public void OverTheNodeWideCap_RefusesEvenWhenNoPrincipalIsAtItsOwnCap()
    {
        CamusConfig.PreparedStatementIdleTimeoutMs = 600_000;
        CamusConfig.RestMaxPreparedStatementsPerPrincipal = 100;
        CamusConfig.RestMaxPreparedStatements = 2;

        Principal alice = new("alice", isSuperuser: false, []);
        Principal bob = new("bob", isSuperuser: false, []);

        registry.Prepare(alice, "db", "SELECT 1");
        registry.Prepare(bob, "db", "SELECT 2");

        // Neither principal is near its own cap; the node-wide budget is what refuses.
        CamusDBException tooMany = Assert.Throws<CamusDBException>(() => registry.Prepare(alice, "db", "SELECT 3"))!;
        Assert.That(tooMany.Code, Is.EqualTo(CamusDBErrorCodes.PreparedStatementLimitExceeded));
        Assert.That(tooMany.Message, Does.Contain("node-wide"));
    }

    [Test]
    public void QuotaAccountingSurvivesCloseRacingPrepare()
    {
        // The published-then-counted order used to let a close decrement a counter that did not exist
        // yet, install zero, and leave the owner permanently one statement over. Sequential tests
        // cannot see it — the interleaving has to be produced.
        CamusConfig.PreparedStatementIdleTimeoutMs = 600_000;
        CamusConfig.RestMaxPreparedStatementsPerPrincipal = 4;

        for (int round = 0; round < 200; round++)
        {
            string? handle = null;
            using System.Threading.Barrier barrier = new(2);

            Task prepare = Task.Run(() =>
            {
                barrier.SignalAndWait();
                (handle, _) = registry.Prepare(null, "db", $"SELECT {round}");
            });

            Task close = Task.Run(() =>
            {
                barrier.SignalAndWait();
                // Spin briefly so the close lands as near the publish as possible.
                for (int i = 0; i < 50 && handle is null; i++)
                    Thread.SpinWait(20);
                if (handle is not null)
                    registry.Close(null, handle);
            });

            Task.WaitAll(prepare, close);
            if (handle is not null)
                registry.Close(null, handle);
        }

        // Whatever order the rounds resolved in, nothing is live — so the quota must be free. If the
        // counter had drifted, this next prepare would fail at the cap.
        Assert.That(registry.Count, Is.Zero);
        Assert.That(registry.RetainedBytes, Is.Zero, "released statements must return their bytes");
        Assert.DoesNotThrow(() => registry.Prepare(null, "db", "SELECT after"));
    }

    [Test]
    public void ConcurrentPreparesCannotOverfillTheLastFreeSlot()
    {
        // Check-then-insert let every concurrent caller observe the same free slot and publish. With
        // admission taken atomically, exactly one of them can win.
        CamusConfig.PreparedStatementIdleTimeoutMs = 600_000;
        CamusConfig.RestMaxPreparedStatementsPerPrincipal = 4;
        CamusConfig.RestMaxPreparedStatements = 4;

        registry.Prepare(null, "db", "SELECT 1");
        registry.Prepare(null, "db", "SELECT 2");
        registry.Prepare(null, "db", "SELECT 3");

        int admitted = 0;
        using System.Threading.Barrier gate = new(8);

        Parallel.For(0, 8, i =>
        {
            gate.SignalAndWait();
            try
            {
                registry.Prepare(null, "db", $"SELECT {1000 + i}");
                Interlocked.Increment(ref admitted);
            }
            catch (CamusDBException e) when (e.Code == CamusDBErrorCodes.PreparedStatementLimitExceeded)
            {
                // Expected for everyone but the winner.
            }
        });

        Assert.That(admitted, Is.EqualTo(1), "only one caller may take the last slot");
        Assert.That(registry.Count, Is.EqualTo(4));
    }

    [Test]
    public void AStatementLargerThanThePerStatementLimitIsRejected()
    {
        int savedMax = CamusConfig.MaxPreparedStatementBytes;
        CamusConfig.MaxPreparedStatementBytes = 256;
        try
        {
            string padding = new('x', 4096);
            CamusDBException tooBig = Assert.Throws<CamusDBException>(
                () => registry.Prepare(null, "db", $"SELECT 1 -- {padding}"))!;

            // Invalid input, not a quota failure: closing other statements would not make it fit.
            Assert.That(tooBig.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
            Assert.That(registry.Count, Is.Zero);
            Assert.That(registry.RetainedBytes, Is.Zero, "a rejected statement must reserve nothing");
        }
        finally
        {
            CamusConfig.MaxPreparedStatementBytes = savedMax;
        }
    }

    [Test]
    public void TheRetainedByteBudgetRefusesEvenWhenTheCountCapWouldNot()
    {
        long savedNodeBytes = CamusConfig.RestMaxPreparedStatementBytes;
        long savedOwnerBytes = CamusConfig.RestMaxPreparedStatementBytesPerPrincipal;
        CamusConfig.RestMaxPreparedStatementsPerPrincipal = 1000;
        CamusConfig.RestMaxPreparedStatements = 1000;
        CamusConfig.RestMaxPreparedStatementBytesPerPrincipal = 8 * 1024;
        CamusConfig.RestMaxPreparedStatementBytes = 1024 * 1024;
        CamusConfig.MaxPreparedStatementBytes = 65_536;

        try
        {
            string padding = new('x', 1024);
            int admitted = 0;
            CamusDBException? refused = null;

            for (int i = 0; i < 100 && refused is null; i++)
            {
                try
                {
                    registry.Prepare(null, "db", $"SELECT {i} -- {padding}");
                    admitted++;
                }
                catch (CamusDBException e) when (e.Code == CamusDBErrorCodes.PreparedStatementLimitExceeded)
                {
                    refused = e;
                }
            }

            // Counting statements alone would have let all 100 in; the byte budget is what stops it.
            Assert.That(refused, Is.Not.Null, "the retained-byte budget must eventually refuse");
            Assert.That(refused!.Message, Does.Contain("retained-byte"));
            Assert.That(admitted, Is.LessThan(100).And.GreaterThan(0));
            Assert.That(registry.RetainedBytes, Is.LessThanOrEqualTo(8 * 1024));
        }
        finally
        {
            CamusConfig.RestMaxPreparedStatementBytes = savedNodeBytes;
            CamusConfig.RestMaxPreparedStatementBytesPerPrincipal = savedOwnerBytes;
        }
    }

    // ─── Prepared DML through the endpoints ───────────────────────────────────

    [Test]
    public async Task PreparedUpdateAndDelete_BindPositionally()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)");

        PrepareStatementResponse insert = await PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");
        await Sql(new { statementId = insert.StatementId, positionalParameters = Values(Str("optimus"), Num(1984)) })
            .ExecuteNonSQLQuery();
        await Sql(new { statementId = insert.StatementId, positionalParameters = Values(Str("wall-e"), Num(2008)) })
            .ExecuteNonSQLQuery();

        PrepareStatementResponse update = await PrepareAsync(
            db, "UPDATE robots SET name = @name WHERE year = @year");
        ExecuteNonSQLQueryResponse updated = (ExecuteNonSQLQueryResponse)(await Sql(new
        {
            statementId = update.StatementId,
            positionalParameters = Values(Str("optimus prime"), Num(1984)),
        }).ExecuteNonSQLQuery()).Value!;

        Assert.That(updated.Status, Is.EqualTo("ok"), updated.Message);
        Assert.That(updated.Rows, Is.EqualTo(1));

        PrepareStatementResponse delete = await PrepareAsync(db, "DELETE FROM robots WHERE year = @year");
        ExecuteNonSQLQueryResponse deleted = (ExecuteNonSQLQueryResponse)(await Sql(new
        {
            statementId = delete.StatementId,
            positionalParameters = Values(Num(2008)),
        }).ExecuteNonSQLQuery()).Value!;

        Assert.That(deleted.Status, Is.EqualTo("ok"), deleted.Message);
        Assert.That(deleted.Rows, Is.EqualTo(1));

        ExecuteSQLQueryResponse remaining = await QueryInlineAsync(db, "SELECT name FROM robots");
        Assert.That(remaining.Total, Is.EqualTo(1), "only the updated row should survive");
    }

    [Test]
    public async Task PreparedExecutionsRunConcurrentlyAndEachBindsItsOwnValues()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)");

        PrepareStatementResponse insert = await PrepareAsync(
            db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");

        const int count = 16;
        await Task.WhenAll(Enumerable.Range(0, count).Select(i =>
            Sql(new
            {
                statementId = insert.StatementId,
                positionalParameters = Values(Str($"robot-{i}"), Num(2000 + i)),
            }).ExecuteNonSQLQuery()));

        // One immutable entry shared by every concurrent request: values must not cross over.
        ExecuteSQLQueryResponse all = await QueryInlineAsync(db, "SELECT name, year FROM robots ORDER BY year");
        Assert.That(all.Total, Is.EqualTo(count));
    }

    [Test]
    public async Task ACacheHintedPreparedSelect_MissesThenHitsLikeAnInlineOne()
    {
        // A real cache, or the executor reports "cache-disabled" and a broken hit path would pass.
        QueryResultCache cache = new(sweepIntervalMs: -1);
        CommandExecutor cachedExecutor = new(
            new CommandValidator(), new CatalogsManager(logger), logger,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false, cache: cache);
        HttpTransactionCoordinator cachedCoordinator = new(cachedExecutor);

        ExecuteSQLController CachedSql(object body) =>
            new(cachedExecutor, cachedCoordinator, registry, logger) { ControllerContext = Context(body) };

        try
        {
            string db = "db" + Guid.NewGuid().ToString("n");
            await CachedSql(new { sql = $"CREATE DATABASE {db}" }).ExecuteSQLDDL();
            await CachedSql(new
            {
                databaseName = db,
                sql = "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)",
            }).ExecuteSQLDDL();
            await CachedSql(new
            {
                databaseName = db,
                sql = "INSERT INTO robots (id, name, year) VALUES (gen_id(), 'optimus', 1984)",
            }).ExecuteNonSQLQuery();

            JsonResult prepareResult = await new PreparedStatementsController(
                cachedExecutor, cachedCoordinator, registry, logger)
            {
                ControllerContext = Context(new
                {
                    databaseName = db,
                    sql = "SELECT name FROM robots{cache=rest_prepared} WHERE year = @year",
                }),
            }.PrepareSQLStatement();

            string statementId = ((PrepareStatementResponse)prepareResult.Value!).StatementId!;

            object body() => new { statementId, positionalParameters = Values(Num(1984)) };

            ExecuteSQLQueryResponse cold = (ExecuteSQLQueryResponse)(await CachedSql(body()).ExecuteSQLQuery()).Value!;
            ExecuteSQLQueryResponse warm = (ExecuteSQLQueryResponse)(await CachedSql(body()).ExecuteSQLQuery()).Value!;

            Assert.That(cold.Status, Is.EqualTo("ok"), cold.Message);
            Assert.That(cold.CacheName, Is.EqualTo("rest_prepared"));
            Assert.That(cold.CacheStatus, Is.EqualTo("miss"));

            // The hint rides the SQL text, which a prepared execution reuses instance-identically, so
            // the second execution must hit exactly as an inline repeat would.
            Assert.That(warm.CacheStatus, Is.EqualTo("hit"));
            Assert.That(warm.Total, Is.EqualTo(1));
        }
        finally
        {
            try { await cachedExecutor.DisposeAsync(); } catch { }
        }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private async Task<ExecuteSQLQueryResponse> QueryInlineAsync(string db, string sql)
    {
        JsonResult result = await Sql(new { databaseName = db, sql }).ExecuteSQLQuery();
        return (ExecuteSQLQueryResponse)result.Value!;
    }

    private static string ReadBody(ControllerBase controller)
    {
        Stream body = controller.Response.Body;
        body.Seek(0, SeekOrigin.Begin);
        using StreamReader reader = new(body, Encoding.UTF8, leaveOpen: true);
        return reader.ReadToEnd();
    }
}
