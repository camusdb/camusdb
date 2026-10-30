/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The caps that keep one client from exhausting a node with cheap inputs: the buffered result
/// ceiling, the inline statement size, the open explicit transaction count, the database count, the
/// cluster activity fan-out, the client connection count, and the IN-list hash set that replaces a
/// per-row scan. Each test drives the real entry point (controller, ticket, coordinator) with an
/// engine built with the limit it tests, and checks both the refusal and the path the limit must not
/// touch.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestInputResourceLimits : BaseTest
{
    private ILogger<ICamusDB> Logger => logger;

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private ExecuteSQLController SqlController(CommandExecutor executor, CamusDBOptions options, object body) =>
        new(executor, new HttpTransactionCoordinator(executor), new PreparedStatementRegistry(options), Logger, options)
        {
            ControllerContext = Context(body)
        };

    private static async Task Run(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Query(
        CommandExecutor executor, string dbname, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    // ── Buffered result ceiling ────────────────────────────────────────────────────

    /// <summary>
    /// A short statement whose result passes the row cap is refused on the buffered endpoint with
    /// CADB0556 and HTTP 400, instead of being held in memory until the process dies.
    /// </summary>
    [Test]
    public async Task BufferedQuery_PastRowCap_IsRefused()
    {
        CamusDBOptions options = Options with { MaxBufferedResultRows = 1_000 };
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(options);

        JsonResult result = await SqlController(executor, options, new
        {
            databaseName = dbname,
            sql = "SELECT i FROM generate_series(1, 5000) AS i",
        }).ExecuteSQLQuery();

        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;
        Assert.AreEqual("failed", response.Status);
        Assert.AreEqual(CamusDBErrorCodes.ResultSetTooLarge, response.Code);
        Assert.AreEqual(400, result.StatusCode);
    }

    /// <summary>A result at exactly the row cap is still returned whole.</summary>
    [Test]
    public async Task BufferedQuery_AtRowCap_IsReturned()
    {
        CamusDBOptions options = Options with { MaxBufferedResultRows = 1_000 };
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(options);

        JsonResult result = await SqlController(executor, options, new
        {
            databaseName = dbname,
            sql = "SELECT i FROM generate_series(1, 1000) AS i",
        }).ExecuteSQLQuery();

        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;
        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(1_000, response.Total);
    }

    /// <summary>The byte cap refuses a result of few, wide rows that the row cap allows.</summary>
    [Test]
    public async Task BufferedQuery_PastByteCap_IsRefused()
    {
        CamusDBOptions options = Options with { MaxBufferedResultRows = 0, MaxBufferedResultBytes = 64 * 1024 };
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(options);

        JsonResult result = await SqlController(executor, options, new
        {
            databaseName = dbname,
            sql = $"SELECT '{new string('x', 4096)}' AS wide FROM generate_series(1, 100) AS i",
        }).ExecuteSQLQuery();

        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;
        Assert.AreEqual(CamusDBErrorCodes.ResultSetTooLarge, response.Code, response.Message);
    }

    /// <summary>
    /// The streaming endpoint sends rows as it reads them, so the buffered cap must not apply to it:
    /// it is the endpoint the refusal tells the caller to use.
    /// </summary>
    [Test]
    public async Task StreamQuery_PastRowCap_StreamsEveryRow()
    {
        CamusDBOptions options = Options with { MaxBufferedResultRows = 1_000 };
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(options);

        ExecuteSQLController controller = SqlController(executor, options, new
        {
            databaseName = dbname,
            sql = "SELECT i FROM generate_series(1, 5000) AS i",
        });
        await controller.ExecuteSQLQueryStream();

        string[] lines = Encoding.UTF8.GetString(((MemoryStream)controller.Response.Body).ToArray())
            .Split('\n', StringSplitOptions.RemoveEmptyEntries);

        using JsonDocument trailer = JsonDocument.Parse(lines[^1]);
        Assert.AreEqual("ok", trailer.RootElement.GetProperty("status").GetString(), lines[^1]);
        Assert.AreEqual(5_000, trailer.RootElement.GetProperty("total").GetInt32());
    }

    /// <summary>The JSON <c>/query</c> endpoint, which has no LIMIT field, gets the same cap.</summary>
    [Test]
    public async Task JsonQuery_PastRowCap_IsRefused()
    {
        CamusDBOptions options = Options with { MaxBufferedResultRows = 10 };
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(options);

        await Run(executor, dbname, "CREATE TABLE t (id int64 PRIMARY KEY NOT NULL)");
        await Run(executor, dbname, "INSERT INTO t (id) SELECT i FROM generate_series(1, 50) AS i");

        QueryController controller = new(executor, new HttpTransactionCoordinator(executor), Logger, options)
        {
            ControllerContext = Context(new { databaseName = dbname, tableName = "t" })
        };

        JsonResult result = await controller.Query();
        QueryResponse response = (QueryResponse)result.Value!;
        Assert.AreEqual(CamusDBErrorCodes.ResultSetTooLarge, response.Code, response.Message);
        Assert.AreEqual(400, result.StatusCode);
    }

    /// <summary>
    /// <c>/query-by-id</c> reported every failure with HTTP 200. An unknown table must now carry
    /// the status its error code maps to.
    /// </summary>
    [Test]
    public async Task QueryById_Failure_CarriesErrorStatus()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        QueryController controller = new(executor, new HttpTransactionCoordinator(executor), Logger, Options)
        {
            ControllerContext = Context(new { databaseName = dbname, tableName = "missing", id = "6849f3a1c2e7d50b4f8a91d3" })
        };

        JsonResult result = await controller.QueryById();
        QueryResponse response = (QueryResponse)result.Value!;
        Assert.AreEqual("failed", response.Status);
        Assert.AreEqual(CamusDBErrorCodes.GetHttpStatus(response.Code!), result.StatusCode);
        Assert.AreNotEqual(200, result.StatusCode);
    }

    // ── Inline statement size ──────────────────────────────────────────────────────

    /// <summary>The guard counts UTF-8 bytes: two-byte characters reach the limit at half the length.</summary>
    [Test]
    public void SizeGuard_CountsUtf8Bytes()
    {
        StatementSizeGuard.Check(new string('a', 1_000), 1_000);
        StatementSizeGuard.Check(new string('é', 500), 1_000);
        StatementSizeGuard.Check(new string('a', 5_000), 0);

        CamusDBException tooManyChars = Assert.Throws<CamusDBException>(() => StatementSizeGuard.Check(new string('a', 1_001), 1_000))!;
        Assert.AreEqual(CamusDBErrorCodes.StatementTooLarge, tooManyChars.Code);

        CamusDBException tooManyBytes = Assert.Throws<CamusDBException>(() => StatementSizeGuard.Check(new string('é', 501), 1_000))!;
        Assert.AreEqual(CamusDBErrorCodes.StatementTooLarge, tooManyBytes.Code);
    }

    /// <summary>
    /// An oversized statement is refused through each entry: the REST endpoints (which parse to route
    /// before any ticket exists), and a ticket sent to the engine directly. A statement under the
    /// limit runs.
    /// </summary>
    [Test]
    public async Task OversizedStatement_IsRefusedOnEveryEntry()
    {
        CamusDBOptions options = Options with { MaxStatementBytes = 4_096 };
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);

        string oversized = "SELECT 1 -- " + new string('x', 5_000);

        JsonResult query = await SqlController(executor, options, new { databaseName = dbname, sql = oversized }).ExecuteSQLQuery();
        Assert.AreEqual(CamusDBErrorCodes.StatementTooLarge, ((ExecuteSQLQueryResponse)query.Value!).Code);
        Assert.AreEqual(413, query.StatusCode);

        JsonResult ddl = await SqlController(executor, options, new { databaseName = dbname, sql = "CREATE TABLE big (id int64 PRIMARY KEY) -- " + new string('x', 5_000) }).ExecuteSQLDDL();
        Assert.AreEqual(CamusDBErrorCodes.StatementTooLarge, ((ExecuteDDLSQLResponse)ddl.Value!).Code);

        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException direct = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "DELETE FROM nowhere -- " + new string('x', 5_000), null)))!;
        Assert.AreEqual(CamusDBErrorCodes.StatementTooLarge, direct.Code);
        await database.Transactions.RollbackAsync(tx);

        List<QueryResultRow> rows = await Query(executor, dbname, "SELECT 1 AS one");
        Assert.AreEqual(1, rows.Count);
    }

    /// <summary>A PREPARE of oversized text is refused before the parse, like an inline statement.</summary>
    [Test]
    public void OversizedPrepare_IsRefused()
    {
        CamusDBException e = Assert.Throws<CamusDBException>(() =>
            PreparedStatementBinder.Create("db", "SELECT 1 -- " + new string('x', 5_000), maxStatementBytes: 4_096))!;
        Assert.AreEqual(CamusDBErrorCodes.StatementTooLarge, e.Code);
    }

    // ── Open explicit transactions ─────────────────────────────────────────────────

    /// <summary>
    /// At the cap a new explicit begin is refused with the retryable concurrency code, an autocommit
    /// begin is not counted, and a finished transaction frees its slot.
    /// </summary>
    [Test]
    public async Task ExplicitTransactions_PastCap_AreRefusedUntilOneEnds()
    {
        CamusDBOptions options = Options with { MaxOpenExplicitTransactions = 2 };
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(options);
        HttpTransactionCoordinator coordinator = new(executor);

        KvTransaction first = await coordinator.StartAsync(dbname, null, null, deferStart: true, sessionOwned: true);
        KvTransaction second = await coordinator.StartAsync(dbname, null, null, deferStart: true, sessionOwned: true);
        Assert.AreEqual(2, coordinator.OpenExplicitCount);

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await coordinator.StartAsync(dbname, null, null, deferStart: true, sessionOwned: true))!;
        Assert.AreEqual(CamusDBErrorCodes.ConcurrencyLimitExceeded, refused.Code);
        Assert.AreEqual(503, CamusDBErrorCodes.GetHttpStatus(refused.Code));
        Assert.AreEqual(2, coordinator.OpenExplicitCount, "a refused begin holds no slot");

        // An autocommit statement's transaction is not an explicit one and is never refused.
        KvTransaction autocommit = await coordinator.StartAsync(dbname, null, null);
        await coordinator.RollbackAsync(autocommit);

        await coordinator.RollbackAsync(first);
        Assert.AreEqual(1, coordinator.OpenExplicitCount);

        KvTransaction third = await coordinator.StartAsync(dbname, null, null, deferStart: true, sessionOwned: true);
        await coordinator.RollbackAsync(second);
        await coordinator.RollbackAsync(third);
        Assert.AreEqual(0, coordinator.OpenExplicitCount);
    }

    /// <summary>A begin that fails (an unknown database) must release the slot it reserved.</summary>
    [Test]
    public async Task ExplicitTransaction_FailedBegin_ReleasesItsSlot()
    {
        CamusDBOptions options = Options with { MaxOpenExplicitTransactions = 1 };
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(options);
        HttpTransactionCoordinator coordinator = new(executor);

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await coordinator.StartAsync("nosuchdb" + Guid.NewGuid().ToString("n"), null, null, sessionOwned: true));
        Assert.AreEqual(0, coordinator.OpenExplicitCount);

        KvTransaction tx = await coordinator.StartAsync(dbname, null, null, deferStart: true, sessionOwned: true);
        await coordinator.RollbackAsync(tx);
    }

    /// <summary>The idle reaper frees the slot of the transaction it rolls back.</summary>
    [Test]
    public async Task ExplicitTransaction_Reaped_ReleasesItsSlot()
    {
        CamusDBOptions options = Options with { MaxOpenExplicitTransactions = 1 };
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(options);
        HttpTransactionCoordinator coordinator = new(executor);

        await coordinator.StartAsync(dbname, null, null, deferStart: true, sessionOwned: true);
        Assert.AreEqual(1, await coordinator.ReapIdleAsync(TimeSpan.Zero));
        Assert.AreEqual(0, coordinator.OpenExplicitCount);

        KvTransaction tx = await coordinator.StartAsync(dbname, null, null, deferStart: true, sessionOwned: true);
        await coordinator.RollbackAsync(tx);
    }

    // ── Database count ─────────────────────────────────────────────────────────────

    /// <summary>A create past the database cap is refused with SchemaLimitExceeded and registers nothing.</summary>
    [Test]
    public async Task CreateDatabase_PastCap_IsRefused()
    {
        CommandExecutor probe = CreateCommandExecutor();
        int existing = (await probe.GetDatabaseRegistryAsync()).Count;

        CamusDBOptions options = Options with { MaxDatabases = existing + 1 };
        (_, _, CommandExecutor executor) = await CreateDatabase(options);

        string refusedName = "capdb" + Guid.NewGuid().ToString("n");
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.CreateDatabase(new CreateDatabaseTicket(name: refusedName, ifNotExists: false)))!;
        Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, e.Code);

        DatabaseRegistry registry = await executor.GetDatabaseRegistryAsync();
        Assert.IsNull(registry.Get(refusedName));
    }

    /// <summary>The authoritative check under the registry's write lock, without the early check before it.</summary>
    [Test]
    public async Task RegistryRegister_PastCap_IsRefused()
    {
        (_, _, CommandExecutor executor) = await CreateDatabase();
        DatabaseRegistry registry = await executor.GetDatabaseRegistryAsync();

        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await registry.RegisterAsync("capdb" + Guid.NewGuid().ToString("n"), await registry.AllocateIdAsync(), maxDatabases: registry.Count))!;
        Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, e.Code);
    }

    // ── Cluster activity fan-out ───────────────────────────────────────────────────

    /// <summary>A peer transport whose list calls wait until the test releases them.</summary>
    private sealed class BlockingTransport : IClusterActivityTransport
    {
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<List<QueryActivityRow>> ListQueriesAsync(string target, ActivityViewer viewer, CancellationToken ct)
        {
            Entered.TrySetResult();
            await Release.Task.WaitAsync(ct);
            return [];
        }

        public async Task<List<ConnectionActivityRow>> ListConnectionsAsync(string target, ActivityViewer viewer, CancellationToken ct)
        {
            Entered.TrySetResult();
            await Release.Task.WaitAsync(ct);
            return [];
        }

        public Task<QueryCancelOutcome> CancelAsync(string target, string queryId, ActivityViewer viewer, CancellationToken ct)
            => Task.FromResult(QueryCancelOutcome.NotFound);
    }

    /// <summary>
    /// With the cap at one, a second cluster statement while the first waits on its peers is refused;
    /// the node-local form still answers, and the slot is free again once the first ends.
    /// </summary>
    [Test]
    public async Task ClusterFanOut_PastCap_IsRefused()
    {
        CamusDBOptions options = Options with { ClusterActivityMaxConcurrentFanOuts = 1, ClusterActivityPeerTimeoutMs = 30_000 };
        BlockingTransport transport = new();
        QueryActivityService service = new(
            new QueryActivityRegistry(options, () => "node-a:2070"), transport, () => ["node-b:2070"], options);

        Task<List<QueryActivityRow>> first = service.ListQueriesAsync(cluster: true, ActivityViewer.All, CancellationToken.None);
        await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await service.ListConnectionsAsync(cluster: true, ActivityViewer.All, CancellationToken.None))!;
        Assert.AreEqual(CamusDBErrorCodes.ConcurrencyLimitExceeded, refused.Code);

        Assert.IsNotNull(await service.ListQueriesAsync(cluster: false, ActivityViewer.All, CancellationToken.None));

        transport.Release.SetResult();
        await first;

        Assert.IsNotNull(await service.ListConnectionsAsync(cluster: true, ActivityViewer.All, CancellationToken.None));
    }

    // ── Client connections ─────────────────────────────────────────────────────────

    /// <summary>The admission counter refuses past the limit and frees a slot on release; zero admits all.</summary>
    [Test]
    public void ConnectionAdmission_PastLimit_IsRefused()
    {
        ClientConnectionRegistry connections = new QueryActivityRegistry(Options, () => "node-a:2070").Connections;

        Assert.IsTrue(connections.TryAdmit(2));
        Assert.IsTrue(connections.TryAdmit(2));
        Assert.IsFalse(connections.TryAdmit(2));

        connections.ReleaseAdmission();
        Assert.IsTrue(connections.TryAdmit(2));
        Assert.IsTrue(connections.TryAdmit(0), "a limit of zero admits every connection");
    }

    // ── IN-list hash set ───────────────────────────────────────────────────────────

    private static NodeAst Membership(NodeType type, IReadOnlyList<NodeAst> items) =>
        new(type, NodeAst.Null, ExpressionChains.Combine(NodeType.ExprList, items.ToList()), null, null, null, null, null, null);

    private static NodeAst IntLiteral(long value) => new(NodeType.Integer, null, null, null, null, null, null, null, value.ToString());

    /// <summary>
    /// A long list of literals gets one set, kept for the node; a short list, and a list with a
    /// column reference, keep the per-row scan.
    /// </summary>
    [Test]
    public void InListMemo_QualifiesOnlyLongConstantLists()
    {
        NodeAst longList = Membership(NodeType.ExprInMembership, Enumerable.Range(1, 20).Select(i => IntLiteral(i)).ToList());
        PreparedInSet? set = InListMemo.TryGet(longList, null);
        Assert.IsNotNull(set);
        Assert.AreSame(set, InListMemo.TryGet(longList, null), "the set is built once per node");

        NodeAst shortList = Membership(NodeType.ExprInMembership, Enumerable.Range(1, 5).Select(i => IntLiteral(i)).ToList());
        Assert.IsNull(InListMemo.TryGet(shortList, null));

        List<NodeAst> withColumn = Enumerable.Range(1, 20).Select(i => IntLiteral(i)).ToList();
        withColumn.Add(new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "id"));
        Assert.IsNull(InListMemo.TryGet(Membership(NodeType.ExprInMembership, withColumn), null));
    }

    /// <summary>
    /// The set path gives the scan's answers through SQL, in a position the planner does not lift
    /// (a CASE): matches, misses, NOT IN, a NULL item, and a NULL left side.
    /// </summary>
    [Test]
    public async Task InList_InCase_MatchesScanSemantics()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await Run(executor, dbname, "CREATE TABLE t (id int64 PRIMARY KEY NOT NULL, v int64 NULL)");
        await Run(executor, dbname, "INSERT INTO t (id, v) VALUES (1, 3), (2, 50), (3, NULL)");

        string list = string.Join(", ", Enumerable.Range(1, 20));

        List<QueryResultRow> rows = await Query(executor, dbname,
            $"SELECT id, CASE WHEN v IN ({list}) THEN 'in' WHEN v NOT IN ({list}) THEN 'out' ELSE 'unknown' END AS r FROM t ORDER BY id");
        CollectionAssert.AreEqual(new[] { "in", "out", "unknown" }, rows.Select(r => r.Row["r"].StrValue).ToArray());

        rows = await Query(executor, dbname,
            $"SELECT id, CASE WHEN v NOT IN ({list}, NULL) THEN 'out' WHEN v IN ({list}, NULL) THEN 'in' ELSE 'unknown' END AS r FROM t ORDER BY id");
        CollectionAssert.AreEqual(new[] { "in", "unknown", "unknown" }, rows.Select(r => r.Row["r"].StrValue).ToArray(),
            "a NULL item makes a miss UNKNOWN for IN and for NOT IN");

        rows = await Query(executor, dbname, $"SELECT 1.0 IN ({list}) AS mixed");
        Assert.IsTrue(rows[0].Row["mixed"].BoolValue, "mixed numeric items widen, as with =");
    }

    /// <summary>
    /// One statement text, so one cached parse tree, with two parameter sets: each gets its own set,
    /// keyed by its parameter dictionary.
    /// </summary>
    [Test]
    public async Task InList_WithParameters_UsesEachStatementsValues()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        string list = string.Join(", ", Enumerable.Range(1, 12).Select(i => $"@p{i}"));
        string sql = $"SELECT CASE WHEN 5 IN ({list}) THEN 'in' ELSE 'out' END AS r";

        Dictionary<string, ColumnValue> Parameters(long start)
        {
            Dictionary<string, ColumnValue> parameters = new();
            for (int i = 1; i <= 12; i++)
                parameters[$"@p{i}"] = new ColumnValue(ColumnType.Integer64, start + i);
            return parameters;
        }

        Assert.AreEqual("in", (await Query(executor, dbname, sql, Parameters(0)))[0].Row["r"].StrValue);
        Assert.AreEqual("out", (await Query(executor, dbname, sql, Parameters(100)))[0].Row["r"].StrValue);
        Assert.AreEqual("in", (await Query(executor, dbname, sql, Parameters(0)))[0].Row["r"].StrValue);
    }

    /// <summary>
    /// The scan stops at the first match and never evaluates a later item that throws. A list whose
    /// set cannot be built keeps that behavior instead of failing the row.
    /// </summary>
    [Test]
    public async Task InList_ItemThatThrows_KeepsScanBehavior()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        // Only @p1 is bound: the set cannot be built, and the scan matches @p1 before it reaches @p2.
        string list = string.Join(", ", Enumerable.Range(1, 12).Select(i => $"@p{i}"));
        Dictionary<string, ColumnValue> parameters = new() { ["@p1"] = new ColumnValue(ColumnType.Integer64, 1L) };

        List<QueryResultRow> rows = await Query(executor, dbname, $"SELECT CASE WHEN 1 IN ({list}) THEN 'in' ELSE 'out' END AS r", parameters);
        Assert.AreEqual("in", rows[0].Row["r"].StrValue);
    }
}
