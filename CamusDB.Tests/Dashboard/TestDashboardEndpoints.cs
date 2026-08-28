/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using System.Net;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Dashboard;

/// <summary>
/// The dashboard endpoints, driven as actions against a real engine.
///
/// <para>Two properties are worth more than the payload shapes. First, <b>no panel may have a side
/// effect</b>: the page polls on a timer and every open tab multiplies one poll, so an endpoint that
/// opens a transaction, or that loads a database descriptor to describe it, would change the very
/// node it reports on. Second, <b>a refused panel must degrade rather than fail the page</b>.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public class TestDashboardEndpoints : BaseTest
{
    /// <summary>
    /// Builds the controller with a remote address, so the loopback rule is exercised as written
    /// rather than bypassed by a null <c>HttpContext</c>.
    /// </summary>
    private DashboardController CreateController(
        CommandExecutor executor,
        CamusDBOptions options,
        IPAddress? remote = null,
        ForegroundRequestGauge? gauge = null,
        HttpTransactionCoordinator? coordinator = null)
    {
        DefaultHttpContext http = new();
        http.Connection.RemoteIpAddress = remote ?? IPAddress.Loopback;

        DashboardController controller = new(
            executor,
            coordinator ?? new HttpTransactionCoordinator(executor),
            logger,
            options,
            TestNode!,
            gauge ?? new ForegroundRequestGauge(),
            new PreparedStatementRegistry(options))
        {
            ControllerContext = new ControllerContext { HttpContext = http },
        };

        return controller;
    }

    private static T Payload<T>(IActionResult result) where T : class
    {
        JsonResult json = (JsonResult)result;
        return (T)json.Value!;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Slow queries
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A node with the log off must say so, rather than answering with an empty list. The two look
    /// identical to a reader, and "no slow statements" is the opposite conclusion from "not
    /// recording".
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task SlowQueriesReportsTheLogAsDisabledRatherThanEmpty()
    {
        (string _, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        DashboardController controller = CreateController(executor, Options);
        DashboardSlowQueriesResponse body =
            Payload<DashboardSlowQueriesResponse>(await controller.GetSlowQueries());

        Assert.AreEqual("ok", body.Status);
        Assert.IsFalse(body.LogEnabled);
        Assert.IsEmpty(body.Rows);
        Assert.AreEqual(0, body.NewestSequence);
    }

    /// <summary>
    /// With the log on, the panel reports the statements this node ran, newest first, and carries
    /// the execution facts that explain each duration — which is the panel's whole reason to exist.
    ///
    /// <para>The engine is built with the log enabled through <c>CreateDatabase(options)</c>. Setting
    /// the flag on an engine that already exists would be a no-op that still passes.</para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task SlowQueriesReportsRecordedStatementsNewestFirst()
    {
        CamusDBOptions logging = Options with { SlowQueryLogEnabled = true, SlowQueryLogThresholdMs = 0 };

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(logging);

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: ddl, database: dbname,
            sql: "CREATE TABLE dash_slow (id OID PRIMARY KEY, name STRING NOT NULL)", parameters: null));
        await database.Transactions.CommitAsync(ddl);

        await DrainAsync(executor, dbname, "SELECT * FROM dash_slow WHERE name = 'absent'");

        DashboardController controller = CreateController(executor, logging);
        DashboardSlowQueriesResponse body =
            Payload<DashboardSlowQueriesResponse>(await controller.GetSlowQueries());

        Assert.IsTrue(body.LogEnabled);
        Assert.AreEqual(0, body.ThresholdMs);
        Assert.AreEqual(logging.SlowQueryLogMaxEntries, body.Capacity);
        Assert.IsNotEmpty(body.Rows);

        DashboardSlowQueryRow? scan = body.Rows.Find(entry => entry.Sql.Contains("dash_slow"));
        Assert.IsNotNull(scan, "the SELECT should have been recorded");
        Assert.AreEqual("select", scan!.Kind);
        Assert.AreEqual(dbname, scan.Database);
        Assert.AreEqual("completed", scan.Outcome);
        Assert.IsNull(scan.ErrorCode);

        // Newest first, and the reported newest sequence is the highest one present.
        long[] sequences = body.Rows.ConvertAll(entry => entry.Seq).ToArray();
        for (int i = 1; i < sequences.Length; i++)
            Assert.Less(sequences[i], sequences[i - 1], "entries must come back newest first");

        Assert.AreEqual(sequences[0], body.NewestSequence);
    }

    /// <summary>
    /// The panel clips statement text. The log's own limit is an operator's choice and can be
    /// kilobytes, which is a payload the page would re-truncate on every poll for every entry.
    /// A clipped entry must say it was clipped.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task SlowQueriesClipsLongStatementTextAndFlagsIt()
    {
        CamusDBOptions logging = Options with
        {
            SlowQueryLogEnabled = true,
            SlowQueryLogThresholdMs = 0,
            SlowQueryLogMaxSqlLength = 8192,
        };

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(logging);

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: ddl, database: dbname,
            sql: "CREATE TABLE dash_long (id OID PRIMARY KEY, name STRING NOT NULL)", parameters: null));
        await database.Transactions.CommitAsync(ddl);

        // A predicate long enough to pass the log's own limit and still exceed the panel's.
        string wide = string.Join(" OR ", Enumerable.Range(0, 60).Select(i => $"name = 'padding value {i}'"));
        await DrainAsync(executor, dbname, $"SELECT * FROM dash_long WHERE {wide}");

        DashboardController controller = CreateController(executor, logging);
        DashboardSlowQueriesResponse body =
            Payload<DashboardSlowQueriesResponse>(await controller.GetSlowQueries());

        DashboardSlowQueryRow? entry = body.Rows.Find(row => row.Sql.Contains("dash_long"));
        Assert.IsNotNull(entry);
        Assert.LessOrEqual(entry!.Sql.Length, 300);
        Assert.IsTrue(entry.Truncated, "a clipped statement must say it was clipped");
    }

    /// <summary>
    /// The panel is unreachable without a superuser session.
    ///
    /// <para>The gate is not implemented in the controller — the panel runs <c>SHOW SLOW QUERIES</c>
    /// through the executor and inherits that statement's bar. This test drives the endpoint anyway,
    /// because "the statement is gated" and "the panel is gated" are two claims, and the second is
    /// the one an operator relies on. The rows carry other users' SQL text, so a panel that quietly
    /// stopped inheriting the gate would be a disclosure, not a cosmetic slip.</para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task SlowQueriesRefusesANonSuperuser()
    {
        CamusDBOptions guarded = Options with
        {
            SlowQueryLogEnabled = true,
            SlowQueryLogThresholdMs = 0,
            AuthenticationEnabled = true,
        };

        (string _, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase(guarded);

        DashboardController controller = CreateController(executor, guarded);
        controller.HttpContext.Items["camus.principal"] =
            new Principal("dash_reader", isSuperuser: false, grants: []);

        JsonResult refusal = (JsonResult)await controller.GetSlowQueries();
        DashboardFailureResponse body = (DashboardFailureResponse)refusal.Value!;

        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, body.Code);
        Assert.AreEqual(StatusCodes.Status403Forbidden, refusal.StatusCode);
    }

    /// <summary>Runs a statement and drains its cursor, which is what ends the recorded duration.</summary>
    private static async Task DrainAsync(CommandExecutor executor, string dbname, string sql)
    {
        (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), dbname, sql, null));

        await foreach (QueryResultRow _ in cursor) { }
    }

    [Test]
    [NonParallelizable]
    public async Task SummaryReportsIdentityAndLoad()
    {
        (string _, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        ForegroundRequestGauge gauge = new();
        DashboardController controller = CreateController(executor, Options, gauge: gauge);

        DashboardSummaryResponse summary = Payload<DashboardSummaryResponse>(await controller.GetSummary());

        Assert.AreEqual("ok", summary.Status);
        Assert.IsTrue(summary.Initialized);
        Assert.AreEqual(0, summary.InFlightRequests);
        Assert.AreEqual(0, summary.ActiveTransactions);
        Assert.AreEqual(0, summary.PreparedStatements);

        // Authentication is off in this fixture, so there is no principal to fall short of the bar.
        Assert.IsTrue(summary.CanReadPrivileged);
    }

    /// <summary>
    /// The interval is served rather than hardcoded in the page, and it is clamped: a mistyped value
    /// must not turn every open browser tab into a hot loop against a production node.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task RefreshIntervalIsClamped()
    {
        (string _, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        DashboardController tooFast = CreateController(executor, Options with { DashboardRefreshSeconds = 0 });
        Assert.AreEqual(1, Payload<DashboardSummaryResponse>(await tooFast.GetSummary()).RefreshSeconds);

        DashboardController tooSlow = CreateController(executor, Options with { DashboardRefreshSeconds = 99999 });
        Assert.AreEqual(300, Payload<DashboardSummaryResponse>(await tooSlow.GetSummary()).RefreshSeconds);
    }

    [Test]
    [NonParallelizable]
    public async Task DatabasesListsTheRegistryWithoutOpeningAnything()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        int openBefore = executor.OpenDatabaseCount;

        DashboardController controller = CreateController(executor, Options);
        DashboardDatabasesResponse databases = Payload<DashboardDatabasesResponse>(await controller.GetDatabases());

        Assert.AreEqual("ok", databases.Status);
        DashboardDatabaseRow? row = databases.Databases.Find(d => d.Name == dbname);
        Assert.IsNotNull(row, "the created database should be listed");
        Assert.IsNotEmpty(row!.Id);
        Assert.IsNull(row.BranchedFrom, "a root database has no parent");

        // The listing reads the registry and the descriptor cache. If it opened anything, an idle
        // database would be pulled back into memory by nothing more than a page refresh, and idle
        // eviction would never reclaim a database anyone can see.
        Assert.AreEqual(openBefore, executor.OpenDatabaseCount);
    }

    /// <summary>
    /// The drill-down is the one endpoint that opens a descriptor. It is called on an explicit
    /// selection, never on the timer — this test pins what it returns, and the test above pins that
    /// the timer-driven listing does not do the same thing.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TableDrillDownListsRelations()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!,
            database: dbname,
            sql: "CREATE TABLE dash_probe (id OID PRIMARY KEY, name STRING)",
            parameters: null));

        DashboardController controller = CreateController(executor, Options);
        DashboardTablesResponse tables = Payload<DashboardTablesResponse>(await controller.GetTables(dbname));

        Assert.AreEqual("ok", tables.Status);
        Assert.AreEqual(dbname, tables.Database);
        Assert.IsTrue(tables.Tables.Exists(t => t.Name == "dash_probe"), "the created table should be listed");
    }

    /// <summary>
    /// A polling page must leave no trace. This drives every timer-driven endpoint and then asserts
    /// the two signals that would reveal one: the foreground gauge that makes auto-analyze back off,
    /// and the coordinator that tracks open transactions.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task PollingLeavesNoTransactionAndNoLoad()
    {
        (string _, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        ForegroundRequestGauge gauge = new();
        HttpTransactionCoordinator coordinator = new(executor);
        DashboardController controller = CreateController(executor, Options, gauge: gauge, coordinator: coordinator);

        for (int round = 0; round < 3; round++)
        {
            await controller.GetSummary();
            await controller.GetMetrics();
            await controller.GetDatabases();
            await controller.GetConfig();
        }

        Assert.AreEqual(0, gauge.InFlight, "the dashboard must not register as foreground load");
        Assert.AreEqual(0, coordinator.ActiveCount, "the dashboard must not leave a transaction open");
    }

    [Test]
    [NonParallelizable]
    public async Task MetricsReportInstrumentsWhenCollectionIsOn()
    {
        (string _, DatabaseDescriptor _, CommandExecutor executor) =
            await CreateDatabase(Options with { EngineMetricsEnabled = true });

        DashboardController controller = CreateController(executor, Options with { EngineMetricsEnabled = true });
        DashboardMetricsResponse metrics = Payload<DashboardMetricsResponse>(await controller.GetMetrics());

        Assert.IsTrue(metrics.MetricsEnabled);
        Assert.AreEqual(0, metrics.Omitted, "the curated set should fit well inside the row cap");

        // Values are raw and cumulative; the browser computes rates from two samples. The monotonic
        // reading is what it divides by, because a wall clock can step.
        Assert.Greater(metrics.MonotonicMs, 0);
    }

    /// <summary>
    /// The disabled arm needs its own engine. A component fixes its configuration when it is
    /// constructed, so flipping the flag on an already-built executor would test nothing while still
    /// passing — which reads as coverage and is not.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task MetricsReportDisabledRatherThanFailing()
    {
        CamusDBOptions off = Options with { EngineMetricsEnabled = false };
        (string _, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase(off);

        DashboardController controller = CreateController(executor, off);
        IActionResult result = await controller.GetMetrics();

        DashboardMetricsResponse metrics = Payload<DashboardMetricsResponse>(result);

        // An error would blank the panel over a condition that is a deliberate choice, and an empty
        // list alone cannot be told apart from a node that has recorded nothing yet.
        Assert.AreEqual("ok", metrics.Status);
        Assert.IsFalse(metrics.MetricsEnabled);
        Assert.IsEmpty(metrics.Rows);
    }

    [Test]
    [NonParallelizable]
    public async Task ConfigReportsVariablesWithSecretsMasked()
    {
        (string _, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        DashboardController controller = CreateController(executor, Options);
        DashboardConfigResponse config = Payload<DashboardConfigResponse>(await controller.GetConfig());

        Assert.AreEqual("ok", config.Status);
        Assert.IsNotEmpty(config.Variables);

        DashboardVariableRow? dashboardEnabled = config.Variables.Find(v => v.Name == "dashboard_enabled");
        Assert.IsNotNull(dashboardEnabled, "the new setting must be reportable like any other");
        Assert.AreEqual("node", dashboardEnabled!.Scope);
        Assert.AreEqual("restart", dashboardEnabled.Mutability);

        DashboardVariableRow? refresh = config.Variables.Find(v => v.Name == "dashboard_refresh_seconds");
        Assert.IsNotNull(refresh);
        Assert.AreEqual("runtime", refresh!.Mutability);

        // Masking is the statement's job, and this endpoint must not become a second path that
        // reveals what SHOW VARIABLES hides.
        foreach (string secret in new[] { "node_secret", "access_token_server_key", "bootstrap_superuser_password" })
        {
            DashboardVariableRow? row = config.Variables.Find(v => v.Name == secret);
            if (row?.Value is { Length: > 0 } value)
                Assert.AreEqual("********", value, secret);
        }
    }

    /// <summary>
    /// With the dashboard switched off the whole surface answers 404, matching its page. A 403 would
    /// advertise a surface the operator chose to remove.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task DisabledDashboardAnswersNotFoundEverywhere()
    {
        CamusDBOptions off = Options with { DashboardEnabled = false };
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase(off);

        DashboardController controller = CreateController(executor, off);

        Assert.IsInstanceOf<NotFoundResult>(await controller.GetSummary());
        Assert.IsInstanceOf<NotFoundResult>(await controller.GetMetrics());
        Assert.IsInstanceOf<NotFoundResult>(await controller.GetDatabases());
        Assert.IsInstanceOf<NotFoundResult>(await controller.GetConfig());
        Assert.IsInstanceOf<NotFoundResult>(await controller.GetTables(dbname));
    }

    /// <summary>
    /// With authentication off there is no principal to gate any panel on, so a caller from the
    /// network is refused outright rather than served a page of empty panels.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task NetworkCallerIsRefusedWhenAuthenticationIsOff()
    {
        CamusDBOptions anonymous = Options with { AuthenticationEnabled = false };
        (string _, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase(anonymous);

        DashboardController remote = CreateController(executor, anonymous, IPAddress.Parse("192.168.10.8"));
        JsonResult refused = (JsonResult)await remote.GetSummary();

        Assert.AreEqual(CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.InsufficientPrivilege), refused.StatusCode);
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ((DashboardFailureResponse)refused.Value!).Code);

        // The same engine serves loopback.
        DashboardController local = CreateController(executor, anonymous, IPAddress.Loopback);
        Assert.AreEqual("ok", Payload<DashboardSummaryResponse>(await local.GetSummary()).Status);
    }
}
