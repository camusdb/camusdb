/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net;
using System.Reflection;
using Microsoft.AspNetCore.Mvc;
using Kommander;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.App.Models;
using CamusDB.App.Services;

namespace CamusDB.App.Controllers;

/// <summary>
/// Read-only data behind the browser operator dashboard: node identity and load, a curated slice of
/// the engine's own metrics, the database registry, one database's relations, and the configuration.
///
/// <para><b>Nothing here mutates anything.</b> No endpoint writes, opens a transaction, or takes a
/// lock. That is not a stylistic preference: the page polls on a timer, and every open browser tab
/// multiplies whatever one poll costs.</para>
///
/// <para><b>The routes deliberately do not start with <c>/execute</c>.</b>
/// <see cref="ForegroundRequestGaugeMiddleware"/> counts any path with that prefix as foreground
/// load, and the auto-analyze scheduler backs off when the gauge is high. A dashboard polling an
/// <c>/execute</c> route would make an idle node look busy and suppress its own background
/// maintenance — the page would change the thing it exists to observe.</para>
///
/// <para><b>Privilege comes from the statements, not from here.</b> The metrics and configuration
/// panels run <c>SHOW ENGINE STATS</c>, <c>SHOW VARIABLES</c> and <c>SHOW CLUSTER SETTINGS</c>
/// through the executor, so the superuser bar those statements already enforce applies unchanged and
/// cannot drift from a second copy. A refusal surfaces as a normal failure envelope, which the page
/// renders as one line inside that panel while every other panel keeps working.</para>
/// </summary>
[ApiController]
public sealed class DashboardController : CommandsController
{
    /// <summary>
    /// Instruments the engine panel shows. It is an allowlist rather than the whole
    /// <c>SHOW ENGINE STATS</c> dump: that dump runs to hundreds of rows across three meters, which
    /// is a fine answer for a SQL client and an unreadable one for an operator glancing at a card.
    /// A name absent here is still reachable through the statement itself.
    /// </summary>
    private static readonly HashSet<string> CuratedMetrics = new(StringComparer.Ordinal)
    {
        // CamusDB's own request and statement path.
        "camus.request.count",
        "camus.request.duration",
        "camus.request.in_flight",
        "camus.execute.duration",
        "camus.sql.parse.duration",
        "camus.sql.parse.cache",
        "camus.query.rows",
        "camus.query.scan.duration",
        "camus.query_cache.requests",
        "camus.transaction.count",
        "camus.transaction.active",
        "camus.transaction.commit.duration",
        "camus.transaction.staged_mutations",
        "camus.kv.retry_waits",

        // Consensus, from Kommander.
        "raft.executor.operations_total",
        "raft.executor.operation_duration_ms",
        "raft.executor.client_queue_depth",
        "raft.wal.operations_total",
        "raft.wal.batches_total",
        "raft.wal.batch_size",
        "raft.wal.queue_depth",
        "raft.heartbeat_delay_ms",
        "raft.elections_started_total",
        "raft.backfill.no_progress_episodes_total",
        "raft.snapshot.transfer_failures_total",

        // Storage, from Kahuna.
        "kahuna.kv.write.batches",
        "kahuna.kv.write.batch_bytes",
        "kahuna.kv.write.batch_items",
        "kahuna.kv.write.entries",
        "kahuna.kv.write.raft_duration",
        "kahuna.kv.write.queue_age",
        "kahuna.kv.write.rejections",
        "kahuna.placement.leader_hint_hits",
        "kahuna.placement.leader_hint_misses",
    };

    /// <summary>
    /// Ceiling on returned metric rows. A curated instrument can still split into many rows by tag,
    /// and an unbounded payload polled every few seconds is its own load. What the cap drops is
    /// reported in <c>omitted</c> rather than silently trimmed, so a truncated panel never reads as
    /// a complete one.
    /// </summary>
    private const int MaxMetricRows = 400;

    /// <summary>Bounds on the served refresh interval, so a typo cannot produce a hot loop.</summary>
    private const int MinRefreshSeconds = 1;
    private const int MaxRefreshSeconds = 300;

    /// <summary>
    /// Process start, captured once. It is read from the OS where that works, and otherwise falls
    /// back to first use of this type — which under-reports uptime rather than throwing, because an
    /// approximate uptime is worth more on this page than a failed panel.
    /// </summary>
    private static readonly DateTime ProcessStartUtc = ResolveProcessStart();

    private readonly EmbeddedKahuna kahuna;

    private readonly ForegroundRequestGauge foregroundRequests;

    private readonly PreparedStatementRegistry preparedStatements;

    public DashboardController(
        CommandExecutor executor,
        HttpTransactionCoordinator transactions,
        ILogger<ICamusDB> logger,
        CamusDBOptions options,
        EmbeddedKahuna kahuna,
        ForegroundRequestGauge foregroundRequests,
        PreparedStatementRegistry preparedStatements)
        : base(executor, transactions, logger, options)
    {
        this.kahuna = kahuna;
        this.foregroundRequests = foregroundRequests;
        this.preparedStatements = preparedStatements;
    }

    /// <summary>
    /// Node identity and live load, in the one payload the page's top band renders.
    /// </summary>
    [HttpGet]
    [Route("/v1/dashboard/summary")]
    public async Task<IActionResult> GetSummary()
    {
        IActionResult? refusal = RefuseIfUnavailable();
        if (refusal is not null)
            return refusal;

        try
        {
            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            ClusterHealthResponse health = ClusterController.BuildHealth(kahuna.Raft);

            string endpoint;
            try
            {
                endpoint = kahuna.Raft.GetLocalEndpoint();
            }
            catch (Exception)
            {
                // A node early in boot has no endpoint yet. The band shows the rest regardless.
                endpoint = "";
            }

            return new JsonResult(new DashboardSummaryResponse
            {
                LocalEndpoint = endpoint,
                LocalRole = health.LocalRole,
                Initialized = health.Initialized,
                Ready = health.Ready,
                HostedPartitions = health.HostedPartitions,
                CommitStalled = health.CommitStalled,

                ClusterMode = kahuna.IsClusterMode,
                AuthenticationEnabled = options.AuthenticationEnabled,
                DataDirectory = options.DataDirectory ?? "",
                Version = ResolveVersion(),
                UptimeSeconds = (long)(DateTime.UtcNow - ProcessStartUtc).TotalSeconds,

                InFlightRequests = foregroundRequests.InFlight,
                ActiveTransactions = transactions.ActiveCount,
                PreparedStatements = preparedStatements.Count,
                PreparedStatementBytes = preparedStatements.RetainedBytes,

                RefreshSeconds = Math.Clamp(options.DashboardRefreshSeconds, MinRefreshSeconds, MaxRefreshSeconds),
                CanReadPrivileged = !options.AuthenticationEnabled || principal?.IsSuperuser == true,
            });
        }
        catch (CamusDBException e)
        {
            return Failure(e);
        }
        catch (Exception e)
        {
            return Unexpected(e);
        }
    }

    /// <summary>
    /// The curated instrument set, as raw cumulative values.
    ///
    /// <para>No rate is computed here. The collector accumulates from process start and never resets,
    /// and this endpoint holds no previous sample, so a rate is the browser's job: it keeps the last
    /// value per instrument and divides by elapsed monotonic time. <c>monotonicMs</c> is served for
    /// exactly that division — a wall clock can step, and a rate measured across a step is wrong.</para>
    /// </summary>
    [HttpGet]
    [Route("/v1/dashboard/metrics")]
    public async Task<IActionResult> GetMetrics()
    {
        IActionResult? refusal = RefuseIfUnavailable();
        if (refusal is not null)
            return refusal;

        try
        {
            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            DashboardMetricsResponse response = new()
            {
                MetricsEnabled = options.EngineMetricsEnabled,
                SampledAtUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                MonotonicMs = Environment.TickCount64,
            };

            // Off is a state, not a failure: the statement itself answers with no rows and no error,
            // and a script probing a fleet should behave the same either way.
            if (!options.EngineMetricsEnabled)
                return new JsonResult(response);

            int omitted = 0;

            await foreach (QueryResultRow row in RunServerStatementAsync("SHOW ENGINE STATS", principal).ConfigureAwait(false))
            {
                string metric = Text(row, "metric");
                if (!CuratedMetrics.Contains(metric))
                    continue;

                if (response.Rows.Count >= MaxMetricRows)
                {
                    omitted++;
                    continue;
                }

                if (response.Node.Length == 0)
                    response.Node = Text(row, "node");

                response.Rows.Add(new DashboardMetricRow(
                    Source: Text(row, "source"),
                    Metric: metric,
                    Tags: Text(row, "tags"),
                    Kind: Text(row, "kind"),
                    Count: Integer(row, "count"),
                    Total: Number(row, "total"),
                    Min: Number(row, "min"),
                    Max: Number(row, "max"),
                    Last: Number(row, "last")));
            }

            response.Omitted = omitted;
            return new JsonResult(response);
        }
        catch (CamusDBException e)
        {
            return Failure(e);
        }
        catch (Exception e)
        {
            return Unexpected(e);
        }
    }

    /// <summary>
    /// The database registry, filtered to what the caller may see.
    ///
    /// <para>This reads the registry and the descriptor cache only. It opens no database, which is
    /// what makes it safe on the refresh timer — see the table endpoint for the half that is not.</para>
    /// </summary>
    [HttpGet]
    [Route("/v1/dashboard/databases")]
    public async Task<IActionResult> GetDatabases()
    {
        IActionResult? refusal = RefuseIfUnavailable();
        if (refusal is not null)
            return refusal;

        try
        {
            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            DatabaseRegistry registry = await executor.GetDatabaseRegistryAsync().ConfigureAwait(false);
            IReadOnlyList<DatabaseRegistryEntry> entries = registry.List();

            // A branch records its parent by id, and the panel shows names. Build the map once
            // rather than scanning the list per row.
            Dictionary<string, string> namesById = new(entries.Count, StringComparer.Ordinal);
            foreach (DatabaseRegistryEntry entry in entries)
                namesById[entry.Id] = entry.Name;

            DashboardDatabasesResponse response = new();

            foreach (DatabaseRegistryEntry entry in entries.OrderBy(e => e.Name, StringComparer.Ordinal))
            {
                // A database the caller has no grant on is simply absent, matching SHOW DATABASES.
                if (principal is not null && !principal.CanSeeDatabase(entry.Id))
                    continue;

                string? parent = null;
                if (entry.Ancestors.Count > 0)
                {
                    string parentId = entry.Ancestors[0].DatabaseId;
                    parent = namesById.TryGetValue(parentId, out string? parentName) ? parentName : parentId;
                }

                response.Databases.Add(new DashboardDatabaseRow
                {
                    Name = entry.Name,
                    Id = entry.Id,
                    BranchedFrom = parent,
                    Resident = executor.IsDatabaseResident(entry.Id),
                });
            }

            return new JsonResult(response);
        }
        catch (CamusDBException e)
        {
            return Failure(e);
        }
        catch (Exception e)
        {
            return Unexpected(e);
        }
    }

    /// <summary>
    /// Relations of one database, for the drill-down panel.
    ///
    /// <para><b>Never call this from a refresh timer.</b> Unlike the registry listing, this opens a
    /// <c>DatabaseDescriptor</c>. A descriptor loaded on a schedule defeats idle eviction, so every
    /// database an operator can see would stay resident for as long as a browser tab is open — the
    /// page would quietly pin the whole fleet's working set. The client requests it on an explicit
    /// selection and never repeats it.</para>
    /// </summary>
    [HttpGet]
    [Route("/v1/dashboard/databases/{database}/tables")]
    public async Task<IActionResult> GetTables(string database)
    {
        IActionResult? refusal = RefuseIfUnavailable();
        if (refusal is not null)
            return refusal;

        try
        {

            if (string.IsNullOrWhiteSpace(database))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "A database name is required");

            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            DashboardTablesResponse response = new() { Database = database };

            await foreach (QueryResultRow row in RunDatabaseStatementAsync(database, "SHOW TABLES", principal).ConfigureAwait(false))
                response.Tables.Add(new DashboardTableRow { Name = Text(row, "tables") });

            await foreach (QueryResultRow row in RunDatabaseStatementAsync(database, "SHOW VIEWS", principal).ConfigureAwait(false))
                response.Tables.Add(new DashboardTableRow { Name = Text(row, "views"), Kind = "view" });

            return new JsonResult(response);
        }
        catch (CamusDBException e)
        {
            return Failure(e);
        }
        catch (Exception e)
        {
            return Unexpected(e);
        }
    }

    /// <summary>
    /// Resolved configuration plus the live cluster-settings overlay. Superuser only, because both
    /// underlying statements are — the whole configuration surface describes this node's security
    /// posture and limits, which no per-database grant scopes down.
    /// </summary>
    [HttpGet]
    [Route("/v1/dashboard/config")]
    public async Task<IActionResult> GetConfig()
    {
        IActionResult? refusal = RefuseIfUnavailable();
        if (refusal is not null)
            return refusal;

        try
        {
            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            DashboardConfigResponse response = new();

            await foreach (QueryResultRow row in RunServerStatementAsync("SHOW VARIABLES", principal).ConfigureAwait(false))
                response.Variables.Add(new DashboardVariableRow(
                    Name: Text(row, "variable"),
                    Value: Text(row, "value"),
                    Default: Text(row, "default"),
                    Source: Text(row, "source"),
                    Mutability: Text(row, "mutability"),
                    Scope: Text(row, "scope")));

            // An engine composed without a cluster-settings service rejects the statement. That is a
            // shape of this deployment, not a fault, so it is reported as a flag: throwing here would
            // blank a panel that has perfectly good variables to show.
            try
            {
                await foreach (QueryResultRow row in RunServerStatementAsync("SHOW CLUSTER SETTINGS", principal).ConfigureAwait(false))
                    response.ClusterSettings.Add(new DashboardClusterSettingRow(
                        Text(row, "setting"), Text(row, "value")));

                response.OverlayAvailable = true;
            }
            catch (CamusDBException e) when (e.Code == CamusDBErrorCodes.InvalidInternalOperation)
            {
                response.OverlayAvailable = false;
            }

            return new JsonResult(response);
        }
        catch (CamusDBException e)
        {
            return Failure(e);
        }
        catch (Exception e)
        {
            return Unexpected(e);
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Runs a server-level SHOW statement in-process, with no database context and no transaction.
    ///
    /// <para>The statements routed here return before the executor opens anything, so a null
    /// transaction and an empty database name are correct rather than a shortcut. Going through the
    /// executor is the point: the privilege gate, the masking of secrets and the exact column set all
    /// come from the statement, so this controller cannot disagree with the SQL surface.</para>
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> RunServerStatementAsync(string sql, Principal? principal)
    {
        ExecuteSQLTicket ticket = new(
            txnState: null!,
            database: "",
            sql: sql,
            parameters: null,
            principal: principal);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket).ConfigureAwait(false);

        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// Runs a SHOW statement that needs a database context. This opens a descriptor — see
    /// <see cref="GetTables"/> for why that must stay off the refresh timer.
    ///
    /// <para>Unlike the server-level statements, this one <b>must</b> carry a transaction: the
    /// executor marks the transaction as having run a statement before it dispatches, so a null
    /// here is a null reference rather than a shortcut. What it carries is the cheapest kind there
    /// is — a synthetic read-only transaction at timestamp zero, which reads the latest committed
    /// value per key. It makes no round trip to Kahuna to begin, takes no locks, and needs no
    /// commit or rollback, so an unattended page polling it adds no coordinator traffic. It is
    /// deliberately not promoted: promotion exists to hold a range lock for a phantom-free scan,
    /// and this statement reads schema, not rows.</para>
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> RunDatabaseStatementAsync(string database, string sql, Principal? principal)
    {
        KvTransaction readOnly = await transactions
            .BeginReadOnlyAsync(database, promote: false, causalToken: null)
            .ConfigureAwait(false);

        ExecuteSQLTicket ticket = new(
            txnState: readOnly,
            database: database,
            sql: sql,
            parameters: null,
            principal: principal);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket).ConfigureAwait(false);

        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
            yield return row;
    }

    /// <summary>
    /// Fail-closed gate for the whole dashboard surface. Returns the answer to send, or null when
    /// the request may proceed.
    ///
    /// <para>It refuses in three ways, and the difference between them is deliberate. A dashboard
    /// switched off answers <b>404</b>, exactly as its page does: the surface does not exist on this
    /// node, and a "forbidden" would advertise something an operator chose to remove. A credential
    /// over plaintext is refused by the usual transport rule. And with authentication disabled there
    /// is no principal to gate on at all, so the surface is restricted to loopback — the same rule
    /// the backup admin surface applies, for the same reason.</para>
    ///
    /// <para>It returns a result rather than throwing because "switched off" is not an error the
    /// engine classifies, and routing it through <see cref="CamusDBException"/> would map it to the
    /// wrong status.</para>
    /// </summary>
    private IActionResult? RefuseIfUnavailable()
    {
        if (!options.DashboardEnabled)
            return NotFound();

        try
        {
            EnsureSecureTransport();
        }
        catch (CamusDBException e)
        {
            return Failure(e);
        }

        if (!DashboardSession.IsServedTo(options, HttpContext.Connection.RemoteIpAddress))
            return Failure(new CamusDBException(
                CamusDBErrorCodes.InsufficientPrivilege,
                DashboardSession.NetworkRefusalMessage));

        return null;
    }

    private JsonResult Failure(CamusDBException e)
    {
        LogCommandFailure(e);
        return new JsonResult(new DashboardFailureResponse { Code = e.Code, Message = e.Message })
        {
            StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code),
        };
    }

    /// <summary>
    /// Last-resort answer for a failure the engine did not classify.
    ///
    /// <para>Without it an unexpected exception reaches the framework's error handler, which answers
    /// with an HTML page. The dashboard's script asks for JSON, so it would degrade the panel with
    /// "the node did not answer" — true but useless, and it hides the real fault from whoever is
    /// looking at the page. A JSON envelope keeps the panel's message honest, and the exception is
    /// logged in full for the operator.</para>
    /// </summary>
    private JsonResult Unexpected(Exception e)
    {
        logger.LogError(e, "Dashboard request failed unexpectedly");
        return new JsonResult(new DashboardFailureResponse
        {
            // "CA0000" is the house code for a failure the engine did not classify, matching
            // the SQL endpoints.
            Code = "CA0000",
            Message = "The node could not answer this panel. The failure is in the server log.",
        })
        {
            StatusCode = StatusCodes.Status500InternalServerError,
        };
    }

    private static string ResolveVersion() =>
        Assembly.GetEntryAssembly()?.GetName().Version?.ToString() ?? "";

    private static DateTime ResolveProcessStart()
    {
        try
        {
            return System.Diagnostics.Process.GetCurrentProcess().StartTime.ToUniversalTime();
        }
        catch (Exception)
        {
            return DateTime.UtcNow;
        }
    }

    private static string Text(QueryResultRow row, string column) =>
        row.Row.TryGetValue(column, out ColumnValue? value) ? value.StrValue ?? "" : "";

    private static long Integer(QueryResultRow row, string column) =>
        row.Row.TryGetValue(column, out ColumnValue? value) ? value.LongValue : 0;

    /// <summary>
    /// Reads a nullable numeric cell. <c>SHOW ENGINE STATS</c> leaves total/min/max/last as SQL NULL
    /// wherever the metric kind does not define them, and a null must stay null rather than become a
    /// zero the page would draw as a real reading.
    /// </summary>
    private static double? Number(QueryResultRow row, string column)
    {
        if (!row.Row.TryGetValue(column, out ColumnValue? value))
            return null;

        return value.Type switch
        {
            ColumnType.Null => null,
            ColumnType.Integer64 => value.LongValue,
            _ => value.FloatValue,
        };
    }
}
