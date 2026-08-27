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
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Grpc;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using ProtoQueryStreamMessage = CamusDB.Grpc.QueryStreamMessage;
using ProtoValue = CamusDB.Grpc.Value;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Transport-level coverage for <c>SHOW RANGES</c>: the REST endpoint a client actually calls, the
/// gRPC row encoders, the prepared-statement surface both transports share, and the privilege gate.
///
/// <para>These matter separately from the engine tests because the statement reaches a client as a
/// declared column schema plus <b>positional</b> rows. Nine of its fifteen columns are NULL on a
/// hash-routed span, so a codec that dropped a null instead of emitting it would shift every later
/// value by one — and the engine would look perfectly healthy while the client decoded nonsense.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestShowRangesTransports : BaseTest
{
    private ILogger<ICamusDB> Logger => logger;

    private static readonly string[] ExpectedColumns =
    [
        "relation", "key_space", "routing", "span",
        "start_key", "end_key", "raw_start_key", "raw_end_key",
        "partition_id", "generation",
        "leader", "leader_is_local", "hosted_locally", "replicas", "probe_key",
    ];

    private static ControllerContext Context(string body, string? bearer = null)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(body);
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        if (bearer is not null)
            http.Request.Headers.Authorization = $"Bearer {bearer}";
        return new ControllerContext { HttpContext = http };
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupRobots()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",      new ColumnIndexInfo[] { new("id",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "year_idx", new ColumnIndexInfo[] { new("year", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 6; i++)
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "Robot" + i) },
                        { "year", new(ColumnType.Integer64, (long)(2000 + i)) },
                    }
                }));
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // REST
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Runs over <c>/execute-sql-query</c> with no caller-supplied transaction, which is the
    /// autocommit path. Asserts the whole envelope a client decodes — the declared columns and the
    /// positional rows — rather than merely that the call did not throw.
    /// </summary>
    [Test]
    public async Task RestQueryEndpointReturnsSchemaAndPositionalRows()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots();

        ExecuteSQLController controller = new(
            executor, new HttpTransactionCoordinator(executor), new PreparedStatementRegistry(Options), Logger, Options)
        {
            ControllerContext = Context(JsonSerializer.Serialize(
                new { databaseName = dbname, sql = "SHOW RANGES FROM TABLE robots" }))
        };

        JsonResult result = await controller.ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status);
        Assert.AreEqual(
            ExpectedColumns,
            response.Columns.Select(c => c.Name).ToArray(),
            "clients decode rows positionally, so the declared schema is the contract");

        Assert.Greater(response.Total, 0);

        string json = JsonSerializer.Serialize(response);
        StringAssert.Contains("\"routing\"", json);
        StringAssert.Contains("null", json,
            "an unbounded bound and an unlocated probe key are legitimately NULL and must encode as JSON null");
    }

    /// <summary>
    /// The index form must survive the REST path unchanged. The <c>@</c> in the target is the part
    /// worth proving here: it is a single scanner token, and a transport that re-parsed or re-encoded
    /// the statement text would be where that broke.
    /// </summary>
    [Test]
    public async Task RestQueryEndpointAcceptsTheQualifiedIndexForm()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots();

        ExecuteSQLController controller = new(
            executor, new HttpTransactionCoordinator(executor), new PreparedStatementRegistry(Options), Logger, Options)
        {
            ControllerContext = Context(JsonSerializer.Serialize(
                new { databaseName = dbname, sql = "SHOW RANGES FROM INDEX robots@year_idx" }))
        };

        JsonResult result = await controller.ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status);
        Assert.Greater(response.Total, 0);
    }

    /// <summary>Domain errors reach the client as domain errors, not as a 500.</summary>
    [Test]
    public async Task RestQueryEndpointReportsDomainErrors()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots();

        ExecuteSQLController controller = new(
            executor, new HttpTransactionCoordinator(executor), new PreparedStatementRegistry(Options), Logger, Options)
        {
            ControllerContext = Context(JsonSerializer.Serialize(
                new { databaseName = dbname, sql = "SHOW RANGES FROM INDEX robots@nosuchindex" }))
        };

        JsonResult result = await controller.ExecuteSQLQuery();

        Assert.IsInstanceOf<ExecuteSQLQueryResponse>(result.Value);
        Assert.AreNotEqual("ok", ((ExecuteSQLQueryResponse)result.Value!).Status);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Prepared statements
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Both transports register statements through the same preparable set.
    ///
    /// <para>The parameter count is the assertion that matters. Without the scanner's qualified-name
    /// token, <c>robots@year_idx</c> would lex as an identifier followed by a placeholder, and this
    /// would report one parameter — a value the client would then be asked for and could never
    /// supply, for a parameter nobody wrote.</para>
    /// </summary>
    [Test]
    public void StatementIsPreparable()
    {
        Assert.IsTrue(PreparedStatementBinder.IsPreparable(NodeType.ShowRanges));

        PreparedStatement prepared = PreparedStatementBinder.Create("somedb", "SHOW RANGES FROM TABLE robots");
        Assert.AreEqual(NodeType.ShowRanges, prepared.RootNodeType,
            "the routing type is resolved once at prepare time, so both transports must see it here");
        Assert.AreEqual(0, prepared.ParameterCount);

        PreparedStatement qualified = PreparedStatementBinder.Create(
            "somedb", "SHOW RANGES FROM INDEX robots@year_idx");
        Assert.AreEqual(0, qualified.ParameterCount,
            "the '@' in a qualified index name is not a bind parameter");

        PreparedStatement withParameter = PreparedStatementBinder.Create(
            "somedb", "SHOW RANGE FROM TABLE robots FOR ROW (@id)");
        Assert.AreEqual(1, withParameter.ParameterCount,
            "a real placeholder in FOR ROW still binds");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // gRPC
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Encodes real result rows through the service's own builders, which is what the streaming and
    /// batch sinks call.
    ///
    /// <para>NULL cells are the point. On a hash-routed span both bounds, both raw bounds and the
    /// probe key are NULL, so an encoder that dropped them would leave every later value in the wrong
    /// position — a client would read the partition id out of the <c>leader</c> slot and never see a
    /// type error.</para>
    /// </summary>
    [Test]
    public async Task GrpcEncodersCarrySchemaAndNullCells()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        KvTransaction txn = await database.Transactions.BeginAsync();
        QuerySchemaHolder schemaHolder = new();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: dbname, sql: "SHOW RANGES FROM TABLE robots", parameters: null),
            schemaOut: schemaHolder);

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);
        await database.Transactions.CommitAsync(txn);

        IReadOnlyList<DerivedColumnSchema> schema = schemaHolder.Schema!;

        ProtoQueryStreamMessage schemaMessage = CamusSqlService.BuildSchema(schema);
        Assert.AreEqual(ExpectedColumns, schemaMessage.Schema.Columns.Select(c => c.Name).ToArray());

        ResultRowBinder binder = new();
        ProtoQueryStreamMessage rowMessage = CamusSqlService.BuildRow(rows[0].Row, schema, binder);

        Assert.AreEqual(schema.Count, rowMessage.Row.Values.Count,
            "a positional row must carry one value per declared column, NULLs included");

        int IndexOf(string name) => schema.Select((c, i) => (c.Name, i)).First(x => x.Name == name).i;

        Assert.AreEqual(ProtoValue.KindOneofCase.NullValue, rowMessage.Row.Values[IndexOf("start_key")].KindCase,
            "an unbounded bound must encode as an explicit null");
        Assert.AreEqual(ProtoValue.KindOneofCase.NullValue, rowMessage.Row.Values[IndexOf("probe_key")].KindCase,
            "the plural form locates no key, and that must encode as an explicit null");

        Assert.AreEqual("hash", rowMessage.Row.Values[IndexOf("routing")].StringValue);
        Assert.AreEqual(1, rowMessage.Row.Values[IndexOf("span")].Int64Value);
        Assert.IsTrue(rowMessage.Row.Values[IndexOf("hosted_locally")].BoolValue);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Authorization
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A decoded split bound is a real value out of the table's own columns, so reading one requires
    /// the privilege to read the table — no more (it is deliberately not superuser-gated the way the
    /// engine metrics are) and no less.
    /// </summary>
    [Test]
    public async Task RequiresSelectOnTheTable()
    {
        CamusDBOptions authOptions = Options with
        {
            AuthenticationEnabled = true,
            AccessTokenServerKey = "test-key",
            BootstrapSuperuser = "root",
            BootstrapSuperuserPassword = "root-pw",
        };

        CommandExecutor executor = CreateCommandExecutor(authOptions);
        string dbname = "rangesauth" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));
        TrackDatabase(dbname, executor);

        await executor.EnsureBootstrapSuperuserAsync(
            authOptions.BootstrapSuperuser, authOptions.BootstrapSuperuserPassword);
        Principal root = await executor.ResolvePrincipalAsync((await executor.LoginAsync("root", "root-pw")).Token);

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);

        await Ddl(database, executor, dbname,
            "CREATE TABLE robots (id int64 PRIMARY KEY NOT NULL, year int64 NULL)", root);
        await ServerDdl(executor, "CREATE USER reader IDENTIFIED BY 'reader-pw'", root);
        await ServerDdl(executor, "CREATE USER outsider IDENTIFIED BY 'outsider-pw'", root);
        await ServerDdl(executor, $"GRANT SELECT ON {dbname}.robots TO reader", root);

        Principal reader = await executor.ResolvePrincipalAsync((await executor.LoginAsync("reader", "reader-pw")).Token);
        Principal outsider = await executor.ResolvePrincipalAsync((await executor.LoginAsync("outsider", "outsider-pw")).Token);

        Assert.DoesNotThrowAsync(
            async () => await RunShow(executor, database, dbname, reader),
            "SELECT on the table is enough to read where its key space divides");

        CamusDBException denied = Assert.ThrowsAsync<CamusDBException>(
            async () => await RunShow(executor, database, dbname, outsider))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, denied.Code);
    }

    private static async Task RunShow(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, Principal principal)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: txn, database: dbname, sql: "SHOW RANGES FROM TABLE robots",
                    parameters: null, principal: principal));
            await foreach (QueryResultRow _ in cursor) { }
            await database.Transactions.CommitAsync(txn);
        }
        catch (Exception)
        {
            await database.Transactions.RollbackIfNotCompletedAsync(txn);
            throw;
        }
    }

    private static async Task Ddl(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, Principal? principal)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: txn, database: dbname, sql: sql,
            parameters: null, principal: principal));
        await database.Transactions.CommitAsync(txn);
    }

    private static Task ServerDdl(CommandExecutor executor, string sql, Principal? principal)
        => executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql,
            parameters: null, principal: principal));
}
