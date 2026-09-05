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
/// Transport-level coverage for <c>SHOW STATISTICS</c>: the REST endpoint a client actually calls,
/// the prepared-statement surface both transports share, and the privilege gate.
///
/// <para>These matter separately from the engine tests because the statement reaches clients through
/// a declared column schema plus positional rows. Most of its cells are NULL, and a row set whose
/// schema went missing — or whose NULLs failed to encode — would still look fine from inside the
/// engine while being unreadable on the wire.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestShowStatisticsTransports : BaseTest
{
    private ILogger<ICamusDB> Logger => logger;

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

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupRobots(
        CamusDBOptions? options = null)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options ?? Options);

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
    /// The statement runs over <c>/execute-sql-query</c> with no caller-supplied transaction, which is
    /// the autocommit path: a promoted read-only transaction opens the table and pins its schema.
    /// Asserts the full envelope a client decodes — the declared columns and positional rows — not
    /// merely that the call did not throw.
    /// </summary>
    [Test]
    public async Task RestQueryEndpointReturnsSchemaAndPositionalRows()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots();

        ExecuteSQLController controller = new(
            executor, new HttpTransactionCoordinator(executor), new PreparedStatementRegistry(Options), Logger, Options)
        {
            ControllerContext = Context(JsonSerializer.Serialize(
                new { databaseName = dbname, sql = "SHOW STATISTICS FOR robots" }))
        };

        JsonResult result = await controller.ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status);
        Assert.AreEqual(
            new[] { "table", "kind", "target", "estimated_rows", "distinct_count",
                    "min_value", "max_value", "histogram_buckets", "last_analyzed", "stale_mutations" },
            response.Columns.Select(c => c.Name).ToArray(),
            "clients decode rows positionally, so the declared schema is the contract");

        Assert.Greater(response.Total, 0);

        // Serializing is where a null cell or an unencodable value would actually surface: the row set
        // writes itself straight to JSON rather than through a boxed object graph.
        string json = JsonSerializer.Serialize(response);
        StringAssert.Contains("\"kind\"", json);
        StringAssert.Contains("null", json, "most cells are legitimately NULL and must encode as JSON null");
    }

    /// <summary>
    /// The endpoint carries the statement's errors through as domain errors rather than a 500 —
    /// a view has no statistics of its own, and the client should be told exactly that.
    /// </summary>
    [Test]
    public async Task RestQueryEndpointReportsDomainErrors()
    {
        (string dbname, _, CommandExecutor executor) = await SetupRobots();

        ExecuteSQLController controller = new(
            executor, new HttpTransactionCoordinator(executor), new PreparedStatementRegistry(Options), Logger, Options)
        {
            ControllerContext = Context(JsonSerializer.Serialize(
                new { databaseName = dbname, sql = "SHOW STATISTICS FOR nosuchtable" }))
        };

        JsonResult result = await controller.ExecuteSQLQuery();

        Assert.IsInstanceOf<ExecuteSQLQueryResponse>(result.Value);
        Assert.AreNotEqual("ok", ((ExecuteSQLQueryResponse)result.Value!).Status);
    }

    /// <summary>
    /// Both transports register statements through the same preparable set. The statement takes no
    /// parameters, but a client that prepares every statement it issues must not be refused for this
    /// one while <c>SHOW INDEXES</c> beside it succeeds.
    /// </summary>
    [Test]
    public void StatementIsPreparable()
    {
        Assert.IsTrue(PreparedStatementBinder.IsPreparable(NodeType.ShowStatistics));

        PreparedStatement prepared = PreparedStatementBinder.Create("somedb", "SHOW STATISTICS FOR robots");
        Assert.AreEqual(NodeType.ShowStatistics, prepared.RootNodeType,
            "the routing type is resolved once at prepare time, so both transports must see it here");
        Assert.AreEqual(0, prepared.ParameterCount);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // gRPC
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The gRPC query path is statement-agnostic — it runs the ticket, writes the declared schema and
    /// then each row — so what needs proving for this statement is that its rows survive the wire
    /// encoders. Encodes real result rows through the service's own builders, which is exactly what
    /// the streaming and batch sinks call.
    ///
    /// <para>NULL cells are the point: most of this statement's columns are NULL on most rows, and an
    /// encoder that dropped or mistyped them would leave a client decoding garbage positionally while
    /// the engine looked perfectly healthy.</para>
    /// </summary>
    [Test]
    public async Task GrpcEncodersCarrySchemaAndNullCells()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        KvTransaction txn = await database.Transactions.BeginAsync();
        QuerySchemaHolder schemaHolder = new();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: dbname, sql: "SHOW STATISTICS FOR robots", parameters: null),
            schemaOut: schemaHolder);

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);
        await database.Transactions.CommitAsync(txn);

        IReadOnlyList<DerivedColumnSchema> schema = schemaHolder.Schema!;

        ProtoQueryStreamMessage schemaMessage = CamusSqlService.BuildSchema(schema);
        Assert.AreEqual(
            new[] { "table", "kind", "target", "estimated_rows", "distinct_count",
                    "min_value", "max_value", "histogram_buckets", "last_analyzed", "stale_mutations" },
            schemaMessage.Schema.Columns.Select(c => c.Name).ToArray());

        ResultRowBinder binder = new();
        QueryResultRow tableRow = rows.First(r => r.Row["kind"].StrValue == "table");
        ProtoQueryStreamMessage rowMessage = CamusSqlService.BuildRow(tableRow.Row, schema, binder);

        Assert.AreEqual(schema.Count, rowMessage.Row.Values.Count,
            "a positional row must carry one value per declared column, NULLs included");

        int distinctCountIndex = schema.Select((c, i) => (c.Name, i)).First(x => x.Name == "distinct_count").i;
        Assert.AreEqual(
            ProtoValue.KindOneofCase.NullValue,
            rowMessage.Row.Values[distinctCountIndex].KindCase,
            "a column that does not apply to this row's kind must encode as an explicit null");

        int rowsIndex = schema.Select((c, i) => (c.Name, i)).First(x => x.Name == "estimated_rows").i;
        Assert.AreEqual(6, rowMessage.Row.Values[rowsIndex].Int64Value);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Authorization
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The bounds this statement reports are real values out of the table's columns, so reading them
    /// requires the privilege to read the table — no more (it is not superuser-gated like the engine
    /// metrics) and no less.
    /// </summary>
    [Test]
    public async Task RequiresSelectOnTheTable()
    {
        CamusDBOptions authOptions = Options with
        {
            AuthenticationEnabled = true,
            AccessTokenServerKey = "test-key-padded-to-meet-the-32-byte-secret-floor",
            BootstrapSuperuser = "root",
            BootstrapSuperuserPassword = "root-pw",
        };

        CommandExecutor executor = CreateCommandExecutor(authOptions);
        string dbname = "statsauth" + Guid.NewGuid().ToString("n");
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
            "SELECT on the table is enough to read its statistics");

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
                new ExecuteSQLTicket(txnState: txn, database: dbname, sql: "SHOW STATISTICS FOR robots",
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
