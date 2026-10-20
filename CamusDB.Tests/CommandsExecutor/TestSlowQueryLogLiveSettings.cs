/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>slow_query_log_enabled</c> is a runtime setting, so a live <c>SET CLUSTER SETTING</c> must
/// start and stop recording on an engine that is already running — including one that booted with
/// the log off, which holds no ring until the first enable creates it.
///
/// <para>Each test drives the real path: the SQL statement, the settings service, the options
/// holder, the engine's fan-out, and <c>SHOW SLOW QUERIES</c>. The engine boots with the log off and
/// the default 1000 ms threshold; every change after that is made through SQL, never by building a
/// second engine, because a second engine would pass whether or not the live change works.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestSlowQueryLogLiveSettings : BaseTest
{
    private CamusDBOptionsHolder holder = null!;

    private ClusterSettingsService settings = null!;

    private CamusDBOptions boot = null!;

    private CamusDBOptions savedAmbient = null!;

    [SetUp]
    public async Task SetUpLiveSettings()
    {
        savedAmbient = CamusDBConfig.Ambient;

        boot = Options with { SlowQueryLogEnabled = false, SlowQueryLogThresholdMs = 1000 };
        holder = new CamusDBOptionsHolder(boot);

        // The base definition carries the same boot values, so re-resolution after a SET changes
        // only the key the test names.
        string dataDir = Options.DataDirectory;
        settings = new ClusterSettingsService(
            TestNode!,
            holder,
            () => new ConfigDefinition
            {
                DataDir = dataDir,
                SlowQueryLogEnabled = false,
                SlowQueryLogThresholdMs = 1000,
            },
            postResolve: null,
            isClusterMode: false,
            forwarder: null,
            logger);

        await settings.StartAsync();
    }

    [TearDown]
    public async Task TearDownLiveSettings()
    {
        if (settings is not null)
            await settings.DisposeAsync();

        CamusDBConfig.SetAmbient(savedAmbient);
    }

    private async Task<(string Db, DatabaseDescriptor Database, CommandExecutor Executor)> CreateEngineWithTable()
    {
        CommandExecutor executor = CreateCommandExecutor(holder.Current, holder, settings);
        (string db, DatabaseDescriptor database, _) = await CreateDatabaseWith(executor);

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db,
            "CREATE TABLE robots (id int64 PRIMARY KEY, name string(32))", null));
        await database.Transactions.CommitAsync(ddl);

        KvTransaction insert = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(insert, db,
            "INSERT INTO robots (id, name) VALUES (1, 'r2'), (2, 'c3po'), (3, 'bb8')", null));
        await database.Transactions.CommitAsync(insert);

        return (db, database, executor);
    }

    private static async Task NonQueryAsync(CommandExecutor executor, string db, string sql)
        => await executor.ExecuteNonSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null));

    private static async Task<List<QueryResultRow>> QueryAsync(
        CommandExecutor executor, string db, string sql, KvTransaction? txnState)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState!, database: db, sql: sql, parameters: null));

        return await cursor.ToListAsync();
    }

    private static Task<List<QueryResultRow>> SelectAsync(CommandExecutor executor, string db, string sql)
        => QueryAsync(executor, db, sql, KvTransaction.CreateReadOnly());

    /// <summary>Reads the log without a transaction, the way a server-level statement is reached.</summary>
    private static Task<List<QueryResultRow>> ShowSlowQueriesAsync(CommandExecutor executor, string db)
        => QueryAsync(executor, db, "SHOW SLOW QUERIES", txnState: null);

    private static List<QueryResultRow> WithSql(IEnumerable<QueryResultRow> rows, string fragment)
        => rows.Where(row => row.Row["sql"].StrValue?.Contains(fragment, StringComparison.OrdinalIgnoreCase) == true).ToList();

    /// <summary>
    /// The case the setting exists for: an operator turns the log on, on a running node that booted
    /// with it off, to find a slow statement. The threshold is set first, while the log is still off,
    /// and the enabling statement itself starts before the change, so the only statement the log can
    /// hold is the <c>SELECT</c> that follows.
    /// </summary>
    [Test]
    public async Task EnablingTheLogLiveOnANodeThatBootedWithItOffRecordsTheNextStatement()
    {
        (string db, _, CommandExecutor executor) = await CreateEngineWithTable();

        await SelectAsync(executor, db, "SELECT * FROM robots WHERE name = 'before_enable'");
        Assert.IsEmpty(await ShowSlowQueriesAsync(executor, db), "the log is off at boot");

        await NonQueryAsync(executor, db, "SET CLUSTER SETTING slow_query_log_threshold_ms = 0");
        await NonQueryAsync(executor, db, "SET CLUSTER SETTING slow_query_log_enabled = true");
        Assert.IsTrue(executor.Options.SlowQueryLogEnabled);

        await SelectAsync(executor, db, "SELECT * FROM robots WHERE name = 'after_enable'");

        List<QueryResultRow> rows = await ShowSlowQueriesAsync(executor, db);

        Assert.AreEqual(1, rows.Count, "only the SELECT after the enable may be recorded");
        Assert.AreEqual(1, WithSql(rows, "after_enable").Count);
        Assert.AreEqual("select", rows[0].Row["kind"].StrValue);
        Assert.AreEqual(0, rows[0].Row["rows_returned"].LongValue, "the predicate matches no robot");
    }

    /// <summary>
    /// Off and on again, live. Turning the log off stops new entries and keeps the old ones; turning
    /// it back on writes into the same ring, so the sequence numbers keep counting rather than
    /// restarting at 1 in a new ring that lost the history.
    /// </summary>
    [Test]
    public async Task TogglingTheLogLiveKeepsOneRingAndItsEntries()
    {
        (string db, _, CommandExecutor executor) = await CreateEngineWithTable();

        await NonQueryAsync(executor, db, "SET CLUSTER SETTING slow_query_log_threshold_ms = 0");
        await NonQueryAsync(executor, db, "SET CLUSTER SETTING slow_query_log_enabled = true");
        await SelectAsync(executor, db, "SELECT * FROM robots WHERE name = 'first_on'");

        await NonQueryAsync(executor, db, "SET CLUSTER SETTING slow_query_log_enabled = false");
        await SelectAsync(executor, db, "SELECT * FROM robots WHERE name = 'while_off'");

        List<QueryResultRow> afterOff = await ShowSlowQueriesAsync(executor, db);
        List<QueryResultRow> first = WithSql(afterOff, "first_on");
        Assert.AreEqual(1, first.Count, "turning the log off keeps what it already recorded");
        Assert.IsEmpty(WithSql(afterOff, "while_off"), "nothing is recorded while the log is off");

        await NonQueryAsync(executor, db, "SET CLUSTER SETTING slow_query_log_enabled = true");
        await SelectAsync(executor, db, "SELECT * FROM robots WHERE name = 'second_on'");

        List<QueryResultRow> afterOn = await ShowSlowQueriesAsync(executor, db);
        List<QueryResultRow> second = WithSql(afterOn, "second_on");

        Assert.AreEqual(1, second.Count);
        Assert.AreEqual(1, WithSql(afterOn, "first_on").Count, "re-enabling must not replace the ring");
        Assert.Greater(second[0].Row["seq"].LongValue, first[0].Row["seq"].LongValue);
    }

    /// <summary>
    /// The dashboard panel shows a log enabled live. The host registers <see cref="CamusDBOptions"/>
    /// as transient, resolved from the holder per activation, so a controller built after the SET
    /// carries the live snapshot; the controller here is built the same way. The panel's rows come
    /// from the engine's recorder, so this also proves the ring created by the live enable is the one
    /// the panel reads.
    /// </summary>
    [Test]
    public async Task TheDashboardPanelFollowsALiveEnable()
    {
        (string db, _, CommandExecutor executor) = await CreateEngineWithTable();

        await NonQueryAsync(executor, db, "SET CLUSTER SETTING slow_query_log_threshold_ms = 0");
        await NonQueryAsync(executor, db, "SET CLUSTER SETTING slow_query_log_enabled = true");
        await SelectAsync(executor, db, "SELECT * FROM robots WHERE name = 'dashboard_live'");

        DefaultHttpContext http = new();
        http.Connection.RemoteIpAddress = IPAddress.Loopback;

        DashboardController controller = new(
            executor,
            new HttpTransactionCoordinator(executor),
            logger,
            holder.Current,
            TestNode!,
            new ForegroundRequestGauge(),
            new PreparedStatementRegistry(holder.Current))
        {
            ControllerContext = new ControllerContext { HttpContext = http },
        };

        DashboardSlowQueriesResponse body = (DashboardSlowQueriesResponse)((JsonResult)await controller.GetSlowQueries()).Value!;

        Assert.IsTrue(body.LogEnabled, "the panel must report the live flag");
        Assert.AreEqual(0, body.ThresholdMs);
        Assert.AreEqual(boot.SlowQueryLogMaxEntries, body.Capacity);
        Assert.IsNotNull(body.Rows.Find(row => row.Sql.Contains("dashboard_live")), "the SELECT should be on the panel");
    }
}
