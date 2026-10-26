/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

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
/// <c>statement_stats_enabled</c> and <c>index_usage_enabled</c> are runtime settings, so a live
/// <c>SET CLUSTER SETTING</c> must start and stop them on an engine that is already running.
///
/// <para>Each test drives the real path: the SQL statement, the settings service, the options
/// holder, the engine's fan-out, and the <c>SHOW</c> statement. The engine boots with statement
/// statistics off and index usage on — the defaults — and every change after that is made through
/// SQL, never by building a second engine, because a second engine would pass whether or not the
/// live change works.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestDiagnosticsLiveSettings : BaseTest
{
    private CamusDBOptionsHolder holder = null!;

    private ClusterSettingsService settings = null!;

    private CamusDBOptions savedAmbient = null!;

    [SetUp]
    public async Task SetUpLiveSettings()
    {
        savedAmbient = CamusDBConfig.Ambient;

        CamusDBOptions boot = Options with { StatementStatsEnabled = false, IndexUsageEnabled = true };
        holder = new CamusDBOptionsHolder(boot);

        string dataDir = Options.DataDirectory;
        settings = new ClusterSettingsService(
            TestNode!,
            holder,
            () => new ConfigDefinition
            {
                DataDir = dataDir,
                StatementStatsEnabled = false,
                IndexUsageEnabled = true,
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

    private async Task<(string Db, CommandExecutor Executor)> CreateEngineWithTable()
    {
        CommandExecutor executor = CreateCommandExecutor(holder.Current, holder, settings);
        (string db, DatabaseDescriptor database, _) = await CreateDatabaseWith(executor);

        foreach (string ddl in new[] { "CREATE TABLE robots (id int64 PRIMARY KEY, year int64)", "CREATE INDEX year_idx ON robots (year)" })
        {
            KvTransaction tx = await database.Transactions.BeginAsync();
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, ddl, null));
            await database.Transactions.CommitAsync(tx);
        }

        KvTransaction insert = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(insert, db,
            "INSERT INTO robots (id, year) VALUES " + string.Join(", ", Enumerable.Range(1, 20).Select(i => $"({i}, {2000 + i})")), null));
        await database.Transactions.CommitAsync(insert);

        return (db, executor);
    }

    private static async Task SetAsync(CommandExecutor executor, string db, string sql)
        => await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null));

    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string db, string sql, KvTransaction? tx)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx!, database: db, sql: sql, parameters: null));

        return await cursor.ToListAsync();
    }

    private static Task<List<QueryResultRow>> SelectAsync(CommandExecutor executor, string db, string sql)
        => QueryAsync(executor, db, sql, KvTransaction.CreateReadOnly());

    private static async Task<long> YearIndexReadsAsync(CommandExecutor executor, string db)
        => (await SelectAsync(executor, db, "SHOW INDEX USAGE FROM robots"))
            .Single(row => row.Row["index"].StrValue == "year_idx").Row["reads"].LongValue;

    [Test]
    public async Task EnablingStatementStatsLiveRecordsTheNextStatement()
    {
        (string db, CommandExecutor executor) = await CreateEngineWithTable();

        await SelectAsync(executor, db, "SELECT * FROM robots WHERE id = 1");
        Assert.IsEmpty(await QueryAsync(executor, "", "SHOW STATEMENT STATS", null), "off at boot");

        await SetAsync(executor, db, "SET CLUSTER SETTING statement_stats_enabled = true");
        Assert.IsTrue(executor.Options.StatementStatsEnabled);

        await SelectAsync(executor, db, "SELECT * FROM robots WHERE id = 2");
        await SelectAsync(executor, db, "SELECT * FROM robots WHERE id = 3");

        List<QueryResultRow> rows = await QueryAsync(executor, "", "SHOW STATEMENT STATS", null);
        QueryResultRow row = rows.Single(r => r.Row["kind"].StrValue == "select");

        Assert.AreEqual(2, row.Row["calls"].LongValue, "only the statements after the enable");

        await SetAsync(executor, db, "SET CLUSTER SETTING statement_stats_enabled = false");
        await SelectAsync(executor, db, "SELECT * FROM robots WHERE id = 1");

        rows = await QueryAsync(executor, "", "SHOW STATEMENT STATS", null);
        Assert.AreEqual(2, rows.Single(r => r.Row["kind"].StrValue == "select").Row["calls"].LongValue,
            "turning it off stops recording and keeps the totals");
    }

    [Test]
    public async Task DisablingIndexUsageLiveStopsTheCountsOnTheNextStatement()
    {
        (string db, CommandExecutor executor) = await CreateEngineWithTable();
        const string select = "SELECT * FROM robots WHERE year = 2002";

        CollectionAssert.Contains(
            (await SelectAsync(executor, db, "EXPLAIN " + select)).Select(row => row.Row["node"].StrValue), "index-range-scan");

        long start = await YearIndexReadsAsync(executor, db);
        await SelectAsync(executor, db, select);
        long counted = await YearIndexReadsAsync(executor, db);
        Assert.AreEqual(1, counted - start);

        await SetAsync(executor, db, "SET CLUSTER SETTING index_usage_enabled = false");
        await SelectAsync(executor, db, select);
        Assert.AreEqual(counted, await YearIndexReadsAsync(executor, db), "off: no count, and the old count is kept");

        await SetAsync(executor, db, "SET CLUSTER SETTING index_usage_enabled = true");
        await SelectAsync(executor, db, select);
        Assert.AreEqual(counted + 1, await YearIndexReadsAsync(executor, db));
    }
}
