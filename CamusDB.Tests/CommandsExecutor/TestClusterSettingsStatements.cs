/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Standalone end-to-end coverage for <c>SET / RESET CLUSTER SETTING</c> and
/// <c>SHOW CLUSTER SETTINGS</c>, driven through the SQL statement — the real entry point — so the
/// grammar, dispatch, validation, apply, persistence and re-resolution pipeline all run. Standalone
/// mode takes the same pipeline as cluster mode minus replication, so these tests exercise the real
/// apply path rather than a bypass; cross-node behavior belongs to the cluster test project.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestClusterSettingsStatements : BaseTest
{
    private CamusDBOptionsHolder holder = null!;

    private ClusterSettingsService settings = null!;

    private CommandExecutor executor = null!;

    private string db = null!;

    private CamusDBOptions savedAmbient = null!;

    [SetUp]
    public async Task SetUpClusterSettings()
    {
        savedAmbient = CamusDBConfig.Ambient;

        holder = new CamusDBOptionsHolder(Options);

        // The base definition mirrors what a config file naming only data_dir would produce, so
        // re-resolution keeps this test's isolated directory while every other key falls back
        // through the normal chain.
        string dataDir = Options.DataDirectory;
        settings = new ClusterSettingsService(
            TestNode!,
            holder,
            () => new ConfigDefinition { DataDir = dataDir },
            postResolve: null,
            isClusterMode: false,
            forwarder: null,
            logger);

        await settings.StartAsync();

        executor = CreateCommandExecutor(Options, holder, settings);
        (db, _, _) = await CreateDatabaseWith(executor);
    }

    [TearDown]
    public async Task TearDownClusterSettings()
    {
        if (settings is not null)
            await settings.DisposeAsync();

        CamusDBConfig.SetAmbient(savedAmbient);
    }

    private async Task<List<QueryResultRow>> QueryAsync(string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        return rows;
    }

    private async Task NonQueryAsync(string sql)
        => await executor.ExecuteNonSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null));

    private static string? Cell(QueryResultRow row, string column)
        => row.Row.TryGetValue(column, out ColumnValue? value) && value.Type != ColumnType.Null ? value.StrValue : null;

    // ── the full round trip: set, observe, list, reset ────────────────────────

    [Test]
    public async Task SetChangesTheEffectiveConfigurationAndResetRestoresIt()
    {
        await NonQueryAsync("SET CLUSTER SETTING max_mutations_per_transaction = 44444");

        // The engine's own snapshot swapped, not just a report.
        Assert.AreEqual(44444, executor.Options.MaxMutationsPerTransaction);

        // SHOW VARIABLES reports the merged value with cluster provenance — the row that explains a
        // node whose behavior contradicts its own config file.
        QueryResultRow variable = (await QueryAsync(
            "SHOW VARIABLES LIKE 'max_mutations_per_transaction'")).Single();
        Assert.AreEqual("44444", Cell(variable, "value"));
        Assert.AreEqual("cluster", Cell(variable, "source"));

        // SHOW CLUSTER SETTINGS lists exactly what the cluster carries.
        QueryResultRow entry = (await QueryAsync("SHOW CLUSTER SETTINGS")).Single();
        Assert.AreEqual("max_mutations_per_transaction", Cell(entry, "setting"));
        Assert.AreEqual("44444", Cell(entry, "value"));

        await NonQueryAsync("RESET CLUSTER SETTING max_mutations_per_transaction");

        Assert.AreEqual(20_000, executor.Options.MaxMutationsPerTransaction);
        Assert.IsEmpty(await QueryAsync("SHOW CLUSTER SETTINGS"));

        variable = (await QueryAsync("SHOW VARIABLES LIKE 'max_mutations_per_transaction'")).Single();
        Assert.AreEqual("20000", Cell(variable, "value"));
        Assert.AreNotEqual("cluster", Cell(variable, "source"));
    }

    /// <summary>Enum-valued settings take bare identifiers, using the file's underscored spelling.</summary>
    [Test]
    public async Task EnumAndBoolValuesUseTheConfigFileSpelling()
    {
        await NonQueryAsync("SET CLUSTER SETTING default_isolation_level = read_committed");
        await NonQueryAsync("SET CLUSTER SETTING query_tracing_enabled = true");

        Assert.AreEqual(
            CamusDB.Core.Transactions.CamusIsolationLevel.ReadCommitted,
            executor.Options.DefaultIsolationLevel);
        Assert.IsTrue(executor.Options.QueryTracingEnabled);
    }

    // ── the rejection taxonomy: three different errors for three different mistakes ──

    [Test]
    public void UnknownKeyIsRejectedAsUnknown()
    {
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(
            () => NonQueryAsync("SET CLUSTER SETTING no_such_setting = 1"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, e.Code);
        StringAssert.Contains("Unknown cluster setting", e.Message);
    }

    [Test]
    public void RestartClassKeyIsRejectedAsRestartBound()
    {
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(
            () => NonQueryAsync("SET CLUSTER SETTING data_dir = '/elsewhere'"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, e.Code);
        StringAssert.Contains("restart", e.Message);

        // kahuna.* keys are restart-class by the section rule, and must say so rather than
        // "unknown" — the operator would otherwise hunt for a typo that isn't there.
        e = Assert.ThrowsAsync<CamusDBException>(
            () => NonQueryAsync("SET CLUSTER SETTING kahuna.wal_sync_writes = false"))!;
        StringAssert.Contains("restart", e.Message);
    }

    /// <summary>
    /// A value that breaks a cross-field invariant is rejected before it is applied anywhere — the
    /// validator's own message names both sides. Changing EITHER side of a pair can violate it.
    /// </summary>
    [Test]
    public async Task ValueViolatingACrossFieldInvariantIsRejected()
    {
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(
            () => NonQueryAsync("SET CLUSTER SETTING ttl_span_lease_renew_interval_ms = 999999999"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, e.Code);
        StringAssert.Contains("ttl_span_lease_ms", e.Message);

        // The rejected change left nothing behind.
        Assert.IsEmpty(await QueryAsync("SHOW CLUSTER SETTINGS"));
        Assert.AreEqual(10_000, executor.Options.TtlSpanLeaseRenewIntervalMs);
    }

    [Test]
    public void MalformedValueIsRejectedWithTheKeyNamed()
    {
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(
            () => NonQueryAsync("SET CLUSTER SETTING max_mutations_per_transaction = banana"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, e.Code);
        StringAssert.Contains("max_mutations_per_transaction", e.Message);
    }

    // ── durability: a fresh service over the same store loads the overlay ─────

    [Test]
    public async Task OverlaySurvivesAServiceRestart()
    {
        await NonQueryAsync("SET CLUSTER SETTING max_mutations_per_transaction = 55555");

        string dataDir = Options.DataDirectory;
        CamusDBOptionsHolder rebornHolder = new(Options);
        ClusterSettingsService reborn = new(
            TestNode!,
            rebornHolder,
            () => new ConfigDefinition { DataDir = dataDir },
            postResolve: null,
            isClusterMode: false,
            forwarder: null,
            logger);

        try
        {
            await reborn.StartAsync();

            Assert.AreEqual(
                ("max_mutations_per_transaction", "55555"),
                reborn.List().Single());
            Assert.AreEqual(55555, rebornHolder.Current.MaxMutationsPerTransaction);
        }
        finally
        {
            await reborn.DisposeAsync();
        }
    }

    // ── SET pastes back what SHOW VARIABLES prints ─────────────────────────────

    /// <summary>
    /// The coercion round trip: for a sample of every scalar shape, the text SHOW VARIABLES prints
    /// is accepted back by SET CLUSTER SETTING and produces the same effective value — a silent
    /// disagreement here would mean a value that means one thing in a file and another in a
    /// statement.
    /// </summary>
    [Test]
    public async Task ShowVariablesOutputPastesBackUnchanged()
    {
        foreach (string key in new[]
        {
            "max_mutations_per_transaction",   // int
            "orphan_retention_ms",             // long
            "net_weight",                      // double
            "ttl_enabled",                     // bool
            "default_transaction_locking",     // enum-as-string
            "ttl_default_job_cron",            // string
        })
        {
            QueryResultRow before = (await QueryAsync($"SHOW VARIABLES LIKE '{key}'")).Single();
            string printed = Cell(before, "value")!;

            await NonQueryAsync($"SET CLUSTER SETTING {key} = '{printed}'");

            QueryResultRow after = (await QueryAsync($"SHOW VARIABLES LIKE '{key}'")).Single();
            Assert.AreEqual(printed, Cell(after, "value"), key);
            Assert.AreEqual("cluster", Cell(after, "source"), key);
        }
    }
}
