
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Nito.AsyncEx;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Per-table auto-analyze control via
/// <c>ALTER TABLE t SET (sql_stats_automatic_collection_enabled = false)</c>: parsing, persistence,
/// and that the background scheduler honors the opt-out while manual ANALYZE ignores it.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestAutoAnalyzeTableSetting : BaseTest
{
    private bool savedEnabled;
    private double savedFraction;
    private long savedMinRows;
    private int savedMaxRowsPerSecond;
    private int savedCheckRows;

    [SetUp]
    public void SnapshotConfig()
    {
        savedEnabled          = CamusDBConfig.AutoAnalyzeEnabled;
        savedFraction         = CamusDBConfig.AutoAnalyzeFractionStaleRows;
        savedMinRows          = CamusDBConfig.AutoAnalyzeMinStaleRows;
        savedMaxRowsPerSecond = CamusDBConfig.AutoAnalyzeMaxRowsPerSecond;
        savedCheckRows        = CamusDBConfig.AutoAnalyzeOwnershipCheckRows;
    }

    [TearDown]
    public void RestoreConfig()
    {
        CamusDBConfig.AutoAnalyzeEnabled            = savedEnabled;
        CamusDBConfig.AutoAnalyzeFractionStaleRows   = savedFraction;
        CamusDBConfig.AutoAnalyzeMinStaleRows        = savedMinRows;
        CamusDBConfig.AutoAnalyzeMaxRowsPerSecond    = savedMaxRowsPerSecond;
        CamusDBConfig.AutoAnalyzeOwnershipCheckRows  = savedCheckRows;
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupRobotsTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "year_idx", new ColumnIndexInfo[] { new("year", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private static async Task InsertRobotsAsync(CommandExecutor executor, DatabaseDescriptor database, string dbname, int count)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < count; i++)
            await executor.Insert(new InsertTicket(
                txnState: txn, databaseName: dbname, tableName: "robots",
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
    }

    private static async Task<TableDescriptor> OpenTableAsync(DatabaseDescriptor db, string tableName)
    {
        if (db.TableDescriptors.TryGetValue(tableName, out AsyncLazy<TableDescriptor>? lazy))
            return await lazy;
        throw new InvalidOperationException($"Table '{tableName}' not found");
    }

    private static async Task ExecDdlAsync(CommandExecutor executor, string dbname, string sql)
        => await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: dbname, sql: sql, parameters: null));

    private static async Task RunManualAnalyzeAsync(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(txn, dbname, "ANALYZE robots", null));
        await foreach (QueryResultRow _ in cursor) { }
        await database.Transactions.CommitAsync(txn);
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task SetFalseSkipsAutoAnalyzeThenReEnableResumes()
    {
        CamusDBConfig.AutoAnalyzeEnabled = true;
        CamusDBConfig.AutoAnalyzeFractionStaleRows = 0.0;
        CamusDBConfig.AutoAnalyzeMinStaleRows = 5;

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();
        TableDescriptor table = await OpenTableAsync(database, "robots");
        await InsertRobotsAsync(executor, database, dbname, 20);

        // Opt the table out of automatic collection.
        await ExecDdlAsync(executor, dbname, "ALTER TABLE robots SET (sql_stats_automatic_collection_enabled = false)");
        Assert.IsFalse(table.Schema.AutoStatsCollectionEnabled, "Setting must be reflected in the schema");

        int analyzed = await executor.RunAutoAnalyzeForTestsAsync();
        Assert.AreEqual(0, analyzed, "A disabled table must not be auto-analyzed even when stale");
        Assert.IsNull(executor.Statistics.GetColumnNdv(database, table, "year"),
            "No statistics may be built for a disabled table");

        // Re-enable and confirm the sweep now picks it up.
        await ExecDdlAsync(executor, dbname, "ALTER TABLE robots SET (sql_stats_automatic_collection_enabled = true)");
        Assert.IsTrue(table.Schema.AutoStatsCollectionEnabled);

        int analyzed2 = await executor.RunAutoAnalyzeForTestsAsync();
        Assert.GreaterOrEqual(analyzed2, 1, "A re-enabled stale table must be analyzed");
        Assert.IsNotNull(executor.Statistics.GetColumnNdv(database, table, "year"));
    }

    [Test]
    public async Task ManualAnalyzeRunsOnDisabledTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();
        TableDescriptor table = await OpenTableAsync(database, "robots");
        await InsertRobotsAsync(executor, database, dbname, 10);

        await ExecDdlAsync(executor, dbname, "ALTER TABLE robots SET (sql_stats_automatic_collection_enabled = false)");

        // The opt-out gates only the background scheduler; an explicit ANALYZE still runs.
        await RunManualAnalyzeAsync(executor, database, dbname);
        Assert.IsNotNull(executor.Statistics.GetColumnNdv(database, table, "year"),
            "Manual ANALYZE must run regardless of the auto-collection setting");
    }

    [Test]
    public async Task SettingSurvivesReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        await ExecDdlAsync(executor, dbname, "ALTER TABLE robots SET (sql_stats_automatic_collection_enabled = false)");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        TableSchema? schema = reopened.Schema.Tables.GetValueOrDefault("robots");

        Assert.IsNotNull(schema);
        Assert.IsFalse(schema!.AutoStatsCollectionEnabled, "The opt-out must persist across a reopen");
    }

    [Test]
    public void RejectsUnknownSettingKey()
    {
        Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (string dbname, _, CommandExecutor executor) = await SetupRobotsTable();
            await ExecDdlAsync(executor, dbname, "ALTER TABLE robots SET (some_unknown_param = false)");
        });
    }

    [Test]
    public void RejectsNonBooleanValue()
    {
        Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (string dbname, _, CommandExecutor executor) = await SetupRobotsTable();
            await ExecDdlAsync(executor, dbname, "ALTER TABLE robots SET (sql_stats_automatic_collection_enabled = 3)");
        });
    }

    [Test]
    public async Task SettingNameIsCaseInsensitive()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();
        TableDescriptor table = await OpenTableAsync(database, "robots");

        // Upper/mixed-case name and value — ordinary SQL identifiers are case-insensitive.
        await ExecDdlAsync(executor, dbname, "ALTER TABLE robots SET (SQL_STATS_Automatic_Collection_Enabled = FALSE)");
        Assert.IsFalse(table.Schema.AutoStatsCollectionEnabled, "Case-insensitive setting name must be honored");
    }

    [Test]
    public void RejectsDuplicateKey()
    {
        Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (string dbname, _, CommandExecutor executor) = await SetupRobotsTable();
            await ExecDdlAsync(executor, dbname,
                "ALTER TABLE robots SET (sql_stats_automatic_collection_enabled = false, sql_stats_automatic_collection_enabled = true)");
        });
    }

    [Test]
    public async Task UnrelatedAlterPreservesSetting()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();
        TableDescriptor table = await OpenTableAsync(database, "robots");

        await ExecDdlAsync(executor, dbname, "ALTER TABLE robots SET (sql_stats_automatic_collection_enabled = false)");
        // A subsequent, unrelated schema change must not drop the setting.
        await ExecDdlAsync(executor, dbname, "ALTER TABLE robots ADD COLUMN extra INT64");

        Assert.IsFalse(table.Schema.AutoStatsCollectionEnabled, "The opt-out must survive an unrelated ALTER");
    }

    [Test]
    public async Task OptOutSurvivesDeferredDropAndRelink()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();
        TableDescriptor table = await OpenTableAsync(database, "robots");
        string tableId = table.Id!;

        await ExecDdlAsync(executor, dbname, "ALTER TABLE robots SET (sql_stats_automatic_collection_enabled = false)");

        // Deferred (non-force) drop retains the table as a recoverable orphan.
        await executor.DropTable(new DropTableTicket(dbname, "robots", ifExists: false));

        // Relink it under a new name; the opt-out must be restored from the orphan, not reset to default.
        await ExecDdlAsync(executor, dbname, $"CREATE TABLE robots_recovered RELINK TO \"{tableId}\"");

        Assert.IsTrue(database.Schema.Tables.TryGetValue("robots_recovered", out TableSchema? schema));
        Assert.IsFalse(schema!.AutoStatsCollectionEnabled,
            "The auto-analyze opt-out must survive deferred drop + relink");
    }

    [Test]
    public async Task DisableDuringScanAbortsPublication()
    {
        CamusDBConfig.AutoAnalyzeMaxRowsPerSecond = 50;   // slow scan so the opt-out lands mid-scan
        CamusDBConfig.AutoAnalyzeOwnershipCheckRows = 25; // re-check the setting frequently

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();
        TableDescriptor table = await OpenTableAsync(database, "robots");
        await InsertRobotsAsync(executor, database, dbname, 300);

        // Start a slow background analyze, then opt the table out while it is still scanning.
        Task analyze = executor.RunBackgroundAnalyzeForTestsAsync(database, table, shouldPause: null, CancellationToken.None);
        await Task.Delay(500);
        await ExecDdlAsync(executor, dbname, "ALTER TABLE robots SET (sql_stats_automatic_collection_enabled = false)");

        Exception? thrown = null;
        try { await analyze; }
        catch (Exception ex) { thrown = ex; }

        Assert.IsNotNull(thrown, "A scan must abort once the table is opted out mid-scan");
        Assert.IsNull(executor.Statistics.GetColumnNdv(database, table, "year"),
            "No statistics may be published for a table disabled during its scan");
    }
}
