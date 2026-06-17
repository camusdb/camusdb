
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading.Tasks;
using System.Collections.Generic;

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
/// R8 — Lightweight Table Statistics tests.
/// </summary>
[TestFixture]
public sealed class TestStatisticsManager : BaseTest
{
    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTableAsync(
        string tableName = "robots")
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: tableName,
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(tableTicket);
        return (dbname, database, executor);
    }

    private static InsertTicket MakeInsertTicket(KvTransaction txn, string dbname, string tableName, string name, long year)
        => new(
            txnState: txn,
            databaseName: dbname,
            tableName: tableName,
            values: new()
            {
                new()
                {
                    { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, name) },
                    { "year", new(ColumnType.Integer64, year) },
                }
            }
        );

    private static async Task<TableDescriptor> GetTableDescriptorAsync(
        CommandExecutor executor, string dbname, string tableName)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);

        if (db.TableDescriptors.TryGetValue(tableName, out AsyncLazy<TableDescriptor>? lazy))
            return await lazy;

        throw new InvalidOperationException($"Table '{tableName}' not found in '{dbname}'");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R8_InsertTracksRowCount()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 5; i++)
            await executor.Insert(MakeInsertTicket(txn, dbname, "robots", "R" + i, 2020 + i));
        await database.Transactions.CommitAsync(txn);

        await Task.Delay(50);

        TableDescriptor table = await GetTableDescriptorAsync(executor, dbname, "robots");
        long? estimate = executor.Statistics.GetRowCountEstimate(database, table);

        Assert.IsNotNull(estimate, "Expected a row-count estimate after inserts");
        Assert.AreEqual(5L, estimate!.Value);
    }

    [Test]
    public async Task R8_DeleteReducesRowCount()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 6; i++)
            await executor.Insert(MakeInsertTicket(txn, dbname, "robots", "R" + i, 2020 + i));
        await database.Transactions.CommitAsync(txn);

        KvTransaction deleteTxn = await database.Transactions.BeginAsync();
        await executor.Delete(new DeleteTicket(
            txnState: deleteTxn,
            databaseName: dbname,
            tableName: "robots",
            where: null,
            filters: new List<QueryFilter>
            {
                new("year", "=", new ColumnValue(ColumnType.Integer64, 2020L))
            }
        ));

        await Task.Delay(50);

        TableDescriptor table = await GetTableDescriptorAsync(executor, dbname, "robots");
        long? estimate = executor.Statistics.GetRowCountEstimate(database, table);

        Assert.IsNotNull(estimate);
        Assert.AreEqual(5L, estimate!.Value);
    }

    [Test]
    public async Task R8_NoStatsReturnedBeforeAnyDml_DoesNotThrow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        TableDescriptor table = await GetTableDescriptorAsync(executor, dbname, "robots");

        // No inserts — stats not yet loaded or tracked. Must not throw.
        long? estimate = executor.Statistics.GetRowCountEstimate(database, table);

        // Either null (not loaded yet) or 0 — both are acceptable.
        Assert.IsTrue(estimate is null or >= 0, "Estimate must be null or non-negative before any DML");
    }

    [Test]
    public async Task R8_RowCountNeverGoesNegative()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.Insert(MakeInsertTicket(txn, dbname, "robots", "Solo", 2024));
        await database.Transactions.CommitAsync(txn);

        KvTransaction del1 = await database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        await executor.Delete(new DeleteTicket(
            txnState: del1,
            databaseName: dbname,
            tableName: "robots",
            where: null,
            filters: new List<QueryFilter>
            {
                new("name", "=", new ColumnValue(ColumnType.String, "Solo"))
            }
        ));

        // Delete again — nothing left; delta = 0, count stays 0.
        KvTransaction del2 = await database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        await executor.Delete(new DeleteTicket(
            txnState: del2,
            databaseName: dbname,
            tableName: "robots",
            where: null,
            filters: new List<QueryFilter>
            {
                new("name", "=", new ColumnValue(ColumnType.String, "Solo"))
            }
        ));

        await Task.Delay(50);

        TableDescriptor table = await GetTableDescriptorAsync(executor, dbname, "robots");
        long? estimate = executor.Statistics.GetRowCountEstimate(database, table);

        Assert.IsTrue(estimate is null or >= 0, "Row count must never be negative");
    }

    [Test]
    public async Task R8_StatsPersistedAndReloadedAfterReopen()
    {
        // Phase 1: create table, insert 10 rows, flush, close.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync("robots2");

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 10; i++)
            await executor.Insert(MakeInsertTicket(txn, dbname, "robots2", "R" + i, 2020L + i));
        await database.Transactions.CommitAsync(txn);

        // Explicitly flush stats before closing so persistence is guaranteed.
        TableDescriptor tableBeforeClose = await GetTableDescriptorAsync(executor, dbname, "robots2");
        await executor.Statistics.FlushAsync(database, tableBeforeClose);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));

        // Phase 2: reopen with a fresh executor (empty in-memory cache).
        CommandExecutor executor2 = CreateCommandExecutor();
        TrackDatabase(dbname, executor2);
        DatabaseDescriptor db2 = await executor2.OpenDatabase(dbname);

        TableDescriptor table2 = await executor2.OpenTable(new OpenTableTicket(dbname, "robots2"))
            .WaitAsync(TimeSpan.FromSeconds(10));

        await executor2.Statistics.LoadByIdAsync(db2, table2.Id);

        long? estimate = executor2.Statistics.GetRowCountEstimate(db2, table2);
        Assert.IsNotNull(estimate, "Expected persisted stats to be reloaded after database reopen");
        Assert.AreEqual(10L, estimate!.Value);
    }

    [Test]
    public async Task R8_CloseHookFlushesStatsSoReopenSeesLatestCount()
    {
        // Insert 8 rows, then close WITHOUT an explicit FlushAsync.
        // The close-hook in CommandExecutor.CloseDatabase should flush for us.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync("ch_robots");

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 8; i++)
            await executor.Insert(MakeInsertTicket(txn, dbname, "ch_robots", "R" + i, 2020L + i));
        await database.Transactions.CommitAsync(txn);

        // Allow the background load (triggered by first DML) to complete so stats are Loaded=true.
        // Without this, FlushInternalAsync guards against flushing pre-load entries.
        await Task.Delay(200);

        // Close — no explicit FlushAsync; the close-hook must do it.
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));

        // Reopen and verify the persisted count is 8.
        CommandExecutor executor2 = CreateCommandExecutor();
        TrackDatabase(dbname, executor2);
        DatabaseDescriptor db2 = await executor2.OpenDatabase(dbname);

        TableDescriptor table2 = await executor2.OpenTable(new OpenTableTicket(dbname, "ch_robots"))
            .WaitAsync(TimeSpan.FromSeconds(10));

        await executor2.Statistics.LoadByIdAsync(db2, table2.Id);

        long? estimate = executor2.Statistics.GetRowCountEstimate(db2, table2);
        Assert.IsNotNull(estimate, "Close hook should have persisted stats so reopen finds them");
        Assert.AreEqual(8L, estimate!.Value);
    }

    [Test]
    public async Task R8_DmlBeforeLoadMergesBaseNotClobbers()
    {
        // Arrange: create table, insert 10 rows, flush, close.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync("robots3");

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 10; i++)
            await executor.Insert(MakeInsertTicket(txn, dbname, "robots3", "R" + i, 2020L + i));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor tbl = await GetTableDescriptorAsync(executor, dbname, "robots3");
        await executor.Statistics.FlushAsync(database, tbl);
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));

        // Act: reopen, then do 2 more inserts BEFORE any GetRowCountEstimate call.
        // This exercises the DML-before-load path.
        CommandExecutor executor2 = CreateCommandExecutor();
        TrackDatabase(dbname, executor2);
        DatabaseDescriptor db2 = await executor2.OpenDatabase(dbname);

        // These inserts will create cache entries with Loaded=false, pending delta = 2.
        KvTransaction txn2 = await db2.Transactions.BeginAsync();
        await executor2.Insert(MakeInsertTicket(txn2, dbname, "robots3", "Extra1", 2030));
        await executor2.Insert(MakeInsertTicket(txn2, dbname, "robots3", "Extra2", 2031));
        await db2.Transactions.CommitAsync(txn2);

        // Now load the base from Kahuna. Should merge: 10 (base) + 2 (delta) = 12.
        // OpenTable resolves the lazy descriptor that was populated by the inserts above.
        TableDescriptor table2 = await executor2.OpenTable(new OpenTableTicket(dbname, "robots3"))
            .WaitAsync(TimeSpan.FromSeconds(10));
        await executor2.Statistics.LoadByIdAsync(db2, table2.Id);

        long? estimate = executor2.Statistics.GetRowCountEstimate(db2, table2);
        Assert.IsNotNull(estimate, "Estimate should be available after load");
        Assert.AreEqual(12L, estimate!.Value, "Base (10) + pending delta (2) should be 12, not 2");
    }
}
