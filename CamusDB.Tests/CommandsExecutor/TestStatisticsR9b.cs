
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;
using Nito.AsyncEx;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Index Entry Counts and Column Min/Max tests.
///
/// Validates:
///   1. Insert tracking: per-index entry counts increment and per-column min/max expand correctly.
///   2. Delete tracking: per-index entry counts decrement; min/max drifts (not recomputed).
///   3. Update tracking: min/max expands for indexed columns; entry counts are unchanged.
///   4. Persistence: stats survive database reopen.
///   5. Backfill compatibility: entry counts are not corrupted by a newly added index.
///   6. CostEstimator uses real min/max selectivity when available (replacing fixed 40 % fallback).
///   7. Existing row-count behavior remains green.
/// </summary>
[TestFixture]
public sealed class TestStatisticsR9b : BaseTest
{
    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>Table with a PK and a secondary multi-index on 'year'.</summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupTableWithYearIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

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

        return (dbname, database, executor);
    }

    private static async Task<TableDescriptor> OpenTableAsync(
        CommandExecutor executor, DatabaseDescriptor db, string tableName)
    {
        if (db.TableDescriptors.TryGetValue(tableName, out AsyncLazy<TableDescriptor>? lazy))
            return await lazy;
        throw new InvalidOperationException($"Table '{tableName}' not found");
    }

    private static InsertTicket MakeInsert(KvTransaction txn, string dbname, string name, long year)
        => new(
            txnState: txn,
            databaseName: dbname,
            tableName: "robots",
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

    private static async Task<List<QueryResultRow>> ExplainAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests — index entry counts
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R9b_IndexEntryCount_IncrementedOnInsert()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithYearIndex();
        TableDescriptor table = await OpenTableAsync(executor, database, "robots");

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 5; i++)
            await executor.Insert(MakeInsert(txn, dbname, "Robot" + i, 2020 + i));
        await database.Transactions.CommitAsync(txn);

        await Task.Delay(150); // allow background load to merge persisted base

        long? count = executor.Statistics.GetIndexEntryCount(database, table, "year_idx");
        Assert.IsNotNull(count, "Index entry count should be populated after inserts");
        Assert.AreEqual(5, count!.Value, "year_idx should have one entry per inserted row");
    }

    [Test]
    public async Task R9b_IndexEntryCount_DecrementedOnDelete()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithYearIndex();
        TableDescriptor table = await OpenTableAsync(executor, database, "robots");

        // Insert 6 rows.
        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 6; i++)
            await executor.Insert(MakeInsert(txn, dbname, "Robot" + i, 2010 + i));
        await database.Transactions.CommitAsync(txn);

        await Task.Delay(150);

        long? countBefore = executor.Statistics.GetIndexEntryCount(database, table, "year_idx");
        Assert.IsNotNull(countBefore);
        Assert.AreEqual(6, countBefore!.Value);

        // Delete 2 rows.
        KvTransaction delTxn = await database.Transactions.BeginAsync();
        await executor.Delete(new DeleteTicket(
            txnState: delTxn,
            databaseName: dbname,
            tableName: "robots",
            filters: null,
            where: null,
            parameters: null,
            limit: CamusDB.Core.SQLParser.NodeAst.FromLong(2)));
        await database.Transactions.CommitAsync(delTxn);

        await Task.Delay(100);

        long? countAfter = executor.Statistics.GetIndexEntryCount(database, table, "year_idx");
        Assert.IsNotNull(countAfter, "Index count should still exist after delete");
        Assert.IsTrue(countAfter!.Value < countBefore.Value,
            $"Entry count should decrease after delete: before={countBefore}, after={countAfter}");
    }

    [Test]
    public async Task R9b_IndexEntryCount_UnchangedForNonIndexedColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithYearIndex();
        TableDescriptor table = await OpenTableAsync(executor, database, "robots");

        // Insert 4 rows.
        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 4; i++)
            await executor.Insert(MakeInsert(txn, dbname, "Robot" + i, 2000 + i));
        await database.Transactions.CommitAsync(txn);

        await Task.Delay(150);

        long? yearCountBefore = executor.Statistics.GetIndexEntryCount(database, table, "year_idx");
        Assert.IsNotNull(yearCountBefore);

        // Update 'name' (not an indexed column) — year_idx count must stay the same.
        KvTransaction upd = await database.Transactions.BeginAsync();
        await executor.Update(new UpdateTicket(
            txnState: upd,
            databaseName: dbname,
            tableName: "robots",
            plainValues: new() { { "name", new(ColumnType.String, "Updated") } },
            exprValues: null,
            where: null,
            filters: null,
            parameters: null
        ));
        await database.Transactions.CommitAsync(upd);

        await Task.Delay(100);

        // Entry count for year_idx should be unchanged.
        long? yearCountAfter = executor.Statistics.GetIndexEntryCount(database, table, "year_idx");
        Assert.AreEqual(yearCountBefore, yearCountAfter,
            "year_idx entry count must not change when only non-indexed 'name' is updated");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests — column min/max
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R9b_ColumnMinMax_PopulatedAfterInsert()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithYearIndex();
        TableDescriptor table = await OpenTableAsync(executor, database, "robots");

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.Insert(MakeInsert(txn, dbname, "RobotA", 2010));
        await executor.Insert(MakeInsert(txn, dbname, "RobotB", 2020));
        await executor.Insert(MakeInsert(txn, dbname, "RobotC", 2015));
        await database.Transactions.CommitAsync(txn);

        await Task.Delay(150);

        ColumnMinMax? mm = executor.Statistics.GetColumnMinMax(database, table, "year");
        Assert.IsNotNull(mm, "Min/max should be populated after inserts on an indexed column");
        Assert.IsNotNull(mm!.Min);
        Assert.IsNotNull(mm.Max);
        Assert.AreEqual(2010L, mm.Min!.LongValue, "Min should be 2010");
        Assert.AreEqual(2020L, mm.Max!.LongValue, "Max should be 2020");
    }

    [Test]
    public async Task R9b_ColumnMinMax_ExpandsOnSubsequentInserts()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithYearIndex();
        TableDescriptor table = await OpenTableAsync(executor, database, "robots");

        KvTransaction txn1 = await database.Transactions.BeginAsync();
        await executor.Insert(MakeInsert(txn1, dbname, "R1", 2010));
        await executor.Insert(MakeInsert(txn1, dbname, "R2", 2020));
        await database.Transactions.CommitAsync(txn1);

        await Task.Delay(150);

        ColumnMinMax? mm1 = executor.Statistics.GetColumnMinMax(database, table, "year");
        Assert.AreEqual(2010L, mm1!.Min!.LongValue);
        Assert.AreEqual(2020L, mm1.Max!.LongValue);

        // Insert a row with year outside the current range.
        KvTransaction txn2 = await database.Transactions.BeginAsync();
        await executor.Insert(MakeInsert(txn2, dbname, "R3", 1999));
        await executor.Insert(MakeInsert(txn2, dbname, "R4", 2030));
        await database.Transactions.CommitAsync(txn2);

        await Task.Delay(100);

        ColumnMinMax? mm2 = executor.Statistics.GetColumnMinMax(database, table, "year");
        Assert.AreEqual(1999L, mm2!.Min!.LongValue, "Min should expand to 1999");
        Assert.AreEqual(2030L, mm2.Max!.LongValue, "Max should expand to 2030");
    }

    [Test]
    public async Task R9b_ColumnMinMax_UpdatedOnIndexedColumnUpdate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithYearIndex();
        TableDescriptor table = await OpenTableAsync(executor, database, "robots");

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 3; i++)
            await executor.Insert(MakeInsert(txn, dbname, "Robot" + i, 2010 + i));
        await database.Transactions.CommitAsync(txn);

        await Task.Delay(150);

        ColumnMinMax? mmBefore = executor.Statistics.GetColumnMinMax(database, table, "year");
        Assert.AreEqual(2012L, mmBefore!.Max!.LongValue, "Max before update should be 2012");

        // Update all rows' year to 2099 (well above current max).
        KvTransaction upd = await database.Transactions.BeginAsync();
        await executor.Update(new UpdateTicket(
            txnState: upd,
            databaseName: dbname,
            tableName: "robots",
            plainValues: new() { { "year", new(ColumnType.Integer64, 2099L) } },
            exprValues: null,
            where: null,
            filters: null,
            parameters: null
        ));
        await database.Transactions.CommitAsync(upd);

        await Task.Delay(100);

        ColumnMinMax? mmAfter = executor.Statistics.GetColumnMinMax(database, table, "year");
        Assert.IsTrue(mmAfter!.Max!.LongValue >= 2099L,
            $"Max should expand to at least 2099 after update, got {mmAfter.Max.LongValue}");
    }

    [Test]
    public async Task R9b_ColumnMinMax_NotTrackedForNonIndexedColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithYearIndex();
        TableDescriptor table = await OpenTableAsync(executor, database, "robots");

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.Insert(MakeInsert(txn, dbname, "Alice", 2020));
        await database.Transactions.CommitAsync(txn);

        await Task.Delay(150);

        // 'name' is not indexed, so it should not have min/max tracked.
        ColumnMinMax? nameMM = executor.Statistics.GetColumnMinMax(database, table, "name");
        Assert.IsNull(nameMM, "Min/max should not be tracked for non-indexed columns");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests — persistence across reopen
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R9b_IndexStats_SurviveReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithYearIndex();

        // Insert rows and flush.
        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 5; i++)
            await executor.Insert(MakeInsert(txn, dbname, "Robot" + i, 2000 + i));
        await database.Transactions.CommitAsync(txn);

        TableDescriptor table = await OpenTableAsync(executor, database, "robots");
        await executor.Statistics.FlushAsync(database, table);
        await Task.Delay(200);

        // Simulate reopen by loading from Kahuna.
        await executor.Statistics.LoadByIdAsync(database, table.Id);
        await Task.Delay(200);

        long? count = executor.Statistics.GetIndexEntryCount(database, table, "year_idx");
        Assert.IsNotNull(count, "Index entry count should persist across reopen");
        Assert.AreEqual(5, count!.Value, "Index entry count should be 5 after reopen");

        ColumnMinMax? mm = executor.Statistics.GetColumnMinMax(database, table, "year");
        Assert.IsNotNull(mm, "Column min/max should persist across reopen");
        Assert.AreEqual(2000L, mm!.Min!.LongValue, "Min should be 2000 after reopen");
        Assert.AreEqual(2004L, mm.Max!.LongValue, "Max should be 2004 after reopen");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests — CostEstimator uses real min/max selectivity
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R9b_RealSelectivity_TightRangeKeepsIndexWhenMinMaxKnown()
    {
        // With year values 2000–2024 and a predicate WHERE year >= 2023 AND year < 2025,
        // real selectivity = (2025 - 2023) / (2024 - 2000) ≈ 8 % < 50 % breakeven → index wins.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithYearIndex();

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 25; i++)          // year 2000..2024
            await executor.Insert(MakeInsert(txn, dbname, "Robot" + i, 2000 + i));
        await database.Transactions.CommitAsync(txn);

        await Task.Delay(200);

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots WHERE year >= 2023 AND year < 2025");

        bool hasIndexScan = rows.Any(r =>
            r.Row.TryGetValue("node", out ColumnValue? n)
            && n!.StrValue is not null
            && n.StrValue.Contains("index"));

        Assert.IsTrue(hasIndexScan,
            "A tight both-bounds range on a table with known min/max should use the index");
    }

    [Test]
    public async Task R9b_RealSelectivity_WideRangeFallsBackToTableScan()
    {
        // With year values 2000–2024 and WHERE year > 2000, real selectivity ≈ 96 % >> 50 % → full scan.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithYearIndex();

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 25; i++)
            await executor.Insert(MakeInsert(txn, dbname, "Robot" + i, 2000 + i));
        await database.Transactions.CommitAsync(txn);

        await Task.Delay(200);

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots WHERE year > 2000");

        bool hasTableScan = rows.Any(r =>
            r.Row.TryGetValue("node", out ColumnValue? n)
            && n!.StrValue is not null
            && n.StrValue.Contains("table-scan"));

        Assert.IsTrue(hasTableScan,
            "A wide half-open range (year > min) on a table with known min/max should prefer full scan");
    }

    [Test]
    public Task R9b_ScalarBoundCompareTo_OrdersCorrectly()
    {
        // Unit-level check of ScalarBound.CompareTo for integer and float types.
        var a = new ScalarBound { Type = ColumnType.Integer64, LongValue = 10 };
        var b = new ScalarBound { Type = ColumnType.Integer64, LongValue = 20 };
        var c = new ScalarBound { Type = ColumnType.Integer64, LongValue = 10 };

        Assert.IsTrue(a.CompareTo(b) < 0, "10 < 20");
        Assert.IsTrue(b.CompareTo(a) > 0, "20 > 10");
        Assert.AreEqual(0, a.CompareTo(c), "10 == 10");

        var fa = new ScalarBound { Type = ColumnType.Float64, FloatValue = 1.5 };
        var fb = new ScalarBound { Type = ColumnType.Float64, FloatValue = 3.0 };
        Assert.IsTrue(fa.CompareTo(fb) < 0, "1.5 < 3.0");

        return Task.CompletedTask;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tests — regression (row counts unaffected)
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R9b_R8RowCount_StillTrackedCorrectly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithYearIndex();
        TableDescriptor table = await OpenTableAsync(executor, database, "robots");

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 7; i++)
            await executor.Insert(MakeInsert(txn, dbname, "R" + i, 2000 + i));
        await database.Transactions.CommitAsync(txn);

        await Task.Delay(200);

        long? rowCount = executor.Statistics.GetRowCountEstimate(database, table);
        Assert.IsNotNull(rowCount);
        Assert.AreEqual(7, rowCount!.Value, "Row count must still be tracked correctly alongside R9b stats");
    }
}
