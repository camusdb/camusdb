
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;
using Nito.AsyncEx;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Manager-level histogram persistence round-trips against a real Kahuna node.
///
/// The pure-model tests in <c>TestStatisticsHistogram</c> exercise the histogram math in
/// isolation; these tests close the gap by driving the actual load/flush paths:
///   1. A persisted histogram is re-merged when the entry is reloaded through the lazy
///      <c>GetRowCountEstimate</c> → background-load path (not only the explicit load-by-id path).
///   2. Writing histograms on an entry that has not been preloaded still persists — the writer
///      must load the entry first so the flush is not skipped on an unloaded entry.
///
/// Both reloads are forced with a cache-eviction seam so the assertion reads bytes that genuinely
/// came back from Kahuna rather than a value still resident in the in-memory cache.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestStatisticsHistogramPersistence : BaseTest
{
    private static ScalarBound Int(long v) => new() { Type = ColumnType.Integer64, LongValue = v };

    /// <summary>Two-bucket equi-depth histogram over a 'year' column: 80 rows ≤ 2010, 20 above.</summary>
    private static Dictionary<string, ColumnHistogram> BuildYearHistogram() => new()
    {
        ["year"] = new ColumnHistogram
        {
            TotalRows = 100,
            Buckets =
            [
                new() { UpperBound = Int(2010), CumulativeRows = 80,  DistinctInBucket = 11 },
                new() { UpperBound = Int(2030), CumulativeRows = 100, DistinctInBucket = 20 },
            ],
        },
    };

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

    private static async Task<TableDescriptor> OpenTableAsync(DatabaseDescriptor db, string tableName)
    {
        if (db.TableDescriptors.TryGetValue(tableName, out AsyncLazy<TableDescriptor>? lazy))
            return await lazy;
        throw new InvalidOperationException($"Table '{tableName}' not found");
    }

    private static async Task InsertRowsAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, int count, int baseYear)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < count; i++)
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
                        { "year", new(ColumnType.Integer64, (long)(baseYear + i)) },
                    }
                }));
        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>Kicks the lazy background load and polls until the reloaded histogram appears.</summary>
    private static async Task<ColumnHistogram?> WaitForReloadedHistogramAsync(
        CommandExecutor executor, DatabaseDescriptor database, TableDescriptor table, string column)
    {
        // GetRowCountEstimate fires LoadAndCacheAsync in the background on a cache miss.
        _ = executor.Statistics.GetRowCountEstimate(database, table);

        Stopwatch sw = Stopwatch.StartNew();
        while (sw.Elapsed < TimeSpan.FromSeconds(5))
        {
            ColumnHistogram? h = executor.Statistics.GetColumnHistogram(database, table, column);
            if (h is not null)
                return h;
            await Task.Delay(50);
        }
        return null;
    }

    [Test]
    public async Task HistogramReloadsThroughLazyLoadPath()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithYearIndex();
        await InsertRowsAsync(executor, database, dbname, count: 5, baseYear: 2008);
        TableDescriptor table = await OpenTableAsync(database, "robots");

        // Persist histograms synchronously via the ANALYZE-facing writer.
        await executor.Statistics.SetHistogramsAsync(database, table, BuildYearHistogram());

        // Drop the in-memory entry so the next access must reload from Kahuna.
        executor.Statistics.EvictForTesting(database, table);
        Assert.IsNull(executor.Statistics.GetColumnHistogram(database, table, "year"),
            "After eviction the histogram must not be resident in memory");

        // Reload through the lazy GetRowCountEstimate path (the one Bug 1 left unmerged).
        ColumnHistogram? reloaded = await WaitForReloadedHistogramAsync(executor, database, table, "year");

        Assert.IsNotNull(reloaded, "Histogram must be re-merged when the entry reloads via the lazy path");
        Assert.AreEqual(100, reloaded!.TotalRows);
        Assert.AreEqual(2, reloaded.Buckets.Count);
        Assert.AreEqual(2010L, reloaded.Buckets[0].UpperBound!.LongValue);
        Assert.AreEqual(80,    reloaded.Buckets[0].CumulativeRows);
        Assert.AreEqual(100,   reloaded.Buckets[1].CumulativeRows);
    }

    [Test]
    public async Task SetHistogramsPersistsWhenEntryNotPreloaded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithYearIndex();
        await InsertRowsAsync(executor, database, dbname, count: 4, baseYear: 2005);
        TableDescriptor table = await OpenTableAsync(database, "robots");

        // Force the "entry not loaded" precondition that Bug 2 mishandled: with no cached entry,
        // SetHistogramsAsync must load it first, otherwise the flush is silently skipped.
        executor.Statistics.EvictForTesting(database, table);
        await executor.Statistics.SetHistogramsAsync(database, table, BuildYearHistogram());

        // Evict again and reload from Kahuna to prove the write was actually durable.
        executor.Statistics.EvictForTesting(database, table);
        await executor.Statistics.LoadByIdAsync(database, table.Id);

        ColumnHistogram? reloaded = executor.Statistics.GetColumnHistogram(database, table, "year");
        Assert.IsNotNull(reloaded, "Histograms set on a non-preloaded entry must be persisted");
        Assert.AreEqual(100, reloaded!.TotalRows);
        Assert.AreEqual(2010L, reloaded.Buckets[0].UpperBound!.LongValue);
        Assert.AreEqual(80,    reloaded.Buckets[0].CumulativeRows);
    }
}
