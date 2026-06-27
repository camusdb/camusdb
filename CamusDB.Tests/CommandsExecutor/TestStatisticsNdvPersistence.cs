
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
using CamusDB.Core.Statistics;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Manager-level NDV persistence round-trips against a real Kahuna node.
///
/// The pure-model tests in <c>TestStatisticsNdv</c> exercise JSON serialization in isolation.
/// These tests close the gap by driving the actual load/flush paths:
///   1. Persisted NDV is re-merged when the entry is reloaded through the lazy
///      <c>GetRowCountEstimate</c> → background-load path — the LoadAndCacheAsync path that
///      MergeNdv must be present in, exactly analogous to the Bug-1 histogram fix.
///   2. Writing NDV on an entry that has not been preloaded still persists — SetNdvAsync
///      must load the entry first so the flush is not skipped on an unloaded entry, analogous
///      to the Bug-2 histogram fix.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestStatisticsNdvPersistence : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupTableWithCompositeIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",     ColumnType.Id),
                new("city",   ColumnType.String, notNull: true),
                new("year",   ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",          new ColumnIndexInfo[] { new("id",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "city_year_idx", new ColumnIndexInfo[] { new("city", OrderType.Ascending), new("year", OrderType.Ascending) }),
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
        CommandExecutor executor, DatabaseDescriptor database, string dbname, int count)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        string[] cities = ["London", "Paris", "Berlin", "Tokyo", "Sydney"];
        for (int i = 0; i < count; i++)
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id,       ObjectIdGenerator.Generate().ToString()) },
                        { "city", new(ColumnType.String,    cities[i % cities.Length]) },
                        { "year", new(ColumnType.Integer64, (long)(2010 + i % 15)) },
                    }
                }));
        await database.Transactions.CommitAsync(txn);
    }

    private static Dictionary<string, long> BuildColumnNdv() => new()
    {
        ["city"] = 5,
        ["year"] = 15,
    };

    private static Dictionary<string, long> BuildKeyNdv()
    {
        string sig = StatisticsManager.KeyTupleSignature(["city", "year"]);
        return new Dictionary<string, long> { [sig] = 40 };
    }

    /// <summary>Kicks the lazy background load and polls until the reloaded column NDV appears.</summary>
    private static async Task<long?> WaitForReloadedColumnNdvAsync(
        CommandExecutor executor, DatabaseDescriptor database, TableDescriptor table, string column)
    {
        _ = executor.Statistics.GetRowCountEstimate(database, table);

        Stopwatch sw = Stopwatch.StartNew();
        while (sw.Elapsed < TimeSpan.FromSeconds(5))
        {
            long? ndv = executor.Statistics.GetColumnNdv(database, table, column);
            if (ndv is not null)
                return ndv;
            await Task.Delay(50);
        }
        return null;
    }

    [Test]
    public async Task NdvReloadsThroughLazyLoadPath()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithCompositeIndex();
        await InsertRowsAsync(executor, database, dbname, count: 10);
        TableDescriptor table = await OpenTableAsync(database, "robots");

        string keySig = StatisticsManager.KeyTupleSignature(["city", "year"]);

        await executor.Statistics.SetNdvAsync(database, table, BuildColumnNdv(), BuildKeyNdv());

        // Evict so the next access must reload from Kahuna.
        executor.Statistics.EvictForTesting(database, table);
        Assert.IsNull(executor.Statistics.GetColumnNdv(database, table, "city"),
            "After eviction column NDV must not be resident");
        Assert.IsNull(executor.Statistics.GetKeyNdv(database, table, ["city", "year"]),
            "After eviction key NDV must not be resident");

        // Reload through the lazy path — the one that MergeNdv must be wired into.
        long? reloadedCity = await WaitForReloadedColumnNdvAsync(executor, database, table, "city");

        Assert.IsNotNull(reloadedCity, "ColumnNdv must be re-merged through the lazy load path");
        Assert.AreEqual(5L, reloadedCity);

        long? reloadedYear = executor.Statistics.GetColumnNdv(database, table, "year");
        Assert.AreEqual(15L, reloadedYear);

        long? reloadedKey = executor.Statistics.GetKeyNdv(database, table, ["city", "year"]);
        Assert.IsNotNull(reloadedKey, "KeyNdv must be re-merged through the lazy load path");
        Assert.AreEqual(40L, reloadedKey);
    }

    [Test]
    public async Task SetNdvPersistsWhenEntryNotPreloaded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithCompositeIndex();
        await InsertRowsAsync(executor, database, dbname, count: 8);
        TableDescriptor table = await OpenTableAsync(database, "robots");

        string keySig = StatisticsManager.KeyTupleSignature(["city", "year"]);

        // Force the "entry not loaded" precondition: with no cached entry, SetNdvAsync must
        // load it first so the flush is not skipped on an unloaded entry.
        executor.Statistics.EvictForTesting(database, table);
        await executor.Statistics.SetNdvAsync(database, table, BuildColumnNdv(), BuildKeyNdv());

        // Evict again and reload from Kahuna to confirm the write was actually durable.
        executor.Statistics.EvictForTesting(database, table);
        await executor.Statistics.LoadByIdAsync(database, table.Id);

        long? cityNdv = executor.Statistics.GetColumnNdv(database, table, "city");
        long? yearNdv = executor.Statistics.GetColumnNdv(database, table, "year");
        long? keyNdv  = executor.Statistics.GetKeyNdv(database, table, ["city", "year"]);

        Assert.IsNotNull(cityNdv, "ColumnNdv[city] set on a non-preloaded entry must be persisted");
        Assert.AreEqual(5L,  cityNdv);
        Assert.AreEqual(15L, yearNdv);
        Assert.IsNotNull(keyNdv, "KeyNdv set on a non-preloaded entry must be persisted");
        Assert.AreEqual(40L, keyNdv);
    }
}
