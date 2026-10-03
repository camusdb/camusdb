/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Kahuna;
using Microsoft.Extensions.Logging;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.MicroBenchmarks;

/// <summary>
/// One embedded node and two tables, <c>cities(id, name UNIQUE)</c> and
/// <c>weather(id, city, temp)</c>. With a constraint, <c>weather.city</c> references
/// <c>cities.name</c> and the constraint owns an index on <c>city</c>; without one, a plain index on
/// <c>city</c> takes its place. The two shapes then differ only in the foreign-key checks, so a
/// difference between their numbers is the cost of the checks. The setup uses SQL only, so the
/// no-constraint classes also run on a build that predates foreign keys, for the baseline.
/// </summary>
internal sealed class ForeignKeyBenchHarness
{
    private static readonly ILoggerFactory LoggerFactory =
        Microsoft.Extensions.Logging.LoggerFactory.Create(b => b.AddFilter("*", LogLevel.Warning));

    private static readonly ILogger<ICamusDB> Logger = LoggerFactory.CreateLogger<ICamusDB>();

    /// <summary>The parents every child row references, in turn.</summary>
    public static readonly string[] ParentNames = ["lima", "quito", "bogota"];

    private EmbeddedKahuna node = null!;
    private DatabaseRegistry registry = null!;

    public CommandExecutor Executor { get; private set; } = null!;

    public DatabaseDescriptor Database { get; private set; } = null!;

    public string DbName { get; private set; } = null!;

    /// <summary>The next unused primary key, shared by both tables.</summary>
    public long NextId { get; set; } = 1_000_000;

    public async Task StartAsync(bool constraint)
    {
        node = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            NodeName = "fk-bench-node",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
        });

        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("fk-bench-warmup", CancellationToken.None);
        await node.FlushAsync();

        registry = await DatabaseRegistry.OpenAsync(node, CamusDBOptions.Default);

        Executor = new CommandExecutor(new CommandValidator(CamusDBOptions.Default), new CatalogsManager(Logger), Logger, CamusDBOptions.Default,
            sharedNode: node, registry: registry, isClusterMode: false);

        DbName = "fkbench" + Guid.NewGuid().ToString("N")[..12];
        CamusDBConfig.DataDirectory = Path.Combine(Path.GetTempPath(), "camusdb-fkbench-" + DbName);
        Directory.CreateDirectory(CamusDBConfig.DataDirectory);

        Database = await Executor.CreateDatabase(new CreateDatabaseTicket(DbName, ifNotExists: false));

        await DdlAsync("CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, population int64, UNIQUE KEY cities_name (name))");
        await DdlAsync(constraint
            ? "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string, temp int64, CONSTRAINT weather_city_fk FOREIGN KEY (city) REFERENCES cities (name))"
            : "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string, temp int64, KEY weather_city (city))");

        await ExecuteAsync("INSERT INTO cities (id, name, population) VALUES (1, 'lima', 0), (2, 'quito', 0), (3, 'bogota', 0)");
    }

    public async Task StopAsync()
    {
        await Executor.DisposeAsync();
        await registry.DisposeAsync();
        await node.DisposeAsync();
    }

    public async Task DdlAsync(string sql)
    {
        KvTransaction tx = await Database.Transactions.BeginAsync();
        await Executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, DbName, sql, parameters: null));
        await Database.Transactions.CommitAsync(tx);
    }

    /// <summary>One statement in its own transaction.</summary>
    public async Task ExecuteAsync(string sql)
    {
        KvTransaction tx = await Database.Transactions.BeginAsync();
        await Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, DbName, sql, parameters: null));
        await Database.Transactions.CommitAsync(tx);
    }

    /// <summary>A multi-row INSERT of <paramref name="rows"/> children with new ids, built before it is measured.</summary>
    public string ChildInsertSql(int rows)
    {
        StringBuilder sql = new("INSERT INTO weather (id, city, temp) VALUES ");

        for (int i = 0; i < rows; i++)
        {
            long id = NextId++;
            sql.Append(i == 0 ? "" : ", ")
               .Append('(').Append(id).Append(", '").Append(ParentNames[id % ParentNames.Length]).Append("', 20)");
        }

        return sql.ToString();
    }

    /// <summary>Inserts <paramref name="rows"/> children in statements of 500.</summary>
    public async Task SeedChildrenAsync(int rows)
    {
        for (int done = 0; done < rows; done += 500)
            await ExecuteAsync(ChildInsertSql(Math.Min(500, rows - done)));
    }
}

/// <summary>
/// A child INSERT of 1, 100 and 1000 rows that reference 3 parents. With the constraint, each statement
/// takes one rendezvous lock per distinct parent key and reads the parents in one batch. The table grows
/// as the benchmark runs, so each iteration is one invocation, and its SQL is built before it is timed.
/// </summary>
[SimpleJob(RunStrategy.Monitoring, warmupCount: 3, iterationCount: 20)]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public abstract class ChildInsertBenchmarks
{
    [Params(1, 100, 1000)]
    public int Rows { get; set; }

    private readonly ForeignKeyBenchHarness harness = new();
    private string sql = "";

    protected abstract bool Constraint { get; }

    [GlobalSetup]
    public void GlobalSetup() => harness.StartAsync(Constraint).GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup() => harness.StopAsync().GetAwaiter().GetResult();

    [IterationSetup]
    public void IterationSetup() => sql = harness.ChildInsertSql(Rows);

    [Benchmark]
    public Task InsertChildren() => harness.ExecuteAsync(sql);
}

public class ChildInsertNoConstraint : ChildInsertBenchmarks
{
    protected override bool Constraint => false;
}

public class ChildInsertWithConstraint : ChildInsertBenchmarks
{
    protected override bool Constraint => true;
}

/// <summary>
/// A parent DELETE of 1 and 100 rows. Each iteration inserts its own parents first, outside the
/// measurement, and deletes them in one statement. With the constraint, every removed key is probed in
/// the child index, which holds 1000 children of other parents, so a probe reads a populated index.
/// </summary>
[SimpleJob(RunStrategy.Monitoring, warmupCount: 3, iterationCount: 20)]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public abstract class ParentDeleteBenchmarks
{
    [Params(1, 100)]
    public int Rows { get; set; }

    private readonly ForeignKeyBenchHarness harness = new();
    private string sql = "";

    protected abstract bool Constraint { get; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        harness.StartAsync(Constraint).GetAwaiter().GetResult();
        harness.SeedChildrenAsync(1000).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup() => harness.StopAsync().GetAwaiter().GetResult();

    [IterationSetup]
    public void IterationSetup()
    {
        long first = harness.NextId;
        StringBuilder insert = new("INSERT INTO cities (id, name, population) VALUES ");

        for (int i = 0; i < Rows; i++)
        {
            long id = harness.NextId++;
            insert.Append(i == 0 ? "" : ", ").Append('(').Append(id).Append(", 'gone").Append(id).Append("', 0)");
        }

        harness.ExecuteAsync(insert.ToString()).GetAwaiter().GetResult();
        sql = $"DELETE FROM cities WHERE id >= {first}";
    }

    [Benchmark]
    public Task DeleteParents() => harness.ExecuteAsync(sql);
}

public class ParentDeleteNoConstraint : ParentDeleteBenchmarks
{
    protected override bool Constraint => false;
}

public class ParentDeleteWithConstraint : ParentDeleteBenchmarks
{
    protected override bool Constraint => true;
}

/// <summary>
/// An UPDATE of a column that no constraint uses, on 100 child rows and on the 3 parents. With the
/// constraint, both tables have one, and the statement must still do no foreign-key work.
/// </summary>
[SimpleJob(RunStrategy.Monitoring, warmupCount: 3, iterationCount: 20)]
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public abstract class UnrelatedUpdateBenchmarks
{
    private readonly ForeignKeyBenchHarness harness = new();
    private string childUpdate = "";

    protected abstract bool Constraint { get; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        harness.StartAsync(Constraint).GetAwaiter().GetResult();
        long first = harness.NextId;
        harness.SeedChildrenAsync(1000).GetAwaiter().GetResult();
        childUpdate = $"UPDATE weather SET temp = temp + 1 WHERE id >= {first} AND id < {first + 100}";
    }

    [GlobalCleanup]
    public void GlobalCleanup() => harness.StopAsync().GetAwaiter().GetResult();

    [Benchmark]
    public Task UpdateChildOtherColumn() => harness.ExecuteAsync(childUpdate);

    [Benchmark]
    public Task UpdateParentOtherColumn() => harness.ExecuteAsync("UPDATE cities SET population = population + 1 WHERE id <= 3");
}

public class UnrelatedUpdateNoConstraint : UnrelatedUpdateBenchmarks
{
    protected override bool Constraint => false;
}

public class UnrelatedUpdateWithConstraint : UnrelatedUpdateBenchmarks
{
    protected override bool Constraint => true;
}
