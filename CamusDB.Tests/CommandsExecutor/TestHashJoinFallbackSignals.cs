/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// With spill off, a hash join whose build side passes <c>hash_join_max_build_rows</c> runs as a
/// nested-loop join. Each such run adds one to <c>join.nested_loop_fallbacks</c> in
/// <c>SHOW ENGINE STATS</c> and writes a warning, at most one each minute for one engine. Every case
/// goes through SQL, on an engine whose logger the fixture reads.
/// </summary>
[NonParallelizable]
internal sealed class TestHashJoinFallbackSignals : BaseTest
{
    private const string JoinSql =
        "SELECT c.name, o.id FROM customers c JOIN orders o ON o.customer_id = c.id WHERE o.note <> 'secret-literal'";

    private const string WarningText = "ran as a nested-loop join";

    private readonly CapturingLogger capture = new();

    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, capture, options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
    }

    [SetUp]
    public void ClearTheLog() => capture.Clear();

    private static async Task Ddl(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Dml(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Query(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>The <c>join.*</c> rows of <c>SHOW ENGINE STATS</c>, which runs with no transaction.</summary>
    private static async Task<List<QueryResultRow>> JoinStats(CommandExecutor executor, string db)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(null!, db, "SHOW ENGINE STATS LIKE 'join.%'", null));
        return await cursor.ToListAsync();
    }

    private List<string> Warnings() => capture.Entries
        .Where(entry => entry.Level == LogLevel.Warning && entry.Message.Contains(WarningText))
        .Select(entry => entry.Message)
        .ToList();

    /// <summary>
    /// Four customers and four orders, with the planner held to a hash join. Each name and note
    /// starts with <paramref name="padLength"/> characters, so a test can make both sides wide.
    /// </summary>
    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync(CamusDBOptions options, int padLength = 0)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);
        string pad = new('x', padLength);

        await Ddl(executor, database, db, "CREATE TABLE customers (id INT64 PRIMARY KEY, name STRING)");
        await Ddl(executor, database, db, "CREATE TABLE orders (id INT64 PRIMARY KEY, customer_id INT64, note STRING)");
        await Dml(executor, database, db,
            $"INSERT INTO customers (id, name) VALUES (1, '{pad}ana'), (2, '{pad}bo'), (3, '{pad}cy'), (4, '{pad}di')");
        await Dml(executor, database, db,
            $"INSERT INTO orders (id, customer_id, note) VALUES (10, 1, '{pad}a'), (11, 1, '{pad}b'), (12, 2, '{pad}c'), (13, 3, '{pad}d')");

        executor.Statistics.ForceHashJoinForTesting = true;
        return (db, database, executor);
    }

    [Test]
    public async Task EachFallback_IsCounted_AndOneWarningIsWrittenInAMinute()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupAsync(Options with { SpillEnabled = false, HashJoinMaxBuildRows = 2 });

        Assert.That(await Query(executor, database, db, JoinSql), Has.Count.EqualTo(4));
        Assert.That(await Query(executor, database, db, JoinSql), Has.Count.EqualTo(4));

        List<QueryResultRow> stats = await JoinStats(executor, db);
        Assert.That(stats, Has.Count.EqualTo(1));
        Assert.That(stats[0].Row["source"].StrValue, Is.EqualTo("camusdb"));
        Assert.That(stats[0].Row["metric"].StrValue, Is.EqualTo("join.nested_loop_fallbacks"));
        Assert.That(stats[0].Row["kind"].StrValue, Is.EqualTo("counter"));
        Assert.That(stats[0].Row["count"].LongValue, Is.EqualTo(2));

        // The second fallback ran inside the minute of the first warning, so it wrote no line.
        List<string> warnings = Warnings();
        Assert.That(warnings, Has.Count.EqualTo(1), string.Join("\n", capture.Entries.Select(e => e.Message)));

        string warning = warnings[0];
        Assert.That(warning, Does.Contain(db)
            .And.Contain("hash_join_max_build_rows (2)")
            .And.Contain("spill_enabled")
            .And.Contain("previous warning: 0"));
        Assert.That(warning, Does.Contain("Right side of the join: orders").Or.Contain("Right side of the join: customers"));

        // A literal of the statement can be private data, so the warning holds none.
        Assert.That(warning, Does.Not.Contain("secret-literal"));
    }

    [Test]
    public async Task AFallbackAtTheByteLimit_NamesTheByteSetting()
    {
        // Rows of about 2 KB stop the build at its second row, far below the row limit.
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(
            Options with { SpillEnabled = false, HashJoinMaxBuildRows = 1000, HashJoinMaxBuildBytes = 1024 }, padLength: 1000);

        Assert.That(await Query(executor, database, db, JoinSql), Has.Count.EqualTo(4));

        Assert.That((await JoinStats(executor, db)).Single().Row["count"].LongValue, Is.EqualTo(1));

        string warning = Warnings().Single();
        Assert.That(warning, Does.Contain("hash_join_max_build_bytes (1024)").And.Not.Contain("hash_join_max_build_rows"));
    }

    [Test]
    public async Task AHashJoinUnderTheCap_IsNotCounted()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupAsync(Options with { SpillEnabled = false, HashJoinMaxBuildRows = 1000 });

        Assert.That(await Query(executor, database, db, JoinSql), Has.Count.EqualTo(4));

        // With spill off the fallback can run, so the row is there, and zero says it did not.
        List<QueryResultRow> stats = await JoinStats(executor, db);
        Assert.That(stats, Has.Count.EqualTo(1));
        Assert.That(stats[0].Row["count"].LongValue, Is.EqualTo(0));
        Assert.That(Warnings(), Is.Empty);
    }

    [Test]
    public async Task WithSpillOn_TheGraceHashJoinRuns_AndNothingIsReported()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupAsync(Options with { SpillEnabled = true, ForceSpillThresholdRows = 1 });

        Assert.That(await Query(executor, database, db, JoinSql), Has.Count.EqualTo(4));

        List<QueryResultRow> plan = await Query(executor, database, db, "EXPLAIN (ANALYZE) " + JoinSql);
        Assert.That(plan.Any(row => row.Row["detail"].StrValue?.Contains("ran=grace-hash") == true),
            "a build past the spill threshold goes to the Grace hash join");

        Assert.That(await JoinStats(executor, db), Is.Empty);
        Assert.That(Warnings(), Is.Empty);
    }

    /// <summary>Keeps the level and the formatted text of each log entry.</summary>
    private sealed class CapturingLogger : ILogger<ICamusDB>
    {
        private readonly List<(LogLevel Level, string Message)> entries = [];

        public List<(LogLevel Level, string Message)> Entries
        {
            get
            {
                lock (entries)
                    return [.. entries];
            }
        }

        public void Clear()
        {
            lock (entries)
                entries.Clear();
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (entries)
                entries.Add((logLevel, formatter(state, exception)));
        }
    }
}

/// <summary>
/// The count and the warning rate limit of <see cref="JoinFallbackMetrics"/>, with a clock the test
/// moves, and the row that <c>SHOW ENGINE STATS</c> reports for it.
/// </summary>
internal sealed class TestJoinFallbackMetrics
{
    [Test]
    public void TheFirstFallback_WritesAWarning()
    {
        JoinFallbackMetrics metrics = new(() => 5);

        Assert.That(metrics.NoteNestedLoopFallback(out long notLogged), Is.True);
        Assert.That(notLogged, Is.EqualTo(0));
        Assert.That(metrics.NestedLoopFallbacks, Is.EqualTo(1));
    }

    [Test]
    public void FallbacksInsideTheInterval_WriteNoWarning_AndTheNextWarningCountsThem()
    {
        long now = 1_000;
        JoinFallbackMetrics metrics = new(() => now);

        Assert.That(metrics.NoteNestedLoopFallback(out _), Is.True);

        now += 1;
        Assert.That(metrics.NoteNestedLoopFallback(out _), Is.False);

        now += JoinFallbackMetrics.LogIntervalMs - 2;
        Assert.That(metrics.NoteNestedLoopFallback(out _), Is.False);

        now += 1;
        Assert.That(metrics.NoteNestedLoopFallback(out long notLogged), Is.True);
        Assert.That(notLogged, Is.EqualTo(2));
        Assert.That(metrics.NestedLoopFallbacks, Is.EqualTo(4));

        now += 1;
        Assert.That(metrics.NoteNestedLoopFallback(out _), Is.False);
    }

    [Test]
    public void ThreadsInOneInterval_WriteOneWarning_AndCountEveryFallback()
    {
        long now = 0;
        JoinFallbackMetrics metrics = new(() => Interlocked.Read(ref now));
        int warnings = 0;
        long countedByTheFirstWarning = 0;

        Parallel.For(0, 8000, new ParallelOptions { MaxDegreeOfParallelism = 8 }, _ =>
        {
            if (!metrics.NoteNestedLoopFallback(out long notLoggedBefore))
                return;

            Interlocked.Increment(ref warnings);
            Interlocked.Add(ref countedByTheFirstWarning, notLoggedBefore);
        });

        Assert.That(warnings, Is.EqualTo(1));
        Assert.That(metrics.NestedLoopFallbacks, Is.EqualTo(8000));

        Interlocked.Exchange(ref now, JoinFallbackMetrics.LogIntervalMs);
        Assert.That(metrics.NoteNestedLoopFallback(out long notLogged), Is.True);

        // A thread that loses the interval can count before the thread that won it reads the count, so the
        // first warning can report some of them. Each one is reported once.
        Assert.That(countedByTheFirstWarning + notLogged, Is.EqualTo(7999));
    }

    [Test]
    public void TheEngineStatsRow_IsAbsentOnlyWithSpillOnAndNoFallback()
    {
        JoinFallbackMetrics metrics = new(() => 0);

        Assert.That(JoinFallbackMetricsReporter.Build(metrics, spillEnabled: true), Is.Null);

        EngineMetricRow zero = JoinFallbackMetricsReporter.Build(metrics, spillEnabled: false)!.Single();
        Assert.That(zero.Metric, Is.EqualTo("join.nested_loop_fallbacks"));
        Assert.That(zero.Count, Is.EqualTo(0));

        metrics.NoteNestedLoopFallback(out _);

        EngineMetricRow one = JoinFallbackMetricsReporter.Build(metrics, spillEnabled: true)!.Single();
        Assert.That(one.Kind, Is.EqualTo(EngineMetricKind.Counter));
        Assert.That(one.Count, Is.EqualTo(1));
        Assert.That(one.Total, Is.EqualTo(1));
    }
}
