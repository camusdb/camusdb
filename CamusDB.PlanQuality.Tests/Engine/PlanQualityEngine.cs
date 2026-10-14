/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.Extensions.Logging;

using Kahuna;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Tests;

namespace CamusDB.PlanQuality.Tests.Engine;

/// <summary>
/// One in-memory, single-node CamusDB for the plan-quality harness: an embedded Kahuna node, a
/// database registry, the engine under test, and the one database that holds the data set.
///
/// <para><b>Why the harness does not use the main suite's <c>BaseTest</c>.</b> That base boots a
/// node and a database for each test method and drops both after it. The harness loads its data
/// set once and then runs a whole corpus on it, so it owns the lifetime itself. It boots the node
/// with the same single-node timings (<c>TestNodeDefaults</c>, linked from the main suite).</para>
///
/// <para><b>Configuration is fixed when an engine is built.</b> <see cref="HarnessOptions"/> turns
/// off the background work that would change data, statistics or plans while the corpus runs:
/// auto-analyze (its reservoir sample is seeded by the time of the run), row TTL, the result and
/// plan caches, and the primary-key row-id cache. That last cache lets a repeated primary-key
/// lookup skip its index read, so with it on the counters of a query depend on the queries that ran
/// before it. A second configuration, such as the reference engine, is a second engine from
/// <see cref="CreateEngineAsync"/>, never a changed option on this one.</para>
///
/// <para><b>The slow query log is on, with a threshold of 0.</b> It is the one place where the
/// engine counts the rows a whole statement read, its subqueries included
/// (<see cref="CamusDB.Core.Diagnostics.StatementProbe"/>). The plan nodes of <c>EXPLAIN (ANALYZE)</c>
/// do not see the reads of a correlated subquery, so <see cref="StatementRowsReadAsync"/> reads
/// the count from the log after each query.</para>
/// </summary>
internal sealed class PlanQualityEngine : IAsyncDisposable
{
    private static readonly ILoggerFactory LoggerFactoryInstance = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private readonly EmbeddedKahuna node;

    private readonly DatabaseRegistry registry;

    private readonly string dataDirectory;

    private readonly ILogger<ICamusDB> logger;

    private readonly List<CommandExecutor> engines = new();

    private PlanQualityEngine(EmbeddedKahuna node, DatabaseRegistry registry, string dataDirectory,
        ILogger<ICamusDB> logger, CommandExecutor executor, DatabaseDescriptor database, string databaseName)
    {
        this.node = node;
        this.registry = registry;
        this.dataDirectory = dataDirectory;
        this.logger = logger;
        Executor = executor;
        Database = database;
        DatabaseName = databaseName;
        engines.Add(executor);
    }

    /// <summary>The engine under test.</summary>
    public CommandExecutor Executor { get; }

    /// <summary>The database that holds the data set.</summary>
    public DatabaseDescriptor Database { get; }

    /// <summary>The name of <see cref="Database"/>. It starts with a letter, because a name that starts with a digit does not lex as an identifier in SQL text.</summary>
    public string DatabaseName { get; }

    /// <summary>
    /// The options of every engine the harness builds, from the shipped defaults. Only the
    /// background work that could change the measured plans between two runs is turned off.
    /// </summary>
    public static CamusDBOptions HarnessOptions(string dataDirectory) => CamusDBOptions.Default with
    {
        DataDirectory = dataDirectory,
        AutoAnalyzeEnabled = false,
        TtlEnabled = false,
        QueryResultCacheEnabled = false,
        PlanCacheEnabled = false,
        PointReadRowIdCacheEnabled = false,
        SlowQueryLogEnabled = true,
        SlowQueryLogThresholdMs = 0,
        SlowQueryLogMaxSqlLength = 65_536,
    };

    /// <summary>Boots a node, builds the engine under test with <see cref="HarnessOptions"/>, and creates an empty database.</summary>
    public static async Task<PlanQualityEngine> StartAsync(Func<CamusDBOptions, CamusDBOptions>? configure = null)
    {
        string dataDirectory = Path.Combine(Path.GetTempPath(), "camusdb-plan-quality-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(dataDirectory);
        CamusDBConfig.DataDirectory = dataDirectory;

        CamusDBOptions options = HarnessOptions(dataDirectory);
        if (configure is not null)
            options = configure(options);

        EmbeddedKahuna node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            NodeName = $"plan-quality-{Guid.NewGuid():N}",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
        }.WithTestNodeDefaults());

        await node.StartAsync(CancellationToken.None).ConfigureAwait(false);
        await node.WaitForLeaderAsync("warmup", CancellationToken.None).ConfigureAwait(false);
        await node.FlushAsync().ConfigureAwait(false);

        DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(node, options).ConfigureAwait(false);
        ILogger<ICamusDB> logger = LoggerFactoryInstance.CreateLogger<ICamusDB>();

        CommandExecutor executor = Build(options, node, registry, logger);

        string databaseName = "pq_" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(databaseName, ifNotExists: false))
            .ConfigureAwait(false);

        return new PlanQualityEngine(node, registry, dataDirectory, logger, executor, database, databaseName);
    }

    /// <summary>
    /// A second engine on the same node and registry, with its own configuration. It reads the same
    /// database through its own descriptor, so the data set is loaded only once. The harness uses it
    /// for the reference engine (both cost-based switches off) when it updates a baseline.
    /// </summary>
    public async Task<(CommandExecutor Executor, DatabaseDescriptor Database)> CreateEngineAsync(
        Func<CamusDBOptions, CamusDBOptions> configure)
    {
        CommandExecutor executor = Build(configure(HarnessOptions(dataDirectory)), node, registry, logger);
        engines.Add(executor);
        DatabaseDescriptor database = await executor.OpenDatabase(DatabaseName).ConfigureAwait(false);
        return (executor, database);
    }

    private static CommandExecutor Build(CamusDBOptions options, EmbeddedKahuna node, DatabaseRegistry registry, ILogger<ICamusDB> logger) =>
        new(new CommandValidator(options), new CatalogsManager(logger), logger, options,
            sharedNode: node, registry: registry, isClusterMode: false);

    /// <summary>
    /// Runs one DDL statement on the engine under test, in its own transaction. A view needs the
    /// transaction: its body is bound, and the bind pins the schema versions of the tables it reads.
    /// </summary>
    public async Task ExecuteDdlAsync(string sql)
    {
        KvTransaction tx = await Database.Transactions.BeginAsync().ConfigureAwait(false);
        await Executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, DatabaseName, sql, null)).ConfigureAwait(false);
        await Database.Transactions.CommitAsync(tx).ConfigureAwait(false);
    }

    /// <summary>
    /// Inserts one batch of rows in its own transaction. A column left out of a row is NULL.
    ///
    /// <para>A commit that the coordinator aborts (<see cref="CamusDBErrorCodes.TransactionConflict"/>)
    /// is a definite non-commit, so the batch runs again from a new transaction, as a client does.
    /// A large load can meet it: in the JOB tier at scale 0.01 one batch commit was aborted after an
    /// 11-minute load. Any other error, and an abort that repeats past the attempt limit, fails the load.</para>
    /// </summary>
    public async Task InsertAsync(string table, List<Dictionary<string, ColumnValue>> rows)
    {
        const int maxAttempts = 5;

        for (int attempt = 1; ; attempt++)
        {
            KvTransaction tx = await Database.Transactions.BeginAsync().ConfigureAwait(false);
            try
            {
                await Executor.Insert(new InsertTicket(tx, DatabaseName, table, rows)).ConfigureAwait(false);
                await Database.Transactions.CommitAsync(tx).ConfigureAwait(false);
                return;
            }
            catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.TransactionConflict && attempt < maxAttempts)
            {
                // The commit path already rolled the transaction back; an abort before the commit
                // leaves it open, and a rollback of a finished transaction is a no-op.
                try { await Database.Transactions.RollbackAsync(tx).ConfigureAwait(false); } catch { }
            }
        }
    }

    /// <summary>Runs <c>ANALYZE</c> on one table. The explicit command reads the table in key order, so the same rows give the same statistics.</summary>
    public async Task AnalyzeAsync(string table)
    {
        KvTransaction tx = await Database.Transactions.BeginAsync().ConfigureAwait(false);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, DatabaseName, "ANALYZE " + table, null)).ConfigureAwait(false);
        await foreach (QueryResultRow _ in cursor.ConfigureAwait(false)) { }
        await Database.Transactions.CommitAsync(tx).ConfigureAwait(false);
    }

    /// <summary>Runs a read statement on the engine under test, in a read-only transaction, and returns every row.</summary>
    public Task<List<QueryResultRow>> QueryAsync(string sql, CancellationToken cancellationToken) =>
        QueryAsync(Executor, DatabaseName, sql, cancellationToken);

    /// <summary>Runs a read statement on <paramref name="executor"/>, in a read-only transaction, and returns every row.</summary>
    public static async Task<List<QueryResultRow>> QueryAsync(
        CommandExecutor executor, string databaseName, string sql, CancellationToken cancellationToken)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), databaseName, sql, null, cancellationToken: cancellationToken))
            .ConfigureAwait(false);

        List<QueryResultRow> rows = new();
        await foreach (QueryResultRow row in cursor.WithCancellation(cancellationToken).ConfigureAwait(false))
            rows.Add(row);
        return rows;
    }

    /// <summary>
    /// The rows that the last run of <paramref name="sql"/> read from storage, as the slow query log
    /// recorded them, or null when the log has no entry for it. The log holds the statement text as
    /// the client sent it, so the match is on the exact text and the newest entry wins.
    /// </summary>
    public async Task<long?> StatementRowsReadAsync(string sql, CancellationToken cancellationToken)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(null!, DatabaseName, "SHOW SLOW QUERIES", null, cancellationToken: cancellationToken))
            .ConfigureAwait(false);

        long bestSequence = long.MinValue;
        long? rowsRead = null;
        await foreach (QueryResultRow row in cursor.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (row.Row["sql"].StrValue != sql || row.Row["truncated"].BoolValue)
                continue;

            long sequence = row.Row["seq"].LongValue;
            if (sequence > bestSequence)
            {
                bestSequence = sequence;
                rowsRead = row.Row["rows_read"].LongValue;
            }
        }

        return rowsRead;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (CommandExecutor executor in engines)
        {
            try { await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName)).ConfigureAwait(false); } catch { }
            try { await executor.DisposeAsync().ConfigureAwait(false); } catch { }
        }

        try { await registry.DisposeAsync().ConfigureAwait(false); } catch { }
        try { await node.DisposeAsync().ConfigureAwait(false); } catch { }

        try { Directory.Delete(dataDirectory, recursive: true); } catch { }
    }
}
