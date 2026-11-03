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
using CamusDB.Tests;

namespace CamusDB.SqlFuzz.Tests.Engine;

/// <summary>
/// One in-memory, single-node CamusDB for the fuzzer: an embedded Kahuna node, a database registry
/// and the engine under test. Each fuzz round creates its own database on it
/// (<see cref="CreateDatabaseAsync"/>) and drops it after the round, so a round starts from an empty
/// catalog without the cost of a new node.
///
/// <para><b>Why the fuzzer does not use the main suite's <c>BaseTest</c>.</b> This project does not
/// reference <c>CamusDB.Tests</c>, so that the fuzzer stays out of the long main suite. It boots the
/// node with the same single-node timings (<c>TestNodeDefaults</c>, linked from the main suite).</para>
///
/// <para><b>Configuration is fixed when an engine is built.</b> A plan-equivalence arm that needs
/// another configuration is a second engine from <see cref="CreateArmAsync"/>, never a changed option
/// on the engine under test: an option set after construction does nothing, and the oracle then
/// compares a result with itself. An arm shares the node, the registry and so the stored data, but it
/// has its own executor, catalog, descriptor cache and planner state.</para>
///
/// <para><b>Background work is off.</b> <see cref="FuzzOptions"/> turns off auto-analyze, row TTL and
/// the result cache. Each of them can change data, statistics or a cached answer between the two
/// queries of one oracle check, and the oracle then reports a difference that no plan caused.</para>
/// </summary>
internal sealed class FuzzEngine : IAsyncDisposable
{
    private static readonly ILoggerFactory LoggerFactoryInstance = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Critical).AddFilter("Kahuna", LogLevel.Critical).AddFilter("Kommander", LogLevel.Critical).AddConsole());

    private readonly EmbeddedKahuna node;

    private readonly DatabaseRegistry registry;

    private readonly string dataDirectory;

    private readonly ILogger<ICamusDB> logger;

    private FuzzEngine(EmbeddedKahuna node, DatabaseRegistry registry, string dataDirectory,
        ILogger<ICamusDB> logger, CamusDBOptions options, CommandExecutor executor)
    {
        this.node = node;
        this.registry = registry;
        this.dataDirectory = dataDirectory;
        this.logger = logger;
        Options = options;
        Executor = executor;
    }

    /// <summary>The options the engine under test was built with.</summary>
    public CamusDBOptions Options { get; }

    /// <summary>The engine under test.</summary>
    public CommandExecutor Executor { get; }

    /// <summary>
    /// The options of every engine the fuzzer builds, from the shipped defaults. Only the background
    /// work that can change data, statistics or a cached result between two queries is turned off.
    /// </summary>
    public static CamusDBOptions FuzzOptions(string dataDirectory) => CamusDBOptions.Default with
    {
        DataDirectory = dataDirectory,
        AutoAnalyzeEnabled = false,
        TtlEnabled = false,
        QueryResultCacheEnabled = false,
    };

    /// <summary>Boots a node and builds the engine under test with <see cref="FuzzOptions"/>.</summary>
    public static async Task<FuzzEngine> StartAsync(Func<CamusDBOptions, CamusDBOptions>? configure = null)
    {
        string dataDirectory = Path.Combine(Path.GetTempPath(), "camusdb-sqlfuzz-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(dataDirectory);
        CamusDBConfig.DataDirectory = dataDirectory;

        CamusDBOptions options = FuzzOptions(dataDirectory);
        if (configure is not null)
            options = configure(options);

        EmbeddedKahuna node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            NodeName = $"sqlfuzz-{Guid.NewGuid():N}",
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
        return new FuzzEngine(node, registry, dataDirectory, logger, options, executor);
    }

    /// <summary>
    /// Creates an empty database on the engine under test. Its name starts with a letter, because a
    /// name that starts with a digit does not lex as an identifier in SQL text.
    /// </summary>
    public async Task<FuzzArm> CreateDatabaseAsync()
    {
        string databaseName = "fz_" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor database = await Executor.CreateDatabase(new CreateDatabaseTicket(databaseName, ifNotExists: false))
            .ConfigureAwait(false);
        return new FuzzArm("default", Executor, database, databaseName, OwnsExecutor: false);
    }

    /// <summary>Drops a database that <see cref="CreateDatabaseAsync"/> made. A failure is ignored: the node is in memory and goes away with the engine.</summary>
    public async Task DropDatabaseAsync(string databaseName)
    {
        try { await Executor.DropDatabase(new DropDatabaseTicket(databaseName, ifExists: true)).ConfigureAwait(false); }
        catch { }
    }

    /// <summary>
    /// A second engine on the same node and registry, with its own configuration. It opens
    /// <paramref name="databaseName"/> through its own descriptor, so the generated data is loaded only
    /// once. Open an arm after the schema, the data and any ANALYZE are in place: the arm reads the
    /// table schemas and the statistics when it opens a table. Release it with <see cref="ReleaseArmAsync"/>.
    /// </summary>
    /// <param name="prepare">Sets a test seam of the arm's own executor after it is built, for a path no option selects (a forced join operator).</param>
    public async Task<FuzzArm> CreateArmAsync(string databaseName, string name, Func<CamusDBOptions, CamusDBOptions> configure, Action<CommandExecutor>? prepare = null)
    {
        CamusDBOptions options = configure(FuzzOptions(dataDirectory));
        CommandExecutor executor = Build(options, node, registry, logger);
        try
        {
            prepare?.Invoke(executor);
            DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);
            return new FuzzArm(name, executor, database, databaseName, OwnsExecutor: true);
        }
        catch
        {
            await executor.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Closes the database on an arm from <see cref="CreateArmAsync"/> and disposes its executor. The engine under test is left open.</summary>
    public static async Task ReleaseArmAsync(FuzzArm arm)
    {
        if (!arm.OwnsExecutor)
            return;

        try { await arm.Executor.CloseDatabase(new CloseDatabaseTicket(arm.DatabaseName)).ConfigureAwait(false); } catch { }
        try { await arm.Executor.DisposeAsync().ConfigureAwait(false); } catch { }
    }

    private static CommandExecutor Build(CamusDBOptions options, EmbeddedKahuna node, DatabaseRegistry registry, ILogger<ICamusDB> logger) =>
        new(new CommandValidator(options), new CatalogsManager(logger), logger, options,
            sharedNode: node, registry: registry, isClusterMode: false);

    public async ValueTask DisposeAsync()
    {
        try { await Executor.DisposeAsync().ConfigureAwait(false); } catch { }
        try { await registry.DisposeAsync().ConfigureAwait(false); } catch { }
        try { await node.DisposeAsync().ConfigureAwait(false); } catch { }
        try { Directory.Delete(dataDirectory, recursive: true); } catch { }
    }
}

/// <summary>
/// One engine that the oracles can send statements to: the engine under test, or a plan-equivalence
/// arm with its own configuration. <see cref="Name"/> appears in a finding, so a report names the arm
/// that disagreed. <see cref="OwnsExecutor"/> is true for an arm that has its own executor, which
/// <see cref="FuzzEngine.ReleaseArmAsync"/> disposes.
/// </summary>
internal sealed record FuzzArm(string Name, CommandExecutor Executor, DatabaseDescriptor Database, string DatabaseName, bool OwnsExecutor);
