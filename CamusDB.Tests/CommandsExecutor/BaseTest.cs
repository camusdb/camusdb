
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using Kahuna;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

public abstract class BaseTest
{
    // One logger factory for the entire test run — avoids spawning a ConsoleLoggerProvider
    // thread per fixture, which previously caused OutOfMemoryException after ~166 tests.
    protected static readonly ILoggerFactory SharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    protected readonly ILogger<ICamusDB> logger;

    // Per-test isolated directory, shared Kahuna node, and registry.
    private string? tempDir;
    private EmbeddedKahuna? testNode;
    protected DatabaseRegistry? sharedRegistry;

    // Databases opened in the current test method — closed in TearDown.
    private readonly List<(string dbname, CommandExecutor executor)> openDatabases = new();

    protected BaseTest()
    {
        logger = SharedLoggerFactory.CreateLogger<ICamusDB>();
    }

    /// <summary>
    /// Creates a fresh per-test temp directory and a <see cref="DatabaseRegistry"/> for it.
    /// Each test gets an isolated DataDirectory so concurrent SQLite nodes never collide.
    /// </summary>
    /// <summary>
    /// When true (the default), <see cref="SetUpTestEnvironment"/> starts a per-test
    /// in-memory Kahuna node and makes it available via <see cref="TestNode"/>.
    /// Subclasses that manage their own class-level node should override this to false
    /// to avoid spinning up a second, unused node per test method.
    /// </summary>
    protected virtual bool NeedsPerTestNode => true;

    /// <summary>
    /// Raft partitions the per-test node starts with. One is enough for everything that routes by
    /// hash, which is why it is the default: a table's whole key space hashes onto a single partition
    /// either way, and extra partitions only cost startup time.
    ///
    /// <para>A fixture exercising key-range routing raises this. Under that mode a key space is
    /// divided into ranges that are meant to be served by <em>different</em> partitions, so with a
    /// single one there is nowhere for a range to move to and the behavior under test cannot
    /// occur.</para>
    /// </summary>
    protected virtual int NodeInitialPartitions => 1;

    /// <summary>The per-test in-memory node, available after <see cref="SetUpTestEnvironment"/>.</summary>
    protected EmbeddedKahuna? TestNode => testNode;

    [SetUp]
    public async Task SetUpTestEnvironment()
    {
        tempDir = Path.Combine(Path.GetTempPath(), "camusdb-test-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(tempDir);
        CamusConfig.DataDirectory = tempDir;

        if (NeedsPerTestNode)
        {
            testNode = new EmbeddedKahuna(new EmbeddedKahunaOptions
            {
                ReadIOThreads = 1,
                WriteIOThreads = 1,
                NodeName = $"test-{Guid.NewGuid():N}",
                Storage = "memory",
                WalStorage = "memory",
                InitialPartitions = NodeInitialPartitions
            }.WithTestNodeDefaults());
            await testNode.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await testNode.WaitForLeaderAsync("warmup", CancellationToken.None).ConfigureAwait(false);
            await testNode.FlushAsync().ConfigureAwait(false);
        }

        sharedRegistry = await CreateRegistryAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Creates the <see cref="DatabaseRegistry"/> for the current test.
    /// Override in shared-node fixtures to use a different node.
    /// </summary>
    protected virtual Task<DatabaseRegistry> CreateRegistryAsync()
        => DatabaseRegistry.OpenAsync(testNode!, Options);

    /// <summary>
    /// Builds a <see cref="CommandExecutor"/> for the current test. All executors within
    /// one test share the same <see cref="DatabaseRegistry"/> and Kahuna node so they can
    /// resolve each other's databases.
    /// Override in shared-node fixtures to pass a different cluster node.
    /// </summary>
    /// <summary>
    /// Configuration for the engine under test. A fixture that needs a non-default knob overrides
    /// <see cref="ConfigureOptions"/>; nothing here mutates shared state, so two fixtures wanting
    /// different settings do not conflict.
    ///
    /// <para>It starts from the shipped defaults, not from any process-wide value, so a test's
    /// configuration cannot be influenced by whatever ran before it. The only thing layered on top is
    /// this test's own isolated data directory.</para>
    /// </summary>
    protected CamusDBOptions Options =>
        ConfigureOptions(CamusDBOptions.Default with { DataDirectory = tempDir! });

    /// <summary>
    /// Hook for a fixture to state the configuration it needs, applied once per test before anything is
    /// constructed. Override with a <c>with</c> expression — e.g.
    /// <c>defaults with { SpillEnabled = true }</c> — instead of assigning to a static and restoring it
    /// in tear-down. Configuration is fixed when a component is built, so setting it afterwards has no
    /// effect on an engine that already exists.
    /// </summary>
    protected virtual CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults;

    protected virtual CommandExecutor CreateCommandExecutor() => CreateCommandExecutor(Options);

    /// <summary>
    /// An engine configured with <paramref name="options"/> rather than the fixture's defaults, for a
    /// case that needs a setting the rest of the fixture does not — or two settings at once. Engines
    /// built this way are independent: nothing one is configured with is visible to another.
    /// </summary>
    protected virtual CommandExecutor CreateCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
                   sharedNode: testNode!, registry: sharedRegistry!, isClusterMode: false);
    }

    /// <summary>
    /// An engine wired to a swappable options holder, for tests that exercise runtime
    /// configuration change. Engines built without a holder (every other factory here) keep their
    /// configuration for life, which is the isolation the rest of the suite relies on.
    /// </summary>
    protected CommandExecutor CreateCommandExecutor(
        CamusDBOptions options,
        CamusDB.Core.Config.CamusDBOptionsHolder holder,
        CamusDB.Core.Config.ClusterSettingsService? clusterSettings = null)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
                   sharedNode: testNode!, registry: sharedRegistry!, isClusterMode: false,
                   optionsHolder: holder, clusterSettings: clusterSettings);
    }

    /// <summary>
    /// Creates a fresh database, tracks it for automatic cleanup after each test.
    /// </summary>
    protected async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateDatabase()
    {
        // Routed through the parameterless factory on purpose: fixtures that override it — to inject a
        // query cache, for instance — must still be consulted. Only CreateDatabase(options) bypasses it.
        return await CreateDatabaseWith(CreateCommandExecutor()).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a fresh database through an engine configured with <paramref name="options"/>, tracked
    /// for cleanup like any other. This is how a test states the configuration it needs: the engine
    /// fixes its settings when it is built, so they are supplied here rather than assigned to a
    /// process-wide value beforehand.
    /// </summary>
    protected Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateDatabase(
        CamusDBOptions options)
        => CreateDatabaseWith(CreateCommandExecutor(options));

    protected async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateDatabaseWith(
        CommandExecutor executor)
    {
        string dbname = Guid.NewGuid().ToString("n");


        CreateDatabaseTicket databaseTicket = new(name: dbname, ifNotExists: false);
        DatabaseDescriptor database = await executor.CreateDatabase(databaseTicket);

        openDatabases.Add((dbname, executor));

        return (dbname, database, executor);
    }

    /// <summary>
    /// Registers an externally-created database for cleanup. Use when SetupDatabase
    /// needs extra return values beyond what CreateDatabase provides.
    /// </summary>
    protected void TrackDatabase(string dbname, CommandExecutor executor)
    {
        openDatabases.Add((dbname, executor));
    }

    [TearDown]
    public async Task CloseAllDatabases()
    {
        // Collect unique executors for disposal.
        HashSet<CommandExecutor> executors = new(ReferenceEqualityComparer.Instance);

        foreach ((string dbname, CommandExecutor executor) in openDatabases)
        {
            executors.Add(executor);

            try
            {
                DatabaseDescriptor database = await executor.OpenDatabase(dbname);
                await database.Transactions.RollbackAllActiveAsync();
            }
            catch
            {
                // best-effort cleanup — ignore errors
            }

            try
            {
                await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
            }
            catch
            {
                // best-effort cleanup — ignore errors
            }
        }

        openDatabases.Clear();

        foreach (CommandExecutor executor in executors)
        {
            try { await executor.DisposeAsync(); } catch { }
        }

        // Dispose the shared registry last (executors above did not own it).
        if (sharedRegistry is not null)
        {
            try { await sharedRegistry.DisposeAsync(); } catch { }
            sharedRegistry = null;
        }

        // Dispose the per-test Kahuna node after the registry (which holds transactions on it).
        if (testNode is not null)
        {
            try { await testNode.DisposeAsync(); } catch { }
            testNode = null;
        }

        // Delete the per-test temp directory.
        if (tempDir is not null && Directory.Exists(tempDir))
        {
            try { Directory.Delete(tempDir, recursive: true); } catch { }
            tempDir = null;
        }
    }
}
