
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
                InitialPartitions = 1
            });
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
        => DatabaseRegistry.OpenAsync(testNode!);

    /// <summary>
    /// Builds a <see cref="CommandExecutor"/> for the current test. All executors within
    /// one test share the same <see cref="DatabaseRegistry"/> and Kahuna node so they can
    /// resolve each other's databases.
    /// Override in shared-node fixtures to pass a different cluster node.
    /// </summary>
    protected virtual CommandExecutor CreateCommandExecutor()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger,
                   sharedNode: testNode!, registry: sharedRegistry!, isClusterMode: false);
    }

    /// <summary>
    /// Creates a fresh database, tracks it for automatic cleanup after each test.
    /// </summary>
    protected async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateDatabase()
    {
        string dbname = Guid.NewGuid().ToString("n");

        CommandExecutor executor = CreateCommandExecutor();

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
