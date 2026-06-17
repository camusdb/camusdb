
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

using CamusDB.Core;
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

    // Per-test isolated directory and registry.
    private string? tempDir;
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
    [SetUp]
    public async Task SetUpTestEnvironment()
    {
        tempDir = Path.Combine(Path.GetTempPath(), "camusdb-test-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(tempDir);
        CamusConfig.DataDirectory = tempDir;
        sharedRegistry = await CreateRegistryAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Creates the <see cref="DatabaseRegistry"/> for the current test.
    /// Override in shared-node fixtures to use the cluster node instead of standalone SQLite.
    /// </summary>
    protected virtual Task<DatabaseRegistry> CreateRegistryAsync()
        => DatabaseRegistry.OpenAsync(clusterNode: null, loggerFactory: SharedLoggerFactory);

    /// <summary>
    /// Builds a <see cref="CommandExecutor"/> for the current test. All executors within
    /// one test share the same <see cref="DatabaseRegistry"/> so they can resolve each
    /// other's databases without opening a second SQLite system store.
    /// Override in shared-node fixtures to pass a process-level cluster node.
    /// </summary>
    protected virtual CommandExecutor CreateCommandExecutor()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, registry: sharedRegistry!);
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

        // Delete the entire per-test temp directory (all database dirs + _system registry).
        if (tempDir is not null && Directory.Exists(tempDir))
        {
            try { Directory.Delete(tempDir, recursive: true); } catch { }
            tempDir = null;
        }
    }
}
