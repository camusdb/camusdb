
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

public abstract class BaseTest
{
    // One logger factory for the entire test run — avoids spawning a ConsoleLoggerProvider
    // thread per fixture, which previously caused OutOfMemoryException after ~166 tests.
    protected static readonly ILoggerFactory SharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    protected readonly ILogger<ICamusDB> logger;

    // Databases opened in the current test method — closed in TearDown.
    private readonly List<(string dbname, CommandExecutor executor)> openDatabases = new();

    protected BaseTest()
    {
        logger = SharedLoggerFactory.CreateLogger<ICamusDB>();
    }

    /// <summary>
    /// Builds a <see cref="CommandExecutor"/> for the current test. Override in shared-node
    /// fixtures to pass a process-level cluster node instead of standalone per-database nodes.
    /// </summary>
    protected virtual CommandExecutor CreateCommandExecutor()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger);
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
        foreach ((string dbname, CommandExecutor executor) in openDatabases)
        {
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

            try
            {
                string dataPath = Path.Combine(CamusConfig.DataDirectory, dbname);
                if (Directory.Exists(dataPath))
                    Directory.Delete(dataPath, recursive: true);
            }
            catch
            {
                // best-effort cleanup
            }
        }

        openDatabases.Clear();
    }
}
