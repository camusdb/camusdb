
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
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

namespace CamusDB.Tests.CommandsExecutor;

public abstract class BaseTest
{
    // One logger factory for the entire test run — avoids spawning a ConsoleLoggerProvider
    // thread per fixture, which previously caused OutOfMemoryException after ~166 tests.
    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    protected readonly ILogger<ICamusDB> logger;

    // Databases opened in the current test method — closed in TearDown.
    private readonly List<(string dbname, CommandExecutor executor)> openDatabases = new();

    protected BaseTest()
    {
        logger = sharedLoggerFactory.CreateLogger<ICamusDB>();
    }

    /// <summary>
    /// Creates a fresh database, tracks it for automatic cleanup after each test.
    /// </summary>
    protected async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateDatabase()
    {
        string dbname = Guid.NewGuid().ToString("n");

        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        CommandExecutor executor = new(validator, catalogsManager, logger);

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
                await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
            }
            catch
            {
                // best-effort cleanup — ignore errors
            }
        }

        openDatabases.Clear();
    }
}
