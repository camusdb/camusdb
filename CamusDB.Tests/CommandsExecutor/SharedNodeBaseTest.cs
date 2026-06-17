
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Base fixture for CommandsExecutor tests that reuse one in-memory embedded Kahuna node
/// for the whole test class. Each test still gets an isolated database via a unique GUID name.
/// Leaked per-test transactions are rolled back in <see cref="BaseTest.CloseAllDatabases"/>.
/// </summary>
public abstract class SharedNodeBaseTest : BaseTest
{
    private EmbeddedKahuna? sharedNode;

    [OneTimeSetUp]
    public async Task SharedNodeOneTimeSetUp()
    {
        sharedNode = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            NodeName = $"test-shared-{Guid.NewGuid():N}",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        });

        await sharedNode.StartAsync(CancellationToken.None);
        await sharedNode.WaitForLeaderAsync("warmup", CancellationToken.None);
        await sharedNode.FlushAsync();
    }

    [OneTimeTearDown]
    public async Task SharedNodeOneTimeTearDown()
    {
        if (sharedNode is not null)
            await sharedNode.DisposeAsync();
    }

    protected override Task<DatabaseRegistry> CreateRegistryAsync()
        => DatabaseRegistry.OpenAsync(clusterNode: sharedNode!, loggerFactory: SharedLoggerFactory);

    protected override CommandExecutor CreateCommandExecutor()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger,
            loggerFactory: SharedLoggerFactory, clusterNode: sharedNode!,
            registry: sharedRegistry!);
    }
}
