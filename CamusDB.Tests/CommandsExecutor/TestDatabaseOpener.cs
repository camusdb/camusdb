
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using Kahuna;

namespace CamusDB.Tests.CommandsExecutor;

public sealed class TestDatabaseOpener : BaseTest
{    
    [Test]
    [NonParallelizable]
    public async Task TestOpenDatabase()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);

        Assert.AreEqual(dbname, database.Name);

        Assert.IsInstanceOf<SystemSchema>(database.SystemSchema);
        Assert.IsInstanceOf<Schema>(database.Schema);

        Assert.AreEqual(database.TableDescriptors.Count, 0);
    }

    /// <summary>
    /// Closing one database must not dispose the shared cluster Kahuna node;
    /// other databases on the same node must remain usable.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task ClusterMode_CloseOneDatabase_OtherDatabaseStillWorks()
    {
        await using EmbeddedKahuna clusterNode = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            NodeName = "test-cluster-opener",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 3
        }.WithFastTestTimers());

        await clusterNode.StartAsync(CancellationToken.None);
        await clusterNode.WaitForLeaderAsync("warmup", CancellationToken.None);

        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        CommandExecutor executor = new(validator, catalogsManager, logger, CamusDBConfig.Ambient, sharedNode: clusterNode, isClusterMode: true);

        string db1 = System.Guid.NewGuid().ToString("n");
        string db2 = System.Guid.NewGuid().ToString("n");

        try
        {
            await executor.CreateDatabase(new CreateDatabaseTicket(db1, ifNotExists: false));
            DatabaseDescriptor second = await executor.CreateDatabase(new CreateDatabaseTicket(db2, ifNotExists: false));

            Assert.AreSame(clusterNode, second.Kahuna);

            await executor.CloseDatabase(new CloseDatabaseTicket(db1));

            // Shared node must still serve the remaining database.
            DatabaseDescriptor reopened = await executor.OpenDatabase(db2);
            Assert.AreSame(clusterNode, reopened.Kahuna);
        }
        finally
        {
            try { await executor.CloseDatabase(new CloseDatabaseTicket(db2)); } catch { }
        }
    }
}
