
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using NUnit.Framework;
using System.Threading.Tasks;

using CamusDB.Tests.Utils;
using CamusDB.Core.Catalogs;
using CamusDB.Core.BufferPool;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

public class TestDatabaseOpener
{
    [SetUp]
    public void Setup()
    {
        SetupDb.Remove("test");
    }

    [Test]
    [NonParallelizable]
    public async Task TestOpenDatabase()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        CreateDatabaseTicket databaseTicket = new(
            name: "test"
        );

        await executor.CreateDatabase(databaseTicket);

        DatabaseDescriptor database = await executor.OpenDatabase("test");

        Assert.AreEqual("test", database.Name);

        Assert.IsInstanceOf<BufferPoolHandler>(database.SchemaSpace);
        Assert.IsInstanceOf<BufferPoolHandler>(database.SystemSpace);
        Assert.IsInstanceOf<BufferPoolHandler>(database.TableSpace);

        Assert.IsInstanceOf<SystemSchema>(database.SystemSchema);
        Assert.IsInstanceOf<Schema>(database.Schema);

        Assert.AreEqual(database.TableDescriptors.Count, 0);
    }
}
