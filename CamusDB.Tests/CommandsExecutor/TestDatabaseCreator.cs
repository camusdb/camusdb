
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using NUnit.Framework;
using System.Threading.Tasks;

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

internal class TestDatabaseCreator
{
    [SetUp]
    public void Setup()
    {
        //SetupDb.Remove("test");
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateDatabase()
    {
        string dbname = System.Guid.NewGuid().ToString("n");

        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        CreateDatabaseTicket databaseTicket = new(
            name: dbname,
            ifNotExists: false
        );

        await executor.CreateDatabase(databaseTicket);

        string path = Path.Combine(CamusConfig.DataDirectory, dbname);

        Assert.IsTrue(Directory.Exists(path));
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateDatabaseIfNotExists()
    {
        string dbname = System.Guid.NewGuid().ToString("n");

        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        CreateDatabaseTicket databaseTicket = new(
            name: dbname,
            ifNotExists: false
        );

        await executor.CreateDatabase(databaseTicket);

        string path = Path.Combine(CamusConfig.DataDirectory, dbname);

        Assert.IsTrue(Directory.Exists(path));

        await executor.OpenDatabase(dbname);

        databaseTicket = new(
            name: dbname,
            ifNotExists: true
        );

        await executor.CreateDatabase(databaseTicket);

        path = Path.Combine(CamusConfig.DataDirectory, dbname);

        Assert.IsTrue(Directory.Exists(path));
    }
}

