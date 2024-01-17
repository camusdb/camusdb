
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
using CamusDB.Core.Util.Time;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

internal class TestDatabaseCreator : BaseTest
{
    [Test]
    [NonParallelizable]
    public async Task TestCreateDatabase()
    {
        string dbname = System.Guid.NewGuid().ToString("n");

        HybridLogicalClock hlc = new();
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        CommandExecutor executor = new(hlc, validator, catalogsManager, logger);

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

        HybridLogicalClock hlc = new();
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        CommandExecutor executor = new(hlc, validator, catalogsManager, logger);

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

