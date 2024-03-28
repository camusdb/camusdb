
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

internal sealed class TestDatabaseDropper : BaseTest
{    
    [Test]
    [NonParallelizable]
    public async Task TestDropDatabase()
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

        await executor.OpenDatabase(dbname);

        string path = Path.Combine(CamusConfig.DataDirectory, dbname);

        Assert.IsTrue(Directory.Exists(path));

        DropDatabaseTicket dropTicket = new(
            name: dbname
        );

        await executor.DropDatabase(dropTicket);

        path = Path.Combine(CamusConfig.DataDirectory, dbname);

        Assert.IsFalse(Directory.Exists(path));
    }
}