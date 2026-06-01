
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System.Threading.Tasks;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

[NonParallelizable]
internal class TestDatabaseCreator : BaseTest
{
    [Test]
    [NonParallelizable]
    public async Task TestCreateDatabase()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual(dbname, database.Name);
        Assert.IsNotNull(database.Schema);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreateDatabaseIfNotExists()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await executor.OpenDatabase(dbname);

        CreateDatabaseTicket databaseTicket = new(
            name: dbname,
            ifNotExists: true
        );

        await executor.CreateDatabase(databaseTicket);

        DatabaseDescriptor database2 = await executor.OpenDatabase(dbname);

        Assert.AreEqual(dbname, database2.Name);
    }
}
