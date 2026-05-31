
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

internal sealed class TestDatabaseDropper : SharedNodeBaseTest
{
    [Test]
    [NonParallelizable]
    public async Task TestDropDatabase()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        DropDatabaseTicket dropTicket = new(name: dbname);

        await executor.DropDatabase(dropTicket);

        // After drop, opening the same name should create a fresh empty database.
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        Assert.AreEqual(0, reopened.Schema.Tables.Count);
    }
}
