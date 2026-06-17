
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.IO;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestDatabaseDropper : BaseTest
{
    [Test]
    public async Task TestDropDatabase()
    {
        (string dbname, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        DropDatabaseTicket dropTicket = new(name: dbname);

        await executor.DropDatabase(dropTicket);

        // After drop, opening the same name throws DatabaseDoesntExist (no magic-create, DB2).
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.OpenDatabase(dbname));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
    }

    /// <summary>
    /// Drop must remove the on-disk data directory in standalone mode.
    /// After DropDatabase the id-based directory must no longer exist.
    /// </summary>
    [Test]
    public async Task DropDatabase_StandaloneMode_DataDirectoryDeleted()
    {
        (string dbname, DatabaseDescriptor descriptor, CommandExecutor executor) = await CreateDatabase();

        string id = descriptor.Id;
        string dataPath = Path.Combine(CamusConfig.DataDirectory, id);

        Assert.IsTrue(Directory.Exists(Path.Combine(dataPath, "kv")),
            "kv directory must exist before drop");

        await executor.DropDatabase(new DropDatabaseTicket(dbname));

        Assert.IsFalse(Directory.Exists(dataPath),
            "data directory must be deleted after drop");
    }

    /// <summary>
    /// Drop-vs-use guard: a Use() reference acquired before Drop causes Drop to
    /// wait for the reference to be released, then Drop completes.  After Drop,
    /// Use() throws DatabaseDoesntExist.
    /// </summary>
    [Test]
    public async Task DropDatabase_UseGuard_DrainsThenDisallowsNewUse()
    {
        (string dbname, DatabaseDescriptor descriptor, CommandExecutor executor) = await CreateDatabase();

        // Acquire a use-reference directly on the descriptor — simulates an in-flight operation.
        descriptor.AddRef();

        Task dropTask = executor.DropDatabase(new DropDatabaseTicket(dbname));

        // Drop should be blocked waiting for our ref to be released.
        // Give it a moment to confirm it hasn't completed yet.
        await Task.Delay(50);
        Assert.IsFalse(dropTask.IsCompleted, "Drop must wait while a use-ref is held");

        // Release the ref — Drop should unblock.
        descriptor.Release();
        await dropTask.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.IsTrue(dropTask.IsCompletedSuccessfully, "Drop must complete after ref is released");
        Assert.IsTrue(descriptor.IsDropped, "descriptor must be marked dropped");

        // New Use() after drop must throw DatabaseDoesntExist.
        CamusDBException? ex = Assert.Throws<CamusDBException>(() => descriptor.Use());
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
    }
}
