
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
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
    /// Drop removes the registry entry so the name can be reused immediately.
    /// A drop-then-recreate cycle must produce a fresh id and empty data.
    /// </summary>
    [Test]
    public async Task DropDatabase_RegistryEntryRemovedAndNameCanBeReused()
    {
        (string dbname, DatabaseDescriptor first, CommandExecutor executor) = await CreateDatabase();
        string firstId = first.Id;

        await executor.DropDatabase(new DropDatabaseTicket(dbname));

        // Recreate — must succeed and produce a distinct id.
        DatabaseDescriptor second = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));
        Assert.AreNotEqual(firstId, second.Id, "Recreated database must have a fresh id");
        Assert.AreEqual(dbname, second.Name);

        // Old id-based directory is gone; new one exists.
        Assert.IsFalse(Directory.Exists(Path.Combine(CamusConfig.DataDirectory, firstId)),
            "Old id directory must be deleted");
        Assert.IsTrue(Directory.Exists(Path.Combine(CamusConfig.DataDirectory, second.Id, "kv")),
            "New id directory must exist");
    }

    [Test]
    public async Task DropDatabase_UnknownName_WithIfExists_IsNoOp()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string ghost = Guid.NewGuid().ToString("n");

        // IF EXISTS on an unknown name must not throw.
        await executor.DropDatabase(new DropDatabaseTicket(ghost, ifExists: true));
    }

    [Test]
    public async Task DropDatabase_UnknownName_WithoutIfExists_ThrowsDatabaseDoesntExist()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string ghost = Guid.NewGuid().ToString("n");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.DropDatabase(new DropDatabaseTicket(ghost)));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
    }

    /// <summary>
    /// DROP DATABASE via SQL removes the registry entry and directory.
    /// DROP DATABASE IF EXISTS on an unknown name succeeds as a no-op.
    /// Uses letter-prefixed names so the SQL parser treats them as identifiers.
    /// </summary>
    [Test]
    public async Task DropDatabase_ViaSql_RemovesRegistryAndDirectory()
    {
        // Use a letter-prefixed name so the SQL parser tokenises it as TIDENTIFIER.
        string dbname = "db_" + Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(dbname, executor);
        DatabaseDescriptor descriptor = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));
        string dataPath = Path.Combine(CamusConfig.DataDirectory, descriptor.Id);

        Assert.IsTrue(Directory.Exists(dataPath));

        KvTransaction tx = await descriptor.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, $"DROP DATABASE {dbname}", null));

        Assert.IsFalse(Directory.Exists(dataPath),
            "Data directory must be removed after DROP DATABASE via SQL");

        CommandExecutor executor2 = CreateCommandExecutor();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor2.OpenDatabase(dbname));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
    }

    [Test]
    public async Task DropDatabase_ViaSql_IfExistsOnUnknown_IsNoOp()
    {
        string dbname = "db_" + Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(dbname, executor);
        DatabaseDescriptor descriptor = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));
        string ghost = "ghost_" + Guid.NewGuid().ToString("n");

        KvTransaction tx = await descriptor.Transactions.BeginAsync();
        // Must not throw.
        await executor.ExecuteNonSQLQuery(
            new ExecuteSQLTicket(tx, dbname, $"DROP DATABASE IF EXISTS {ghost}", null));
    }

    /// <summary>
    /// Drop-vs-concurrent-Open ordering (DB6.1b).
    /// Fires DropDatabase and a large fan of concurrent OpenDatabase calls at the same time.
    /// Every racing Open must either succeed (if it beat UnregisterAsync) or throw
    /// DatabaseDoesntExist.  Neither SystemSpaceCorrupt nor any other exception is acceptable
    /// because that would mean Open loaded the descriptor after the directory was already deleted
    /// (the pre-fix failure mode).
    /// </summary>
    [Test]
    public async Task DropDatabase_ConcurrentOpen_NeverCorrupts()
    {
        const int openers = 20;

        (string dbname, DatabaseDescriptor _, CommandExecutor dropExecutor) = await CreateDatabase();

        // All openers share a separate executor so they go through the real Open path.
        CommandExecutor openExecutor = CreateCommandExecutor();

        using SemaphoreSlim gate = new(0, 1);

        // Each opener waits for the gate then tries to Open — racing against the drop below.
        Task[] openerTasks = Enumerable.Range(0, openers).Select(_ => Task.Run(async () =>
        {
            await gate.WaitAsync();
            gate.Release(); // let the next one through
            try
            {
                await openExecutor.OpenDatabase(dbname);
            }
            catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.DatabaseDoesntExist)
            {
                // acceptable: name was already removed from the registry
            }
            // Any other exception propagates and fails the test.
        })).ToArray();

        Task dropTask = dropExecutor.DropDatabase(new DropDatabaseTicket(dbname));

        // Release all openers simultaneously with the drop in flight.
        gate.Release();

        await Task.WhenAll([.. openerTasks, dropTask]).WaitAsync(TimeSpan.FromSeconds(30));
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
        await dropTask.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.IsTrue(dropTask.IsCompletedSuccessfully, "Drop must complete after ref is released");
        Assert.IsTrue(descriptor.IsDropped, "descriptor must be marked dropped");

        // New Use() after drop must throw DatabaseDoesntExist.
        CamusDBException? ex = Assert.Throws<CamusDBException>(() => descriptor.Use());
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);
    }
}
